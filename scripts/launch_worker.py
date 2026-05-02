#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import json
import os
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
FEED_PATH = DATA_DIR / "launch-feed.json"
DB_PATH = DATA_DIR / "launch-db.json"
STATS_PATH = DATA_DIR / "launch-stats.json"
STATE_PATH = DATA_DIR / "worker-state.json"
SATELLITE_TLE_PATH = DATA_DIR / "active-satellites.tle"
SATELLITE_HISTORY_PATH = DATA_DIR / "satellite-live-history.json"
ISS_OEM_PATH = DATA_DIR / "iss-oem-j2k.txt"

LL_BASE = "https://ll.thespacedevs.com/2.2.0"
UPCOMING_URL = f"{LL_BASE}/launch/upcoming/?limit=48&mode=detailed"
PREVIOUS_URL = f"{LL_BASE}/launch/previous/?limit={{limit}}&mode=detailed"
SATELLITE_SOURCE_URL = "https://celestrak.org/NORAD/elements/gp.php?GROUP=active&FORMAT=tle"
ISS_OEM_SOURCE_URL = "https://nasa-public-data.s3.amazonaws.com/iss-coords/current/ISS_OEM/ISS.OEM_J2K_EPH.txt"

USER_AGENT = os.environ.get(
    "LAUNCH_WORKER_USER_AGENT",
    "earth-launch-worker/1.0",
)

FEED_REFRESH_INTERVAL = timedelta(hours=1)
SATELLITE_REFRESH_INTERVAL = timedelta(hours=2)
ISS_OEM_REFRESH_INTERVAL = timedelta(hours=2)
PREFLIGHT_WINDOW = timedelta(minutes=15)
POSTFLIGHT_DELAY = timedelta(minutes=30)
DETAIL_RECHECK_INTERVAL = timedelta(minutes=30)
PREFLIGHT_MISSED_MARK_AFTER = timedelta(hours=6)
DB_LIMIT = 1000
SATELLITE_HISTORY_MAX_DAYS = 90

TERMINAL_OUTCOMES = {"success", "failure", "cancelled"}
OBSERVED_OUTCOMES = TERMINAL_OUTCOMES | {"delayed"}
HISTORICAL_SUCCESSFUL_LAUNCHES_BY_YEAR = {
    # Launch Library 2 count for 2025 with status=Launch Successful.
    2025: 329,
}

EMPTY_FEED = {
    "generatedAt": None,
    "source": "seed-empty",
    "nextRefreshAfter": None,
    "launches": [],
}

EMPTY_DB = {
    "generatedAt": None,
    "source": "launch-worker",
    "launches": [],
}

EMPTY_STATS = {
    "generatedAt": None,
    "source": "launch-db",
    "timezone": "UTC",
    "week": {"current": 0, "previous": None, "delta": None},
    "month": {"current": 0, "previous": None, "delta": None},
    "year": {"current": 0, "previous": None, "delta": None},
}

EMPTY_SATELLITE_HISTORY = {
    "generatedAt": None,
    "source": "celestrak-active-tle",
    "cadenceHours": 2,
    "windowDays": SATELLITE_HISTORY_MAX_DAYS,
    "samples": [],
}

EMPTY_STATE = {
    "lastFeedRefreshAt": None,
    "lastCheckRunAt": None,
    "lastSatelliteRefreshAt": None,
    "lastIssOemRefreshAt": None,
    "pendingChecks": [],
    "lastErrors": [],
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_time(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def read_json(path: Path, default: dict) -> dict:
    if not path.exists():
        return copy.deepcopy(default)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise RuntimeError(f"{path} is not valid JSON; refusing to overwrite it: {error}") from error


def stable_json(data: dict) -> str:
    return json.dumps(data, ensure_ascii=True, indent=2) + "\n"


def write_json_if_changed(path: Path, data: dict) -> bool:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    new_text = stable_json(data)
    old_text = path.read_text(encoding="utf-8") if path.exists() else ""
    if old_text == new_text:
        return False
    path.write_text(new_text, encoding="utf-8")
    return True


def write_text_if_changed(path: Path, text: str) -> bool:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    normalized = text.replace("\r\n", "\n").strip() + "\n"
    old_text = path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""
    if old_text == normalized:
        return False
    path.write_text(normalized, encoding="utf-8")
    return True


def request_json(url: str, timeout: int = 30) -> dict:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return json.loads(response.read().decode(charset, errors="replace"))


def request_text(url: str, timeout: int = 45) -> str:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/plain",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


def nested(data: dict, *keys: str) -> object:
    current: object = data
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def text_value(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def number_value(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed else None


def first_url(entries: object) -> str:
    if not isinstance(entries, list):
        return ""
    for entry in entries:
        if isinstance(entry, str) and entry.startswith(("http://", "https://")):
            return entry
        if isinstance(entry, dict):
            url = text_value(entry.get("url"))
            if url.startswith(("http://", "https://")):
                return url
    return ""


def first_video_url(raw: dict) -> str:
    for key in ("vidURLs", "vid_urls", "videos"):
        url = first_url(raw.get(key))
        if url:
            return url
    return ""


def first_info_url(raw: dict) -> str:
    for key in ("infoURLs", "info_urls", "program"):
        url = first_url(raw.get(key))
        if url:
            return url
    return ""


def launch_status_text(raw: dict) -> str:
    status = raw.get("status")
    parts = [
        raw.get("outcome"),
        raw.get("status"),
        raw.get("statusName"),
        raw.get("statusAbbrev"),
        raw.get("statusDescription"),
    ]
    if isinstance(status, dict):
        parts.extend([status.get("abbrev"), status.get("name"), status.get("description")])
    return " ".join(text_value(part) for part in parts if text_value(part)).lower()


def classify_launch(raw: dict) -> str:
    status = raw.get("status")
    status_id = status.get("id") if isinstance(status, dict) else None
    try:
        status_id = int(status_id)
    except (TypeError, ValueError):
        status_id = None

    by_id = {
        1: "go",
        2: "delayed",
        3: "success",
        4: "failure",
        5: "delayed",
        6: "live",
        7: "failure",
        8: "delayed",
    }
    if status_id in by_id:
        return by_id[status_id]

    text = launch_status_text(raw)
    if not text:
        return "scheduled"
    if re.search(r"\b(success|successful)\b", text):
        return "success"
    if re.search(r"\b(partial failure|failure|failed|lost)\b", text):
        return "failure"
    if re.search(r"\b(cancel|cancelled|canceled|scrub|scrubbed)\b", text):
        return "cancelled"
    if re.search(r"\b(hold|delay|delayed|postponed|slip|tbc|tbd|to be confirmed|to be determined|unconfirmed)\b", text):
        return "delayed"
    if re.search(r"\b(in flight|flight|liftoff|lift-off|launch in progress)\b", text):
        return "live"
    if re.search(r"\b(go|confirmed|ready|on schedule)\b", text):
        return "go"
    return "scheduled"


def normalize_launch(raw: dict, now: datetime) -> dict:
    launch_id = text_value(raw.get("id") or raw.get("slug") or raw.get("url") or raw.get("name"))
    net = parse_time(raw.get("net") or raw.get("window_start"))
    status_obj = raw.get("status") if isinstance(raw.get("status"), dict) else {}
    mission = raw.get("mission") if isinstance(raw.get("mission"), dict) else {}
    pad = raw.get("pad") if isinstance(raw.get("pad"), dict) else {}
    pad_location = pad.get("location") if isinstance(pad.get("location"), dict) else {}
    rocket_config = nested(raw, "rocket", "configuration")
    rocket_config = rocket_config if isinstance(rocket_config, dict) else {}
    provider = raw.get("launch_service_provider")
    provider = provider if isinstance(provider, dict) else {}

    livestream_url = first_video_url(raw)
    info_url = first_info_url(raw)
    api_url = text_value(raw.get("url"))
    status = classify_launch(raw)

    return {
        "id": launch_id,
        "name": text_value(raw.get("name")) or "Unbenannter Start",
        "net": to_iso(net),
        "provider": text_value(provider.get("name") or nested(rocket_config, "manufacturer", "name")) or "Unbekannt",
        "rocket": text_value(rocket_config.get("full_name") or rocket_config.get("name")) or "Rakete unbekannt",
        "pad": text_value(pad.get("name")) or "Unbekanntes Pad",
        "padLocation": text_value(pad_location.get("name")),
        "latitude": number_value(pad.get("latitude")),
        "longitude": number_value(pad.get("longitude")),
        "mission": text_value(mission.get("name")),
        "missionDescription": text_value(mission.get("description")),
        "orbit": text_value(nested(mission, "orbit", "name")),
        "status": status,
        "outcome": status if status in OBSERVED_OUTCOMES else "",
        "statusName": text_value(status_obj.get("name")) or "Status unbekannt",
        "statusAbbrev": text_value(status_obj.get("abbrev")),
        "statusDescription": text_value(status_obj.get("description")),
        "preflightStatus": "",
        "postflightStatus": "",
        "preflightCheckedAt": None,
        "postflightCheckedAt": None,
        "livestreamUrl": livestream_url,
        "sourceUrl": info_url or livestream_url or api_url,
        "apiUrl": api_url,
        "updatedAt": to_iso(now),
    }


def is_earth_launch(launch: dict) -> bool:
    return launch.get("latitude") is not None and launch.get("longitude") is not None


def sort_key_net(launch: dict) -> datetime:
    return parse_time(launch.get("net")) or datetime.max.replace(tzinfo=timezone.utc)


def existing_launches_by_id(db_payload: dict) -> dict[str, dict]:
    launches = db_payload.get("launches")
    if not isinstance(launches, list):
        return {}
    return {text_value(item.get("id")): dict(item) for item in launches if isinstance(item, dict) and text_value(item.get("id"))}


def merge_launch(existing: dict | None, incoming: dict, now: datetime, phase: str | None = None) -> dict:
    merged = dict(existing or {})
    old_net = merged.get("net")
    existing_terminal = merged.get("outcome") in TERMINAL_OUTCOMES or merged.get("status") in TERMINAL_OUTCOMES

    merged.update({key: value for key, value in incoming.items() if value not in ("", None)})
    merged["id"] = incoming["id"]
    merged["updatedAt"] = to_iso(now)
    merged.setdefault("firstSeenAt", to_iso(now))

    if existing and old_net and incoming.get("net") and old_net != incoming.get("net"):
        previous = merged.get("previousNets")
        if not isinstance(previous, list):
            previous = []
        if old_net not in previous:
            previous.append(old_net)
        merged["previousNets"] = previous[-10:]
        if incoming.get("status") not in TERMINAL_OUTCOMES:
            merged["preflightCheckedAt"] = None
            merged["postflightCheckedAt"] = None
            merged["preflightStatus"] = ""
            merged["postflightStatus"] = ""
            if existing.get("outcome") == "delayed" and incoming.get("status") in {"scheduled", "go", "live"}:
                merged["outcome"] = ""

    if existing_terminal and incoming.get("status") not in TERMINAL_OUTCOMES and phase is None:
        merged["outcome"] = existing.get("outcome")
        merged["status"] = existing.get("status")

    status = incoming.get("status") or merged.get("status") or "scheduled"
    if phase == "preflight":
        merged["preflightCheckedAt"] = to_iso(now)
        merged["preflightStatus"] = "go" if status == "live" else status
        if status in {"delayed", "cancelled"}:
            merged["outcome"] = status
    elif phase == "postflight":
        merged["postflightCheckedAt"] = to_iso(now)
        merged["postflightStatus"] = status
        if status in OBSERVED_OUTCOMES:
            merged["outcome"] = status
        if status in TERMINAL_OUTCOMES:
            merged["status"] = status

    if merged.get("status") in OBSERVED_OUTCOMES and not merged.get("outcome"):
        merged["outcome"] = merged["status"]

    return merged


def detail_url(launch: dict) -> str:
    api_url = text_value(launch.get("apiUrl"))
    if api_url.startswith(("http://", "https://")):
        separator = "&" if "?" in api_url else "?"
        return api_url if "mode=" in api_url else f"{api_url}{separator}mode=detailed"
    return f"{LL_BASE}/launch/{launch['id']}/?mode=detailed"


def due_phase(launch: dict, now: datetime) -> str | None:
    net = parse_time(launch.get("net"))
    if net is None:
        return None

    status = text_value(launch.get("status"))
    outcome = text_value(launch.get("outcome"))
    post_checked = parse_time(launch.get("postflightCheckedAt"))
    pre_checked = parse_time(launch.get("preflightCheckedAt"))
    last_post_age = now - post_checked if post_checked else None

    if now >= net + POSTFLIGHT_DELAY:
        needs_first_post = post_checked is None
        needs_recheck = outcome not in TERMINAL_OUTCOMES and (
            post_checked is None or last_post_age >= DETAIL_RECHECK_INTERVAL
        )
        if needs_first_post or needs_recheck:
            return "postflight"

    if status in TERMINAL_OUTCOMES or outcome in TERMINAL_OUTCOMES:
        return None

    if pre_checked is None and now >= net - PREFLIGHT_WINDOW:
        if now <= net + PREFLIGHT_MISSED_MARK_AFTER:
            return "preflight"
        return "mark-missed-preflight"

    return None


def pending_checks(launches: list[dict], now: datetime) -> list[dict]:
    pending: list[dict] = []
    for launch in launches:
        net = parse_time(launch.get("net"))
        if net is None:
            continue
        outcome = text_value(launch.get("outcome"))
        status = text_value(launch.get("status"))
        is_terminal = outcome in TERMINAL_OUTCOMES or status in TERMINAL_OUTCOMES

        if not launch.get("preflightCheckedAt") and not is_terminal:
            pending.append(
                {
                    "id": launch.get("id"),
                    "phase": "preflight",
                    "dueAt": to_iso(net - PREFLIGHT_WINDOW),
                    "overdue": now >= net - PREFLIGHT_WINDOW,
                }
            )
        if not launch.get("postflightCheckedAt"):
            pending.append(
                {
                    "id": launch.get("id"),
                    "phase": "postflight",
                    "dueAt": to_iso(net + POSTFLIGHT_DELAY),
                    "overdue": now >= net + POSTFLIGHT_DELAY,
                }
            )
    pending.sort(key=lambda item: (item["dueAt"] or "", item["id"] or ""))
    return pending[:100]


def refresh_feed(now: datetime, db_by_id: dict[str, dict]) -> dict:
    payload = request_json(UPCOMING_URL)
    launches = [
        normalize_launch(item, now)
        for item in payload.get("results", [])
        if isinstance(item, dict)
    ]
    launches = sorted([item for item in launches if is_earth_launch(item)], key=sort_key_net)[:24]
    for launch in launches:
        db_by_id[launch["id"]] = merge_launch(db_by_id.get(launch["id"]), launch, now)
    return {
        "generatedAt": to_iso(now),
        "source": "launch-worker:launch/upcoming",
        "nextRefreshAfter": to_iso(now + FEED_REFRESH_INTERVAL),
        "launches": launches,
    }


def seed_history(now: datetime, db_by_id: dict[str, dict], limit: int) -> int:
    payload = request_json(PREVIOUS_URL.format(limit=limit))
    seeded = 0
    for item in payload.get("results", []):
        if not isinstance(item, dict):
            continue
        launch = normalize_launch(item, now)
        if not launch["id"]:
            continue
        existing = db_by_id.get(launch["id"])
        phase = "postflight" if launch.get("status") in OBSERVED_OUTCOMES else None
        db_by_id[launch["id"]] = merge_launch(existing, launch, now, phase=phase)
        seeded += 1
    return seeded


def run_detail_checks(now: datetime, db_by_id: dict[str, dict], max_checks: int, errors: list[str]) -> int:
    checked = 0
    candidates = sorted(db_by_id.values(), key=sort_key_net)
    for launch in candidates:
        phase = due_phase(launch, now)
        if phase is None:
            continue
        if phase == "mark-missed-preflight":
            updated = dict(launch)
            updated["preflightCheckedAt"] = to_iso(now)
            updated["preflightStatus"] = "missed"
            updated["updatedAt"] = to_iso(now)
            db_by_id[launch["id"]] = updated
            continue
        if checked >= max_checks:
            continue
        try:
            detail = request_json(detail_url(launch))
            normalized = normalize_launch(detail, now)
            if not normalized.get("id"):
                normalized["id"] = launch["id"]
            updated = merge_launch(launch, normalized, now, phase=phase)
            if phase == "postflight" and not updated.get("preflightCheckedAt"):
                updated["preflightCheckedAt"] = to_iso(now)
                updated["preflightStatus"] = "missed"
            db_by_id[updated["id"]] = updated
            checked += 1
        except Exception as error:  # noqa: BLE001
            errors.append(f"{phase} check failed for {launch.get('id')}: {error}")
    return checked


def period_bounds(now_local: datetime, period: str) -> tuple[datetime, datetime, datetime]:
    start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    if period == "week":
        current_start = start - timedelta(days=start.weekday())
        previous_start = current_start - timedelta(days=7)
        return current_start, previous_start, current_start
    if period == "month":
        current_start = start.replace(day=1)
        year = current_start.year if current_start.month > 1 else current_start.year - 1
        month = current_start.month - 1 if current_start.month > 1 else 12
        previous_start = current_start.replace(year=year, month=month)
        return current_start, previous_start, current_start
    current_start = start.replace(month=1, day=1)
    previous_start = current_start.replace(year=current_start.year - 1)
    return current_start, previous_start, current_start


def compute_stats(launches: list[dict], now: datetime, tz_name: str) -> dict:
    tz = timezone.utc
    if ZoneInfo is not None:
        try:
            tz = ZoneInfo(tz_name)
        except Exception:  # noqa: BLE001
            tz = timezone.utc
            tz_name = "UTC"
    now_local = now.astimezone(tz)

    successful_times = []
    for launch in launches:
        if launch.get("outcome") != "success" and launch.get("status") != "success":
            continue
        net = parse_time(launch.get("net"))
        if net is not None:
            successful_times.append(net.astimezone(tz))

    def make_period(period: str) -> dict:
        current_start, previous_start, previous_end = period_bounds(now_local, period)
        current = sum(1 for item in successful_times if current_start <= item < now_local)
        previous = sum(1 for item in successful_times if previous_start <= item < previous_end)
        if period == "year":
            previous = HISTORICAL_SUCCESSFUL_LAUNCHES_BY_YEAR.get(previous_start.year, previous)
        return {
            "current": current,
            "previous": previous,
            "delta": current - previous,
        }

    return {
        "generatedAt": to_iso(now),
        "source": "launch-db",
        "timezone": tz_name,
        "week": make_period("week"),
        "month": make_period("month"),
        "year": make_period("year"),
        "historicalSuccessfulLaunchesByYear": HISTORICAL_SUCCESSFUL_LAUNCHES_BY_YEAR,
    }


def should_refresh(last_value: object, interval: timedelta, now: datetime, force: bool = False) -> bool:
    if force:
        return True
    last = parse_time(last_value)
    return last is None or now - last >= interval


def count_tle_satellites(text: str) -> int:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    count = 0
    for index, line in enumerate(lines[:-1]):
        if line.startswith("1 ") and lines[index + 1].startswith("2 "):
            count += 1
    return count


def append_satellite_history_sample(history: dict, now: datetime, live_count: int) -> dict:
    samples = history.get("samples")
    if not isinstance(samples, list):
        samples = []

    cutoff = now - timedelta(days=SATELLITE_HISTORY_MAX_DAYS)
    cleaned = []
    for sample in samples:
        if not isinstance(sample, dict):
            continue
        timestamp = parse_time(sample.get("timestamp"))
        count = sample.get("liveCount")
        if timestamp is None or not isinstance(count, int) or timestamp < cutoff:
            continue
        cleaned.append({"timestamp": to_iso(timestamp), "liveCount": count})

    now_iso = to_iso(now)
    if cleaned and cleaned[-1].get("timestamp") == now_iso:
        cleaned[-1]["liveCount"] = live_count
    else:
        cleaned.append({"timestamp": now_iso, "liveCount": live_count})

    return {
        "generatedAt": now_iso,
        "source": "celestrak-active-tle",
        "cadenceHours": 2,
        "windowDays": SATELLITE_HISTORY_MAX_DAYS,
        "samples": cleaned,
    }


def refresh_satellites(now: datetime, state: dict, force: bool, errors: list[str]) -> tuple[bool, int | None]:
    if not should_refresh(state.get("lastSatelliteRefreshAt"), SATELLITE_REFRESH_INTERVAL, now, force) and SATELLITE_TLE_PATH.exists():
        return False, None
    try:
        text = request_text(SATELLITE_SOURCE_URL)
        if not text.strip():
            raise RuntimeError("CelesTrak returned an empty TLE payload")
        live_count = count_tle_satellites(text)
        if live_count <= 0:
            raise RuntimeError("CelesTrak TLE payload did not contain valid satellite records")
        changed = write_text_if_changed(SATELLITE_TLE_PATH, text)
        state["lastSatelliteRefreshAt"] = to_iso(now)
        return changed, live_count
    except Exception as error:  # noqa: BLE001
        errors.append(f"satellite refresh failed: {error}")
        return False, None


def refresh_iss_oem(now: datetime, state: dict, force: bool, errors: list[str]) -> bool:
    if not should_refresh(state.get("lastIssOemRefreshAt"), ISS_OEM_REFRESH_INTERVAL, now, force) and ISS_OEM_PATH.exists():
        return False
    try:
        text = request_text(ISS_OEM_SOURCE_URL)
        if not text.strip() or "META_START" not in text:
            raise RuntimeError("NASA ISS OEM returned an unexpected payload")
        changed = write_text_if_changed(ISS_OEM_PATH, text)
        state["lastIssOemRefreshAt"] = to_iso(now)
        return changed
    except Exception as error:  # noqa: BLE001
        errors.append(f"ISS OEM refresh failed: {error}")
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh static launch data for GitHub Pages.")
    parser.add_argument("--force-feed", action="store_true", help="Refresh launch-feed.json even inside the hourly guard.")
    parser.add_argument("--force-satellites", action="store_true", help="Refresh active-satellites.tle even inside the two-hour guard.")
    parser.add_argument("--force-iss-oem", action="store_true", help="Refresh NASA ISS OEM ephemeris even inside the two-hour guard.")
    parser.add_argument("--seed-history", action="store_true", help="Backfill launch-db.json from Launch Library previous launches.")
    parser.add_argument("--seed-limit", type=int, default=int(os.environ.get("SEED_HISTORY_LIMIT", "100")))
    parser.add_argument("--max-detail-checks", type=int, default=int(os.environ.get("MAX_DETAIL_CHECKS", "8")))
    args = parser.parse_args()

    env_seed = os.environ.get("SEED_HISTORY", "").lower() in {"1", "true", "yes", "on"}
    args.seed_history = args.seed_history or env_seed

    now = utc_now()
    errors: list[str] = []
    changed_paths: list[str] = []

    try:
        feed_payload = read_json(FEED_PATH, EMPTY_FEED)
        db_payload = read_json(DB_PATH, EMPTY_DB)
        satellite_history_payload = read_json(SATELLITE_HISTORY_PATH, EMPTY_SATELLITE_HISTORY)
        state = read_json(STATE_PATH, EMPTY_STATE)
    except RuntimeError as error:
        print(error, file=sys.stderr)
        return 2

    db_by_id = existing_launches_by_id(db_payload)

    if args.seed_history:
        try:
            seeded = seed_history(now, db_by_id, max(1, args.seed_limit))
            print(f"Seeded {seeded} previous launches into launch-db.json")
        except Exception as error:  # noqa: BLE001
            errors.append(f"history seed failed: {error}")

    feed_due = should_refresh(feed_payload.get("generatedAt") or state.get("lastFeedRefreshAt"), FEED_REFRESH_INTERVAL, now, args.force_feed)
    if feed_due:
        try:
            feed_payload = refresh_feed(now, db_by_id)
            state["lastFeedRefreshAt"] = to_iso(now)
        except Exception as error:  # noqa: BLE001
            errors.append(f"feed refresh failed: {error}")

    checked = run_detail_checks(now, db_by_id, max(0, args.max_detail_checks), errors)

    launches = sorted(db_by_id.values(), key=lambda item: parse_time(item.get("net")) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    launches = launches[:DB_LIMIT]
    db_payload = {
        "generatedAt": to_iso(now),
        "source": "launch-worker:observed-db",
        "launches": launches,
    }

    stats_payload = compute_stats(launches, now, os.environ.get("STATS_TIMEZONE", "Europe/Berlin"))
    satellite_changed, satellite_live_count = refresh_satellites(now, state, args.force_satellites, errors)
    if satellite_live_count is not None:
        satellite_history_payload = append_satellite_history_sample(satellite_history_payload, now, satellite_live_count)
    iss_oem_changed = refresh_iss_oem(now, state, args.force_iss_oem, errors)

    state["lastCheckRunAt"] = to_iso(now)
    state["pendingChecks"] = pending_checks(launches, now)
    state["lastErrors"] = errors[-20:]

    writes = [
        (FEED_PATH, feed_payload),
        (DB_PATH, db_payload),
        (STATS_PATH, stats_payload),
        (SATELLITE_HISTORY_PATH, satellite_history_payload),
        (STATE_PATH, state),
    ]
    for path, payload in writes:
        if write_json_if_changed(path, payload):
            changed_paths.append(str(path.relative_to(REPO_ROOT)))
    if satellite_changed:
        changed_paths.append(str(SATELLITE_TLE_PATH.relative_to(REPO_ROOT)))
    if iss_oem_changed:
        changed_paths.append(str(ISS_OEM_PATH.relative_to(REPO_ROOT)))

    print(
        json.dumps(
            {
                "changed": changed_paths,
                "feedRefreshed": feed_due and not any(error.startswith("feed refresh failed") for error in errors),
                "detailChecks": checked,
                "pendingChecks": len(state["pendingChecks"]),
                "errors": errors,
            },
            ensure_ascii=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
