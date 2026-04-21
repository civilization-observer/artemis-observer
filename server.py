from __future__ import annotations

import argparse
import hashlib
import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
CACHE_DIR = ROOT_DIR / ".cache"
SATELLITE_CACHE_PATH = CACHE_DIR / "active_satellites.tle"
SATELLITE_META_PATH = CACHE_DIR / "active_satellites.meta.json"
SATELLITE_SOURCE_URL = "https://celestrak.org/NORAD/elements/gp.php?GROUP=active&FORMAT=tle"
SATCAT_RECORDS_URL = "https://celestrak.org/satcat/records.php"
WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
SATELLITE_CACHE_MAX_AGE_S = 2 * 60 * 60
SATELLITE_PROFILE_CACHE_VERSION = "v2"
SATELLITE_PROFILE_CACHE_MAX_AGE_S = 7 * 24 * 60 * 60
SATELLITE_API_PATH = "/api/satellites/active.tle"
SATELLITE_PROFILE_API_PATH = "/api/satellites/profile"

cache_lock = threading.Lock()
profile_lock = threading.Lock()


def ensure_cache_dir() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def read_cache_text() -> str | None:
    if not SATELLITE_CACHE_PATH.exists():
        return None
    return SATELLITE_CACHE_PATH.read_text(encoding="utf-8", errors="ignore")


def read_cache_meta() -> dict:
    if not SATELLITE_META_PATH.exists():
        return {}
    try:
        return json.loads(SATELLITE_META_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def write_cache(raw_text: str) -> None:
    ensure_cache_dir()
    SATELLITE_CACHE_PATH.write_text(raw_text, encoding="utf-8")
    SATELLITE_META_PATH.write_text(
        json.dumps({"saved_at": time.time()}, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def cache_age_seconds() -> float | None:
    meta = read_cache_meta()
    saved_at = meta.get("saved_at")
    if not isinstance(saved_at, (int, float)):
        return None
    return max(0.0, time.time() - float(saved_at))


def fetch_satellite_source() -> str:
    request = urllib.request.Request(
        SATELLITE_SOURCE_URL,
        headers={
            "User-Agent": "CivilizationObserver/1.0 (+local cache proxy)",
            "Accept": "text/plain",
        },
    )
    with urllib.request.urlopen(request, timeout=25) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


def profile_cache_path(cache_key: str) -> Path:
    digest = hashlib.sha256(cache_key.encode("utf-8")).hexdigest()[:24]
    return CACHE_DIR / f"satellite_profile_{digest}.json"


def read_profile_cache(cache_key: str) -> dict | None:
    path = profile_cache_path(cache_key)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    saved_at = payload.get("saved_at")
    if not isinstance(saved_at, (int, float)):
        return None
    if time.time() - float(saved_at) > SATELLITE_PROFILE_CACHE_MAX_AGE_S:
        return None
    data = payload.get("data")
    return data if isinstance(data, dict) else None


def write_profile_cache(cache_key: str, data: dict) -> None:
    ensure_cache_dir()
    profile_cache_path(cache_key).write_text(
        json.dumps({"saved_at": time.time(), "data": data}, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def fetch_json(url: str, headers: dict[str, str] | None = None) -> object:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "CivilizationObserver/1.0 (+local satellite metadata cache)",
            "Accept": "application/json",
            **(headers or {}),
        },
    )
    with urllib.request.urlopen(request, timeout=25) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return json.loads(response.read().decode(charset, errors="replace"))


def fetch_satcat_record(catnr: str, name: str) -> dict | None:
    def query_records(mode: str) -> list:
        params: dict[str, str] = {"FORMAT": "JSON"}
        if mode == "catnr" and catnr:
            params["CATNR"] = catnr
        elif mode == "name" and name:
            params["NAME"] = name
            params["MAX"] = "1"
        else:
            return []
        payload = fetch_json(f"{SATCAT_RECORDS_URL}?{urllib.parse.urlencode(params)}")
        return payload if isinstance(payload, list) else []

    records = query_records("catnr" if catnr else "name")
    if not records and catnr and name:
        records = query_records("name")
    if catnr:
        for record in records:
            if str(record.get("NORAD_CAT_ID", "")) == str(catnr):
                return record
    return records[0] if records else None


def wikidata_profile_from_bindings(bindings: list[dict]) -> dict | None:
    if not bindings:
        return None
    row = bindings[0]

    def value(name: str) -> str:
        item = row.get(name)
        return str(item.get("value", "")) if isinstance(item, dict) else ""

    def number(name: str) -> float | None:
        raw = value(name)
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    return {
        "label": value("itemLabel"),
        "operator": value("operatorLabel") or value("manufacturerLabel") or value("ownerLabel"),
        "lengthM": number("length"),
        "widthM": number("width"),
        "heightM": number("height"),
        "diameterM": number("diameter"),
    }


def fetch_wikidata_record(catnr: str) -> dict | None:
    if not catnr:
        return None
    raw_catnr = str(catnr)
    padded_catnr = raw_catnr.zfill(5)
    query = f"""
SELECT ?item ?itemLabel ?operatorLabel ?manufacturerLabel ?ownerLabel ?length ?width ?height ?diameter WHERE {{
  VALUES ?scn {{ "{raw_catnr}" "{padded_catnr}" }}
  ?item wdt:P377 ?scn.
  OPTIONAL {{ ?item wdt:P137 ?operator. }}
  OPTIONAL {{ ?item wdt:P176 ?manufacturer. }}
  OPTIONAL {{ ?item wdt:P127 ?owner. }}
  OPTIONAL {{ ?item wdt:P2043 ?length. }}
  OPTIONAL {{ ?item wdt:P2049 ?width. }}
  OPTIONAL {{ ?item wdt:P2048 ?height. }}
  OPTIONAL {{ ?item wdt:P2386 ?diameter. }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "de,en". }}
}}
LIMIT 1
"""
    params = urllib.parse.urlencode({"format": "json", "query": query})
    payload = fetch_json(f"{WIKIDATA_SPARQL_URL}?{params}")
    if not isinstance(payload, dict):
        return None
    bindings = payload.get("results", {}).get("bindings", [])
    return wikidata_profile_from_bindings(bindings if isinstance(bindings, list) else [])


def get_satellite_profile(catnr: str, name: str) -> dict:
    cache_key = f"{SATELLITE_PROFILE_CACHE_VERSION}|{catnr.strip()}|{name.strip().lower()}"
    with profile_lock:
        cached = read_profile_cache(cache_key)
        if cached is not None:
            return {**cached, "cache": "fresh"}

        data = {"satcat": None, "wikidata": None, "errors": []}
        try:
            data["satcat"] = fetch_satcat_record(catnr.strip(), name.strip())
        except Exception as error:  # noqa: BLE001
            data["errors"].append(f"satcat:{type(error).__name__}")

        try:
            data["wikidata"] = fetch_wikidata_record(catnr.strip())
        except Exception as error:  # noqa: BLE001
            data["errors"].append(f"wikidata:{type(error).__name__}")

        write_profile_cache(cache_key, data)
        return {**data, "cache": "refreshed"}


def get_satellite_payload() -> tuple[int, str, dict[str, str]]:
    with cache_lock:
        cached_text = read_cache_text()
        age_s = cache_age_seconds()
        cache_is_fresh = cached_text is not None and age_s is not None and age_s < SATELLITE_CACHE_MAX_AGE_S

        if cache_is_fresh:
            return HTTPStatus.OK, cached_text, {
                "Content-Type": "text/plain; charset=utf-8",
                "Cache-Control": "no-store",
                "X-Cache-Status": "fresh",
            }

        try:
            fresh_text = fetch_satellite_source()
            write_cache(fresh_text)
            return HTTPStatus.OK, fresh_text, {
                "Content-Type": "text/plain; charset=utf-8",
                "Cache-Control": "no-store",
                "X-Cache-Status": "refreshed",
            }
        except urllib.error.HTTPError as error:
            if cached_text is not None:
                return HTTPStatus.OK, cached_text, {
                    "Content-Type": "text/plain; charset=utf-8",
                    "Cache-Control": "no-store",
                    "X-Cache-Status": f"stale-http-{error.code}",
                }
            message = (
                f"Satellitenquelle aktuell nicht verfuegbar (HTTP {error.code}). "
                f"Es liegt noch kein lokaler Cache vor."
            )
            return HTTPStatus.BAD_GATEWAY, message, {
                "Content-Type": "text/plain; charset=utf-8",
                "Cache-Control": "no-store",
            }
        except Exception as error:  # noqa: BLE001
            if cached_text is not None:
                return HTTPStatus.OK, cached_text, {
                    "Content-Type": "text/plain; charset=utf-8",
                    "Cache-Control": "no-store",
                    "X-Cache-Status": "stale-error",
                }
            message = (
                "Satellitenquelle aktuell nicht verfuegbar "
                f"({type(error).__name__}). Es liegt noch kein lokaler Cache vor."
            )
            return HTTPStatus.BAD_GATEWAY, message, {
                "Content-Type": "text/plain; charset=utf-8",
                "Cache-Control": "no-store",
            }


class CivilizationObserverHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT_DIR), **kwargs)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == SATELLITE_API_PATH:
            self.handle_satellite_request()
            return
        if parsed.path == SATELLITE_PROFILE_API_PATH:
            self.handle_satellite_profile_request(parsed.query)
            return
        super().do_GET()

    def handle_satellite_request(self) -> None:
        status, payload, headers = get_satellite_payload()
        body = payload.encode("utf-8")
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def handle_satellite_profile_request(self, query: str) -> None:
        params = urllib.parse.parse_qs(query)
        catnr = params.get("catnr", [""])[0]
        name = params.get("name", [""])[0]
        payload = get_satellite_profile(catnr, name)
        body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "private, max-age=86400")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args) -> None:
        print(f"[server] {self.address_string()} - {fmt % args}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Civilization Observer local server")
    parser.add_argument("--port", type=int, default=8000, help="Port for the local web server")
    args = parser.parse_args()

    ensure_cache_dir()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), CivilizationObserverHandler)
    print(f"Civilization Observer running on http://localhost:{args.port}")
    print("Satelliten werden lokal ueber /api/satellites/active.tle mit Festplatten-Cache bedient.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer gestoppt.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
