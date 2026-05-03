# Earth

Statische GitHub-Pages-Seite fuer Earth Launch Watch, Artemis-II-Replay und Satellitenansicht.

## Zielarchitektur

- GitHub Pages hostet nur statische Dateien: `index.html`, `styles.css`, `app.js`, `trajectory.js` und `data/*`.
- `scripts/launch_worker.py` ist der einzige Code, der TheSpaceDevs Launch Library 2 abfragt.
- `.github/workflows/launch-worker.yml` fuehrt den Worker geplant aus und committet geaenderte Dateien unter `data/`.
- Kritische Browser-Runtimes und Texturen liegen lokal unter `vendor/` und `assets/textures/`.
- `scripts/satellite_profile_rules.json` enthaelt gepruefte Namens-/Konstellationsregeln fuer SATCAT-Profile.
- Das Frontend liest nur fertige Artefakte:
  - `data/launch-feed.json`
  - `data/launch-db.json`
  - `data/launch-stats.json`
  - `data/active-satellites.tle`, sobald der Worker den Satelliten-Snapshot erzeugt hat
  - `data/satellite-profiles.json` fuer komprimierte SATCAT-Metadaten der aktiven Satelliten
- Ein Seitenaufruf triggert keine Launch-Library-Abfrage und zaehlt nicht als Refresh.

## Worker-Logik

Der GitHub-Action-Scheduler laeuft alle 10 Minuten. Der Worker:

- aktualisiert den normalen Launch Feed hoechstens einmal pro Stunde,
- prueft bei jedem Lauf faellige T-15-Preflight-Fenster,
- prueft bei jedem Lauf faellige T+30-Postflight-Fenster,
- holt verpasste Postflight-Checks nach, wenn ein NET bereits vorbei ist,
- markiert verpasste Preflight-Fenster, wenn sie nicht mehr sinnvoll nachpruefbar sind,
- speichert beobachtete Starts persistent in `data/launch-db.json`,
- berechnet Wochen-, Monats- und Jahreszahlen aus dieser eigenen Datenbank,
- aktualisiert den Satelliten-TLE-Snapshot hoechstens alle zwei Stunden.

Bei API-Fehlern bleiben vorhandene JSON-Dateien erhalten. Fehler werden in `data/worker-state.json` protokolliert.

## GitHub Pages einrichten

1. Repository auf GitHub pushen.
2. Unter `Settings -> Pages` als Source `Deploy from a branch` waehlen.
3. Branch `main` und Ordner `/ (root)` auswaehlen.
4. Unter `Settings -> Actions -> General -> Workflow permissions` sicherstellen, dass Workflows schreiben duerfen: `Read and write permissions`.
5. Die Workflow-Datei setzt zusaetzlich `permissions: contents: write`.

Es sind keine Secrets noetig, weil Launch Library und CelesTrak ohne Token abgefragt werden.

## Backfill / Seed

Die laufende Datenbank baut sich ab dem ersten Worker-Lauf selbst auf. Fuer initiale Vergleichswerte kann einmalig ein Backfill aus Launch Library `/launch/previous/` ausgefuehrt werden:

1. Auf GitHub `Actions -> Launch worker -> Run workflow` oeffnen.
2. `seed_history` aktivieren.
3. Workflow starten.

Danach sollte `seed_history` deaktiviert bleiben. Die geplanten Runs nutzen nur die eigene `data/launch-db.json` plus faellige Detailchecks.

## Lokale Entwicklung

Die Seite ist statisch, sollte aber wegen ES-Modulen ueber einen lokalen Server geoeffnet werden:

```powershell
python server.py --port 8000
```

Dann `http://127.0.0.1:8000` oeffnen.

Der lokale `server.py` bleibt als bequemer Static-File-Server erhalten. Die Website braucht seinen alten Satelliten-Proxy nicht mehr; Satelliten werden aus `data/active-satellites.tle` gelesen, sobald der Worker diese Datei erzeugt hat.

## API-Limit-Strategie

- Feed-Abfrage: maximal einmal pro Stunde.
- Detailchecks: unabhaengig vom Feed-Limit, nur fuer Starts in T-15/T+30 oder verpasste Checks.
- Pro Worker-Lauf werden standardmaessig hoechstens 8 Detailchecks ausgefuehrt (`MAX_DETAIL_CHECKS`).
- Frontend: keine Launch-Library-Abfragen, keine Monitoring-Checks, nur statische Datenreads.

## Cloudflare-Alternative

Falls spaeter deutlich praezisere Schedules, echte KV/D1-Transaktionen oder API-Endpunkte gebraucht werden, ist Option B sinnvoll:

- GitHub Pages bleibt Frontend-Host.
- Cloudflare Worker uebernimmt Scheduler und API-Endpunkte.
- KV oder D1 speichert `launch-db`, Worker-State und Feed.
- Das Frontend liest dann statt `data/*.json` die Worker-Endpunkte.

Fuer den aktuellen Stand ist Option A absichtlich bevorzugt, weil sie ohne externe Plattform neben GitHub auskommt.
