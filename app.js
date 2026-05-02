import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';

(function () {
    'use strict';

    const ARTEMIS = window.ARTEMIS2;
    if (!ARTEMIS) {
        throw new Error('ARTEMIS2 data source is missing.');
    }

    const UI_STORAGE_KEY = 'earth-ui-v2';
    const SATELLITE_CACHE_KEY = 'earth-satellite-cache-v1';
    const LAUNCH_FEED_DATA_URL = 'data/launch-feed.json';
    const LAUNCH_DB_DATA_URL = 'data/launch-db.json';
    const LAUNCH_STATS_DATA_URL = 'data/launch-stats.json';
    const SATELLITE_LIVE_HISTORY_DATA_URL = 'data/satellite-live-history.json';
    const LAUNCH_VERIFY_WINDOW_MS = 15 * 60 * 1000;
    const LAUNCH_SUCCESS_CHECK_DELAY_MS = 30 * 60 * 1000;
    const LAUNCH_DATA_REFRESH_MS = 15 * 60 * 1000;
    const SATELLITE_TLE_URL = 'data/active-satellites.tle';
    const SATELLITE_PROFILE_API_URL = '/api/satellites/profile';
    const SATELLITE_PROFILE_CACHE_VERSION = 'v2';
    const CELESTRAK_SATCAT_RECORDS_URL = 'https://celestrak.org/satcat/records.php';
    const WIKIDATA_SPARQL_URL = 'https://query.wikidata.org/sparql';
    const ISS_OEM_URL = 'data/iss-oem-j2k.txt';
    const ISS_NORAD_ID = '25544';
    const SATELLITE_LIB_CANDIDATES = [
        'https://unpkg.com/satellite.js/dist/satellite.min.js',
        'https://cdn.jsdelivr.net/npm/satellite.js@6.0.2/dist/satellite.min.js',
        'https://unpkg.com/satellite.js@6.0.2/dist/satellite.min.js'
    ];
    const SATELLITE_FETCH_INTERVAL_MS = 2 * 60 * 60 * 1000;
    const SATELLITE_PROPAGATION_INTERVAL_MS = 1000;
    const OBLIQUITY_RAD = 23.4393 * Math.PI / 180;
    const EARTH_POLE = new THREE.Vector3(-Math.sin(OBLIQUITY_RAD), Math.cos(OBLIQUITY_RAD), 0);
    const EARTH_SIDEREAL_REFERENCE_OFFSET_RAD = Math.PI / 2;
    const ORBITS_ALL_DISTANCE = 100000;
    const ZOOM_DIST_MIN = 2.6;
    const ZOOM_DIST_MAX = 10000000;
    const EARTH_TEX_URLS = [
        'https://unpkg.com/three-globe/example/img/earth-blue-marble.jpg',
        'https://cdn.jsdelivr.net/npm/three-globe/example/img/earth-blue-marble.jpg',
        'https://raw.githubusercontent.com/vasturiano/three-globe/master/example/img/earth-blue-marble.jpg'
    ];
    const EARTH_BUMP_TEX_URLS = [
        'https://unpkg.com/three-globe/example/img/earth-topology.png',
        'https://cdn.jsdelivr.net/npm/three-globe/example/img/earth-topology.png',
        'https://raw.githubusercontent.com/vasturiano/three-globe/master/example/img/earth-topology.png'
    ];
    const EARTH_NIGHT_TEX_URLS = [
        'https://unpkg.com/three-globe/example/img/earth-night.jpg',
        'https://cdn.jsdelivr.net/npm/three-globe/example/img/earth-night.jpg',
        'https://raw.githubusercontent.com/vasturiano/three-globe/master/example/img/earth-night.jpg'
    ];
    const EARTH_CLOUD_TEX_URLS = [
        'https://raw.githubusercontent.com/turban/webgl-earth/master/images/fair_clouds_4k.png',
        'https://cdn.jsdelivr.net/gh/turban/webgl-earth/images/fair_clouds_4k.png'
    ];
    const MOON_TEX_URL = 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/a8/Solarsystemscope_texture_2k_moon.jpg/1024px-Solarsystemscope_texture_2k_moon.jpg';
    const SATELLITE_RESULT_LIMIT = 40;
    const SATELLITES_IN_ORBIT_ESTIMATE = 16910;
    const ORBIT_REGIMES = ['LEO', 'MEO', 'GEO', 'HEO'];
    const WGS84_EARTH_RADIUS_KM = 6378.137;
    const EARTH_MU_KM3_S2 = 398600.4418;
    const GEOSTATIONARY_ALTITUDE_KM = 35786;
    const SIDEREAL_DAY_MINUTES = 1436.068;
    const SATELLITE_LAYER_OPACITY = 0.88;
    const SATELLITE_LAYER_DIMMED_OPACITY = 0.22;
    const SATELLITE_PICK_THRESHOLD = 0.18;
    const SATELLITE_POINT_BASE_SIZE = 0.11;
    const SATELLITE_POINT_REDUCED_MIN_SIZE = 0.012;
    const SATELLITE_POINT_REALISTIC_MIN_SIZE = 0.000012;
    const SATELLITE_POINT_REALISTIC_FAR_DISTANCE = 180;
    const SATELLITE_SIZE_MODES = ['default', 'reduced', 'realistic'];
    const LAUNCH_FOCUS_VIEW_DISTANCE = 10.5;
    const LAUNCH_ASCENT_SAMPLE_COUNT = 144;
    const LAUNCH_ORBIT_PREVIEW_SAMPLE_COUNT = 220;
    const LAUNCH_GROUND_TRACK_DEFAULT_REVOLUTIONS = 2;
    const LAUNCH_GROUND_TRACK_MIN_REVOLUTIONS = 1;
    const LAUNCH_GROUND_TRACK_MAX_REVOLUTIONS = 5;
    const SATELLITE_ORBIT_SAMPLE_COUNT = 360;
    const SATELLITE_ORBIT_DEFAULT_REVOLUTIONS = 2;
    const SATELLITE_ORBIT_MIN_REVOLUTIONS = 1;
    const SATELLITE_ORBIT_MAX_REVOLUTIONS = 5;
    const SATELLITE_ORBIT_PERIOD_MIN_MINUTES = 80;
    const SATELLITE_ORBIT_PERIOD_MAX_MINUTES = 8 * SIDEREAL_DAY_MINUTES;
    const SATELLITE_ORBIT_REFRESH_MS = 30 * 1000;
    const SCENE_CLICK_DRAG_TOLERANCE_PX = 7;
    const PROVIDER_STATS_WINDOW_DAYS = 100;

    const SUPPORTED_LANGUAGES = ['en', 'de'];
    const DACH_REGIONS = new Set(['DE', 'AT', 'CH', 'LI']);
    const DACH_TIME_ZONES = new Set(['Europe/Berlin', 'Europe/Vienna', 'Europe/Zurich', 'Europe/Busingen']);

    const TEXT = {
        en: {
            'a11y.language': 'Overlay language',
            'language.title': 'Language',
            'language.description': 'Choose the language used by the overlay. The first visit defaults to German in the DACH region and English everywhere else.',
            'language.label': 'Overlay language',
            'language.english': 'English',
            'language.german': 'Deutsch',
            'settings.artemis.open': 'Open Artemis',
            'settings.artemis.close': 'Close Artemis',
            'artemis.replay.on': 'Space replay on',
            'artemis.replay.off': 'Space replay off',
            'scene.replay': 'Replay mode: Artemis II active in space',
            'scene.home': 'Home mode: Earth Launch Tracker',
            'countdown.reached': 'Launch window reached',
            'common.unknown': 'Unknown',
            'common.none': 'none',
            'common.provider': 'Provider',
            'launch.unknownOrg': 'Unknown',
            'launch.unknownPad': 'Unknown pad',
            'launch.unknownRocket': 'Rocket unknown',
            'launch.unknownStatus': 'Status unknown',
            'launch.unnamed': 'Unnamed launch',
            'launch.noStory': 'No mission description is available for this launch in the feed.',
            'launch.selected': 'Selected launch',
            'launch.chooseFromFeed': 'Select a launch from the Launch Feed on the right.',
            'launch.noUpcoming': 'No upcoming Earth launches with coordinates found.',
            'launch.loading': 'Loading launch data ...',
            'launch.staticUnavailable': 'Static launch data unavailable',
            'launch.historyLoading': 'Loading launch history ...',
            'launch.historyEmpty': 'No historical launches loaded yet.',
            'launch.historyMoreLoading': 'Loading more history ...',
            'launch.historyMoreHint': 'Scroll further for older launches',
            'launch.historyWaiting': 'Launch history waiting ({error})',
            'launch.historyStatus': 'Worker history | {count} observed launches',
            'launch.workerStatus': 'Worker data | {age} | {count} launches',
            'launch.staticHistoryUnavailable': 'Static history unavailable',
            'launch.status.success': 'Successful',
            'launch.status.failure': 'Failed',
            'launch.status.cancelled': 'Scrubbed / cancelled',
            'launch.status.delayed': 'Delayed',
            'launch.status.goT15': 'Go at T-15',
            'launch.intel.cancelled': 'Launch cancelled - watch stays red.',
            'launch.intel.delayed': 'Launch delayed or not yet confirmed.',
            'launch.intel.failure': 'Launch failure confirmed.',
            'launch.intel.success': 'Launch success confirmed.',
            'launch.intel.go': 'T-15 confirmed by worker - timer is live green.',
            'launch.intel.noTime': 'Monitoring is waiting for a valid launch time.',
            'launch.intel.t15Queued': 'Worker has queued the T-15 check.',
            'launch.intel.preflightLoaded': 'Preflight status loaded from worker data.',
            'launch.intel.preflightChecking': 'Worker is checking T-15 status.',
            'launch.intel.windowReached': 'Launch window reached - worker checks T+30.',
            'launch.intel.postflightLoaded': 'Postflight status loaded from worker data.',
            'launch.intel.postflightChecking': 'Worker is catching up the T+30 check.',
            'stream.found': 'Official stream found.',
            'stream.open': 'Open livestream',
            'stream.searchReady': 'No official stream in the feed yet - search prepared.',
            'stream.searchYoutube': 'Search YouTube',
            'stats.deltaSame': 'same vs {label}',
            'stats.previous.week': 'Previous week',
            'stats.previous.month': 'Previous month',
            'stats.previous.year': 'Previous year',
            'stats.title.week': 'Successful week',
            'stats.title.month': 'Successful month',
            'stats.title.year': 'Successful year',
            'stats.title.success': 'Successful launches',
            'stats.title.providers': 'Providers',
            'stats.title.live': 'Live tracked',
            'stats.subtitle.loadingHistory': 'Launch history is loading',
            'stats.subtitle.history': 'Trend from {count} stored launches',
            'stats.subtitle.providers': 'Total launches in the last {days} days',
            'stats.subtitle.workerGlobal': 'Global worker trend',
            'stats.metric.current': 'Current',
            'stats.metric.total': 'Total',
            'stats.metric.providers': 'Providers',
            'stats.metric.now': 'Now',
            'stats.metric.samples': 'Samples',
            'stats.empty.loadingTrend': 'Loading launch data for the trend ...',
            'stats.empty.providersLoading': 'Loading provider statistics ...',
            'stats.empty.providersNone': 'No launches found in the last 100 days.',
            'stats.empty.satHistory': 'No global satellite history loaded yet.',
            'stats.chart.satHistory': 'Live tracked satellites over time',
            'time.range': '{start} to {end}',
            'age.notGenerated': 'not generated yet',
            'age.minutes': '{count} min old',
            'age.hours': '{count} h old',
            'orbit.rev.one': '1 orbit',
            'orbit.rev.many': '{count} orbits',
            'sat.libraryMissing': 'The satellite library could not be loaded.',
            'sat.libraryLoadFailed': 'Satellite library could not be loaded ({count} CDN attempts).',
            'sat.catalogLoading': 'Loading satellite catalog ...',
            'sat.catalogReady': '{total} estimated in orbit | {trackable} publicly trackable | {live} visible live | Filter: {enabled}',
            'sat.catalogIntro': 'Matches will appear here once the public satellite catalog is loaded.',
            'sat.noResults': 'No publicly trackable satellite currently matches your search.',
            'sat.typeFallback': 'Satellite',
            'sat.typeGeo': 'Satellite (GEO)',
            'sat.profilePending': 'Profile pending',
            'sat.satcatPending': 'SATCAT lookup pending',
            'sat.notClear': 'Not clear',
            'sat.tleNoOperator': 'TLE without operator field',
            'sat.focus': 'Focus',
            'sat.follow': 'Follow',
            'sat.following': 'Tracking on',
            'sat.height': 'Altitude',
            'sat.sizeMode.default': 'Normal',
            'sat.sizeMode.reduced': 'Smaller',
            'sat.sizeMode.realistic': 'Realistic',
            'sat.noActiveFollow': 'No tracking active',
            'sat.cacheFallback': 'Satellites from local cache | Live source currently unreachable ({error}).',
            'sat.catalogLoadFailed': 'Satellite catalog could not be loaded ({error}).',
            'sat.libraryUnavailable': 'Satellite library unavailable.',
            'sat.staticCatalogEmpty': 'static satellite catalog empty',
            'sat.unknownError': 'unknown error',
            'sat.rcsNoSize': 'no physical size',
            'satcat.notConfirmed': 'Not confirmed',
            'satcat.unknown': 'Unknown',
            'satcat.payload': 'Payload/satellite',
            'satcat.rocketBody': 'Rocket body',
            'satcat.debris': 'Space debris',
            'satcat.unknownObject': 'Unknown object',
            'profile.queryRunning': 'SATCAT/Wikidata lookup running',
            'profile.offline': 'TLE/NORAD | Profile offline ({error})',
            'profile.fromName': 'Derived from name',
            'profile.tleName': 'TLE name',
            'profile.spaceDebris': 'Space debris',
            'profile.rocketBody': 'Rocket body',
            'profile.uncataloged': 'Uncataloged payload/object',
            'profile.noActiveOperator': 'No active operator',
            'size.classOneMeter': 'approx. 1 m class',
            'size.severalMeters': 'several meters',
            'size.notPublished': 'not published',
            'size.severalMetersDeployed': 'several meters deployed',
            'size.espaCubesat': 'approx. ESPA/CubeSat class',
            'zoom.millionKm': '{value} million km',
            'body.sun': 'Sun',
            'body.earth': 'Earth',
            'body.moon': 'Moon',
            'mission.beforeStart': 'Before launch',
            'mission.phase.earthOrbit': 'Launch & Earth orbit',
            'mission.phase.highOrbit': 'High elliptical orbit',
            'mission.phase.tli': 'TLI burn & Earth departure',
            'mission.phase.outbound': 'Lunar transit (outbound)',
            'mission.phase.flyby': 'Lunar flyby',
            'mission.phase.return': 'Return transit to Earth',
            'mission.phase.entry': 'Reentry & splashdown',
            'mission.milestone.boosterSep': 'Booster separation (SRB)',
            'mission.milestone.icpsSep': 'ICPS separation',
            'mission.milestone.prm': 'Perigee raise maneuver (PRM)',
            'mission.milestone.arb': 'Apogee raise maneuver (ARB)',
            'mission.milestone.icpsProximity': 'Proximity ops test with ICPS',
            'mission.milestone.prb': 'Perigee correction (PRB)',
            'mission.milestone.tli': 'TLI burn (lunar course)',
            'mission.milestone.otc1': 'Outbound trajectory correction OTC-1',
            'mission.milestone.otc2': 'Outbound trajectory correction OTC-2',
            'mission.milestone.moonApproach': 'Approach to the Moon',
            'mission.milestone.signalLoss': 'Signal loss (far side of Moon)',
            'mission.milestone.closestMoon': 'Closest lunar approach',
            'mission.milestone.signalReturn': 'Signal reacquired',
            'mission.milestone.flybyComplete': 'Lunar flyby complete',
            'mission.milestone.rtc1': 'Return trajectory correction RTC-1',
            'mission.milestone.rtc2': 'Return trajectory correction RTC-2',
            'mission.milestone.serviceModuleSep': 'Service module separation',
            'mission.milestone.reentry': 'Reentry (Entry Interface)',
            'mission.milestone.parachute': 'Parachute deployment',
            'mission.milestone.splashdown': 'Splashdown in the Pacific'
        },
        de: {
            'a11y.language': 'Overlay-Sprache',
            'language.title': 'Sprache',
            'language.description': 'Waehle die Sprache fuer das Overlay. Beim ersten Besuch ist Deutsch im DACH-Raum aktiv, sonst Englisch.',
            'language.label': 'Overlay-Sprache',
            'language.english': 'English',
            'language.german': 'Deutsch',
            'settings.artemis.open': 'Artemis oeffnen',
            'settings.artemis.close': 'Artemis schliessen',
            'artemis.replay.on': 'Replay im Raum an',
            'artemis.replay.off': 'Replay im Raum aus',
            'scene.replay': 'Replay-Modus: Artemis II im Raum aktiv',
            'scene.home': 'Home-Modus: Earth Launch Tracker',
            'countdown.reached': 'Startfenster erreicht',
            'common.unknown': 'Unbekannt',
            'common.none': 'keine',
            'common.provider': 'Anbieter',
            'launch.unknownOrg': 'Unbekannt',
            'launch.unknownPad': 'Unbekanntes Pad',
            'launch.unknownRocket': 'Rakete unbekannt',
            'launch.unknownStatus': 'Status unbekannt',
            'launch.unnamed': 'Unbenannter Start',
            'launch.noStory': 'Zu diesem Start liegt im Feed keine Missionsbeschreibung vor.',
            'launch.selected': 'Ausgewaehlter Start',
            'launch.chooseFromFeed': 'Waehle rechts im Launch Feed einen Start aus.',
            'launch.noUpcoming': 'Keine anstehenden Earth-Launches mit Koordinaten gefunden.',
            'launch.loading': 'Lade Startdaten ...',
            'launch.staticUnavailable': 'Statische Startdaten nicht erreichbar',
            'launch.historyLoading': 'Lade Starthistorie ...',
            'launch.historyEmpty': 'Noch keine historischen Starts geladen.',
            'launch.historyMoreLoading': 'Laedt weitere Historie ...',
            'launch.historyMoreHint': 'Weiter scrollen fuer aeltere Starts',
            'launch.historyWaiting': 'Starthistorie wartet ({error})',
            'launch.historyStatus': 'Worker-Historie | {count} beobachtete Starts',
            'launch.workerStatus': 'Worker-Daten | {age} | {count} Starts',
            'launch.staticHistoryUnavailable': 'Statische Historie nicht erreichbar',
            'launch.status.success': 'Erfolgreich',
            'launch.status.failure': 'Fehlgeschlagen',
            'launch.status.cancelled': 'Scrubbed / abgesagt',
            'launch.status.delayed': 'Verschoben',
            'launch.status.goT15': 'Go bei T-15',
            'launch.intel.cancelled': 'Start abgesagt - Watch bleibt auf rot.',
            'launch.intel.delayed': 'Start verzoegert oder noch nicht bestaetigt.',
            'launch.intel.failure': 'Start fehlgeschlagen bestaetigt.',
            'launch.intel.success': 'Start erfolgreich bestaetigt.',
            'launch.intel.go': 'T-15 vom Worker bestaetigt - Timer ist live-gruen.',
            'launch.intel.noTime': 'Monitoring wartet auf eine gueltige Startzeit.',
            'launch.intel.t15Queued': 'Worker hat T-15 Check vorgemerkt.',
            'launch.intel.preflightLoaded': 'Preflight-Status aus Worker-Daten geladen.',
            'launch.intel.preflightChecking': 'Worker prueft den T-15 Status.',
            'launch.intel.windowReached': 'Startfenster erreicht - Worker prueft T+30.',
            'launch.intel.postflightLoaded': 'Postflight-Status aus Worker-Daten geladen.',
            'launch.intel.postflightChecking': 'Worker holt den T+30 Check nach.',
            'stream.found': 'Offizieller Stream gefunden.',
            'stream.open': 'Livestream oeffnen',
            'stream.searchReady': 'Noch kein offizieller Stream im Feed - Suche vorbereitet.',
            'stream.searchYoutube': 'Auf YouTube suchen',
            'stats.deltaSame': 'gleich vs {label}',
            'stats.previous.week': 'Vorwoche',
            'stats.previous.month': 'Vormonat',
            'stats.previous.year': 'Vorjahr',
            'stats.title.week': 'Erfolgreich Woche',
            'stats.title.month': 'Erfolgreich Monat',
            'stats.title.year': 'Erfolgreich Jahr',
            'stats.title.success': 'Erfolgreiche Starts',
            'stats.title.providers': 'Anbieter',
            'stats.title.live': 'Live getrackt',
            'stats.subtitle.loadingHistory': 'Starthistorie wird geladen',
            'stats.subtitle.history': 'Verlauf aus {count} gespeicherten Starts',
            'stats.subtitle.providers': 'Gesamtstarts der letzten {days} Tage',
            'stats.subtitle.workerGlobal': 'Globaler Worker-Verlauf',
            'stats.metric.current': 'Aktuell',
            'stats.metric.total': 'Gesamt',
            'stats.metric.providers': 'Anbieter',
            'stats.metric.now': 'Jetzt',
            'stats.metric.samples': 'Samples',
            'stats.empty.loadingTrend': 'Lade Startdaten fuer den Verlauf ...',
            'stats.empty.providersLoading': 'Lade Anbieterstatistik ...',
            'stats.empty.providersNone': 'Keine Starts in den letzten 100 Tagen gefunden.',
            'stats.empty.satHistory': 'Noch keine globale Satellitenhistorie geladen.',
            'stats.chart.satHistory': 'Live getrackte Satelliten im Verlauf',
            'time.range': '{start} bis {end}',
            'age.notGenerated': 'noch nicht generiert',
            'age.minutes': '{count} min alt',
            'age.hours': '{count} h alt',
            'orbit.rev.one': '1 Umlauf',
            'orbit.rev.many': '{count} Umlaeufe',
            'sat.libraryMissing': 'Die Satellitenbibliothek konnte nicht geladen werden.',
            'sat.libraryLoadFailed': 'Satellitenbibliothek konnte nicht geladen werden ({count} CDN-Versuche).',
            'sat.catalogLoading': 'Lade Satellitenkatalog ...',
            'sat.catalogReady': '{total} im Orbit geschaetzt | {trackable} oeffentlich trackbar | {live} live sichtbar | Filter: {enabled}',
            'sat.catalogIntro': 'Sobald der oeffentliche Satellitenkatalog geladen ist, erscheinen hier Treffer.',
            'sat.noResults': 'Kein oeffentlich trackbarer Satellit passt gerade zu deiner Suche.',
            'sat.typeFallback': 'Satellit',
            'sat.typeGeo': 'Satellit (GEO)',
            'sat.profilePending': 'Profil ausstehend',
            'sat.satcatPending': 'SATCAT-Abfrage ausstehend',
            'sat.notClear': 'Nicht eindeutig',
            'sat.tleNoOperator': 'TLE ohne Betreiberfeld',
            'sat.focus': 'Fokus',
            'sat.follow': 'Folgen',
            'sat.following': 'Verfolgung an',
            'sat.height': 'Hoehe',
            'sat.sizeMode.default': 'Normal',
            'sat.sizeMode.reduced': 'Kleiner',
            'sat.sizeMode.realistic': 'Realistisch',
            'sat.noActiveFollow': 'Keine Verfolgung aktiv',
            'sat.cacheFallback': 'Satelliten aus lokalem Cache | Live-Quelle aktuell nicht erreichbar ({error}).',
            'sat.catalogLoadFailed': 'Satellitenkatalog konnte nicht geladen werden ({error}).',
            'sat.libraryUnavailable': 'Satellitenbibliothek nicht verfuegbar.',
            'sat.staticCatalogEmpty': 'statischer Satellitenkatalog leer',
            'sat.unknownError': 'unbekannter Fehler',
            'sat.rcsNoSize': 'keine Baugroesse',
            'satcat.notConfirmed': 'Nicht bestaetigt',
            'satcat.unknown': 'Unbekannt',
            'satcat.payload': 'Nutzlast/Satellit',
            'satcat.rocketBody': 'Raketenkörper',
            'satcat.debris': 'Weltraumschrott',
            'satcat.unknownObject': 'Unbekanntes Objekt',
            'profile.queryRunning': 'SATCAT/Wikidata-Abfrage laeuft',
            'profile.offline': 'TLE/NORAD | Profil offline ({error})',
            'profile.fromName': 'Aus Name abgeleitet',
            'profile.tleName': 'TLE-Name',
            'profile.spaceDebris': 'Weltraumschrott',
            'profile.rocketBody': 'Raketenkörper',
            'profile.uncataloged': 'Nicht katalogisierte Nutzlast/Objekt',
            'profile.noActiveOperator': 'Kein aktiver Betreiber',
            'size.classOneMeter': 'ca. 1 m Klasse',
            'size.severalMeters': 'mehrere Meter',
            'size.notPublished': 'nicht veroeffentlicht',
            'size.severalMetersDeployed': 'mehrere Meter entfaltet',
            'size.espaCubesat': 'ca. ESPA/CubeSat-Klasse',
            'zoom.millionKm': '{value} Mio km',
            'body.sun': 'Sonne',
            'body.earth': 'Erde',
            'body.moon': 'Mond',
            'mission.beforeStart': 'Vor dem Start',
            'mission.phase.earthOrbit': 'Start & Erdorbit',
            'mission.phase.highOrbit': 'Hochelliptischer Orbit',
            'mission.phase.tli': 'TLI-Burn & Erdabflug',
            'mission.phase.outbound': 'Mondtransit (Hinflug)',
            'mission.phase.flyby': 'Mond-Flyby',
            'mission.phase.return': 'Ruecktransit zur Erde',
            'mission.phase.entry': 'Wiedereintritt & Landung',
            'mission.milestone.boosterSep': 'Booster-Abtrennung (SRB)',
            'mission.milestone.icpsSep': 'ICPS-Abtrennung',
            'mission.milestone.prm': 'Perigaeum-Anhebung (PRM)',
            'mission.milestone.arb': 'Apogaeum-Anhebung (ARB)',
            'mission.milestone.icpsProximity': 'Proximity-Ops-Test mit ICPS',
            'mission.milestone.prb': 'Perigaeum-Korrektur (PRB)',
            'mission.milestone.tli': 'TLI-Burn (Mondkurs)',
            'mission.milestone.otc1': 'Outbound-Kurskorrektur OTC-1',
            'mission.milestone.otc2': 'Outbound-Kurskorrektur OTC-2',
            'mission.milestone.moonApproach': 'Annaeherung an den Mond',
            'mission.milestone.signalLoss': 'Funkverlust (Mondrueckseite)',
            'mission.milestone.closestMoon': 'Naechste Mondannaeherung',
            'mission.milestone.signalReturn': 'Funkkontakt wiederhergestellt',
            'mission.milestone.flybyComplete': 'Mond-Flyby abgeschlossen',
            'mission.milestone.rtc1': 'Return-Kurskorrektur RTC-1',
            'mission.milestone.rtc2': 'Return-Kurskorrektur RTC-2',
            'mission.milestone.serviceModuleSep': 'Service-Modul-Abtrennung',
            'mission.milestone.reentry': 'Wiedereintritt (Entry Interface)',
            'mission.milestone.parachute': 'Fallschirm-Entfaltung',
            'mission.milestone.splashdown': 'Splashdown im Pazifik'
        }
    };

    const STATIC_TRANSLATIONS = {
        en: {
            'Einstellungen': 'Settings',
            'Settings': 'Settings',
            'Settings schliessen': 'Close settings',
            'Sprache': 'Language',
            'Waehle die Sprache fuer das Overlay. Im DACH-Raum startet die App auf Deutsch, sonst auf Englisch.': 'Choose the language for the overlay. In the DACH region the app starts in German, otherwise in English.',
            'Overlay-Sprache': 'Overlay language',
            'Deutsch': 'German',
            'Tracking, Flugbahn und optionale Missionsansichten.': 'Tracking, trajectories, and optional mission views.',
            'Satelliten-Tracking': 'Satellite tracking',
            'Lege fest, wie weit die vorausberechnete Flugbahn fuer den verfolgten Satelliten angezeigt wird.': 'Choose how far the predicted path for the tracked satellite is shown.',
            'Flugbahn-Laenge': 'Path length',
            'Punktgroesse': 'Point size',
            'Normal': 'Normal',
            'Kleiner': 'Smaller',
            'Realistisch': 'Realistic',
            'Launch-Groundtrack': 'Launch ground track',
            'Lege fest, wie weit die blaue Bodenprojektion einer ausgewaehlten Mission vorauslaeuft.': 'Choose how far the blue ground projection for a selected mission runs ahead.',
            'Bodenroute-Laenge': 'Ground route length',
            'Replay-Deck und Missionsmarken nur bei Bedarf anzeigen.': 'Show the replay deck and mission markers only when needed.',
            'Replay im Raum aus': 'Space replay off',
            'Phase': 'Phase',
            'Datum & Uhrzeit': 'Date & time',
            'Entfernung Erde': 'Distance from Earth',
            'Abstand Mond': 'Distance from Moon',
            'Geschwindigkeit': 'Velocity',
            'Missionsposition': 'Mission position',
            'Zum Start': 'To launch',
            'Zur Landung': 'To landing',
            'Orion folgen': 'Follow Orion',
            'Satelliten-Suche': 'Satellite Search',
            'Suche nach ISS, Hubble, Tiangong, Starlink und allen anderen oeffentlich trackbaren Satelliten und filtere die Orbits live nach Regime.': 'Search for ISS, Hubble, Tiangong, Starlink, and all other publicly trackable satellites. Filter live orbits by regime.',
            'Suche schliessen': 'Close search',
            'Lade Satellitenkatalog ...': 'Loading satellite catalog ...',
            'Ergebnisse': 'Results',
            'Jeder Treffer kann fokussiert oder direkt verfolgt werden.': 'Each result can be focused or tracked directly.',
            'Overview': 'Overview',
            'Startseite': 'Home',
            'Launches & Satelliten': 'Launches & Satellites',
            'Getrackte Starts': 'Tracked launches',
            'Naechster Countdown': 'Next countdown',
            'Anbieter': 'Providers',
            'Pads': 'Pads',
            'Erfolgreich Woche': 'Successful week',
            'Erfolgreich Monat': 'Successful month',
            'Erfolgreich Jahr': 'Successful year',
            'Satelliten im Orbit': 'Satellites in orbit',
            'Live getrackt': 'Live tracked',
            'Uebersicht ein- oder ausblenden': 'Show or hide overview',
            'Anbieter-Statistik oeffnen': 'Open provider statistics',
            'Wochenverlauf erfolgreicher Starts oeffnen': 'Open weekly trend for successful launches',
            'Monatsverlauf erfolgreicher Starts oeffnen': 'Open monthly trend for successful launches',
            'Jahresverlauf erfolgreicher Starts oeffnen': 'Open yearly trend for successful launches',
            'Statistik': 'Statistics',
            'Verlauf': 'Trend',
            'Statistik schliessen': 'Close statistics',
            'Mission Control schliessen': 'Close Mission Control',
            'Ausgewaehlter Start': 'Selected launch',
            'LiveStream': 'Livestream',
            'Streamsuche wartet auf T-15.': 'Stream search waits for T-15.',
            'Livestream oeffnen': 'Open livestream',
            'Status': 'Status',
            'Countdown': 'Countdown',
            'Startfenster': 'Launch window',
            'Pad': 'Pad',
            'Koordinaten': 'Coordinates',
            'Zum Startplatz': 'To launch pad',
            'Monitoring wartet auf Startdaten.': 'Monitoring is waiting for launch data.',
            'Sobald Launch-Daten geladen sind, erscheint hier die Missionsbeschreibung oder ein kurzer Kontext zum Flug.': 'Once launch data is loaded, the mission description or a short flight context appears here.',
            'Firma': 'Company',
            'Rakete': 'Rocket',
            'Externe Quelle oeffnen': 'Open external source',
            'Raketenstarts': 'Rocket launches',
            'Launch Feed aktualisieren': 'Refresh Launch Feed',
            'Launch Feed ein- oder ausblenden': 'Show or hide Launch Feed',
            'Launch Feed Ansicht': 'Launch Feed view',
            'Anstehend': 'Upcoming',
            'Starthistorie': 'Launch history',
            'Lade Startdaten ...': 'Loading launch data ...',
            'Satellit verfolgt': 'Satellite tracked',
            'Keine Verfolgung aktiv': 'No tracking active',
            'Satellitenverfolgung beenden': 'Stop satellite tracking',
            'Typ': 'Type',
            'Betreiber': 'Operator',
            'Land/Region': 'Country/region',
            'Zuordnung': 'Assignment',
            'Groesse': 'Size',
            'Regime': 'Regime',
            'Live-Hoehe': 'Live altitude',
            'Perigaeum': 'Perigee',
            'Apogaeum': 'Apogee',
            'Inklination': 'Inclination',
            'Periode': 'Period',
            'Exzentrizitaet': 'Eccentricity',
            'Breite': 'Latitude',
            'Laenge': 'Longitude',
            'Verfolgung beenden': 'Stop tracking',
            'Steuerung ein- oder ausblenden': 'Show or hide controls',
            'Zoom': 'Zoom',
            'Naechster Start': 'Next launch',
            'Erde': 'Earth',
            'Mein Standort': 'My location',
            'Mond': 'Moon',
            'Sonnensystem': 'Solar system',
            'Freie Kamera': 'Free camera',
            'Zeit': 'Time',
            'Jetzt': 'Now',
            'Uebersicht einblenden': 'Show overview',
            'Launch Feed einblenden': 'Show Launch Feed',
            'Steuerung einblenden': 'Show controls',
            'Mobile Navigation': 'Mobile navigation',
            'Steuerung': 'Controls',
            'Info': 'Info',
            'Feed': 'Feed',
            'Satellit': 'Satellite',
            'Start': 'Launch'
        },
        de: {
            'Settings': 'Einstellungen',
            'Overview': 'Uebersicht',
            'Mission Control': 'Mission Control',
            'Launch Feed': 'Launch Feed',
            'Earth': 'Earth'
        }
    };

    const DATA_LABEL_TRANSLATIONS_EN = {
        'Unbekannt': 'Unknown',
        'Nicht eindeutig': 'Not clear',
        'Nicht bestaetigt': 'Not confirmed',
        'Profil ausstehend': 'Profile pending',
        'SATCAT-Abfrage ausstehend': 'SATCAT lookup pending',
        'TLE ohne Betreiberfeld': 'TLE without operator field',
        'Aus Name abgeleitet': 'Derived from name',
        'TLE-Name': 'TLE name',
        'Satellit': 'Satellite',
        'Satellit (GEO)': 'Satellite (GEO)',
        'Nutzlast/Satellit': 'Payload/satellite',
        'Raketenk\u00f6rper': 'Rocket body',
        'Weltraumschrott': 'Space debris',
        'Unbekanntes Objekt': 'Unknown object',
        'Kein aktiver Betreiber': 'No active operator',
        'Nicht katalogisierte Nutzlast/Objekt': 'Uncataloged payload/object',
        'Raumstation/ISS-Modul': 'Space station/ISS module',
        'Raumstation/Stationsmodul': 'Space station/station module',
        'Raumschiff/Versorgung': 'Spacecraft/resupply',
        'Kommunikationssatellit': 'Communications satellite',
        'Kommunikations-/Datenrelaissatellit': 'Communications/data relay satellite',
        'Milit\u00e4rischer Kommunikationssatellit': 'Military communications satellite',
        'Navigationssatellit': 'Navigation satellite',
        'Wetter-/Umweltsatellit': 'Weather/environment satellite',
        'Erdbeobachtung': 'Earth observation',
        'Radar-Erdbeobachtung': 'Radar Earth observation',
        'Weltraumteleskop': 'Space telescope',
        'Wissenschaftssatellit': 'Science satellite',
        'Milit\u00e4r-/Aufkl\u00e4rungssatellit': 'Military/reconnaissance satellite',
        'Milit\u00e4r-/Regierungssatellit': 'Military/government satellite',
        'Regierungs-/Aufkl\u00e4rungssatellit': 'Government/reconnaissance satellite',
        'Aufkl\u00e4rungssatellit': 'Reconnaissance satellite',
        'Wetter-/AIS-Datensatellit': 'Weather/AIS data satellite',
        'RF-Aufkl\u00e4rungssatellit': 'RF reconnaissance satellite',
        'IoT-Kommunikationssatellit': 'IoT communications satellite',
        'Europ\u00e4ische Union': 'European Union',
        'Vereinigtes K\u00f6nigreich': 'United Kingdom',
        'Vereinigtes K\u00f6nigreich/Frankreich': 'United Kingdom/France',
        'Russland': 'Russia',
        'Deutschland': 'Germany',
        'Frankreich': 'France',
        'Frankreich/Europa': 'France/Europe',
        'Kanada': 'Canada',
        'Spanien': 'Spain',
        'T\u00fcrkei': 'Turkey',
        'Saudi-Arabien': 'Saudi Arabia',
        '\u00c4gypten': 'Egypt',
        'Finnland': 'Finland',
        'S\u00fcdkorea': 'South Korea',
        'Indien': 'India',
        'Luxemburg': 'Luxembourg',
        'International': 'International',
        'Europa': 'Europe',
        'USA/Europa': 'USA/Europe',
        'USA/Luxemburg': 'USA/Luxembourg'
    };

    const STATIC_TEXT_NODE_ORIGINALS = new WeakMap();
    const STATIC_ATTRIBUTE_ORIGINALS = new WeakMap();
    const DATE_FORMATTER_CACHE = new Map();

    function normalizeLanguage(language) {
        return SUPPORTED_LANGUAGES.includes(language) ? language : 'en';
    }

    function regionFromLocale(locale) {
        const match = String(locale || '').match(/[-_]([A-Za-z]{2})(?:$|-|_)/);
        return match ? match[1].toUpperCase() : '';
    }

    function defaultUiLanguage() {
        const languages = Array.isArray(navigator.languages) && navigator.languages.length
            ? navigator.languages
            : [navigator.language].filter(Boolean);
        const inDachLocale = languages.some((language) => {
            const region = regionFromLocale(language);
            return DACH_REGIONS.has(region);
        });
        const timeZone = Intl.DateTimeFormat().resolvedOptions().timeZone || '';
        return inDachLocale || DACH_TIME_ZONES.has(timeZone) ? 'de' : 'en';
    }

    function currentLanguage() {
        return normalizeLanguage(state?.panelVisibility?.language || defaultUiLanguage());
    }

    function currentLocale() {
        return currentLanguage() === 'de' ? 'de-DE' : 'en-US';
    }

    function t(key, values = {}) {
        const table = TEXT[currentLanguage()] || TEXT.en;
        const fallback = TEXT.en[key] || TEXT.de[key] || key;
        return String(table[key] || fallback).replace(/\{(\w+)\}/g, (match, name) => (
            Object.prototype.hasOwnProperty.call(values, name) ? String(values[name]) : match
        ));
    }

    function formatNumber(value, options = undefined) {
        return Number(value).toLocaleString(currentLocale(), options);
    }

    function dateFormatter(key, options) {
        const cacheKey = `${currentLocale()}|${key}`;
        if (!DATE_FORMATTER_CACHE.has(cacheKey)) {
            DATE_FORMATTER_CACHE.set(cacheKey, new Intl.DateTimeFormat(currentLocale(), options));
        }
        return DATE_FORMATTER_CACHE.get(cacheKey);
    }

    function formatLocalDateTime(date) {
        return dateFormatter('long', {
            weekday: 'short',
            day: '2-digit',
            month: '2-digit',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false
        }).format(date);
    }

    function formatLocalShortDateTime(date) {
        return dateFormatter('short', {
            day: '2-digit',
            month: '2-digit',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            timeZoneName: 'short'
        }).format(date);
    }

    function formatLocalTimeOnly(date) {
        return dateFormatter('time', {
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false
        }).format(date);
    }

    function translateDataLabel(value) {
        if (currentLanguage() !== 'en' || typeof value !== 'string') return value;
        return DATA_LABEL_TRANSLATIONS_EN[value] || value;
    }

    function getLocalTimeZoneLabel(date = new Date()) {
        try {
            const part = dateFormatter('zone', { timeZoneName: 'short' })
                .formatToParts(date)
                .find((entry) => entry.type === 'timeZoneName');
            return part?.value || Intl.DateTimeFormat().resolvedOptions().timeZone || (currentLanguage() === 'de' ? 'Lokal' : 'Local');
        } catch (error) {
            return currentLanguage() === 'de' ? 'Lokal' : 'Local';
        }
    }

    function getSatelliteLib() {
        return window.satellite || null;
    }

    function loadExternalScript(src) {
        return new Promise((resolve, reject) => {
            const existing = document.querySelector(`script[data-external-script="${src}"]`);
            if (existing) {
                if (existing.dataset.loaded === 'true') {
                    resolve();
                    return;
                }
                if (existing.dataset.failed === 'true') {
                    reject(new Error(`Failed to load ${src}`));
                    return;
                }
                existing.addEventListener('load', () => resolve(), { once: true });
                existing.addEventListener('error', () => reject(new Error(`Failed to load ${src}`)), { once: true });
                return;
            }

            const script = document.createElement('script');
            script.src = src;
            script.async = true;
            script.dataset.externalScript = src;
            script.onload = () => {
                script.dataset.loaded = 'true';
                resolve();
            };
            script.onerror = () => {
                script.dataset.failed = 'true';
                reject(new Error(`Failed to load ${src}`));
            };
            document.head.appendChild(script);
        });
    }

    async function ensureSatelliteLibrary() {
        if (getSatelliteLib()) {
            state.satelliteLibraryReady = true;
            state.satelliteLastError = '';
            return true;
        }

        const failedSources = [];
        for (const source of SATELLITE_LIB_CANDIDATES) {
            try {
                await loadExternalScript(source);
                if (getSatelliteLib()) {
                    state.satelliteLibraryReady = true;
                    state.satelliteLastError = '';
                    return true;
                }
            } catch (error) {
                failedSources.push(source);
            }
        }

        state.satelliteLibraryReady = false;
        state.satelliteLastError = t('sat.libraryLoadFailed', { count: failedSources.length });
        return false;
    }

    const state = {
        scene: null,
        camera: null,
        renderer: null,
        controls: null,
        earthGroup: null,
        earthMesh: null,
        earthCloudMesh: null,
        earthAtmosphereMesh: null,
        earthGlowMesh: null,
        earthNightUniforms: null,
        earthRotationAngle: 0,
        moonMesh: null,
        sunMesh: null,
        sunGlow: null,
        earthLabel: null,
        moonLabel: null,
        orionMarker: null,
        orionGlow: null,
        orionLabel: null,
        pastLine: null,
        futureLine: null,
        moonOrbitLine: null,
        planetMeshes: {},
        planetOrbits: {},
        planetOrbitList: [],
        dynamicLabels: [],
        extraPickableMeshes: [],
        pickableMeshes: [],
        launchMarkerRoot: null,
        launchMarkers: new Map(),
        launchTrajectoryFrame: null,
        launchTrajectoryLine: null,
        launchTrajectoryGroundTrackLine: null,
        launchTrajectoryOrbitLine: null,
        launchTrajectoryKey: '',
        launchTrajectoryEventMs: 0,
        observerMarker: null,
        observerPulse: null,
        observerLocation: null,
        observerWatchId: null,
        satellitePoints: null,
        satelliteHighlight: null,
        satelliteFocusedModelKey: '',
        satelliteOrbitLine: null,
        satelliteGroundTrackLine: null,
        satelliteOrbitLastKey: '',
        satelliteCatalog: [],
        satelliteIndex: new Map(),
        satelliteCatalogLoaded: false,
        satelliteLibraryReady: false,
        satelliteLastError: '',
        satelliteLiveCount: 0,
        satelliteLiveHistory: [],
        satelliteLiveHistoryGeneratedAt: '',
        satelliteLiveHistoryFetchedAt: 0,
        satelliteProfileCache: new Map(),
        satelliteProfilePending: new Map(),
        satelliteSearchQuery: '',
        satelliteFilters: { LEO: true, MEO: true, GEO: true, HEO: true },
        satelliteWorldPositions: new Map(),
        satelliteDrawOrder: [],
        issOemSamples: [],
        issOemLoaded: false,
        issOemPromise: null,
        issOemError: '',
        followSatelliteId: null,
        satelliteAutoHidNews: false,
        followObserver: false,
        satelliteLastPropagationMs: 0,
        satelliteFetchTimer: null,
        focusedBody: null,
        focusLaunchId: null,
        followMoon: false,
        followOrion: false,
        userNavigatingCamera: false,
        freeCameraMode: false,
        flyKeys: { ArrowUp: false, ArrowDown: false, ArrowLeft: false, ArrowRight: false },
        raycaster: new THREE.Raycaster(),
        pointerNdc: new THREE.Vector2(),
        sunDirLight: null,
        fillDirLight: null,
        sunPointLight: null,
        sunScenePos: new THREE.Vector3(),
        simTime: Date.now(),
        lastFrameTime: performance.now(),
        timeWarp: 1,
        warpStepMag: 10,
        warpTrack: 'idle',
        zoomSliderDragging: false,
        missionSliderDragging: false,
        pointerDownScreen: null,
        sceneClickBlockedUntil: 0,
        launches: [],
        selectedLaunchId: null,
        launchDetailActive: false,
        launchFeedMode: 'upcoming',
        launchHistoryItems: [],
        launchHistoryNextUrl: '',
        launchHistoryLoading: false,
        launchHistoryDone: false,
        launchHistoryError: '',
        launchCountdownTimer: null,
        launchFeedFetchTimer: null,
        launchHistoryFetchTimer: null,
        launchWatchList: new Map(),
        launchStreamUiKey: '',
        launchSuccessStats: null,
        launchSuccessStatsFetchedAt: 0,
        launchDataGeneratedAt: '',
        launchDataSource: '',
        statsPanelOpen: false,
        statsPanelMode: '',
        mobileActivePanel: null,
        mobileSheetDrag: null,
        mobileSheetHeights: {},
        fullTrajectory: [],
        totalMissionHours: 240,
        artemisReplayEnabled: false,
        artemisReplayInitialized: false,
        missionTimelineActiveIndex: -1,
        panelVisibility: readUiState()
    };

    const dom = {};

    function sceneTimeMs() {
        return state.simTime;
    }

    function simTimeFromMissionMet(metHours) {
        return ARTEMIS.LAUNCH_UTC + metHours * 3600000;
    }

    function earthReferenceTimeMs() {
        if (state.artemisReplayEnabled || state.timeWarp !== 1) {
            return sceneTimeMs();
        }
        return Date.now();
    }

    function isMobileViewport() {
        return typeof window.matchMedia === 'function' &&
            window.matchMedia('(max-width: 960px), (hover: none) and (pointer: coarse)').matches;
    }

    function clampSatelliteOrbitRevolutions(value) {
        return THREE.MathUtils.clamp(
            Math.round(Number(value) || SATELLITE_ORBIT_DEFAULT_REVOLUTIONS),
            SATELLITE_ORBIT_MIN_REVOLUTIONS,
            SATELLITE_ORBIT_MAX_REVOLUTIONS
        );
    }

    function clampLaunchGroundTrackRevolutions(value) {
        return THREE.MathUtils.clamp(
            Math.round(Number(value) || LAUNCH_GROUND_TRACK_DEFAULT_REVOLUTIONS),
            LAUNCH_GROUND_TRACK_MIN_REVOLUTIONS,
            LAUNCH_GROUND_TRACK_MAX_REVOLUTIONS
        );
    }

    function normalizeSatelliteSizeMode(value, legacyRealistic = false) {
        const mode = String(value || '');
        if (SATELLITE_SIZE_MODES.includes(mode)) return mode;
        return legacyRealistic ? 'realistic' : 'reduced';
    }

    function readUiState() {
        const defaults = {
            news: true,
            watch: true,
            controls: true,
            orbitRevolutions: SATELLITE_ORBIT_DEFAULT_REVOLUTIONS,
            launchGroundTrackRevolutions: LAUNCH_GROUND_TRACK_DEFAULT_REVOLUTIONS,
            satelliteSizeMode: 'reduced',
            language: defaultUiLanguage()
        };
        try {
            const raw = localStorage.getItem(UI_STORAGE_KEY);
            if (!raw) return defaults;
            const parsed = { ...defaults, ...JSON.parse(raw) };
            parsed.orbitRevolutions = clampSatelliteOrbitRevolutions(parsed.orbitRevolutions);
            parsed.launchGroundTrackRevolutions = clampLaunchGroundTrackRevolutions(parsed.launchGroundTrackRevolutions);
            parsed.satelliteSizeMode = normalizeSatelliteSizeMode(
                parsed.satelliteSizeMode,
                Boolean(parsed.satelliteRealisticSize)
            );
            delete parsed.satelliteRealisticSize;
            parsed.language = normalizeLanguage(parsed.language);
            return parsed;
        } catch (error) {
            return defaults;
        }
    }

    function writeUiState() {
        try {
            localStorage.setItem(UI_STORAGE_KEY, JSON.stringify(state.panelVisibility));
        } catch (error) {
            // ignore
        }
    }

    async function fetchStaticJson(url) {
        const response = await fetch(url, {
            cache: 'no-cache',
            headers: { Accept: 'application/json' }
        });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
    }

    function dataAgeLabel(generatedAt) {
        const date = generatedAt ? new Date(generatedAt) : null;
        const savedAt = date && !Number.isNaN(date.getTime()) ? date.getTime() : 0;
        if (!savedAt) return t('age.notGenerated');
        const ageMs = Math.max(0, Date.now() - savedAt);
        const minutes = Math.max(1, Math.round(ageMs / 60000));
        if (minutes < 60) return t('age.minutes', { count: minutes });
        const hours = Math.round(minutes / 60);
        return t('age.hours', { count: hours });
    }

    function writeSatelliteCache(rawText) {
        try {
            localStorage.setItem(SATELLITE_CACHE_KEY, JSON.stringify({
                savedAt: Date.now(),
                rawText
            }));
        } catch (error) {
            // ignore
        }
    }

    function readSatelliteCache() {
        try {
            const raw = localStorage.getItem(SATELLITE_CACHE_KEY);
            if (!raw) return null;
            const parsed = JSON.parse(raw);
            return typeof parsed?.rawText === 'string' ? parsed : null;
        } catch (error) {
            return null;
        }
    }

    function parseOemTimestamp(value) {
        const text = String(value || '').trim();
        const dayOfYearMatch = text.match(/^(\d{4})-(\d{3})T(.+)$/);
        if (dayOfYearMatch) {
            const year = Number(dayOfYearMatch[1]);
            const dayOfYear = Number(dayOfYearMatch[2]);
            const rest = dayOfYearMatch[3].replace(/Z$/, '');
            const [timePart, fractionPart = ''] = rest.split('.');
            const [hour = 0, minute = 0, second = 0] = timePart.split(':').map(Number);
            const millisecond = Number((fractionPart + '000').slice(0, 3)) || 0;
            const dateMs = Date.UTC(year, 0, 1, hour, minute, second, millisecond) + (dayOfYear - 1) * 86400000;
            return Number.isFinite(dateMs) ? dateMs : NaN;
        }

        const dateMs = Date.parse(text.endsWith('Z') ? text : `${text}Z`);
        return Number.isFinite(dateMs) ? dateMs : NaN;
    }

    function parseIssOemText(rawText) {
        return String(rawText || '')
            .split(/\r?\n/)
            .map((line) => line.trim())
            .filter((line) => line && /^\d{4}-/.test(line))
            .map((line) => {
                const parts = line.split(/\s+/);
                if (parts.length < 7) return null;
                const epochMs = parseOemTimestamp(parts[0]);
                const positionKm = parts.slice(1, 4).map(Number);
                const velocityKmS = parts.slice(4, 7).map(Number);
                if (!Number.isFinite(epochMs) ||
                    positionKm.some((entry) => !Number.isFinite(entry)) ||
                    velocityKmS.some((entry) => !Number.isFinite(entry))) {
                    return null;
                }
                return { epochMs, positionKm, velocityKmS };
            })
            .filter(Boolean)
            .sort((a, b) => a.epochMs - b.epochMs);
    }

    function j2kPositionToSceneVector(positionKm) {
        return new THREE.Vector3(
            -positionKm[1] / 1000,
            positionKm[2] / 1000,
            -positionKm[0] / 1000
        );
    }

    function interpolateIssOemPosition(dateMs) {
        const samples = state.issOemSamples;
        if (!samples.length || dateMs < samples[0].epochMs || dateMs > samples[samples.length - 1].epochMs) {
            return null;
        }

        let low = 0;
        let high = samples.length - 1;
        while (high - low > 1) {
            const mid = Math.floor((low + high) / 2);
            if (samples[mid].epochMs <= dateMs) low = mid;
            else high = mid;
        }

        const a = samples[low];
        const b = samples[Math.min(low + 1, samples.length - 1)];
        if (!a || !b || a === b) return a ? j2kPositionToSceneVector(a.positionKm) : null;

        const spanSeconds = (b.epochMs - a.epochMs) / 1000;
        if (spanSeconds <= 0) return j2kPositionToSceneVector(a.positionKm);
        const t = THREE.MathUtils.clamp((dateMs - a.epochMs) / (b.epochMs - a.epochMs), 0, 1);
        const t2 = t * t;
        const t3 = t2 * t;
        const h00 = 2 * t3 - 3 * t2 + 1;
        const h10 = t3 - 2 * t2 + t;
        const h01 = -2 * t3 + 3 * t2;
        const h11 = t3 - t2;
        const positionKm = [0, 1, 2].map((index) =>
            h00 * a.positionKm[index] +
            h10 * spanSeconds * a.velocityKmS[index] +
            h01 * b.positionKm[index] +
            h11 * spanSeconds * b.velocityKmS[index]
        );
        return j2kPositionToSceneVector(positionKm);
    }

    async function loadIssOemData() {
        if (state.issOemPromise) return state.issOemPromise;
        state.issOemPromise = fetch(ISS_OEM_URL, { cache: 'no-cache' })
            .then((response) => {
                if (!response.ok) throw new Error(`HTTP ${response.status}`);
                return response.text();
            })
            .then((rawText) => {
                const samples = parseIssOemText(rawText);
                if (samples.length < 2) throw new Error('ISS OEM enthaelt zu wenige State-Vektoren');
                state.issOemSamples = samples;
                state.issOemLoaded = true;
                state.issOemError = '';
                propagateSatellites(true);
                updateSatelliteOrbitPath(true);
                return samples;
            })
            .catch((error) => {
                state.issOemSamples = [];
                state.issOemLoaded = false;
                state.issOemError = error?.message || 'ISS OEM nicht verfuegbar';
                return [];
            });
        return state.issOemPromise;
    }

    function cacheDom() {
        [
            'canvas-container',
            'overview-panel',
            'stat-insight-panel',
            'stat-insight-close',
            'stat-insight-title',
            'stat-insight-subtitle',
            'stat-insight-body',
            'real-time-zone',
            'real-time-berlin',
            'search-toggle',
            'search-close',
            'search-scrim',
            'search-drawer',
            'satellite-search-input',
            'satellite-search-status',
            'satellite-search-results',
            'satellite-focus-panel',
            'sat-focus-title',
            'sat-focus-subtitle',
            'sat-focus-type',
            'sat-focus-operator',
            'sat-focus-country',
            'sat-focus-profile-source',
            'sat-focus-size',
            'sat-focus-regime',
            'sat-focus-altitude',
            'sat-focus-perigee',
            'sat-focus-apogee',
            'sat-focus-inclination',
            'sat-focus-period',
            'sat-focus-eccentricity',
            'sat-focus-latitude',
            'sat-focus-longitude',
            'sat-focus-stop',
            'sat-focus-stop-wide',
            'settings-toggle',
            'settings-close',
            'settings-scrim',
            'mobile-dock',
            'mobile-sheet-scrim',
            'mobile-nav-info',
            'mobile-nav-feed',
            'mobile-nav-controls',
            'mobile-nav-launch',
            'mobile-nav-satellite',
            'satellite-orbit-revolutions',
            'satellite-orbit-revolutions-readout',
            'satellite-size-mode-readout',
            'satellite-size-mode-default',
            'satellite-size-mode-reduced',
            'satellite-size-mode-realistic',
            'launch-ground-track-revolutions',
            'launch-ground-track-revolutions-readout',
            'language-select',
            'language-option-en',
            'language-option-de',
            'launch-stat-total',
            'launch-stat-countdown',
            'launch-stat-orgs',
            'launch-stat-pads',
            'launch-stat-success-week',
            'launch-stat-success-week-delta',
            'launch-stat-success-month',
            'launch-stat-success-month-delta',
            'launch-stat-success-year',
            'launch-stat-success-year-delta',
            'sat-stat-total',
            'sat-stat-live',
            'scene-mode-pill',
            'mission-control-panel',
            'mission-control-close',
            'focus-next-launch',
            'launch-feed-status',
            'launch-feed-items',
            'launch-feed-refresh',
            'launch-feed-upcoming',
            'launch-feed-history',
            'watch-launch-title',
            'watch-launch-subtitle',
            'watch-launch-provider',
            'watch-launch-rocket',
            'watch-launch-status',
            'watch-launch-countdown',
            'watch-launch-pad',
            'watch-launch-window',
            'watch-launch-coords',
            'watch-launch-story',
            'watch-launch-link',
            'watch-launch-intel',
            'watch-launch-stream',
            'watch-launch-stream-state',
            'watch-launch-stream-frame',
            'watch-launch-stream-link',
            'controls-panel',
            'launch-feed-panel',
            'control-focus-launch',
            'earth-view-btn',
            'observer-view-btn',
            'moon-view-btn',
            'solar-view-btn',
            'free-cam-btn',
            'jump-now-btn',
            'warp-backward-btn',
            'warp-reset-btn',
            'warp-forward-btn',
            'warp-display',
            'zoom-slider',
            'zoom-readout',
            'settings-drawer',
            'toggle-artemis-settings',
            'artemis-settings-panel',
            'toggle-artemis-replay',
            'met-clock',
            'mission-phase',
            'mission-date',
            'dist-earth',
            'dist-moon',
            'velocity',
            'mission-met-slider',
            'mission-met-readout',
            'mission-progress-fill',
            'mission-timeline-items',
            'jump-artemis-start',
            'jump-artemis-end',
            'follow-artemis'
        ].forEach((id) => {
            dom[id] = document.getElementById(id);
        });
    }

    function applyLaunchDetailPanelState() {
        const active = Boolean(state.launchDetailActive);
        const watchVisible = Boolean(state.panelVisibility.watch);
        document.body.classList.toggle('launch-detail-active', active);
        if (dom['mission-control-panel']) {
            dom['mission-control-panel'].setAttribute('aria-hidden', String(!active || !watchVisible));
        }
        if (dom['overview-panel']) {
            dom['overview-panel'].setAttribute('aria-hidden', String(active || !watchVisible));
        }
        applyMobilePanelState();
    }

    function applyPanelVisibility() {
        document.body.classList.toggle('hide-news', !state.panelVisibility.news);
        document.body.classList.toggle('hide-watch', !state.panelVisibility.watch);
        document.body.classList.toggle('hide-controls', !state.panelVisibility.controls);
        applyLaunchDetailPanelState();

        ['news', 'watch', 'controls'].forEach((key) => {
            const button = dom['toggle-' + key];
            if (!button) return;
            button.setAttribute('aria-pressed', String(Boolean(state.panelVisibility[key])));
        });
        applyMobilePanelState();
    }

    function closeLaunchDetailPanel() {
        state.launchDetailActive = false;
        state.selectedLaunchId = null;
        applyLaunchDetailPanelState();
        refreshSelectedLaunchUi();
    }

    function satelliteOrbitRevolutionsLabel(value) {
        return value === 1 ? t('orbit.rev.one') : t('orbit.rev.many', { count: value });
    }

    function syncSatelliteOrbitSettingsUi() {
        const value = clampSatelliteOrbitRevolutions(state.panelVisibility.orbitRevolutions);
        state.panelVisibility.orbitRevolutions = value;
        if (dom['satellite-orbit-revolutions']) {
            dom['satellite-orbit-revolutions'].value = String(value);
        }
        if (dom['satellite-orbit-revolutions-readout']) {
            dom['satellite-orbit-revolutions-readout'].textContent = satelliteOrbitRevolutionsLabel(value);
        }
    }

    function onSatelliteOrbitRevolutionsInput() {
        if (!dom['satellite-orbit-revolutions']) return;
        state.panelVisibility.orbitRevolutions = clampSatelliteOrbitRevolutions(
            dom['satellite-orbit-revolutions'].valueAsNumber
        );
        syncSatelliteOrbitSettingsUi();
        writeUiState();
        updateSatelliteOrbitPath(true);
    }

    function syncSatelliteSizeSettingsUi() {
        const mode = normalizeSatelliteSizeMode(state.panelVisibility.satelliteSizeMode);
        state.panelVisibility.satelliteSizeMode = mode;
        if (dom['satellite-size-mode-readout']) {
            dom['satellite-size-mode-readout'].textContent = t(`sat.sizeMode.${mode}`);
        }
        SATELLITE_SIZE_MODES.forEach((option) => {
            const button = dom[`satellite-size-mode-${option}`];
            if (!button) return;
            const active = option === mode;
            button.setAttribute('aria-pressed', String(active));
            button.classList.toggle('active', active);
        });
    }

    function setSatelliteSizeMode(mode) {
        const next = normalizeSatelliteSizeMode(mode);
        if (state.panelVisibility.satelliteSizeMode === next) return;
        state.panelVisibility.satelliteSizeMode = next;
        syncSatelliteSizeSettingsUi();
        writeUiState();
        const distance = state.camera && state.controls
            ? state.camera.position.distanceTo(state.controls.target)
            : SATELLITE_POINT_REALISTIC_FAR_DISTANCE;
        updateSatellitePointSizing(distance);
        updateSatelliteHighlight();
    }

    function syncLaunchGroundTrackSettingsUi() {
        const value = clampLaunchGroundTrackRevolutions(state.panelVisibility.launchGroundTrackRevolutions);
        state.panelVisibility.launchGroundTrackRevolutions = value;
        if (dom['launch-ground-track-revolutions']) {
            dom['launch-ground-track-revolutions'].value = String(value);
        }
        if (dom['launch-ground-track-revolutions-readout']) {
            dom['launch-ground-track-revolutions-readout'].textContent = satelliteOrbitRevolutionsLabel(value);
        }
    }

    function onLaunchGroundTrackRevolutionsInput() {
        if (!dom['launch-ground-track-revolutions']) return;
        state.panelVisibility.launchGroundTrackRevolutions = clampLaunchGroundTrackRevolutions(
            dom['launch-ground-track-revolutions'].valueAsNumber
        );
        syncLaunchGroundTrackSettingsUi();
        writeUiState();
        updateSelectedLaunchTrajectory(state.selectedLaunchId || state.launchDetailActive ? getSelectedLaunch() : null);
    }

    function translatedStaticText(originalText) {
        const trimmed = String(originalText || '').trim();
        if (!trimmed) return originalText;
        const translated = STATIC_TRANSLATIONS[currentLanguage()]?.[trimmed];
        if (!translated) return originalText;
        const leading = originalText.match(/^\s*/)?.[0] || '';
        const trailing = originalText.match(/\s*$/)?.[0] || '';
        return `${leading}${translated}${trailing}`;
    }

    function applyStaticTranslations() {
        document.documentElement.lang = currentLanguage();
        const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, {
            acceptNode(node) {
                const parent = node.parentElement;
                if (!parent) return NodeFilter.FILTER_REJECT;
                if (parent.closest('script, style, #launch-feed-items, #satellite-search-results, #stat-insight-body, #mission-timeline-items')) {
                    return NodeFilter.FILTER_REJECT;
                }
                return node.nodeValue.trim() ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_REJECT;
            }
        });

        let node = walker.nextNode();
        while (node) {
            if (!STATIC_TEXT_NODE_ORIGINALS.has(node)) {
                STATIC_TEXT_NODE_ORIGINALS.set(node, node.nodeValue);
            }
            node.nodeValue = translatedStaticText(STATIC_TEXT_NODE_ORIGINALS.get(node));
            node = walker.nextNode();
        }

        document.querySelectorAll('[aria-label], input[placeholder], iframe[title]').forEach((element) => {
            ['aria-label', 'placeholder', 'title'].forEach((attribute) => {
                if (!element.hasAttribute(attribute)) return;
                let originals = STATIC_ATTRIBUTE_ORIGINALS.get(element);
                if (!originals) {
                    originals = {};
                    STATIC_ATTRIBUTE_ORIGINALS.set(element, originals);
                }
                if (!Object.prototype.hasOwnProperty.call(originals, attribute)) {
                    originals[attribute] = element.getAttribute(attribute);
                }
                element.setAttribute(attribute, translatedStaticText(originals[attribute]));
            });
        });
    }

    function syncLanguageSettingsUi() {
        const language = currentLanguage();
        if (dom['language-select']) {
            dom['language-select'].textContent = language === 'de' ? t('language.german') : t('language.english');
        }
        ['en', 'de'].forEach((option) => {
            const button = dom[`language-option-${option}`];
            if (!button) return;
            button.setAttribute('aria-pressed', String(language === option));
            button.classList.toggle('active', language === option);
        });
    }

    function refreshLanguageDependentUi() {
        applyStaticTranslations();
        syncLanguageSettingsUi();
        syncSatelliteOrbitSettingsUi();
        syncSatelliteSizeSettingsUi();
        syncLaunchGroundTrackSettingsUi();
        const artemisOpen = dom['toggle-artemis-settings']?.getAttribute('aria-expanded') === 'true';
        setArtemisSettingsOpen(artemisOpen);
        refreshSceneModePill();
        updateLaunchFeedModeUi();
        renderActiveLaunchFeed();
        updateOverviewStats();
        updateLaunchSuccessStatsUi();
        refreshSelectedLaunchUi();
        refreshSatelliteFocusVisuals();
        renderSatelliteSearchResults();
        renderStatsPanel();
        buildMissionTimeline();
        const met = THREE.MathUtils.clamp(ARTEMIS.getMET(sceneTimeMs()), 0, state.totalMissionHours);
        updateArtemisPanel(met);
    }

    function setLanguage(language) {
        const next = normalizeLanguage(language);
        if (state.panelVisibility.language === next) return;
        state.panelVisibility.language = next;
        writeUiState();
        refreshLanguageDependentUi();
    }

    function openSettings() {
        closeSearch();
        document.body.classList.add('settings-open');
        if (dom['settings-drawer']) dom['settings-drawer'].setAttribute('aria-hidden', 'false');
    }

    function closeSettings() {
        document.body.classList.remove('settings-open');
        if (dom['settings-drawer']) dom['settings-drawer'].setAttribute('aria-hidden', 'true');
    }

    function openSearch() {
        closeSettings();
        document.body.classList.add('search-open');
        if (dom['search-drawer']) dom['search-drawer'].setAttribute('aria-hidden', 'false');
        dom['satellite-search-input']?.focus();
        renderSatelliteSearchResults();
    }

    function closeSearch() {
        document.body.classList.remove('search-open');
        if (dom['search-drawer']) dom['search-drawer'].setAttribute('aria-hidden', 'true');
    }

    function setArtemisSettingsOpen(open) {
        dom['artemis-settings-panel']?.classList.toggle('is-collapsed', !open);
        dom['toggle-artemis-settings']?.setAttribute('aria-expanded', String(open));
        if (dom['toggle-artemis-settings']) {
            dom['toggle-artemis-settings'].textContent = open ? t('settings.artemis.close') : t('settings.artemis.open');
        }
    }

    function hasMobileLaunchContext() {
        return Boolean(state.launchDetailActive);
    }

    function hasMobileSatelliteContext() {
        return Boolean(state.followSatelliteId && state.satelliteIndex.has(state.followSatelliteId));
    }

    function hasMobileStatsContext() {
        return Boolean(state.statsPanelOpen);
    }

    function hasMobilePanelContext(panelKey) {
        if (panelKey === 'launch') return hasMobileLaunchContext();
        if (panelKey === 'satellite') return hasMobileSatelliteContext();
        if (panelKey === 'stats') return hasMobileStatsContext();
        return ['info', 'feed', 'controls'].includes(panelKey);
    }

    function mobilePanelTargets(panelKey) {
        if (panelKey === 'info') return ['overview-panel'];
        if (panelKey === 'feed') return ['launch-feed-panel'];
        if (panelKey === 'controls') return ['controls-panel'];
        if (panelKey === 'launch' && hasMobileLaunchContext()) return ['mission-control-panel'];
        if (panelKey === 'satellite' && hasMobileSatelliteContext()) return ['satellite-focus-panel'];
        if (panelKey === 'stats' && hasMobileStatsContext()) return ['stat-insight-panel'];
        return [];
    }

    function mobilePanelKeyForSheetId(id) {
        return {
            'overview-panel': 'info',
            'launch-feed-panel': 'feed',
            'controls-panel': 'controls',
            'mission-control-panel': 'launch',
            'satellite-focus-panel': 'satellite',
            'stat-insight-panel': 'stats'
        }[id] || '';
    }

    function mobileSheetLimits() {
        const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 640;
        const dockHeight = dom['mobile-dock']?.getBoundingClientRect().height || 78;
        const topClearance = 78;
        const max = Math.max(240, viewportHeight - dockHeight - topClearance - 24);
        const min = Math.min(max, 34);
        const defaultHeight = THREE.MathUtils.clamp(Math.round(viewportHeight * 0.54), min, max);
        return { min, max, defaultHeight };
    }

    function ensureMobileSheetHeight(panelKey) {
        if (!state.mobileSheetHeights[panelKey]) {
            state.mobileSheetHeights[panelKey] = mobileSheetLimits().defaultHeight;
        }
        return state.mobileSheetHeights[panelKey];
    }

    function applyMobileSheetHeight(sheetId) {
        const panelKey = mobilePanelKeyForSheetId(sheetId);
        if (!panelKey || !dom[sheetId]) return;
        const height = ensureMobileSheetHeight(panelKey);
        dom[sheetId].style.setProperty('--mobile-sheet-height', `${height}px`);
    }

    function ensureMobileSheetHandles() {
        [
            'overview-panel',
            'launch-feed-panel',
            'controls-panel',
            'mission-control-panel',
            'satellite-focus-panel',
            'stat-insight-panel'
        ].forEach((id) => {
            const panel = dom[id];
            if (!panel || panel.querySelector('.mobile-sheet-resize-handle')) return;
            const handle = document.createElement('div');
            handle.className = 'mobile-sheet-resize-handle';
            handle.setAttribute('aria-hidden', 'true');
            panel.prepend(handle);
        });
    }

    function isMobileSheetDragTarget(event, panel) {
        if (!isMobileViewport()) return false;
        if (event.button !== undefined && event.button !== 0) return false;
        if (event.target.closest('button, a, input, select, textarea, iframe')) return false;
        return Boolean(event.target.closest('.mobile-sheet-resize-handle, .panel-head'));
    }

    function onMobileSheetPointerDown(event) {
        const panel = event.currentTarget;
        if (!isMobileSheetDragTarget(event, panel)) return;
        const panelKey = mobilePanelKeyForSheetId(panel.id);
        if (!panelKey || state.mobileActivePanel !== panelKey) return;
        const rect = panel.getBoundingClientRect();
        state.mobileSheetDrag = {
            panel,
            panelKey,
            pointerId: event.pointerId,
            startY: event.clientY,
            startHeight: rect.height,
            limits: mobileSheetLimits()
        };
        panel.classList.add('mobile-sheet-dragging');
        panel.setPointerCapture?.(event.pointerId);
        window.addEventListener('pointermove', onMobileSheetPointerMove, { capture: true, passive: false });
        window.addEventListener('pointerup', endMobileSheetDrag, { capture: true });
        window.addEventListener('pointercancel', endMobileSheetDrag, { capture: true });
        event.preventDefault();
    }

    function onMobileSheetPointerMove(event) {
        const drag = state.mobileSheetDrag;
        if (!drag || drag.pointerId !== event.pointerId) return;
        const nextHeight = THREE.MathUtils.clamp(
            drag.startHeight + drag.startY - event.clientY,
            drag.limits.min,
            drag.limits.max
        );
        state.mobileSheetHeights[drag.panelKey] = nextHeight;
        drag.panel.style.setProperty('--mobile-sheet-height', `${nextHeight}px`);
        event.preventDefault();
    }

    function endMobileSheetDrag(event) {
        const drag = state.mobileSheetDrag;
        if (!drag || (event?.pointerId !== undefined && drag.pointerId !== event.pointerId)) return;
        drag.panel.classList.remove('mobile-sheet-dragging');
        drag.panel.releasePointerCapture?.(drag.pointerId);
        window.removeEventListener('pointermove', onMobileSheetPointerMove, true);
        window.removeEventListener('pointerup', endMobileSheetDrag, true);
        window.removeEventListener('pointercancel', endMobileSheetDrag, true);
        state.mobileSheetDrag = null;
    }

    function closeMobileSheet() {
        if (!state.mobileActivePanel) return;
        state.mobileActivePanel = null;
        applyMobilePanelState();
    }

    function openMobilePanel(panelKey) {
        if (!isMobileViewport()) return;
        if (!hasMobilePanelContext(panelKey)) return;
        state.mobileActivePanel = panelKey;
        applyMobilePanelState();
    }

    function toggleMobilePanel(panelKey) {
        if (state.mobileActivePanel === panelKey) {
            closeMobileSheet();
        } else {
            openMobilePanel(panelKey);
        }
    }

    function applyMobilePanelState() {
        const mobile = isMobileViewport();
        const hasLaunchContext = hasMobileLaunchContext();
        const hasSatelliteContext = hasMobileSatelliteContext();
        const contextCount = Number(hasLaunchContext) + Number(hasSatelliteContext);
        if (!mobile || !hasMobilePanelContext(state.mobileActivePanel)) {
            state.mobileActivePanel = null;
        }

        document.body.classList.toggle('mobile-ui', mobile);
        document.body.classList.toggle('mobile-has-context-sheet', mobile && contextCount > 0);
        document.body.classList.toggle('mobile-has-dual-context', mobile && contextCount > 1);
        document.body.classList.toggle('mobile-focus-launch', mobile && hasLaunchContext);
        document.body.classList.toggle('mobile-focus-satellite', mobile && hasSatelliteContext);
        document.body.classList.toggle('mobile-sheet-open', mobile && Boolean(state.mobileActivePanel));

        const activeTargetIds = mobile && state.mobileActivePanel
            ? mobilePanelTargets(state.mobileActivePanel)
            : [];

        [
            'overview-panel',
            'launch-feed-panel',
            'controls-panel',
            'mission-control-panel',
            'satellite-focus-panel',
            'stat-insight-panel'
        ].forEach((id) => {
            dom[id]?.classList.remove('mobile-sheet-active');
            if (mobile) dom[id]?.setAttribute('aria-hidden', String(!activeTargetIds.includes(id)));
        });

        if (mobile && state.mobileActivePanel) {
            activeTargetIds.forEach((id) => {
                applyMobileSheetHeight(id);
                dom[id]?.classList.add('mobile-sheet-active');
            });
        }

        ['info', 'feed', 'controls', 'launch', 'satellite'].forEach((key) => {
            const button = dom[`mobile-nav-${key}`];
            if (!button) return;
            const active = mobile && state.mobileActivePanel === key;
            button.setAttribute('aria-pressed', String(active));
        });

        if (dom['mobile-nav-launch']) {
            dom['mobile-nav-launch'].hidden = !(mobile && hasLaunchContext);
            dom['mobile-nav-launch'].setAttribute('aria-hidden', String(!(mobile && hasLaunchContext)));
        }
        if (dom['mobile-nav-satellite']) {
            dom['mobile-nav-satellite'].hidden = !(mobile && hasSatelliteContext);
            dom['mobile-nav-satellite'].setAttribute('aria-hidden', String(!(mobile && hasSatelliteContext)));
        }
    }

    function bindUi() {
        dom['search-toggle']?.addEventListener('click', openSearch);
        dom['search-close']?.addEventListener('click', closeSearch);
        dom['search-scrim']?.addEventListener('click', closeSearch);
        dom['settings-toggle']?.addEventListener('click', openSettings);
        dom['settings-close']?.addEventListener('click', closeSettings);
        dom['settings-scrim']?.addEventListener('click', closeSettings);
        dom['stat-insight-close']?.addEventListener('click', closeStatsPanel);
        ensureMobileSheetHandles();

        document.querySelectorAll('[data-stat-panel]').forEach((button) => {
            button.addEventListener('click', () => {
                const mode = button.getAttribute('data-stat-panel');
                if (mode) openStatsPanel(mode);
            });
        });

        document.querySelectorAll('[data-mobile-panel]').forEach((button) => {
            button.addEventListener('click', () => {
                const key = button.getAttribute('data-mobile-panel');
                if (key) toggleMobilePanel(key);
            });
        });

        [
            'overview-panel',
            'launch-feed-panel',
            'controls-panel',
            'mission-control-panel',
            'satellite-focus-panel'
        ].forEach((id) => {
            dom[id]?.addEventListener('pointerdown', onMobileSheetPointerDown);
        });

        document.querySelectorAll('[data-ui-toggle]').forEach((button) => {
            button.addEventListener('click', () => {
                const key = button.getAttribute('data-ui-toggle');
                if (!key) return;
                if (isMobileViewport() && button.closest('#overview-panel, #launch-feed-panel, #controls-panel')) {
                    closeMobileSheet();
                    return;
                }
                state.panelVisibility[key] = !state.panelVisibility[key];
                applyPanelVisibility();
                writeUiState();
            });
        });

        dom['focus-next-launch']?.addEventListener('click', () => focusSelectedLaunch());
        dom['mission-control-close']?.addEventListener('click', closeLaunchDetailPanel);
        dom['control-focus-launch']?.addEventListener('click', () => focusSelectedLaunch());
        dom['sat-focus-stop']?.addEventListener('click', stopSatelliteFollow);
        dom['sat-focus-stop-wide']?.addEventListener('click', stopSatelliteFollow);
        dom['earth-view-btn']?.addEventListener('click', resetView);
        dom['observer-view-btn']?.addEventListener('click', toggleFollowObserver);
        dom['moon-view-btn']?.addEventListener('click', toggleMoonView);
        dom['solar-view-btn']?.addEventListener('click', solarSystemView);
        dom['free-cam-btn']?.addEventListener('click', toggleFreeCamera);
        dom['jump-now-btn']?.addEventListener('click', jumpToNow);
        dom['warp-backward-btn']?.addEventListener('click', cycleWarpBackward);
        dom['warp-reset-btn']?.addEventListener('click', warpToOne);
        dom['warp-forward-btn']?.addEventListener('click', cycleWarpForward);
        dom['toggle-artemis-settings']?.addEventListener('click', () => {
            const open = dom['toggle-artemis-settings']?.getAttribute('aria-expanded') === 'true';
            setArtemisSettingsOpen(!open);
        });
        dom['toggle-artemis-replay']?.addEventListener('click', () => {
            setArtemisReplayEnabled(!state.artemisReplayEnabled);
        });
        dom['jump-artemis-start']?.addEventListener('click', () => jumpToMissionMet(0));
        dom['jump-artemis-end']?.addEventListener('click', () => jumpToMissionMet(state.totalMissionHours));
        dom['follow-artemis']?.addEventListener('click', toggleFollowOrion);

        dom['satellite-orbit-revolutions']?.addEventListener('input', onSatelliteOrbitRevolutionsInput);
        dom['launch-ground-track-revolutions']?.addEventListener('input', onLaunchGroundTrackRevolutionsInput);
        document.querySelectorAll('[data-satellite-size-mode]').forEach((button) => {
            button.addEventListener('click', () => setSatelliteSizeMode(button.getAttribute('data-satellite-size-mode')));
        });
        document.querySelectorAll('[data-language-option]').forEach((button) => {
            button.addEventListener('click', () => setLanguage(button.getAttribute('data-language-option')));
        });

        if (dom['zoom-slider']) {
            dom['zoom-slider'].addEventListener('pointerdown', () => { state.zoomSliderDragging = true; });
            dom['zoom-slider'].addEventListener('pointerup', () => { state.zoomSliderDragging = false; });
            dom['zoom-slider'].addEventListener('pointercancel', () => { state.zoomSliderDragging = false; });
            dom['zoom-slider'].addEventListener('input', onZoomSliderInput);
        }

        if (dom['mission-met-slider']) {
            dom['mission-met-slider'].addEventListener('pointerdown', () => { state.missionSliderDragging = true; });
            dom['mission-met-slider'].addEventListener('pointerup', () => { state.missionSliderDragging = false; });
            dom['mission-met-slider'].addEventListener('pointercancel', () => { state.missionSliderDragging = false; });
            dom['mission-met-slider'].addEventListener('input', onMissionSliderInput);
        }

        dom['satellite-search-input']?.addEventListener('input', (event) => {
            state.satelliteSearchQuery = event.target.value || '';
            renderSatelliteSearchResults();
        });

        document.querySelectorAll('[data-sat-filter]').forEach((button) => {
            button.addEventListener('click', () => {
                const regime = button.getAttribute('data-sat-filter');
                if (!regime || !Object.prototype.hasOwnProperty.call(state.satelliteFilters, regime)) return;
                state.satelliteFilters[regime] = !state.satelliteFilters[regime];
                button.setAttribute('aria-pressed', String(state.satelliteFilters[regime]));
                const followed = state.followSatelliteId ? state.satelliteIndex.get(state.followSatelliteId) : null;
                if (followed && !orbitRegimeActive(followed.regime)) {
                    clearFocusModes();
                }
                propagateSatellites(true);
                renderSatelliteSearchResults();
            });
        });

        window.addEventListener('resize', onResize);
        window.addEventListener('keydown', onKeyDown);
        window.addEventListener('keyup', onKeyUp);
    }

    function init() {
        cacheDom();
        applyStaticTranslations();
        syncLanguageSettingsUi();
        applyPanelVisibility();
        syncSatelliteOrbitSettingsUi();
        syncSatelliteSizeSettingsUi();
        syncLaunchGroundTrackSettingsUi();
        initScene();
        bindUi();
        buildMissionTimeline();
        initLaunchFeed();
        initObserverLocation();
        initSatelliteTracking();
        ARTEMIS.onDataLoaded(() => {
            state.fullTrajectory = ARTEMIS.getFullTrajectoryPoints(0.5);
            const wp = ARTEMIS.WAYPOINTS;
            if (wp.length > 0) state.totalMissionHours = wp[wp.length - 1].t;
            syncMissionSlider();
            updateArtemisPanel(Math.max(0, Math.min(ARTEMIS.getMET(sceneTimeMs()), state.totalMissionHours)));
        });
        refreshWarpButtons();
        refreshSceneModePill();
        animate();
    }

    function initScene() {
        state.renderer = new THREE.WebGLRenderer({
            antialias: true,
            alpha: false,
            logarithmicDepthBuffer: true
        });
        state.renderer.setSize(window.innerWidth, window.innerHeight);
        state.renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
        state.renderer.toneMapping = THREE.ACESFilmicToneMapping;
        state.renderer.toneMappingExposure = 1.18;
        dom['canvas-container'].appendChild(state.renderer.domElement);

        state.scene = new THREE.Scene();
        state.scene.background = new THREE.Color(0x030610);

        state.camera = new THREE.PerspectiveCamera(50, window.innerWidth / window.innerHeight, 0.02, 20000000);
        state.camera.position.set(0, 160, 300);

        state.controls = new OrbitControls(state.camera, state.renderer.domElement);
        state.controls.enableDamping = true;
        state.controls.dampingFactor = 0.08;
        state.controls.minDistance = 2.6;
        state.controls.maxDistance = ZOOM_DIST_MAX;
        state.controls.target.set(0, 0, 0);
        state.controls.addEventListener('start', onControlStart);
        state.controls.addEventListener('end', onControlEnd);

        state.scene.add(new THREE.AmbientLight(0x334055, 1.6));

        const T0 = ARTEMIS.getJulianCenturies(sceneTimeMs());
        const sp0 = ARTEMIS.getSunPosition(T0);
        state.sunScenePos.set(sp0.x, sp0.y, sp0.z);
        const sunDir0 = state.sunScenePos.clone().normalize();

        state.sunDirLight = new THREE.DirectionalLight(0xffffff, 3);
        state.sunDirLight.position.copy(sunDir0.clone().multiplyScalar(500));
        state.scene.add(state.sunDirLight);

        state.fillDirLight = new THREE.DirectionalLight(0x4168ad, 0.5);
        state.fillDirLight.position.copy(sunDir0.clone().multiplyScalar(-260));
        state.scene.add(state.fillDirLight);

        createStarField();
        createEarth();
        createMoon();
        createSun();
        createPlanets();
        createLabels();
        createNorthPoleAxis();
        createMoonOrbit();
        createArtemisObjects();
        createTrajectoryLines();
        buildPickableList();

        state.renderer.domElement.addEventListener('click', onSceneClick);
        state.renderer.domElement.addEventListener('pointerdown', onScenePointerDown);
        state.renderer.domElement.addEventListener('pointermove', onScenePointerMove);
        state.renderer.domElement.addEventListener('pointerup', onScenePointerUp);
        state.renderer.domElement.addEventListener('pointercancel', onScenePointerCancel);
        updateEarthRotation(earthReferenceTimeMs());
        resetView();
        updateArtemisVisibility();
    }

    function createStarField() {
        addStarLayer(12000, 9000000, 17000000, 1800);
        addStarLayer(4200, 10000000, 18000000, 5200);
    }

    function addStarLayer(count, rMin, rMax, size) {
        const geometry = new THREE.BufferGeometry();
        const positions = new Float32Array(count * 3);
        for (let i = 0; i < count; i++) {
            const r = rMin + Math.random() * (rMax - rMin);
            const theta = Math.random() * Math.PI * 2;
            const phi = Math.acos(2 * Math.random() - 1);
            positions[i * 3] = r * Math.sin(phi) * Math.cos(theta);
            positions[i * 3 + 1] = r * Math.sin(phi) * Math.sin(theta);
            positions[i * 3 + 2] = r * Math.cos(phi);
        }
        geometry.setAttribute('position', new THREE.BufferAttribute(positions, 3));
        const material = new THREE.PointsMaterial({
            color: 0xffffff,
            size,
            sizeAttenuation: true,
            transparent: true,
            opacity: 0.78
        });
        state.scene.add(new THREE.Points(geometry, material));
    }

    function loadTextureCandidates(loader, urls, onLoad, onError = () => {}) {
        const candidates = Array.isArray(urls) ? urls.filter(Boolean) : [urls].filter(Boolean);
        let index = 0;
        const tryNext = () => {
            if (index >= candidates.length) {
                onError();
                return;
            }
            loader.load(candidates[index], onLoad, undefined, () => {
                index += 1;
                tryNext();
            });
        };
        tryNext();
    }

    function prepareColorTexture(texture) {
        texture.colorSpace = THREE.SRGBColorSpace;
        if (state.renderer?.capabilities) {
            texture.anisotropy = Math.min(8, state.renderer.capabilities.getMaxAnisotropy());
        }
        return texture;
    }

    function createFallbackEarthTexture() {
        const canvas = document.createElement('canvas');
        canvas.width = 1024;
        canvas.height = 512;
        const ctx = canvas.getContext('2d');
        const width = canvas.width;
        const height = canvas.height;

        const ocean = ctx.createLinearGradient(0, 0, 0, height);
        ocean.addColorStop(0, '#123f7d');
        ocean.addColorStop(0.46, '#1e6fa8');
        ocean.addColorStop(0.54, '#227cb6');
        ocean.addColorStop(1, '#10376d');
        ctx.fillStyle = ocean;
        ctx.fillRect(0, 0, width, height);

        for (let y = 0; y < height; y += 1) {
            const lat = 90 - (y / height) * 180;
            const polar = Math.pow(Math.abs(lat) / 90, 2.2);
            ctx.fillStyle = `rgba(255,255,255,${0.03 + polar * 0.11})`;
            ctx.fillRect(0, y, width, 1);
        }

        const project = (lon, lat) => ({
            x: ((lon + 180) / 360) * width,
            y: ((90 - lat) / 180) * height
        });
        const drawLand = (points, fill, stroke = 'rgba(227, 222, 185, 0.24)') => {
            ctx.beginPath();
            points.forEach(([lon, lat], index) => {
                const point = project(lon, lat);
                if (index === 0) {
                    ctx.moveTo(point.x, point.y);
                } else {
                    ctx.lineTo(point.x, point.y);
                }
            });
            ctx.closePath();
            ctx.fillStyle = fill;
            ctx.fill();
            ctx.strokeStyle = stroke;
            ctx.lineWidth = 1.2;
            ctx.stroke();
        };

        drawLand([[-168, 72], [-130, 72], [-98, 58], [-82, 46], [-64, 32], [-82, 18], [-103, 20], [-118, 32], [-125, 48], [-150, 58]], '#607b45');
        drawLand([[-82, 13], [-70, 11], [-50, -4], [-40, -20], [-55, -54], [-72, -48], [-80, -20]], '#5c7740');
        drawLand([[-18, 36], [10, 37], [38, 31], [51, 12], [43, -13], [30, -34], [12, -35], [-5, -14], [-16, 9]], '#8a7c45');
        drawLand([[-10, 72], [38, 70], [95, 63], [142, 54], [164, 38], [122, 22], [96, 8], [72, 20], [46, 30], [16, 42], [-8, 50]], '#6e8146');
        drawLand([[39, 31], [58, 28], [77, 18], [89, 8], [76, 5], [52, 13]], '#887640');
        drawLand([[112, -10], [154, -18], [149, -39], [116, -44], [106, -28]], '#8b7542');
        drawLand([[-52, 82], [-28, 76], [-20, 64], [-44, 58], [-62, 66]], '#d8dfdd');
        drawLand([[-180, -64], [-90, -70], [0, -67], [90, -70], [180, -64], [180, -90], [-180, -90]], '#d9e0dd', 'rgba(255,255,255,0.35)');

        ctx.globalAlpha = 0.34;
        ctx.strokeStyle = '#ffffff';
        ctx.lineWidth = 1.3;
        for (let i = 0; i < 48; i += 1) {
            const y = 40 + Math.random() * (height - 80);
            const x = Math.random() * width;
            ctx.beginPath();
            ctx.moveTo(x, y);
            ctx.bezierCurveTo(x + 28, y - 8, x + 62, y + 10, x + 100, y);
            ctx.stroke();
        }
        ctx.globalAlpha = 1;

        const texture = new THREE.CanvasTexture(canvas);
        return prepareColorTexture(texture);
    }

    function createFallbackEarthNightTexture() {
        const canvas = document.createElement('canvas');
        canvas.width = 1024;
        canvas.height = 512;
        const ctx = canvas.getContext('2d');
        const width = canvas.width;
        const height = canvas.height;

        ctx.fillStyle = '#02050a';
        ctx.fillRect(0, 0, width, height);

        const project = (lon, lat) => ({
            x: ((lon + 180) / 360) * width,
            y: ((90 - lat) / 180) * height
        });
        const drawLand = (points) => {
            ctx.beginPath();
            points.forEach(([lon, lat], index) => {
                const point = project(lon, lat);
                if (index === 0) ctx.moveTo(point.x, point.y);
                else ctx.lineTo(point.x, point.y);
            });
            ctx.closePath();
            ctx.fillStyle = 'rgba(48, 88, 126, 0.28)';
            ctx.fill();
            ctx.strokeStyle = 'rgba(116, 173, 218, 0.22)';
            ctx.lineWidth = 1.2;
            ctx.stroke();
        };

        drawLand([[-168, 72], [-130, 72], [-98, 58], [-82, 46], [-64, 32], [-82, 18], [-103, 20], [-118, 32], [-125, 48], [-150, 58]]);
        drawLand([[-82, 13], [-70, 11], [-50, -4], [-40, -20], [-55, -54], [-72, -48], [-80, -20]]);
        drawLand([[-18, 36], [10, 37], [38, 31], [51, 12], [43, -13], [30, -34], [12, -35], [-5, -14], [-16, 9]]);
        drawLand([[-10, 72], [38, 70], [95, 63], [142, 54], [164, 38], [122, 22], [96, 8], [72, 20], [46, 30], [16, 42], [-8, 50]]);
        drawLand([[39, 31], [58, 28], [77, 18], [89, 8], [76, 5], [52, 13]]);
        drawLand([[112, -10], [154, -18], [149, -39], [116, -44], [106, -28]]);
        drawLand([[-52, 82], [-28, 76], [-20, 64], [-44, 58], [-62, 66]]);

        const seedNoise = (value) => {
            const s = Math.sin(value * 12.9898) * 43758.5453;
            return s - Math.floor(s);
        };
        const drawLight = (lon, lat, size, alpha) => {
            const point = project(lon, lat);
            const radius = Math.max(1.6, size);
            const glow = ctx.createRadialGradient(point.x, point.y, 0, point.x, point.y, radius * 3.2);
            glow.addColorStop(0, `rgba(255, 230, 158, ${alpha})`);
            glow.addColorStop(0.28, `rgba(255, 174, 78, ${alpha * 0.54})`);
            glow.addColorStop(1, 'rgba(255, 166, 64, 0)');
            ctx.fillStyle = glow;
            ctx.beginPath();
            ctx.arc(point.x, point.y, radius * 3.2, 0, Math.PI * 2);
            ctx.fill();
        };
        const drawCluster = (lon, lat, count, spreadLon, spreadLat, size = 1.6) => {
            for (let i = 0; i < count; i += 1) {
                const lonOffset = (seedNoise(lon * 13 + lat * 7 + i) - 0.5) * spreadLon;
                const latOffset = (seedNoise(lon * 5 - lat * 11 + i * 3) - 0.5) * spreadLat;
                const alpha = 0.24 + seedNoise(lon + lat + i * 17) * 0.42;
                drawLight(lon + lonOffset, lat + latOffset, size * (0.7 + seedNoise(i + lon) * 0.8), alpha);
            }
        };

        [
            [-74, 40, 34, 18, 11, 1.7],
            [-95, 37, 30, 24, 12, 1.45],
            [-122, 37, 18, 13, 10, 1.45],
            [-46, -23, 22, 16, 10, 1.4],
            [-58, -35, 14, 14, 8, 1.25],
            [-3, 52, 38, 24, 11, 1.55],
            [10, 49, 36, 24, 10, 1.5],
            [30, 31, 18, 14, 9, 1.35],
            [78, 22, 38, 28, 16, 1.45],
            [116, 35, 42, 30, 15, 1.45],
            [139, 36, 28, 14, 9, 1.55],
            [127, 37, 18, 9, 6, 1.4],
            [106, -6, 24, 17, 9, 1.35],
            [151, -33, 14, 10, 7, 1.25],
            [28, -26, 18, 14, 9, 1.35]
        ].forEach(([lon, lat, count, spreadLon, spreadLat, size]) => {
            drawCluster(lon, lat, count, spreadLon, spreadLat, size);
        });

        const texture = new THREE.CanvasTexture(canvas);
        return prepareColorTexture(texture);
    }

    function applyEarthNightShader(material) {
        const uniforms = {
            earthNightSunDirection: { value: new THREE.Vector3(1, 0, 0) },
            earthNightCityIntensity: { value: 0.62 },
            earthNightSurfaceIntensity: { value: 0.08 }
        };
        state.earthNightUniforms = uniforms;

        material.onBeforeCompile = (shader) => {
            Object.assign(shader.uniforms, uniforms);
            shader.vertexShader = shader.vertexShader
                .replace(
                    '#include <common>',
                    '#include <common>\nvarying vec3 vEarthNightWorldNormal;'
                )
                .replace(
                    '#include <beginnormal_vertex>',
                    '#include <beginnormal_vertex>\nvEarthNightWorldNormal = normalize(mat3(modelMatrix) * objectNormal);'
                );
            shader.fragmentShader = shader.fragmentShader
                .replace(
                    '#include <common>',
                    '#include <common>\nuniform vec3 earthNightSunDirection;\nuniform float earthNightCityIntensity;\nuniform float earthNightSurfaceIntensity;\nvarying vec3 vEarthNightWorldNormal;'
                )
                .replace(
                    '#include <emissivemap_fragment>',
                    [
                        '#ifdef USE_EMISSIVEMAP',
                        '    vec4 emissiveColor = texture2D( emissiveMap, vEmissiveMapUv );',
                        '    float sunFacing = dot(normalize(vEarthNightWorldNormal), normalize(earthNightSunDirection));',
                        '    float nightMask = smoothstep(0.14, -0.22, sunFacing);',
                        '    vec3 dimSurface = diffuseColor.rgb * earthNightSurfaceIntensity * nightMask;',
                        '    vec3 cityLights = emissiveColor.rgb * earthNightCityIntensity * nightMask;',
                        '    totalEmissiveRadiance += dimSurface + cityLights;',
                        '#endif'
                    ].join('\n')
                );
        };
        material.customProgramCacheKey = () => 'earth-night-side-lights-v1';
    }

    function createEarth() {
        state.earthGroup = new THREE.Group();
        state.earthGroup.rotation.z = OBLIQUITY_RAD;
        state.scene.add(state.earthGroup);

        const loader = new THREE.TextureLoader();
        loader.crossOrigin = 'anonymous';

        const geometry = new THREE.SphereGeometry(ARTEMIS.EARTH_RADIUS, 128, 128);
        const material = new THREE.MeshPhongMaterial({
            map: createFallbackEarthTexture(),
            color: 0xffffff,
            emissiveMap: createFallbackEarthNightTexture(),
            emissive: 0x0b1422,
            emissiveIntensity: 0.08,
            specular: new THREE.Color(0x274969),
            shininess: 18
        });
        applyEarthNightShader(material);
        loadTextureCandidates(loader, EARTH_TEX_URLS, (texture) => {
            prepareColorTexture(texture);
            if (material.map && material.map !== texture) {
                material.map.dispose();
            }
            material.map = texture;
            material.color.set(0xffffff);
            material.needsUpdate = true;
        });
        loadTextureCandidates(loader, EARTH_BUMP_TEX_URLS, (texture) => {
            material.bumpMap = texture;
            material.bumpScale = 0.22;
            material.needsUpdate = true;
        });
        loadTextureCandidates(loader, EARTH_NIGHT_TEX_URLS, (texture) => {
            prepareColorTexture(texture);
            if (material.emissiveMap && material.emissiveMap !== texture) {
                material.emissiveMap.dispose();
            }
            material.emissiveMap = texture;
            material.needsUpdate = true;
        });

        state.earthMesh = new THREE.Mesh(geometry, material);
        state.earthMesh.userData.pickKind = 'planet';
        state.earthMesh.userData.planetIndex = 2;
        state.earthGroup.add(state.earthMesh);

        state.earthCloudMesh = new THREE.Mesh(
            new THREE.SphereGeometry(ARTEMIS.EARTH_RADIUS * 1.012, 96, 96),
            new THREE.MeshPhongMaterial({
                color: 0xffffff,
                transparent: true,
                opacity: 0.18,
                depthWrite: false
            })
        );
        loadTextureCandidates(loader, EARTH_CLOUD_TEX_URLS, (texture) => {
            prepareColorTexture(texture);
            state.earthCloudMesh.material.map = texture;
            state.earthCloudMesh.material.alphaMap = texture;
            state.earthCloudMesh.material.opacity = 0.34;
            state.earthCloudMesh.material.needsUpdate = true;
        });
        state.earthGroup.add(state.earthCloudMesh);

        state.earthAtmosphereMesh = new THREE.Mesh(
            new THREE.SphereGeometry(ARTEMIS.EARTH_RADIUS * 1.05, 96, 96),
            new THREE.MeshLambertMaterial({
                color: 0x5ab8ff,
                transparent: true,
                opacity: 0.09,
                side: THREE.BackSide,
                depthWrite: false
            })
        );
        state.earthGroup.add(state.earthAtmosphereMesh);

        const glow = new THREE.Mesh(
            new THREE.SphereGeometry(ARTEMIS.EARTH_RADIUS * 1.09, 96, 96),
            new THREE.MeshBasicMaterial({
                color: 0x2f89ff,
                transparent: true,
                opacity: 0.045,
                side: THREE.BackSide,
                depthWrite: false
            })
        );
        state.earthGlowMesh = glow;
        state.earthGroup.add(glow);

        state.launchMarkerRoot = new THREE.Group();
        state.earthMesh.add(state.launchMarkerRoot);

        state.launchTrajectoryFrame = new THREE.Group();
        state.earthGroup.add(state.launchTrajectoryFrame);

        state.launchTrajectoryLine = new THREE.Line(
            new THREE.BufferGeometry(),
            new THREE.LineBasicMaterial({
                color: 0xffd166,
                transparent: true,
                opacity: 0.92,
                depthWrite: false
            })
        );
        state.launchTrajectoryLine.frustumCulled = false;
        state.launchTrajectoryLine.visible = false;
        state.launchTrajectoryFrame.add(state.launchTrajectoryLine);

        state.launchTrajectoryGroundTrackLine = new THREE.Line(
            new THREE.BufferGeometry(),
            new THREE.LineDashedMaterial({
                color: 0x64d8ff,
                dashSize: 0.12,
                gapSize: 0.07,
                transparent: true,
                opacity: 0.68,
                depthWrite: false
            })
        );
        state.launchTrajectoryGroundTrackLine.frustumCulled = false;
        state.launchTrajectoryGroundTrackLine.visible = false;
        state.earthMesh.add(state.launchTrajectoryGroundTrackLine);

        state.launchTrajectoryOrbitLine = new THREE.Line(
            new THREE.BufferGeometry(),
            new THREE.LineDashedMaterial({
                color: 0xffd166,
                dashSize: 0.36,
                gapSize: 0.22,
                transparent: true,
                opacity: 0.62,
                depthWrite: false
            })
        );
        state.launchTrajectoryOrbitLine.frustumCulled = false;
        state.launchTrajectoryOrbitLine.visible = false;
        state.launchTrajectoryFrame.add(state.launchTrajectoryOrbitLine);

        state.observerMarker = new THREE.Group();
        state.observerMarker.visible = false;

        const observerStem = new THREE.Mesh(
            new THREE.CylinderGeometry(0.03, 0.03, 0.55, 12),
            new THREE.MeshBasicMaterial({ color: 0x7affd8 })
        );
        observerStem.position.y = 0.26;

        const observerHead = new THREE.Mesh(
            new THREE.SphereGeometry(0.15, 18, 18),
            new THREE.MeshBasicMaterial({ color: 0xefffff })
        );
        observerHead.position.y = 0.56;

        state.observerPulse = new THREE.Mesh(
            new THREE.SphereGeometry(0.24, 18, 18),
            new THREE.MeshBasicMaterial({
                color: 0x53ffe8,
                transparent: true,
                opacity: 0.18,
                depthWrite: false
            })
        );
        state.observerPulse.position.copy(observerHead.position);

        state.observerMarker.add(observerStem, observerHead, state.observerPulse);
        state.earthMesh.add(state.observerMarker);

        state.satellitePoints = new THREE.Points(
            new THREE.BufferGeometry(),
            new THREE.PointsMaterial({
                size: SATELLITE_POINT_BASE_SIZE,
                sizeAttenuation: true,
                vertexColors: true,
                transparent: true,
                opacity: SATELLITE_LAYER_OPACITY,
                depthWrite: false
            })
        );
        state.satellitePoints.frustumCulled = false;
        state.satellitePoints.visible = false;
        state.earthMesh.add(state.satellitePoints);

        state.satelliteHighlight = createSatelliteHighlightMarker();
        state.satelliteHighlight.visible = false;
        state.earthMesh.add(state.satelliteHighlight);

        state.satelliteOrbitLine = new THREE.Line(
            new THREE.BufferGeometry(),
            new THREE.LineDashedMaterial({
                color: 0xffd36e,
                dashSize: 0.55,
                gapSize: 0.3,
                transparent: true,
                opacity: 0.78,
                depthWrite: false
            })
        );
        state.satelliteOrbitLine.frustumCulled = false;
        state.satelliteOrbitLine.visible = false;
        state.earthGroup.add(state.satelliteOrbitLine);

        state.satelliteGroundTrackLine = new THREE.Line(
            new THREE.BufferGeometry(),
            new THREE.LineDashedMaterial({
                color: 0x53ffe8,
                dashSize: 0.34,
                gapSize: 0.18,
                transparent: true,
                opacity: 0.68,
                depthWrite: false
            })
        );
        state.satelliteGroundTrackLine.frustumCulled = false;
        state.satelliteGroundTrackLine.visible = false;
        state.earthMesh.add(state.satelliteGroundTrackLine);
    }

    function createSatelliteHighlightMarker() {
        const group = new THREE.Group();
        const focusRing = new THREE.Sprite(new THREE.SpriteMaterial({
            map: createSatelliteFocusRingTexture(),
            color: 0xffffff,
            transparent: true,
            opacity: 0.88,
            depthWrite: false,
            depthTest: false
        }));
        focusRing.userData.baseScale = 1.65;
        focusRing.frustumCulled = false;

        const focusLight = new THREE.PointLight(0x9feaff, 1.8, 4.5, 1.8);
        focusLight.position.set(0, 0.65, 0.55);

        group.add(focusRing, focusLight);
        group.userData.focusRing = focusRing;
        group.userData.focusLight = focusLight;
        group.userData.modelRoot = new THREE.Group();
        group.userData.modelRoot.visible = false;
        group.add(group.userData.modelRoot);
        group.frustumCulled = false;
        return group;
    }

    function satellitePointSizeForDistance(distance) {
        const mode = normalizeSatelliteSizeMode(state.panelVisibility.satelliteSizeMode);
        if (mode === 'default') return SATELLITE_POINT_BASE_SIZE;
        if (mode === 'realistic') return SATELLITE_POINT_REALISTIC_MIN_SIZE;
        if (!Number.isFinite(distance)) return SATELLITE_POINT_BASE_SIZE;

        // Scene units are 1000 km. Reduced mode keeps a visible proxy while zooming in.
        const clampedDistance = THREE.MathUtils.clamp(distance, ZOOM_DIST_MIN, SATELLITE_POINT_REALISTIC_FAR_DISTANCE);
        const t = THREE.MathUtils.clamp(
            (Math.log(clampedDistance) - Math.log(ZOOM_DIST_MIN)) /
            (Math.log(SATELLITE_POINT_REALISTIC_FAR_DISTANCE) - Math.log(ZOOM_DIST_MIN)),
            0,
            1
        );
        const eased = t * t * (3 - 2 * t);
        const visibilityBias = Math.pow(eased, 1.05);
        return THREE.MathUtils.lerp(SATELLITE_POINT_REDUCED_MIN_SIZE, SATELLITE_POINT_BASE_SIZE, visibilityBias);
    }

    function updateSatellitePointSizing(cameraTargetDistance) {
        if (!state.satellitePoints?.material) return;
        const size = satellitePointSizeForDistance(cameraTargetDistance);
        if (Math.abs(state.satellitePoints.material.size - size) < 1e-8) return;
        state.satellitePoints.material.size = size;
        state.satellitePoints.material.needsUpdate = true;
    }

    function createSatelliteFocusRingTexture() {
        const canvas = document.createElement('canvas');
        canvas.width = 160;
        canvas.height = 160;
        const ctx = canvas.getContext('2d');
        const cx = 80;
        const cy = 80;

        ctx.clearRect(0, 0, canvas.width, canvas.height);
        ctx.strokeStyle = 'rgba(126, 231, 255, 0.92)';
        ctx.lineWidth = 4;
        ctx.beginPath();
        ctx.arc(cx, cy, 52, 0, Math.PI * 2);
        ctx.stroke();

        ctx.strokeStyle = 'rgba(255, 255, 255, 0.82)';
        ctx.lineWidth = 2;
        ctx.beginPath();
        ctx.moveTo(cx - 68, cy);
        ctx.lineTo(cx - 55, cy);
        ctx.moveTo(cx + 55, cy);
        ctx.lineTo(cx + 68, cy);
        ctx.moveTo(cx, cy - 68);
        ctx.lineTo(cx, cy - 55);
        ctx.moveTo(cx, cy + 55);
        ctx.lineTo(cx, cy + 68);
        ctx.stroke();

        const texture = new THREE.CanvasTexture(canvas);
        texture.colorSpace = THREE.SRGBColorSpace;
        return texture;
    }

    function modelMaterial(color, options = {}) {
        return new THREE.MeshStandardMaterial({
            color,
            roughness: options.roughness ?? 0.55,
            metalness: options.metalness ?? 0.35,
            emissive: options.emissive ?? 0x000000,
            emissiveIntensity: options.emissiveIntensity ?? 0
        });
    }

    function addBox(parent, size, position, material) {
        const mesh = new THREE.Mesh(new THREE.BoxGeometry(size[0], size[1], size[2]), material);
        mesh.position.set(position[0], position[1], position[2]);
        parent.add(mesh);
        return mesh;
    }

    function addCylinder(parent, radiusTop, radiusBottom, height, position, material, radialSegments = 24) {
        const mesh = new THREE.Mesh(new THREE.CylinderGeometry(radiusTop, radiusBottom, height, radialSegments), material);
        mesh.position.set(position[0], position[1], position[2]);
        parent.add(mesh);
        return mesh;
    }

    function addDish(parent, position, rotation, material) {
        const dish = new THREE.Mesh(
            new THREE.CylinderGeometry(0.18, 0.06, 0.08, 32),
            material
        );
        dish.position.set(position[0], position[1], position[2]);
        dish.rotation.set(rotation[0], rotation[1], rotation[2]);
        parent.add(dish);
        return dish;
    }

    function addSolarPanel(parent, size, position, material, frameMaterial) {
        const panel = addBox(parent, size, position, material);
        addBox(parent, [size[0] + 0.035, size[1] + 0.035, size[2] * 0.7], position, frameMaterial);
        panel.renderOrder = 2;
        return panel;
    }

    function satelliteModelFamily(satellite) {
        const n = String(satellite?.name || '').toUpperCase();
        const type = String(satellite?.type || '').toUpperCase();
        if (/^STARLINK\b/.test(n)) return /V2|V2 MINI/.test(n) ? 'starlink-v2' : 'starlink';
        if (/^ONEWEB\b/.test(n)) return 'oneweb';
        if (/^GLOBALSTAR\b/.test(n)) return 'globalstar';
        if (/^IRIDIUM\b/.test(n)) return 'iridium';
        if (/^LEMUR\b|^FLOCK\b|^DOVE\b/.test(n) || /CUBESAT/.test(formatSatelliteSize(satellite).toUpperCase())) return 'cubesat';
        if (/^SENTINEL-1\b|^CAPELLA\b|^ICEYE\b/.test(n) || /SAR|RADAR/.test(type)) return 'radar';
        if (/GPS|NAVSTAR|GALILEO|GLONASS|BEIDOU|QZSS|MICHIBIKI/.test(n) || /NAVIGATION/.test(type)) return 'navigation';
        if (/GEO|COMMUNICATION|KOMMUNIKATION|RELAIS|TDRS|O3B|SES|EUTELSAT|INTELSAT|INMARSAT/.test(`${n} ${type}`)) return 'comms';
        return 'generic';
    }

    function buildSatelliteModel(satellite) {
        const family = satelliteModelFamily(satellite);
        const group = new THREE.Group();
        group.userData.modelFamily = family;

        const bus = modelMaterial(0xb8c3d6, { roughness: 0.42, metalness: 0.55 });
        const darkBus = modelMaterial(0x2f3544, { roughness: 0.5, metalness: 0.45 });
        const panel = modelMaterial(0x143f7e, { roughness: 0.32, metalness: 0.25, emissive: 0x08224f, emissiveIntensity: 0.2 });
        const panelFrame = modelMaterial(0xd7dde8, { roughness: 0.4, metalness: 0.65 });
        const gold = modelMaterial(0xd6a650, { roughness: 0.45, metalness: 0.45 });
        const white = modelMaterial(0xe8edf5, { roughness: 0.48, metalness: 0.25 });

        if (family === 'starlink' || family === 'starlink-v2') {
            const s = family === 'starlink-v2' ? 1.18 : 1;
            addBox(group, [0.86 * s, 0.06, 0.42 * s], [0, 0, 0], darkBus);
            addBox(group, [0.72 * s, 0.026, 0.34 * s], [0, 0.045, 0], panel);
            addBox(group, [0.18 * s, 0.05, 0.10 * s], [0.23 * s, 0.08, 0.02], bus);
            addBox(group, [0.10 * s, 0.045, 0.08 * s], [-0.25 * s, 0.08, -0.02], gold);
            for (let i = -2; i <= 2; i += 1) {
                addBox(group, [0.012, 0.028, 0.39 * s], [i * 0.14 * s, 0.066, 0], panelFrame);
            }
        } else if (family === 'globalstar') {
            addBox(group, [0.34, 0.32, 0.34], [0, 0, 0], white);
            addCylinder(group, 0.18, 0.18, 0.22, [0, 0.19, 0], gold).rotation.x = Math.PI / 2;
            addSolarPanel(group, [0.72, 0.035, 0.26], [-0.58, 0, 0], panel, panelFrame);
            addSolarPanel(group, [0.72, 0.035, 0.26], [0.58, 0, 0], panel, panelFrame);
            addDish(group, [0, -0.1, 0.26], [Math.PI / 2, 0, 0], white);
        } else if (family === 'oneweb' || family === 'iridium') {
            addBox(group, [0.34, 0.42, 0.28], [0, 0, 0], white);
            addSolarPanel(group, [0.56, 0.035, 0.34], [-0.48, 0, 0], panel, panelFrame);
            addSolarPanel(group, [0.56, 0.035, 0.34], [0.48, 0, 0], panel, panelFrame);
            addDish(group, [0, 0.28, 0.16], [Math.PI / 2, 0, 0], gold);
        } else if (family === 'cubesat') {
            addBox(group, [0.34, 0.34, 0.34], [0, 0, 0], darkBus);
            addBox(group, [0.31, 0.012, 0.31], [0, 0.18, 0], panel);
            addBox(group, [0.31, 0.31, 0.012], [0, 0, 0.18], panel);
            addBox(group, [0.018, 0.42, 0.018], [0.2, 0.08, 0.2], gold);
        } else if (family === 'radar') {
            addBox(group, [0.34, 0.28, 0.28], [0, 0, 0], white);
            addSolarPanel(group, [0.46, 0.03, 0.22], [-0.4, 0, 0], panel, panelFrame);
            addBox(group, [0.92, 0.035, 0.24], [0.42, 0.02, 0], gold);
            addDish(group, [0, -0.08, 0.25], [Math.PI / 2, 0, 0], white);
        } else if (family === 'navigation') {
            addBox(group, [0.42, 0.38, 0.34], [0, 0, 0], white);
            addSolarPanel(group, [0.68, 0.035, 0.28], [-0.56, 0, 0], panel, panelFrame);
            addSolarPanel(group, [0.68, 0.035, 0.28], [0.56, 0, 0], panel, panelFrame);
            addCylinder(group, 0.12, 0.12, 0.18, [0, 0.3, 0], gold);
            addDish(group, [0, 0.38, 0], [0, 0, 0], gold);
        } else if (family === 'comms') {
            addBox(group, [0.42, 0.42, 0.42], [0, 0, 0], white);
            addSolarPanel(group, [0.84, 0.035, 0.34], [-0.66, 0, 0], panel, panelFrame);
            addSolarPanel(group, [0.84, 0.035, 0.34], [0.66, 0, 0], panel, panelFrame);
            addDish(group, [0, 0.02, 0.34], [Math.PI / 2, 0, 0], gold);
            addDish(group, [0.16, -0.06, 0.3], [Math.PI / 2, 0.3, 0], white);
        } else {
            addBox(group, [0.34, 0.3, 0.28], [0, 0, 0], white);
            addSolarPanel(group, [0.52, 0.03, 0.24], [-0.44, 0, 0], panel, panelFrame);
            addSolarPanel(group, [0.52, 0.03, 0.24], [0.44, 0, 0], panel, panelFrame);
            addDish(group, [0, 0.18, 0.18], [Math.PI / 2, 0, 0], gold);
        }

        group.traverse((child) => {
            child.frustumCulled = false;
            if (child.isMesh) child.castShadow = false;
        });
        return group;
    }

    function updateFocusedSatelliteModel(satellite) {
        if (!state.satelliteHighlight) return;
        const modelRoot = state.satelliteHighlight.userData.modelRoot;
        if (!modelRoot) return;
        if (!satellite) {
            modelRoot.clear();
            modelRoot.visible = false;
            state.satelliteFocusedModelKey = '';
            return;
        }

        const family = satelliteModelFamily(satellite);
        const modelKey = `${satellite.id}:${family}:${satellite.name}`;
        if (state.satelliteFocusedModelKey !== modelKey) {
            modelRoot.clear();
            modelRoot.add(buildSatelliteModel(satellite));
            state.satelliteFocusedModelKey = modelKey;
        }
        modelRoot.visible = true;
    }

    function orientSatelliteHighlight(localPosition) {
        if (!state.satelliteHighlight || !localPosition) return;
        const radial = localPosition.clone().normalize();
        if (radial.lengthSq() < 1e-8) return;

        const earthSpinAxis = new THREE.Vector3(0, 1, 0);
        let alongTrack = earthSpinAxis.clone().cross(radial).normalize();
        if (alongTrack.lengthSq() < 1e-8) {
            alongTrack = new THREE.Vector3(1, 0, 0).cross(radial).normalize();
        }
        if (state.followSatelliteId) {
            const satellite = state.satelliteIndex.get(state.followSatelliteId);
            if (Number.isFinite(satellite?.inclinationDeg) && satellite.inclinationDeg > 90) {
                alongTrack.multiplyScalar(-1);
            }
        }

        const side = new THREE.Vector3().crossVectors(radial, alongTrack).normalize();
        const correctedTrack = new THREE.Vector3().crossVectors(side, radial).normalize();
        const basis = new THREE.Matrix4().makeBasis(side, radial, correctedTrack.clone().multiplyScalar(-1));
        // Keep the ring parent unrotated so the focus sprite stays circular on screen.
        state.satelliteHighlight.quaternion.identity();

        const modelRoot = state.satelliteHighlight.userData.modelRoot;
        if (modelRoot) {
            modelRoot.quaternion.setFromRotationMatrix(basis);
        }

        const focusLight = state.satelliteHighlight.userData.focusLight;
        if (focusLight) {
            focusLight.position
                .copy(radial)
                .multiplyScalar(0.65)
                .addScaledVector(correctedTrack, -0.55);
        }
    }

    function createMoon() {
        const loader = new THREE.TextureLoader();
        loader.crossOrigin = 'anonymous';

        const geometry = new THREE.SphereGeometry(ARTEMIS.MOON_RADIUS, 32, 32);
        const material = new THREE.MeshPhongMaterial({ color: 0xaaaaaa, shininess: 5 });
        loader.load(MOON_TEX_URL, (texture) => {
            material.map = texture;
            material.color.set(0xffffff);
            material.needsUpdate = true;
        });

        state.moonMesh = new THREE.Mesh(geometry, material);
        state.moonMesh.userData.pickKind = 'moon';
        state.scene.add(state.moonMesh);
    }

    function createSun() {
        const sunVisualRadius = 5000;
        state.sunMesh = new THREE.Mesh(
            new THREE.SphereGeometry(sunVisualRadius, 32, 32),
            new THREE.MeshBasicMaterial({ color: 0xffee88 })
        );
        state.sunMesh.userData.pickKind = 'sun';
        state.sunMesh.position.copy(state.sunScenePos);
        state.scene.add(state.sunMesh);

        const glowCanvas = document.createElement('canvas');
        glowCanvas.width = 256;
        glowCanvas.height = 256;
        const ctx = glowCanvas.getContext('2d');
        const gradient = ctx.createRadialGradient(128, 128, 0, 128, 128, 128);
        gradient.addColorStop(0, 'rgba(255,255,220,1)');
        gradient.addColorStop(0.1, 'rgba(255,230,120,0.8)');
        gradient.addColorStop(0.3, 'rgba(255,200,60,0.3)');
        gradient.addColorStop(1, 'rgba(255,160,0,0)');
        ctx.fillStyle = gradient;
        ctx.fillRect(0, 0, 256, 256);

        state.sunGlow = new THREE.Sprite(new THREE.SpriteMaterial({
            map: new THREE.CanvasTexture(glowCanvas),
            transparent: true,
            blending: THREE.AdditiveBlending,
            depthTest: false
        }));
        state.sunGlow.scale.set(40000, 40000, 1);
        state.sunGlow.position.copy(state.sunScenePos);
        state.scene.add(state.sunGlow);

        state.sunPointLight = new THREE.PointLight(0xffffcc, 0.8, ARTEMIS.AU * 35);
        state.sunPointLight.position.copy(state.sunScenePos);
        state.scene.add(state.sunPointLight);

        const label = makeTextSprite('Sonne', '#ffdc77');
        label.position.copy(state.sunScenePos.clone().add(new THREE.Vector3(0, sunVisualRadius + 3000, 0)));
        label._offsetY = sunVisualRadius + 3000;
        label._anchor = state.sunScenePos;
        state.dynamicLabels.push(label);
        state.scene.add(label);
    }

    function createPlanets() {
        const T0 = ARTEMIS.getJulianCenturies(sceneTimeMs());
        for (let i = 0; i < ARTEMIS.PLANETS.length; i++) {
            if (i === 2) continue;

            const planet = ARTEMIS.PLANETS[i];
            const pos = ARTEMIS.getPlanetPosition(i, T0);
            const displayRadius = Math.max(planet.radius * 100, 1000);

            const mesh = new THREE.Mesh(
                new THREE.SphereGeometry(displayRadius, 24, 24),
                new THREE.MeshBasicMaterial({ color: new THREE.Color(planet.color) })
            );
            mesh.position.set(pos.x, pos.y, pos.z);
            mesh.userData.pickKind = 'planet';
            mesh.userData.planetIndex = i;
            state.planetMeshes[i] = mesh;
            state.scene.add(mesh);

            if (planet.hasRings) {
                const ring = new THREE.Mesh(
                    new THREE.RingGeometry(displayRadius * 1.4, displayRadius * 2.3, 64),
                    new THREE.MeshBasicMaterial({
                        color: 0xddcc99,
                        transparent: true,
                        opacity: 0.5,
                        side: THREE.DoubleSide
                    })
                );
                ring.rotation.x = Math.PI * 0.42;
                ring.position.copy(mesh.position);
                ring.userData.pickKind = 'planet';
                ring.userData.planetIndex = i;
                mesh.userData.saturnRing = ring;
                state.extraPickableMeshes.push(ring);
                state.scene.add(ring);
            }

            const label = makeTextSprite(planet.name, planet.color);
            label.position.set(pos.x, pos.y + displayRadius + 2000, pos.z);
            label._offsetY = displayRadius + 2000;
            label._anchor = mesh.position;
            state.dynamicLabels.push(label);
            state.scene.add(label);

            const orbitLine = new THREE.Line(
                new THREE.BufferGeometry().setFromPoints(
                    ARTEMIS.getPlanetOrbitPoints(i, T0, 256).map((pt) => new THREE.Vector3(pt.x, pt.y, pt.z))
                ),
                new THREE.LineDashedMaterial({
                    color: new THREE.Color(planet.color),
                    dashSize: planet.a * ARTEMIS.AU * 0.02,
                    gapSize: planet.a * ARTEMIS.AU * 0.01,
                    transparent: true,
                    opacity: 0.3
                })
            );
            orbitLine.computeLineDistances();
            orbitLine.visible = false;
            state.planetOrbits[i] = orbitLine;
            state.planetOrbitList.push(orbitLine);
            state.scene.add(orbitLine);
        }

        const earthOrbitLine = new THREE.Line(
            new THREE.BufferGeometry().setFromPoints(
                ARTEMIS.getPlanetOrbitPoints(2, T0, 256).map((pt) => new THREE.Vector3(pt.x, pt.y, pt.z))
            ),
            new THREE.LineDashedMaterial({
                color: 0x4499ff,
                dashSize: 400,
                gapSize: 220,
                transparent: true,
                opacity: 0.35
            })
        );
        earthOrbitLine.computeLineDistances();
        state.planetOrbits[2] = earthOrbitLine;
        state.scene.add(earthOrbitLine);
    }

    function createLabels() {
        state.earthLabel = makeTextSprite('Erde', '#8cc3ff');
        state.earthLabel.position.set(ARTEMIS.EARTH_RADIUS + 8, 0, 0);
        state.scene.add(state.earthLabel);

        state.moonLabel = makeTextSprite('Mond', '#e1e7f2');
        state.scene.add(state.moonLabel);

        state.orionLabel = makeTextSprite('Orion', '#18ffc0');
        state.scene.add(state.orionLabel);
    }

    function createNorthPoleAxis() {
        const axisLength = ARTEMIS.EARTH_RADIUS * 2.5;
        const north = EARTH_POLE.clone().multiplyScalar(axisLength);
        const south = EARTH_POLE.clone().multiplyScalar(-axisLength);
        state.scene.add(new THREE.Line(
            new THREE.BufferGeometry().setFromPoints([south, north]),
            new THREE.LineBasicMaterial({ color: 0xff4e4e, transparent: true, opacity: 0.6 })
        ));
    }

    function createMoonOrbit() {
        state.moonOrbitLine = new THREE.Line(
            new THREE.BufferGeometry().setFromPoints(
                ARTEMIS.getMoonOrbitPoints(360).map((pt) => new THREE.Vector3(pt.x, pt.y, pt.z))
            ),
            new THREE.LineDashedMaterial({
                color: 0xd8e4f0,
                dashSize: 5,
                gapSize: 3,
                transparent: true,
                opacity: 0.52
            })
        );
        state.moonOrbitLine.computeLineDistances();
        state.scene.add(state.moonOrbitLine);
    }

    function createArtemisObjects() {
        state.orionMarker = new THREE.Mesh(
            new THREE.SphereGeometry(1.5, 16, 16),
            new THREE.MeshBasicMaterial({ color: 0x00ffaa })
        );
        state.scene.add(state.orionMarker);

        state.orionGlow = new THREE.Mesh(
            new THREE.SphereGeometry(3, 16, 16),
            new THREE.MeshBasicMaterial({ color: 0x00ffaa, transparent: true, opacity: 0.25 })
        );
        state.scene.add(state.orionGlow);
    }

    function createTrajectoryLines() {
        state.pastLine = new THREE.Line(
            new THREE.BufferGeometry(),
            new THREE.LineBasicMaterial({ color: 0x00ddff, transparent: true, opacity: 0.88 })
        );
        state.futureLine = new THREE.Line(
            new THREE.BufferGeometry(),
            new THREE.LineDashedMaterial({
                color: 0xffffff,
                dashSize: 3,
                gapSize: 2,
                transparent: true,
                opacity: 0.35
            })
        );
        state.scene.add(state.pastLine);
        state.scene.add(state.futureLine);
    }

    function makeTextSprite(text, color) {
        const canvas = document.createElement('canvas');
        const ctx = canvas.getContext('2d');
        canvas.width = 256;
        canvas.height = 64;
        ctx.font = 'bold 28px Bahnschrift, Arial Narrow, Segoe UI';
        ctx.fillStyle = color;
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(text, 128, 32);
        const sprite = new THREE.Sprite(new THREE.SpriteMaterial({
            map: new THREE.CanvasTexture(canvas),
            transparent: true,
            depthTest: false
        }));
        sprite.scale.set(20, 5, 1);
        return sprite;
    }

    function buildPickableList() {
        state.pickableMeshes = [state.sunMesh, state.earthMesh, state.moonMesh];
        Object.keys(state.planetMeshes).forEach((key) => {
            state.pickableMeshes.push(state.planetMeshes[key]);
        });
        state.pickableMeshes.push(...state.extraPickableMeshes);
        state.launchMarkers.forEach((marker) => {
            state.pickableMeshes.push(marker.pickMesh);
        });
    }

    function setFocusTarget(target) {
        state.controls.target.copy(target);
    }

    function updateArtemisVisibility() {
        const visible = state.artemisReplayEnabled;
        [state.orionMarker, state.orionGlow, state.orionLabel, state.pastLine, state.futureLine].forEach((item) => {
            if (item) item.visible = visible;
        });
        if (!visible) {
            state.followOrion = false;
            dom['follow-artemis']?.classList.remove('active');
        }
        refreshSceneModePill();
    }

    function refreshSceneModePill() {
        if (!dom['scene-mode-pill']) return;
        dom['scene-mode-pill'].textContent = state.artemisReplayEnabled
            ? t('scene.replay')
            : t('scene.home');
        dom['scene-mode-pill'].classList.toggle('status-live', state.artemisReplayEnabled);
        if (dom['toggle-artemis-replay']) {
            dom['toggle-artemis-replay'].textContent = state.artemisReplayEnabled
                ? t('artemis.replay.on')
                : t('artemis.replay.off');
            dom['toggle-artemis-replay'].classList.toggle('active', state.artemisReplayEnabled);
        }
    }

    function setArtemisReplayEnabled(enabled) {
        state.artemisReplayEnabled = enabled;
        if (enabled && !state.artemisReplayInitialized) {
            state.artemisReplayInitialized = true;
            jumpToMissionMet(0);
        }
        updateArtemisVisibility();
    }

    function formatLaunchCountdown(target) {
        if (!target) return '--';
        const diff = target.getTime() - Date.now();
        if (diff <= 0) return t('countdown.reached');
        const sec = Math.floor(diff / 1000);
        const days = Math.floor(sec / 86400);
        const hours = Math.floor(sec / 3600) % 24;
        const minutes = Math.floor(sec / 60) % 60;
        const seconds = sec % 60;
        const pad2 = (value) => String(value).padStart(2, '0');
        if (days > 0) return `${days}d ${pad2(hours)}:${pad2(minutes)}:${pad2(seconds)}`;
        return `${pad2(hours)}:${pad2(minutes)}:${pad2(seconds)}`;
    }

    function launchInstant(launch) {
        const raw = launch?.net || launch?.window_start || launch?.windowStart;
        if (!raw) return null;
        const date = new Date(raw);
        return Number.isNaN(date.getTime()) ? null : date;
    }

    function launchLatitude(launch) {
        const value = launch?.latitude ?? launch?.pad?.latitude;
        const parsed = parseFloat(value);
        return Number.isFinite(parsed) ? parsed : NaN;
    }

    function launchLongitude(launch) {
        const value = launch?.longitude ?? launch?.pad?.longitude;
        const parsed = parseFloat(value);
        return Number.isFinite(parsed) ? parsed : NaN;
    }

    function isEarthLaunch(launch) {
        const lat = launchLatitude(launch);
        const lon = launchLongitude(launch);
        return Number.isFinite(lat) && Number.isFinite(lon);
    }

    function launchOrganization(launch) {
        if (typeof launch?.provider === 'string' && launch.provider.trim()) return launch.provider.trim();
        const lsp = launch?.launch_service_provider;
        if (lsp?.name) return String(lsp.name).trim();
        const manufacturer = launch?.rocket?.configuration?.manufacturer;
        if (manufacturer?.name) return String(manufacturer.name).trim();
        return t('launch.unknownOrg');
    }

    function launchKey(launch) {
        return String(launch?.id || `${launch?.name || 'launch'}-${launch?.net || launch?.window_start || ''}`);
    }

    function launchPadLabel(launch) {
        if (typeof launch?.pad === 'string' && launch.pad.trim()) {
            return [launch.pad.trim(), launch.padLocation].filter(Boolean).join(' · ');
        }
        const pad = launch?.pad?.name || t('launch.unknownPad');
        const location = launch?.pad?.location?.name || '';
        return [pad, location].filter(Boolean).join(' · ');
    }

    function launchRocketName(launch) {
        if (typeof launch?.rocket === 'string' && launch.rocket.trim()) return launch.rocket.trim();
        return launch?.rocket?.configuration?.full_name ||
            launch?.rocket?.configuration?.name ||
            t('launch.unknownRocket');
    }

    function launchStatusLabel(launch) {
        if (launch?.statusName) return launch.statusName;
        if (typeof launch?.status === 'string' && launch.status.trim()) return launch.status.trim();
        return launch?.status?.name || t('launch.unknownStatus');
    }

    function launchStatusText(launch) {
        return [
            launch?.outcome,
            typeof launch?.status === 'string' ? launch.status : '',
            launch?.statusName,
            launch?.statusAbbrev,
            launch?.statusDescription,
            launch?.status?.abbrev,
            launch?.status?.name,
            launch?.status?.description
        ].filter(Boolean).join(' ').toLowerCase();
    }

    function classifyLaunchStatus(launch) {
        const text = launchStatusText(launch);
        if (!text) return 'scheduled';
        if (/(success|successful)/.test(text)) return 'success';
        if (/(partial failure|failure|failed|lost)/.test(text)) return 'failure';
        if (/(cancel|cancelled|canceled|scrub|scrubbed)/.test(text)) return 'cancelled';
        if (/(hold|delay|delayed|postponed|slip|tbc|tbd|to be confirmed|to be determined|unconfirmed)/.test(text)) return 'delayed';
        if (/(in flight|flight|liftoff|lift-off|launch in progress)/.test(text)) return 'live';
        if (/(go|confirmed|ready|on schedule)/.test(text)) return 'go';
        return 'scheduled';
    }

    function launchCountdownStatusClass(launch) {
        if (!launch) return '';
        const status = classifyLaunchStatus(launch);
        if (status === 'success') return 'status-success';
        if (status === 'failure') return 'status-failure';
        if (status === 'cancelled') return 'status-cancelled';
        if (status === 'delayed') return 'status-delayed';
        if (status === 'live') return 'status-go';

        const when = launchInstant(launch);
        if (when && when.getTime() - Date.now() <= LAUNCH_VERIFY_WINDOW_MS && launch?.preflightStatus === 'go') {
            return 'status-go';
        }
        if (status === 'go' && when && when.getTime() - Date.now() <= LAUNCH_VERIFY_WINDOW_MS) {
            return 'status-go';
        }
        return '';
    }

    function applyLaunchStatusClass(element, launch) {
        if (!element) return;
        ['status-go', 'status-delayed', 'status-cancelled', 'status-success', 'status-failure'].forEach((name) => {
            element.classList.remove(name);
        });
        const statusClass = launchCountdownStatusClass(launch);
        if (statusClass) element.classList.add(statusClass);
    }

    function launchStatusBadge(launch) {
        if (launch?.outcome) {
            if (launch.outcome === 'success') return { text: t('launch.status.success'), className: 'status-success' };
            if (launch.outcome === 'failure') return { text: t('launch.status.failure'), className: 'status-failure' };
            if (launch.outcome === 'cancelled') return { text: t('launch.status.cancelled'), className: 'status-cancelled' };
            if (launch.outcome === 'delayed') return { text: t('launch.status.delayed'), className: 'status-delayed' };
            if (launch.outcome === 'go') return { text: t('launch.status.goT15'), className: 'status-go' };
        }
        const status = classifyLaunchStatus(launch);
        if (status === 'success') return { text: t('launch.status.success'), className: 'status-success' };
        if (status === 'failure') return { text: t('launch.status.failure'), className: 'status-failure' };
        if (status === 'cancelled') return { text: t('launch.status.cancelled'), className: 'status-cancelled' };
        if (status === 'delayed') return { text: t('launch.status.delayed'), className: 'status-delayed' };
        if (status === 'live') return { text: 'Live', className: 'status-go' };
        if (status === 'go') return { text: 'Go', className: 'status-go' };
        return { text: launchStatusLabel(launch), className: '' };
    }

    function belongsInLaunchHistory(launch, now = Date.now()) {
        const when = launchInstant(launch);
        if (!when) return Boolean(launch?.outcome);
        const isFuture = when.getTime() > now;
        if (isFuture && classifyLaunchStatus(launch) === 'delayed') return false;
        return Boolean(launch?.outcome) || when.getTime() <= now;
    }

    function isTerminalLaunch(launch) {
        const terminalStates = new Set(['success', 'failure', 'cancelled']);
        const outcome = String(launch?.outcome || '').toLowerCase();
        const postflightStatus = String(launch?.postflightStatus || '').toLowerCase();
        return terminalStates.has(outcome) ||
            terminalStates.has(postflightStatus) ||
            terminalStates.has(classifyLaunchStatus(launch));
    }

    function belongsInUpcomingLaunch(launch) {
        return !isTerminalLaunch(launch);
    }

    function launchStory(launch) {
        if (launch?.missionDescription) return launch.missionDescription;
        if (typeof launch?.mission === 'string' && launch.mission.trim()) return launch.mission.trim();
        return launch?.mission?.description ||
            launch?.mission?.name ||
            t('launch.noStory');
    }

    function launchVideoCandidates(launch) {
        const candidates = [];
        const addUrl = (entry) => {
            if (!entry) return;
            if (typeof entry === 'string') {
                candidates.push({ url: entry, title: 'Livestream' });
                return;
            }
            if (entry.url) {
                candidates.push({
                    url: entry.url,
                    title: entry.title || entry.description || entry.source || 'Livestream',
                    featured: Boolean(entry.featured),
                    priority: Number.isFinite(Number(entry.priority)) ? Number(entry.priority) : 999
                });
            }
        };

        addUrl(launch?.livestreamUrl);
        if (Array.isArray(launch?.vidURLs)) launch.vidURLs.forEach(addUrl);
        if (Array.isArray(launch?.vid_urls)) launch.vid_urls.forEach(addUrl);
        if (Array.isArray(launch?.videos)) launch.videos.forEach(addUrl);

        return candidates
            .filter((entry) => /^https?:\/\//i.test(entry.url))
            .sort((a, b) => {
                const aYoutube = /youtu\.?be|youtube\.com/i.test(a.url) ? 0 : 1;
                const bYoutube = /youtu\.?be|youtube\.com/i.test(b.url) ? 0 : 1;
                if (aYoutube !== bYoutube) return aYoutube - bYoutube;
                if (a.featured !== b.featured) return a.featured ? -1 : 1;
                return a.priority - b.priority;
            });
    }

    function launchLivestream(launch) {
        return launchVideoCandidates(launch)[0] || null;
    }

    function youtubeEmbedUrl(url) {
        try {
            const parsed = new URL(url);
            let id = '';
            if (parsed.hostname.includes('youtu.be')) {
                id = parsed.pathname.split('/').filter(Boolean)[0] || '';
            } else if (parsed.pathname.startsWith('/watch')) {
                id = parsed.searchParams.get('v') || '';
            } else if (parsed.pathname.startsWith('/live/') || parsed.pathname.startsWith('/embed/')) {
                id = parsed.pathname.split('/').filter(Boolean)[1] || '';
            }
            return id ? `https://www.youtube.com/embed/${encodeURIComponent(id)}?rel=0` : '';
        } catch (error) {
            return '';
        }
    }

    function launchStreamSearchUrl(launch) {
        const query = [
            launch?.name,
            launchRocketName(launch),
            launchOrganization(launch),
            'launch livestream'
        ].filter(Boolean).join(' ');
        return `https://www.youtube.com/results?search_query=${encodeURIComponent(query)}`;
    }

    function launchExternalUrl(launch) {
        if (launch?.sourceUrl) return launch.sourceUrl;
        if (launch?.livestreamUrl) return launch.livestreamUrl;
        const firstVideo = Array.isArray(launch?.vidURLs) && launch.vidURLs.length > 0 ? launch.vidURLs[0]?.url : '';
        const firstInfo = Array.isArray(launch?.infoURLs) && launch.infoURLs.length > 0 ? launch.infoURLs[0]?.url : '';
        return firstVideo || firstInfo || launch?.url || '';
    }

    function formatCoordinates(launch) {
        const lat = launchLatitude(launch);
        const lon = launchLongitude(launch);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) return '--';
        const latHemisphere = lat >= 0 ? 'N' : 'S';
        const lonHemisphere = lon >= 0 ? 'E' : 'W';
        return `${Math.abs(lat).toFixed(2)}° ${latHemisphere}, ${Math.abs(lon).toFixed(2)}° ${lonHemisphere}`;
    }

    function rememberLaunchesForMonitoring(items) {
        const now = Date.now();
        items.forEach((launch) => {
            const when = launchInstant(launch);
            if (!when) return;
            state.launchWatchList.set(launchKey(launch), launch);
        });

        state.launchWatchList.forEach((launch, key) => {
            const when = launchInstant(launch);
            if (!when || now - when.getTime() > 6 * 60 * 60 * 1000) {
                state.launchWatchList.delete(key);
            }
        });
    }

    function monitoredLaunches() {
        const byId = new Map(state.launchWatchList);
        state.launches.forEach((launch) => byId.set(launchKey(launch), launch));
        return Array.from(byId.values()).sort((a, b) => {
            const ta = launchInstant(a);
            const tb = launchInstant(b);
            if (!ta || !tb) return 0;
            return ta - tb;
        });
    }

    function launchIntelText(launch) {
        if (!launch) return '--';
        const when = launchInstant(launch);
        const status = classifyLaunchStatus(launch);
        if (status === 'cancelled') return t('launch.intel.cancelled');
        if (status === 'delayed') return t('launch.intel.delayed');
        if (status === 'failure') return t('launch.intel.failure');
        if (status === 'success' || launch?.postflightStatus === 'success') return t('launch.intel.success');
        if (launch?.preflightStatus === 'go' || status === 'live') return t('launch.intel.go');
        if (!when) return t('launch.intel.noTime');

        const diff = when.getTime() - Date.now();
        if (diff > LAUNCH_VERIFY_WINDOW_MS) return t('launch.intel.t15Queued');
        if (diff > 0) return launch?.preflightCheckedAt ? t('launch.intel.preflightLoaded') : t('launch.intel.preflightChecking');
        if (diff > -LAUNCH_SUCCESS_CHECK_DELAY_MS) return t('launch.intel.windowReached');
        return launch?.postflightCheckedAt ? t('launch.intel.postflightLoaded') : t('launch.intel.postflightChecking');
    }

    function updateLaunchStreamUi(launch) {
        const card = dom['watch-launch-stream'];
        if (!card) return;
        const stream = launch ? launchLivestream(launch) : null;
        const when = launchInstant(launch);
        const diff = when ? when.getTime() - Date.now() : Number.POSITIVE_INFINITY;
        const nearLiveWindow = when && diff <= LAUNCH_VERIFY_WINDOW_MS && diff > -2 * 60 * 60 * 1000;
        const frame = dom['watch-launch-stream-frame'];
        const link = dom['watch-launch-stream-link'];
        const stateLabel = dom['watch-launch-stream-state'];
        const streamUrl = stream?.url || '';
        const embedUrl = streamUrl ? youtubeEmbedUrl(streamUrl) : '';
        const mode = stream ? 'stream' : nearLiveWindow ? 'search' : 'hidden';
        const uiKey = `${launch ? launchKey(launch) : 'none'}|${mode}|${streamUrl}|${embedUrl}`;

        card.classList.toggle('is-hidden', !stream && !nearLiveWindow);
        if (!stream && !nearLiveWindow) {
            if (state.launchStreamUiKey !== uiKey && frame) frame.src = 'about:blank';
            state.launchStreamUiKey = uiKey;
            return;
        }

        if (state.launchStreamUiKey === uiKey) return;
        state.launchStreamUiKey = uiKey;

        const searchUrl = launch ? launchStreamSearchUrl(launch) : '#';
        if (stream) {
            if (stateLabel) stateLabel.textContent = stream.title || t('stream.found');
            if (link) {
                link.href = streamUrl;
                link.textContent = t('stream.open');
                link.classList.remove('is-hidden');
            }
            if (frame) {
                if (frame.src !== (embedUrl || 'about:blank')) frame.src = embedUrl || 'about:blank';
                frame.title = stream.title || 'Launch Livestream';
                frame.classList.toggle('is-hidden', !embedUrl);
            }
        } else {
            if (stateLabel) stateLabel.textContent = t('stream.searchReady');
            if (link) {
                link.href = searchUrl;
                link.textContent = t('stream.searchYoutube');
                link.classList.remove('is-hidden');
            }
            if (frame) {
                if (frame.src !== 'about:blank') frame.src = 'about:blank';
                frame.classList.add('is-hidden');
            }
        }
    }

    function periodBounds(now, period) {
        const start = new Date(now);
        start.setHours(0, 0, 0, 0);
        if (period === 'week') {
            const mondayOffset = (start.getDay() + 6) % 7;
            start.setDate(start.getDate() - mondayOffset);
            const previousStart = new Date(start);
            previousStart.setDate(previousStart.getDate() - 7);
            return { currentStart: start, previousStart, previousEnd: new Date(start) };
        }
        if (period === 'month') {
            start.setDate(1);
            const previousStart = new Date(start);
            previousStart.setMonth(previousStart.getMonth() - 1);
            return { currentStart: start, previousStart, previousEnd: new Date(start) };
        }
        start.setMonth(0, 1);
        const previousStart = new Date(start);
        previousStart.setFullYear(previousStart.getFullYear() - 1);
        return { currentStart: start, previousStart, previousEnd: new Date(start) };
    }

    function isSuccessfulLaunch(launch) {
        if (launch?.outcome) return launch.outcome === 'success';
        return classifyLaunchStatus(launch) === 'success';
    }

    function countSuccessfulLaunches(items, start, end) {
        return items.filter((launch) => {
            const when = launchInstant(launch);
            return when && when >= start && when < end && isSuccessfulLaunch(launch);
        }).length;
    }

    function launchSuccessStatsFromItems(items, date = new Date()) {
        const oldest = items.reduce((current, launch) => {
            const when = launchInstant(launch);
            if (!when) return current;
            return !current || when < current ? when : current;
        }, null);
        const makeStats = (period) => {
            const bounds = periodBounds(date, period);
            const coversPrevious = oldest && oldest <= bounds.previousStart;
            return {
                current: countSuccessfulLaunches(items, bounds.currentStart, date),
                previous: coversPrevious ? countSuccessfulLaunches(items, bounds.previousStart, bounds.previousEnd) : null
            };
        };
        return {
            week: makeStats('week'),
            month: makeStats('month'),
            year: makeStats('year')
        };
    }

    function formatLaunchDelta(current, previous, label) {
        if (!Number.isFinite(previous)) return { text: `${label} --`, className: 'even' };
        const diff = current - previous;
        if (diff > 0) return { text: `+${diff} vs ${label}`, className: 'up' };
        if (diff < 0) return { text: `${diff} vs ${label}`, className: 'down' };
        return { text: t('stats.deltaSame', { label }), className: 'even' };
    }

    function setStatDelta(id, delta) {
        const element = dom[id];
        if (!element) return;
        element.textContent = delta.text;
        element.classList.remove('up', 'down', 'even');
        element.classList.add(delta.className);
    }

    function addDays(date, days) {
        const next = new Date(date);
        next.setDate(next.getDate() + days);
        return next;
    }

    function addMonths(date, months) {
        const next = new Date(date);
        next.setMonth(next.getMonth() + months);
        return next;
    }

    function launchHistoryForStats() {
        return state.launchHistoryItems
            .map((launch) => ({ launch, when: launchInstant(launch) }))
            .filter((entry) => entry.when && entry.when.getTime() <= Date.now());
    }

    function successfulLaunchHistoryForStats() {
        return launchHistoryForStats().filter((entry) => isSuccessfulLaunch(entry.launch));
    }

    function appendStatMetric(parent, label, value) {
        const metric = document.createElement('div');
        metric.className = 'stat-insight-metric';
        const labelElement = document.createElement('span');
        labelElement.textContent = label;
        const valueElement = document.createElement('strong');
        valueElement.textContent = value;
        metric.append(labelElement, valueElement);
        parent.appendChild(metric);
    }

    function appendStatSummary(parent, metrics) {
        const summary = document.createElement('div');
        summary.className = 'stat-insight-summary';
        metrics.forEach((metric) => appendStatMetric(summary, metric.label, metric.value));
        parent.appendChild(summary);
    }

    function appendEmptyStat(parent, text) {
        const empty = document.createElement('div');
        empty.className = 'stat-empty';
        empty.textContent = text;
        parent.appendChild(empty);
    }

    function appendBarChart(parent, series) {
        const max = Math.max(1, ...series.map((entry) => entry.value));
        const chart = document.createElement('div');
        chart.className = 'stat-chart';
        chart.style.gridTemplateColumns = `repeat(${Math.max(1, series.length)}, minmax(0, 1fr))`;

        series.forEach((entry) => {
            const column = document.createElement('div');
            column.className = 'stat-chart-column';
            column.title = `${entry.title || entry.label}: ${entry.value}`;

            const value = document.createElement('div');
            value.className = 'stat-chart-value';
            value.textContent = entry.value > 0 ? String(entry.value) : '';

            const bar = document.createElement('div');
            bar.className = 'stat-chart-bar';
            bar.style.height = `${Math.max(2, Math.round((entry.value / max) * 100))}%`;

            const label = document.createElement('div');
            label.className = 'stat-chart-label';
            label.textContent = entry.label;

            column.append(value, bar, label);
            chart.appendChild(column);
        });

        parent.appendChild(chart);
    }

    function appendLineChart(parent, series) {
        const chart = document.createElement('div');
        chart.className = 'stat-line-chart';

        const width = 320;
        const height = 170;
        const padding = { top: 16, right: 16, bottom: 30, left: 34 };
        const values = series.map((entry) => entry.value).filter(Number.isFinite);
        const min = values.length ? Math.min(...values) : 0;
        const max = values.length ? Math.max(...values) : 1;
        const span = Math.max(1, max - min);
        const plotWidth = width - padding.left - padding.right;
        const plotHeight = height - padding.top - padding.bottom;
        const pointFor = (entry, index) => {
            const x = padding.left + (series.length <= 1 ? plotWidth : (index / (series.length - 1)) * plotWidth);
            const y = padding.top + plotHeight - ((entry.value - min) / span) * plotHeight;
            return { x, y };
        };
        const points = series.map(pointFor);
        const path = points.map((point, index) => `${index ? 'L' : 'M'} ${point.x.toFixed(1)} ${point.y.toFixed(1)}`).join(' ');
        const area = points.length
            ? `${path} L ${points[points.length - 1].x.toFixed(1)} ${padding.top + plotHeight} L ${points[0].x.toFixed(1)} ${padding.top + plotHeight} Z`
            : '';
        const labels = series
            .map((entry, index) => ({ entry, index }))
            .filter(({ entry, index }) => entry.label && (index === 0 || index === series.length - 1 || index % 6 === 0));

        const svgNs = 'http://www.w3.org/2000/svg';
        const svg = document.createElementNS(svgNs, 'svg');
        svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
        svg.setAttribute('role', 'img');
        svg.setAttribute('aria-label', t('stats.chart.satHistory'));

        const gridTop = document.createElementNS(svgNs, 'line');
        gridTop.setAttribute('x1', String(padding.left));
        gridTop.setAttribute('x2', String(width - padding.right));
        gridTop.setAttribute('y1', String(padding.top));
        gridTop.setAttribute('y2', String(padding.top));
        gridTop.setAttribute('class', 'stat-line-grid');
        svg.appendChild(gridTop);

        const gridBottom = document.createElementNS(svgNs, 'line');
        gridBottom.setAttribute('x1', String(padding.left));
        gridBottom.setAttribute('x2', String(width - padding.right));
        gridBottom.setAttribute('y1', String(padding.top + plotHeight));
        gridBottom.setAttribute('y2', String(padding.top + plotHeight));
        gridBottom.setAttribute('class', 'stat-line-grid');
        svg.appendChild(gridBottom);

        if (area) {
            const areaPath = document.createElementNS(svgNs, 'path');
            areaPath.setAttribute('d', area);
            areaPath.setAttribute('class', 'stat-line-area');
            svg.appendChild(areaPath);
        }

        if (path) {
            const linePath = document.createElementNS(svgNs, 'path');
            linePath.setAttribute('d', path);
            linePath.setAttribute('class', 'stat-line-path');
            svg.appendChild(linePath);
        }

        points.forEach((point, index) => {
            if (index !== 0 && index !== points.length - 1 && index % 6 !== 0) return;
            const circle = document.createElementNS(svgNs, 'circle');
            circle.setAttribute('cx', point.x.toFixed(1));
            circle.setAttribute('cy', point.y.toFixed(1));
            circle.setAttribute('r', index === points.length - 1 ? '3.5' : '2.4');
            circle.setAttribute('class', 'stat-line-point');
            svg.appendChild(circle);
        });

        [
            { text: formatNumber(max), y: padding.top + 4 },
            { text: formatNumber(min), y: padding.top + plotHeight }
        ].forEach((label) => {
            const text = document.createElementNS(svgNs, 'text');
            text.setAttribute('x', '2');
            text.setAttribute('y', String(label.y));
            text.setAttribute('class', 'stat-line-axis');
            text.textContent = label.text;
            svg.appendChild(text);
        });

        labels.forEach(({ entry, index }) => {
            const point = points[index];
            const text = document.createElementNS(svgNs, 'text');
            text.setAttribute('x', point.x.toFixed(1));
            text.setAttribute('y', String(height - 8));
            text.setAttribute('class', 'stat-line-label');
            text.textContent = entry.label;
            svg.appendChild(text);
        });

        chart.appendChild(svg);
        parent.appendChild(chart);
    }

    function appendRanking(parent, rows) {
        const max = Math.max(1, ...rows.map((row) => row.count));
        const list = document.createElement('div');
        list.className = 'stat-ranking-list';

        rows.forEach((row) => {
            const item = document.createElement('div');
            item.className = 'stat-ranking-row';

            const name = document.createElement('div');
            name.className = 'stat-ranking-name';
            name.textContent = row.name;

            const count = document.createElement('div');
            count.className = 'stat-ranking-count';
            count.textContent = String(row.count);

            const bar = document.createElement('div');
            bar.className = 'stat-ranking-bar';
            const fill = document.createElement('div');
            fill.className = 'stat-ranking-fill';
            fill.style.width = `${Math.max(4, Math.round((row.count / max) * 100))}%`;
            bar.appendChild(fill);

            item.append(name, count, bar);
            list.appendChild(item);
        });

        parent.appendChild(list);
    }

    function launchPeriodSeries(period, now = new Date()) {
        const successful = successfulLaunchHistoryForStats();
        if (period === 'week') {
            const bounds = periodBounds(now, 'week');
            const weekday = new Intl.DateTimeFormat(currentLocale(), { weekday: 'short' });
            return Array.from({ length: 7 }, (_, index) => {
                const start = addDays(bounds.currentStart, index);
                const end = addDays(start, 1);
                return {
                    label: weekday.format(start).replace('.', ''),
                    title: formatLocalShortDateTime(start),
                    value: successful.filter((entry) => entry.when >= start && entry.when < end).length
                };
            });
        }

        if (period === 'month') {
            const bounds = periodBounds(now, 'month');
            const end = addMonths(bounds.currentStart, 1);
            const days = Math.round((end - bounds.currentStart) / 86400000);
            return Array.from({ length: days }, (_, index) => {
                const start = addDays(bounds.currentStart, index);
                const dayEnd = addDays(start, 1);
                const showLabel = index === 0 || index === days - 1 || start.getDate() % 5 === 0;
                return {
                    label: showLabel ? String(start.getDate()) : '',
                    title: formatLocalShortDateTime(start),
                    value: successful.filter((entry) => entry.when >= start && entry.when < dayEnd).length
                };
            });
        }

        const bounds = periodBounds(now, 'year');
        const monthLabel = new Intl.DateTimeFormat(currentLocale(), { month: 'short' });
        return Array.from({ length: 12 }, (_, index) => {
            const start = new Date(bounds.currentStart);
            start.setMonth(index, 1);
            const end = addMonths(start, 1);
            return {
                label: monthLabel.format(start).replace('.', ''),
                title: monthLabel.format(start),
                value: successful.filter((entry) => entry.when >= start && entry.when < end).length
            };
        });
    }

    function renderLaunchPeriodStats(period) {
        const titleByPeriod = {
            week: t('stats.title.week'),
            month: t('stats.title.month'),
            year: t('stats.title.year')
        };
        const previousLabelByPeriod = {
            week: t('stats.previous.week'),
            month: t('stats.previous.month'),
            year: t('stats.previous.year')
        };
        const stats = state.launchSuccessStats?.[period] || launchSuccessStatsFromItems(state.launchHistoryItems)[period];
        const body = dom['stat-insight-body'];
        dom['stat-insight-title'].textContent = titleByPeriod[period] || t('stats.title.success');
        dom['stat-insight-subtitle'].textContent = state.launchHistoryLoading
            ? t('stats.subtitle.loadingHistory')
            : t('stats.subtitle.history', { count: state.launchHistoryItems.length });
        body.replaceChildren();
        appendStatSummary(body, [
            { label: t('stats.metric.current'), value: Number.isFinite(stats?.current) ? String(stats.current) : '--' },
            { label: previousLabelByPeriod[period], value: Number.isFinite(stats?.previous) ? String(stats.previous) : '--' }
        ]);
        appendBarChart(body, launchPeriodSeries(period));
        if (!state.launchHistoryItems.length && state.launchHistoryLoading) {
            appendEmptyStat(body, t('stats.empty.loadingTrend'));
        }
    }

    function renderProviderStats() {
        const now = Date.now();
        const since = now - PROVIDER_STATS_WINDOW_DAYS * 86400000;
        const providerCounts = new Map();
        launchHistoryForStats().forEach(({ launch, when }) => {
            const time = when.getTime();
            if (time < since || time > now) return;
            const provider = launchOrganization(launch) || t('common.unknown');
            providerCounts.set(provider, (providerCounts.get(provider) || 0) + 1);
        });

        const rows = Array.from(providerCounts, ([name, count]) => ({ name, count }))
            .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name))
            .slice(0, 12);
        const total = Array.from(providerCounts.values()).reduce((sum, count) => sum + count, 0);
        const body = dom['stat-insight-body'];
        dom['stat-insight-title'].textContent = t('stats.title.providers');
        dom['stat-insight-subtitle'].textContent = t('stats.subtitle.providers', { days: PROVIDER_STATS_WINDOW_DAYS });
        body.replaceChildren();
        appendStatSummary(body, [
            { label: t('stats.metric.total'), value: formatNumber(total) },
            { label: t('stats.metric.providers'), value: formatNumber(providerCounts.size) }
        ]);
        if (rows.length) {
            appendRanking(body, rows);
        } else {
            appendEmptyStat(body, state.launchHistoryLoading ? t('stats.empty.providersLoading') : t('stats.empty.providersNone'));
        }
    }

    function renderSatelliteLiveStats() {
        const body = dom['stat-insight-body'];
        const history = state.satelliteLiveHistory;
        const current = state.satelliteCatalogLoaded ? state.satelliteLiveCount : null;
        const oldest = history[0]?.time || Date.now();
        const latest = history[history.length - 1]?.time || Date.now();
        dom['stat-insight-title'].textContent = t('stats.title.live');
        dom['stat-insight-subtitle'].textContent = history.length > 1
            ? t('time.range', { start: formatLocalShortDateTime(new Date(oldest)), end: formatLocalShortDateTime(new Date(latest)) })
            : t('stats.subtitle.workerGlobal');
        body.replaceChildren();
        appendStatSummary(body, [
            { label: t('stats.metric.now'), value: Number.isFinite(current) ? formatNumber(current) : '--' },
            { label: t('stats.metric.samples'), value: formatNumber(history.length) }
        ]);
        if (history.length) {
            const series = history.slice(-48).map((sample, index, samples) => ({
                label: index === 0 || index === samples.length - 1 || index % 8 === 0
                    ? formatLocalTimeOnly(new Date(sample.time)).slice(0, 5)
                    : '',
                title: formatLocalShortDateTime(new Date(sample.time)),
                value: sample.liveCount
            }));
            appendLineChart(body, series);
        } else {
            appendEmptyStat(body, t('stats.empty.satHistory'));
        }
    }

    function renderStatsPanel() {
        if (!state.statsPanelOpen || !dom['stat-insight-body']) return;
        if (state.statsPanelMode === 'providers') {
            renderProviderStats();
            return;
        }
        if (state.statsPanelMode === 'sat-live') {
            renderSatelliteLiveStats();
            return;
        }
        const period = state.statsPanelMode.replace('launch-', '');
        renderLaunchPeriodStats(['week', 'month', 'year'].includes(period) ? period : 'week');
    }

    async function fetchSatelliteLiveHistory(force = false) {
        const now = Date.now();
        if (!force && state.satelliteLiveHistoryFetchedAt && now - state.satelliteLiveHistoryFetchedAt < SATELLITE_FETCH_INTERVAL_MS) {
            return state.satelliteLiveHistory;
        }
        try {
            const payload = await fetchStaticJson(SATELLITE_LIVE_HISTORY_DATA_URL);
            state.satelliteLiveHistory = (Array.isArray(payload?.samples) ? payload.samples : [])
                .map((sample) => ({
                    time: Date.parse(sample?.timestamp),
                    liveCount: Number(sample?.liveCount)
                }))
                .filter((sample) => Number.isFinite(sample.time) && Number.isFinite(sample.liveCount))
                .sort((a, b) => a.time - b.time);
            state.satelliteLiveHistoryGeneratedAt = payload?.generatedAt || '';
            state.satelliteLiveHistoryFetchedAt = now;
        } catch (error) {
            state.satelliteLiveHistoryFetchedAt = now;
        }
        if (state.statsPanelOpen && state.statsPanelMode === 'sat-live') renderStatsPanel();
        return state.satelliteLiveHistory;
    }

    function openStatsPanel(mode) {
        state.statsPanelOpen = true;
        state.statsPanelMode = mode;
        document.body.classList.add('stat-panel-open');
        dom['stat-insight-panel']?.setAttribute('aria-hidden', 'false');
        renderStatsPanel();
        if ((mode.startsWith('launch-') || mode === 'providers') && !state.launchHistoryItems.length && !state.launchHistoryLoading) {
            fetchLaunchHistoryPage().then(renderStatsPanel);
        }
        if (mode === 'sat-live' && !state.satelliteLiveHistory.length) {
            fetchSatelliteLiveHistory().then(renderStatsPanel);
        }
        if (isMobileViewport()) {
            openMobilePanel('stats');
        } else {
            applyMobilePanelState();
        }
    }

    function closeStatsPanel() {
        state.statsPanelOpen = false;
        state.statsPanelMode = '';
        document.body.classList.remove('stat-panel-open');
        dom['stat-insight-panel']?.setAttribute('aria-hidden', 'true');
        if (state.mobileActivePanel === 'stats') {
            state.mobileActivePanel = null;
        }
        applyMobilePanelState();
    }

    function updateLaunchSuccessStatsUi() {
        const stats = state.launchSuccessStats;
        if (!stats) {
            ['week', 'month', 'year'].forEach((period) => {
                const value = dom[`launch-stat-success-${period}`];
                const delta = dom[`launch-stat-success-${period}-delta`];
                if (value) value.textContent = '--';
                if (delta) delta.textContent = '--';
            });
            return;
        }

        dom['launch-stat-success-week'].textContent = String(stats.week.current);
        dom['launch-stat-success-month'].textContent = String(stats.month.current);
        dom['launch-stat-success-year'].textContent = String(stats.year.current);
        setStatDelta('launch-stat-success-week-delta', formatLaunchDelta(stats.week.current, stats.week.previous, t('stats.previous.week')));
        setStatDelta('launch-stat-success-month-delta', formatLaunchDelta(stats.month.current, stats.month.previous, t('stats.previous.month')));
        setStatDelta('launch-stat-success-year-delta', formatLaunchDelta(stats.year.current, stats.year.previous, t('stats.previous.year')));
        if (state.statsPanelOpen && state.statsPanelMode.startsWith('launch-')) renderStatsPanel();
    }

    function updateLaunchFeedModeUi() {
        dom['launch-feed-upcoming']?.setAttribute('aria-pressed', String(state.launchFeedMode === 'upcoming'));
        dom['launch-feed-history']?.setAttribute('aria-pressed', String(state.launchFeedMode === 'history'));
    }

    function setLaunchFeedStatus(text) {
        if (dom['launch-feed-status']) dom['launch-feed-status'].textContent = text;
    }

    function renderLaunchHistory(items) {
        if (!dom['launch-feed-items'] || state.launchFeedMode !== 'history') return;
        dom['launch-feed-items'].innerHTML = '';

        if (!items.length && state.launchHistoryLoading) {
            const loading = document.createElement('div');
            loading.className = 'launch-empty';
            loading.textContent = t('launch.historyLoading');
            dom['launch-feed-items'].appendChild(loading);
            return;
        }

        if (!items.length) {
            const empty = document.createElement('div');
            empty.className = 'launch-empty';
            empty.textContent = state.launchHistoryError || t('launch.historyEmpty');
            dom['launch-feed-items'].appendChild(empty);
            return;
        }

        items.forEach((launch) => {
            const item = document.createElement('button');
            item.type = 'button';
            item.className = 'launch-item launch-history-item';
            item.dataset.launchId = launchKey(launch);

            const title = document.createElement('div');
            title.className = 'launch-title';
            title.textContent = launch.name || t('launch.unnamed');

            const meta = document.createElement('div');
            meta.className = 'launch-provider';
            meta.textContent = launch.provider || launchOrganization(launch);

            const rocket = document.createElement('div');
            rocket.className = 'launch-rocket';
            rocket.textContent = launch.rocket || launchRocketName(launch);

            const pad = document.createElement('div');
            pad.className = 'launch-pad';
            pad.textContent = launch.pad || launchPadLabel(launch);

            const when = document.createElement('div');
            when.className = 'launch-net';
            const launchDate = launchInstant(launch);
            when.textContent = launchDate ? formatLocalShortDateTime(launchDate) : '--';

            const badgeData = launchStatusBadge(launch);
            const badge = document.createElement('div');
            badge.className = 'launch-history-status';
            if (badgeData.className) badge.classList.add(badgeData.className);
            badge.textContent = badgeData.text;

            item.append(title, meta, rocket, pad, when, badge);
            item.addEventListener('click', () => {
                state.launchWatchList.set(launchKey(launch), launch);
                selectLaunch(launchKey(launch), true);
            });
            dom['launch-feed-items'].appendChild(item);
        });

        if (state.launchHistoryLoading || !state.launchHistoryDone) {
            const footer = document.createElement('div');
            footer.className = 'launch-history-footer';
            footer.textContent = state.launchHistoryLoading ? t('launch.historyMoreLoading') : t('launch.historyMoreHint');
            dom['launch-feed-items'].appendChild(footer);
        }
    }

    function updateLaunchHistoryStatus() {
        if (state.launchFeedMode !== 'history') return;
        if (state.launchHistoryError) {
            setLaunchFeedStatus(t('launch.historyWaiting', { error: state.launchHistoryError }));
            return;
        }
        setLaunchFeedStatus(t('launch.historyStatus', { count: state.launchHistoryItems.length }));
    }

    async function fetchLaunchHistoryPage() {
        state.launchHistoryLoading = true;
        state.launchHistoryError = '';
        renderLaunchHistory(state.launchHistoryItems);
        try {
            const payload = await fetchStaticJson(LAUNCH_DB_DATA_URL);
            const items = Array.isArray(payload?.launches) ? payload.launches : [];
            const now = Date.now();
            state.launchHistoryItems = items
                .filter((launch) => belongsInLaunchHistory(launch, now))
                .sort((a, b) => {
                    const ta = launchInstant(a);
                    const tb = launchInstant(b);
                    if (!ta || !tb) return 0;
                    return tb - ta;
                });
            state.launchHistoryNextUrl = '';
            state.launchHistoryDone = true;
            state.launchHistoryError = '';
        } catch (error) {
            state.launchHistoryError = error?.message || t('launch.staticHistoryUnavailable');
        } finally {
            state.launchHistoryLoading = false;
            updateLaunchHistoryStatus();
            renderLaunchHistory(state.launchHistoryItems);
            if (state.statsPanelOpen && (state.statsPanelMode.startsWith('launch-') || state.statsPanelMode === 'providers')) {
                renderStatsPanel();
            }
        }
    }

    function renderActiveLaunchFeed() {
        updateLaunchFeedModeUi();
        if (state.launchFeedMode === 'history') {
            renderLaunchHistory(state.launchHistoryItems);
            updateLaunchHistoryStatus();
        } else {
            renderLaunchFeed(state.launches);
        }
    }

    function setLaunchFeedMode(mode) {
        if (state.launchFeedMode === mode) return;
        state.launchFeedMode = mode;
        renderActiveLaunchFeed();
        if (mode === 'history' && !state.launchHistoryItems.length) {
            fetchLaunchHistoryPage(true);
        } else if (mode === 'upcoming') {
            setLaunchFeedStatus(state.launches.length
                ? t('launch.workerStatus', { age: dataAgeLabel(state.launchDataGeneratedAt), count: state.launches.length })
                : t('launch.loading'));
        }
    }

    function handleLaunchFeedScroll() {
        // History is a complete static snapshot; no paginated API is used in the browser.
    }

    function renderLaunchFeed(items) {
        if (state.launchFeedMode !== 'upcoming') return;
        if (!dom['launch-feed-items']) return;
        dom['launch-feed-items'].innerHTML = '';
        if (!items.length) {
            const empty = document.createElement('div');
            empty.className = 'launch-empty';
            empty.textContent = t('launch.noUpcoming');
            dom['launch-feed-items'].appendChild(empty);
            return;
        }

        items.forEach((launch) => {
            const item = document.createElement('button');
            item.type = 'button';
            item.className = 'launch-item';
            item.dataset.launchId = launchKey(launch);

            const title = document.createElement('div');
            title.className = 'launch-title';
            title.textContent = launch.name || t('launch.unnamed');

            const provider = document.createElement('div');
            provider.className = 'launch-provider';
            provider.textContent = launchOrganization(launch);

            const rocket = document.createElement('div');
            rocket.className = 'launch-rocket';
            rocket.textContent = launchRocketName(launch);

            const pad = document.createElement('div');
            pad.className = 'launch-pad';
            pad.textContent = launchPadLabel(launch);

            const net = document.createElement('div');
            net.className = 'launch-net';
            const launchDate = launchInstant(launch);
            net.textContent = launchDate ? formatLocalShortDateTime(launchDate) : '--';

            const countdown = document.createElement('div');
            countdown.className = 'launch-countdown';
            countdown.dataset.launchId = launchKey(launch);
            countdown.dataset.net = launchDate?.toISOString() || '';
            countdown.textContent = formatLaunchCountdown(launchDate);
            applyLaunchStatusClass(countdown, launch);

            item.append(title, provider, rocket, pad, net, countdown);
            item.addEventListener('click', () => {
                selectLaunch(launchKey(launch), true);
            });
            dom['launch-feed-items'].appendChild(item);
        });

        refreshSelectedLaunchUi();
    }

    function updateOverviewStats() {
        if (!state.launches.length) {
            dom['launch-stat-total'].textContent = '--';
            dom['launch-stat-countdown'].textContent = '--';
            dom['launch-stat-orgs'].textContent = '--';
            dom['launch-stat-pads'].textContent = '--';
            updateLaunchSuccessStatsUi();
            updateSatelliteStats();
            return;
        }

        const nextLaunch = state.launches[0];
        const providers = new Set(state.launches.map((launch) => launchOrganization(launch)));
        const pads = new Set(state.launches.map((launch) => launchPadLabel(launch)));
        dom['launch-stat-total'].textContent = String(state.launches.length);
        dom['launch-stat-countdown'].textContent = formatLaunchCountdown(launchInstant(nextLaunch));
        dom['launch-stat-orgs'].textContent = String(providers.size);
        dom['launch-stat-pads'].textContent = String(pads.size);
        updateLaunchSuccessStatsUi();
        updateSatelliteStats();
    }

    function getSelectedLaunch() {
        if (state.selectedLaunchId && state.launchWatchList.has(state.selectedLaunchId)) {
            return state.launches.find((launch) => launchKey(launch) === state.selectedLaunchId) ||
                state.launchWatchList.get(state.selectedLaunchId);
        }
        if (state.launches.length) return state.launches[0];
        return monitoredLaunches()[0] || null;
    }

    function launchMarkerLaunches() {
        const markerLaunches = [];
        const seen = new Set();
        const addLaunch = (launch) => {
            if (!launch || !isEarthLaunch(launch)) return;
            const key = launchKey(launch);
            if (seen.has(key)) return;
            seen.add(key);
            markerLaunches.push(launch);
        };

        state.launches.forEach(addLaunch);
        if (state.selectedLaunchId && state.launchWatchList.has(state.selectedLaunchId)) {
            addLaunch(state.launchWatchList.get(state.selectedLaunchId));
        }
        return markerLaunches;
    }

    function refreshSelectedLaunchUi() {
        const launch = state.selectedLaunchId || state.launchDetailActive ? getSelectedLaunch() : null;
        document.querySelectorAll('.launch-item[data-launch-id]').forEach((item) => {
            item.classList.toggle('active', item.dataset.launchId === state.selectedLaunchId);
        });
        state.launchMarkers.forEach((marker, key) => {
            marker.group.userData.active = key === state.selectedLaunchId;
        });
        if (!launch) {
            updateLaunchStreamUi(null);
            clearLaunchTrajectory();
            if (dom['watch-launch-title']) dom['watch-launch-title'].textContent = t('launch.selected');
            if (dom['watch-launch-subtitle']) dom['watch-launch-subtitle'].textContent = '--';
            if (dom['watch-launch-intel']) dom['watch-launch-intel'].textContent = t('launch.chooseFromFeed');
            return;
        }

        const when = launchInstant(launch);
        dom['watch-launch-title'].textContent = launch.name || t('launch.unnamed');
        dom['watch-launch-subtitle'].textContent =
            launch?.padLocation ||
            launch?.orbit ||
            launch?.pad?.location?.name ||
            launch?.mission?.orbit?.name ||
            launchStatusLabel(launch);
        dom['watch-launch-provider'].textContent = launchOrganization(launch);
        dom['watch-launch-rocket'].textContent = launchRocketName(launch);
        dom['watch-launch-status'].textContent = launchStatusLabel(launch);
        dom['watch-launch-countdown'].textContent = formatLaunchCountdown(when);
        applyLaunchStatusClass(dom['watch-launch-status'], launch);
        applyLaunchStatusClass(dom['watch-launch-countdown'], launch);
        dom['watch-launch-pad'].textContent = launchPadLabel(launch);
        dom['watch-launch-window'].textContent = when ? formatLocalShortDateTime(when) : '--';
        dom['watch-launch-coords'].textContent = formatCoordinates(launch);
        dom['watch-launch-story'].textContent = launchStory(launch);
        if (dom['watch-launch-intel']) dom['watch-launch-intel'].textContent = launchIntelText(launch);
        updateLaunchStreamUi(launch);
        updateSelectedLaunchTrajectory(launch);

        const externalUrl = launchExternalUrl(launch);
        if (externalUrl) {
            dom['watch-launch-link'].href = externalUrl;
            dom['watch-launch-link'].classList.remove('is-hidden');
        } else {
            dom['watch-launch-link'].href = '#';
            dom['watch-launch-link'].classList.add('is-hidden');
        }
    }

    function selectLaunch(launchId, focus) {
        state.selectedLaunchId = launchId;
        state.launchDetailActive = true;
        rebuildLaunchMarkers();
        applyLaunchDetailPanelState();
        refreshSelectedLaunchUi();
        const keepSatelliteContext = isMobileViewport() && Boolean(state.followSatelliteId);
        if (focus && !keepSatelliteContext) focusSelectedLaunch();
        if (isMobileViewport()) openMobilePanel('launch');
    }

    async function fetchLaunchSuccessStats(force = false) {
        const now = Date.now();
        if (!force && state.launchSuccessStats && now - state.launchSuccessStatsFetchedAt < LAUNCH_DATA_REFRESH_MS) {
            updateLaunchSuccessStatsUi();
            return;
        }

        try {
            const payload = await fetchStaticJson(LAUNCH_STATS_DATA_URL);
            state.launchSuccessStats = {
                week: payload?.week || { current: 0, previous: null },
                month: payload?.month || { current: 0, previous: null },
                year: payload?.year || { current: 0, previous: null }
            };
            state.launchSuccessStatsFetchedAt = now;
        } catch (error) {
            if (!state.launchSuccessStats) {
                state.launchSuccessStats = launchSuccessStatsFromItems(state.launchHistoryItems);
                state.launchSuccessStatsFetchedAt = now;
            }
        } finally {
            updateLaunchSuccessStatsUi();
        }
    }

    async function fetchLaunches() {
        if (state.launchFeedMode === 'upcoming') setLaunchFeedStatus(t('launch.loading'));

        try {
            const payload = await fetchStaticJson(LAUNCH_FEED_DATA_URL);
            const launches = (Array.isArray(payload?.launches) ? payload.launches : [])
                .filter(isEarthLaunch)
                .filter(belongsInUpcomingLaunch)
                .sort((a, b) => {
                    const ta = launchInstant(a);
                    const tb = launchInstant(b);
                    if (!ta || !tb) return 0;
                    return ta - tb;
                })
                .slice(0, 24);

            state.launches = launches;
            state.launchDataGeneratedAt = payload?.generatedAt || '';
            state.launchDataSource = payload?.source || '';
            rememberLaunchesForMonitoring(state.launches);
            const selectedStillAvailable = state.selectedLaunchId && (
                state.launches.some((launch) => launchKey(launch) === state.selectedLaunchId) ||
                state.launchWatchList.has(state.selectedLaunchId)
            );
            if (state.selectedLaunchId && !selectedStillAvailable) {
                state.selectedLaunchId = null;
                state.launchDetailActive = false;
                applyLaunchDetailPanelState();
            }

            renderLaunchFeed(state.launches);
            updateOverviewStats();
            rebuildLaunchMarkers();

            if (state.launchFeedMode === 'upcoming') {
                setLaunchFeedStatus(t('launch.workerStatus', { age: dataAgeLabel(state.launchDataGeneratedAt), count: state.launches.length }));
            }
        } catch (error) {
            state.launches = [];
            rememberLaunchesForMonitoring([]);
            state.selectedLaunchId = null;
            state.launchDetailActive = false;
            applyLaunchDetailPanelState();
            renderLaunchFeed([]);
            updateOverviewStats();
            rebuildLaunchMarkers();
            if (state.launchFeedMode === 'upcoming') {
                setLaunchFeedStatus(`${t('launch.staticUnavailable')}${error?.message ? ` (${error.message})` : '.'}`);
            }
        }
    }
    function tickLaunchCountdowns() {
        document.querySelectorAll('.launch-countdown[data-net]').forEach((element) => {
            const iso = element.getAttribute('data-net');
            if (!iso) return;
            const launch = state.launches.find((item) => launchKey(item) === element.dataset.launchId);
            element.textContent = formatLaunchCountdown(new Date(iso));
            applyLaunchStatusClass(element, launch);
        });
        updateOverviewStats();
        refreshSelectedLaunchUi();
    }

    function initLaunchFeed() {
        dom['launch-feed-refresh']?.addEventListener('click', () => {
            if (state.launchFeedMode === 'history') {
                fetchLaunchHistoryPage(true);
            } else {
                fetchLaunches();
            }
        });
        dom['launch-feed-upcoming']?.addEventListener('click', () => setLaunchFeedMode('upcoming'));
        dom['launch-feed-history']?.addEventListener('click', () => setLaunchFeedMode('history'));
        dom['launch-feed-items']?.addEventListener('scroll', handleLaunchFeedScroll);
        updateLaunchFeedModeUi();
        fetchLaunches();
        fetchLaunchHistoryPage();
        fetchLaunchSuccessStats();
        if (state.launchFeedFetchTimer) clearInterval(state.launchFeedFetchTimer);
        if (state.launchCountdownTimer) clearInterval(state.launchCountdownTimer);
        if (state.launchHistoryFetchTimer) clearInterval(state.launchHistoryFetchTimer);
        state.launchFeedFetchTimer = setInterval(fetchLaunches, LAUNCH_DATA_REFRESH_MS);
        state.launchCountdownTimer = setInterval(tickLaunchCountdowns, 1000);
        state.launchHistoryFetchTimer = setInterval(() => {
            fetchLaunchHistoryPage();
            fetchLaunchSuccessStats(true);
        }, LAUNCH_DATA_REFRESH_MS);
    }

    function latLonToVector3(lat, lon, radius) {
        const phi = THREE.MathUtils.degToRad(90 - lat);
        const theta = THREE.MathUtils.degToRad(lon + 180);
        return new THREE.Vector3(
            -radius * Math.sin(phi) * Math.cos(theta),
            radius * Math.cos(phi),
            radius * Math.sin(phi) * Math.sin(theta)
        );
    }

    function normalizeLongitude(lon) {
        return THREE.MathUtils.euclideanModulo(lon + 540, 360) - 180;
    }

    function destinationLatLon(latDeg, lonDeg, bearingDeg, angularDistanceDeg) {
        const lat = THREE.MathUtils.degToRad(latDeg);
        const lon = THREE.MathUtils.degToRad(lonDeg);
        const bearing = THREE.MathUtils.degToRad(bearingDeg);
        const angularDistance = THREE.MathUtils.degToRad(angularDistanceDeg);
        const sinLat = Math.sin(lat);
        const cosLat = Math.cos(lat);
        const sinDistance = Math.sin(angularDistance);
        const cosDistance = Math.cos(angularDistance);
        const lat2 = Math.asin(
            sinLat * cosDistance +
            cosLat * sinDistance * Math.cos(bearing)
        );
        const lon2 = lon + Math.atan2(
            Math.sin(bearing) * sinDistance * cosLat,
            cosDistance - sinLat * Math.sin(lat2)
        );
        return {
            lat: THREE.MathUtils.radToDeg(lat2),
            lon: normalizeLongitude(THREE.MathUtils.radToDeg(lon2))
        };
    }

    function launchTrajectoryReferenceMs(launch) {
        const when = launchInstant(launch);
        return when ? when.getTime() : earthReferenceTimeMs();
    }

    function earthFixedToInertial(localPosition, dateMs) {
        return localPosition.clone().applyAxisAngle(new THREE.Vector3(0, 1, 0), earthRotationAngleForMs(dateMs));
    }

    function inertialToEarthFixed(inertialPosition, dateMs) {
        return inertialPosition.clone().applyAxisAngle(new THREE.Vector3(0, 1, 0), -earthRotationAngleForMs(dateMs));
    }

    function launchAscentDurationMs(profile) {
        if (profile.kind === 'suborbital') {
            return profile.downrangeDeg > 60 ? 45 * 60 * 1000 : 14 * 60 * 1000;
        }
        return Number.isFinite(profile.transferApogeeKm) ? 11.5 * 60 * 1000 : 9 * 60 * 1000;
    }

    function orbitPeriodMsForSemiMajorRadius(radius) {
        const radiusKm = Math.max(WGS84_EARTH_RADIUS_KM + 120, radius * 1000);
        return 2 * Math.PI * Math.sqrt((radiusKm ** 3) / EARTH_MU_KM3_S2) * 1000;
    }

    function launchTrajectoryText(launch) {
        const missionName = typeof launch?.mission === 'string'
            ? launch.mission
            : launch?.mission?.name;
        const orbitName = typeof launch?.orbit === 'string'
            ? launch.orbit
            : launch?.orbit?.name;
        return [
            launch?.name,
            launchRocketName(launch),
            launchOrganization(launch),
            missionName,
            launch?.missionDescription,
            orbitName,
            launch?.mission?.orbit?.name,
            launch?.mission?.orbit?.abbrev,
            launch?.padLocation,
            launchPadLabel(launch)
        ].filter((value) => typeof value === 'string' && value.trim())
            .join(' ')
            .toLowerCase();
    }

    function launchOrbitText(launch) {
        const orbitName = typeof launch?.orbit === 'string'
            ? launch.orbit
            : launch?.orbit?.name;
        return [
            orbitName,
            launch?.orbit?.abbrev,
            launch?.mission?.orbit?.name,
            launch?.mission?.orbit?.abbrev
        ].filter((value) => typeof value === 'string' && value.trim())
            .join(' ')
            .toLowerCase();
    }

    function extractAltitudeKm(text) {
        const values = [];
        for (const match of text.matchAll(/(\d+(?:[.,]\d+)?)\s*km\b/g)) {
            const value = parseFloat(match[1].replace(',', '.'));
            if (Number.isFinite(value) && value >= 100 && value <= 60000) {
                values.push(value);
            }
        }
        if (!values.length) return NaN;
        values.sort((a, b) => a - b);
        return values[Math.floor(values.length / 2)];
    }

    function clampReachableInclination(inclinationDeg, latDeg) {
        const minInclination = Math.min(89.5, Math.abs(latDeg));
        if (!Number.isFinite(inclinationDeg)) return Math.max(minInclination, 28.5);
        return THREE.MathUtils.clamp(inclinationDeg, minInclination, 116);
    }

    function prefersNorthboundPolarLaunch(text, latDeg) {
        if (/plesetsk|vostochny|baikonur/.test(text)) return true;
        if (/vandenberg|mahia|wallops|kennedy|cape canaveral|rocket lab|tanegashima|xichang|taiyuan|jiuquan|wenchang/.test(text)) {
            return false;
        }
        return latDeg > 55;
    }

    function suborbitalLaunchAzimuth(text, latDeg, lonDeg) {
        if (/starship|super heavy|starbase|boca chica/.test(text)) return 95;
        if (/wallops|virginia|haste|hypersonic/.test(text)) return 105;
        if (/vandenberg/.test(text)) return 190;
        if (/mahia|rocket lab launch complex 1/.test(text)) return 125;
        if (/kourou|french guiana/.test(text)) return 80;
        if (latDeg > 0 && lonDeg < -20) return 95;
        return latDeg < 0 ? 70 : 90;
    }

    function launchAzimuthForInclination(latDeg, lonDeg, inclinationDeg, launch, text) {
        if (!Number.isFinite(inclinationDeg)) {
            return suborbitalLaunchAzimuth(text, latDeg, lonDeg);
        }
        const latRad = THREE.MathUtils.degToRad(latDeg);
        const incRad = THREE.MathUtils.degToRad(inclinationDeg);
        const ratio = THREE.MathUtils.clamp(Math.cos(incRad) / Math.max(0.01, Math.cos(latRad)), -1, 1);
        const branch = THREE.MathUtils.radToDeg(Math.asin(ratio));
        const northbound = THREE.MathUtils.euclideanModulo(branch, 360);
        const southbound = THREE.MathUtils.euclideanModulo(180 - branch, 360);

        if (/vandenberg|space launch complex 4e|slc-4|vsfb/.test(text)) {
            return southbound;
        }
        if (inclinationDeg >= 88.5) {
            return prefersNorthboundPolarLaunch(text, latDeg) ? northbound : southbound;
        }
        if (latDeg < -1) return southbound;
        return northbound;
    }

    function inferLaunchTrajectoryProfile(launch) {
        const text = launchTrajectoryText(launch);
        const orbitText = launchOrbitText(launch);
        const lat = launchLatitude(launch);
        const lon = launchLongitude(launch);
        const explicitAltitudeKm = extractAltitudeKm(text);
        const absLat = Math.abs(lat);
        const profile = {
            kind: 'orbital',
            targetAltitudeKm: Number.isFinite(explicitAltitudeKm) ? explicitAltitudeKm : 420,
            insertionAltitudeKm: 210,
            transferApogeeKm: NaN,
            inclinationDeg: Math.max(absLat, 51.6),
            downrangeDeg: 23,
            orbitPreviewRevolutions: 0.72
        };

        if (/sub.?orbital/.test(orbitText)) {
            const starship = /starship|super heavy|starbase|boca chica/.test(text);
            const hypersonic = /haste|hypersonic/.test(text);
            profile.kind = 'suborbital';
            profile.targetAltitudeKm = Number.isFinite(explicitAltitudeKm)
                ? explicitAltitudeKm
                : starship ? 220 : hypersonic ? 150 : 120;
            profile.insertionAltitudeKm = profile.targetAltitudeKm;
            profile.inclinationDeg = NaN;
            profile.downrangeDeg = starship ? 142 : hypersonic ? 12 : 7;
            profile.orbitPreviewRevolutions = 0;
        } else if (/geostationary transfer|\bgto\b/.test(orbitText)) {
            profile.targetAltitudeKm = 35786;
            profile.transferApogeeKm = 35786;
            profile.insertionAltitudeKm = 185;
            profile.inclinationDeg = Math.max(absLat, 27);
            profile.downrangeDeg = 29;
        } else if (/geostationary|\bgeo\b/.test(orbitText)) {
            profile.targetAltitudeKm = 35786;
            profile.transferApogeeKm = 35786;
            profile.insertionAltitudeKm = 185;
            profile.inclinationDeg = Math.max(absLat, 0);
            profile.downrangeDeg = 29;
        } else if (/medium earth|\bmeo\b/.test(orbitText) || /gps|galileo|glonass|beidou/.test(text)) {
            profile.targetAltitudeKm = Number.isFinite(explicitAltitudeKm) ? explicitAltitudeKm : 20200;
            profile.transferApogeeKm = profile.targetAltitudeKm;
            profile.insertionAltitudeKm = 185;
            profile.inclinationDeg = /gps/.test(text) ? 55 : 56;
            profile.downrangeDeg = 28;
        } else if (/elliptical|highly elliptical|\bheo\b/.test(orbitText) || /molniya/.test(text)) {
            profile.targetAltitudeKm = Number.isFinite(explicitAltitudeKm) ? explicitAltitudeKm : 12000;
            profile.transferApogeeKm = profile.targetAltitudeKm;
            profile.insertionAltitudeKm = 185;
            profile.inclinationDeg = /molniya/.test(text) ? 63.4 : Math.max(absLat, 51.6);
            profile.downrangeDeg = 27;
        } else if (/sun.?synchronous|\bsso\b/.test(orbitText)) {
            profile.targetAltitudeKm = Number.isFinite(explicitAltitudeKm) ? explicitAltitudeKm : 600;
            profile.insertionAltitudeKm = THREE.MathUtils.clamp(profile.targetAltitudeKm * 0.82, 360, 620);
            profile.inclinationDeg = 97.5;
            profile.downrangeDeg = 22;
        } else if (/polar/.test(orbitText)) {
            profile.targetAltitudeKm = Number.isFinite(explicitAltitudeKm) ? explicitAltitudeKm : 600;
            profile.insertionAltitudeKm = THREE.MathUtils.clamp(profile.targetAltitudeKm * 0.78, 300, 620);
            profile.inclinationDeg = 90;
            profile.downrangeDeg = 21;
        } else if (/iss|international space station|cargo dragon|crew dragon|cygnus|tiangong/.test(text)) {
            profile.targetAltitudeKm = Number.isFinite(explicitAltitudeKm) ? explicitAltitudeKm : 420;
            profile.insertionAltitudeKm = 210;
            profile.inclinationDeg = /tiangong/.test(text) ? 41.5 : 51.64;
            profile.downrangeDeg = 24;
        } else if (/starlink/.test(text)) {
            profile.targetAltitudeKm = Number.isFinite(explicitAltitudeKm) ? explicitAltitudeKm : 550;
            profile.insertionAltitudeKm = 290;
            profile.inclinationDeg = /vandenberg|space launch complex 4e|group 17/.test(text) ? 70 : 53;
            profile.downrangeDeg = 25;
        } else if (/kuiper|amazon leo/.test(text)) {
            profile.targetAltitudeKm = Number.isFinite(explicitAltitudeKm) ? explicitAltitudeKm : 610;
            profile.insertionAltitudeKm = 320;
            profile.inclinationDeg = 51.9;
            profile.downrangeDeg = 24;
        } else if (/low earth|\bleo\b/.test(orbitText)) {
            profile.targetAltitudeKm = Number.isFinite(explicitAltitudeKm) ? explicitAltitudeKm : 500;
            profile.insertionAltitudeKm = THREE.MathUtils.clamp(profile.targetAltitudeKm * 0.65, 210, 520);
            profile.inclinationDeg = Math.max(absLat, 51.6);
            profile.downrangeDeg = 23;
        } else if (/unknown/.test(orbitText)) {
            profile.targetAltitudeKm = 420;
            profile.insertionAltitudeKm = 210;
            profile.inclinationDeg = Math.max(absLat, 45);
            profile.downrangeDeg = 21;
            profile.orbitPreviewRevolutions = 0.45;
        }

        profile.inclinationDeg = clampReachableInclination(profile.inclinationDeg, lat);
        profile.azimuthDeg = profile.kind === 'suborbital'
            ? suborbitalLaunchAzimuth(text, lat, lon)
            : launchAzimuthForInclination(lat, lon, profile.inclinationDeg, launch, text);
        return profile;
    }

    function ascentAltitudeKm(profile, t) {
        const clamped = THREE.MathUtils.clamp(t, 0, 1);
        if (profile.kind === 'suborbital') {
            return profile.targetAltitudeKm * Math.pow(Math.sin(Math.PI * clamped), 0.86);
        }
        const gravityTurn = (1 - Math.exp(-4.25 * clamped)) / (1 - Math.exp(-4.25));
        return profile.insertionAltitudeKm * gravityTurn;
    }

    function buildLaunchAscentPath(launch, profile, referenceMs) {
        const lat = launchLatitude(launch);
        const lon = launchLongitude(launch);
        const ascentPoints = [];
        const groundTrackPoints = [];
        const groundTrackRadius = ARTEMIS.EARTH_RADIUS * 1.018;
        const ascentDurationMs = launchAscentDurationMs(profile);
        for (let i = 0; i <= LAUNCH_ASCENT_SAMPLE_COUNT; i += 1) {
            const t = i / LAUNCH_ASCENT_SAMPLE_COUNT;
            const downrangeT = profile.kind === 'suborbital' ? t : Math.pow(t, 1.42);
            const ground = destinationLatLon(lat, lon, profile.azimuthDeg, profile.downrangeDeg * downrangeT);
            const altitudeKm = ascentAltitudeKm(profile, t);
            const sampleMs = referenceMs + ascentDurationMs * t;
            const localPoint = latLonToVector3(ground.lat, ground.lon, ARTEMIS.EARTH_RADIUS + altitudeKm / 1000);
            ascentPoints.push(earthFixedToInertial(localPoint, sampleMs));
            groundTrackPoints.push(latLonToVector3(ground.lat, ground.lon, groundTrackRadius));
        }
        return { ascentPoints, groundTrackPoints, ascentDurationMs };
    }

    function launchOrbitBasis(ascentPoints) {
        const insertion = ascentPoints[ascentPoints.length - 1]?.clone();
        if (!insertion) return null;
        const radial = insertion.clone().normalize();
        let tangent = insertion.clone().sub(ascentPoints[Math.max(0, ascentPoints.length - 5)] || insertion);
        tangent.sub(radial.clone().multiplyScalar(tangent.dot(radial)));
        if (tangent.lengthSq() < 1e-8) {
            tangent = new THREE.Vector3(0, 1, 0).cross(radial);
        }
        if (tangent.lengthSq() < 1e-8) {
            tangent = new THREE.Vector3(1, 0, 0).cross(radial);
        }
        tangent.normalize();
        const normal = new THREE.Vector3().crossVectors(radial, tangent).normalize();
        const correctedTangent = new THREE.Vector3().crossVectors(normal, radial).normalize();
        return { radial, tangent: correctedTangent };
    }

    function transferElapsedMs(trueAnomaly, eccentricity, periodMs) {
        if (trueAnomaly <= 0) return 0;
        const halfAngle = trueAnomaly / 2;
        const eccentricAnomaly = 2 * Math.atan2(
            Math.sqrt(1 - eccentricity) * Math.sin(halfAngle),
            Math.sqrt(1 + eccentricity) * Math.cos(halfAngle)
        );
        const normalizedEccentricAnomaly = eccentricAnomaly < 0
            ? eccentricAnomaly + Math.PI * 2
            : eccentricAnomaly;
        const meanAnomaly = normalizedEccentricAnomaly - eccentricity * Math.sin(normalizedEccentricAnomaly);
        return (meanAnomaly / (Math.PI * 2)) * periodMs;
    }

    function buildLaunchOrbitSamples(profile, ascentPoints, revolutions, options = {}) {
        if (profile.kind === 'suborbital' || revolutions <= 0) return [];
        const basis = launchOrbitBasis(ascentPoints);
        if (!basis) return [];
        const samples = [];

        if (Number.isFinite(profile.transferApogeeKm)) {
            const rp = ascentPoints[ascentPoints.length - 1].length();
            const ra = ARTEMIS.EARTH_RADIUS + profile.transferApogeeKm / 1000;
            const semiMajor = (rp + ra) / 2;
            const eccentricity = THREE.MathUtils.clamp((ra - rp) / (ra + rp), 0, 0.95);
            const periodMs = orbitPeriodMsForSemiMajorRadius(semiMajor);
            const transferFraction = THREE.MathUtils.clamp(options.transferFraction ?? revolutions * 0.35, 0.05, 1);
            const maxTrueAnomaly = Math.PI * transferFraction;
            for (let i = 0; i <= LAUNCH_ORBIT_PREVIEW_SAMPLE_COUNT; i += 1) {
                const trueAnomaly = maxTrueAnomaly * (i / LAUNCH_ORBIT_PREVIEW_SAMPLE_COUNT);
                const radius = semiMajor * (1 - eccentricity * eccentricity) /
                    (1 + eccentricity * Math.cos(trueAnomaly));
                samples.push({
                    position: basis.radial.clone().multiplyScalar(Math.cos(trueAnomaly) * radius)
                        .add(basis.tangent.clone().multiplyScalar(Math.sin(trueAnomaly) * radius)),
                    elapsedMs: transferElapsedMs(trueAnomaly, eccentricity, periodMs)
                });
            }
            return samples;
        }

        const startRadius = ascentPoints[ascentPoints.length - 1].length();
        const targetRadius = ARTEMIS.EARTH_RADIUS + profile.targetAltitudeKm / 1000;
        const periodMs = orbitPeriodMsForSemiMajorRadius(targetRadius);
        const sampleCount = Math.max(24, Math.floor(LAUNCH_ORBIT_PREVIEW_SAMPLE_COUNT * revolutions));
        const maxAngle = Math.PI * 2 * revolutions;
        for (let i = 0; i <= sampleCount; i += 1) {
            const angle = maxAngle * (i / sampleCount);
            const raiseT = Math.min(1, angle / Math.max(0.001, Math.PI * 0.58));
            const radius = THREE.MathUtils.lerp(
                startRadius,
                targetRadius,
                1 - Math.pow(1 - raiseT, 3)
            );
            samples.push({
                position: basis.radial.clone().multiplyScalar(Math.cos(angle) * radius)
                    .add(basis.tangent.clone().multiplyScalar(Math.sin(angle) * radius)),
                elapsedMs: (angle / (Math.PI * 2)) * periodMs
            });
        }
        return samples;
    }

    function buildLaunchOrbitPreviewPath(profile, ascentPoints) {
        return buildLaunchOrbitSamples(profile, ascentPoints, profile.orbitPreviewRevolutions, {
            transferFraction: 0.58
        }).map((sample) => sample.position);
    }

    function buildLaunchGroundTrackExtension(profile, ascentPoints, referenceMs, ascentDurationMs) {
        const revolutions = clampLaunchGroundTrackRevolutions(state.panelVisibility.launchGroundTrackRevolutions);
        const transferFraction = Number.isFinite(profile.transferApogeeKm)
            ? Math.min(1, 0.35 * revolutions)
            : undefined;
        const orbitSamples = buildLaunchOrbitSamples(profile, ascentPoints, revolutions, { transferFraction });
        const groundTrackRadius = ARTEMIS.EARTH_RADIUS * 1.018;
        return orbitSamples.map((sample) => {
            const local = inertialToEarthFixed(sample.position, referenceMs + ascentDurationMs + sample.elapsedMs);
            return local.normalize().multiplyScalar(groundTrackRadius);
        });
    }

    function setLaunchLinePoints(line, points) {
        if (!line) return;
        line.geometry.dispose();
        line.geometry = new THREE.BufferGeometry().setFromPoints(points);
        line.visible = points.length >= 2;
        if (line.visible) line.computeLineDistances();
    }

    function syncLaunchTrajectoryFrame() {
        if (!state.launchTrajectoryFrame || !state.launchTrajectoryEventMs) return;
        const displayRotation = earthRotationAngleForMs(earthReferenceTimeMs());
        const eventRotation = earthRotationAngleForMs(state.launchTrajectoryEventMs);
        state.launchTrajectoryFrame.rotation.y = displayRotation - eventRotation;
    }

    function clearLaunchTrajectory() {
        state.launchTrajectoryKey = '';
        state.launchTrajectoryEventMs = 0;
        if (state.launchTrajectoryFrame) state.launchTrajectoryFrame.rotation.y = 0;
        [state.launchTrajectoryLine, state.launchTrajectoryGroundTrackLine, state.launchTrajectoryOrbitLine]
            .forEach((line) => {
                if (line) line.visible = false;
            });
        updateSatelliteLayerOpacity();
    }

    function updateSelectedLaunchTrajectory(launch) {
        if (!launch || !isEarthLaunch(launch)) {
            clearLaunchTrajectory();
            return;
        }
        const profile = inferLaunchTrajectoryProfile(launch);
        const groundTrackRevolutions = clampLaunchGroundTrackRevolutions(state.panelVisibility.launchGroundTrackRevolutions);
        const key = [
            launchKey(launch),
            launchLatitude(launch).toFixed(5),
            launchLongitude(launch).toFixed(5),
            profile.kind,
            Math.round(profile.azimuthDeg * 10),
            Math.round(profile.inclinationDeg * 10),
            Math.round(profile.targetAltitudeKm),
            Math.round(profile.insertionAltitudeKm),
            Math.round(Number.isFinite(profile.transferApogeeKm) ? profile.transferApogeeKm : 0),
            groundTrackRevolutions,
            Math.floor(launchTrajectoryReferenceMs(launch) / 60000)
        ].join(':');
        if (state.launchTrajectoryKey === key) return;

        const referenceMs = launchTrajectoryReferenceMs(launch);
        const { ascentPoints, groundTrackPoints, ascentDurationMs } = buildLaunchAscentPath(launch, profile, referenceMs);
        const orbitPreviewPoints = buildLaunchOrbitPreviewPath(profile, ascentPoints);
        const groundTrackExtensionPoints = buildLaunchGroundTrackExtension(profile, ascentPoints, referenceMs, ascentDurationMs);
        setLaunchLinePoints(state.launchTrajectoryLine, ascentPoints);
        setLaunchLinePoints(state.launchTrajectoryGroundTrackLine, groundTrackPoints.concat(groundTrackExtensionPoints.slice(1)));
        setLaunchLinePoints(state.launchTrajectoryOrbitLine, orbitPreviewPoints);
        state.launchTrajectoryEventMs = referenceMs;
        syncLaunchTrajectoryFrame();
        state.launchTrajectoryKey = key;
        updateSatelliteLayerOpacity();
    }

    function updateObserverMarker() {
        if (!state.observerMarker) return;
        const location = state.observerLocation;
        if (!location) {
            state.observerMarker.visible = false;
            return;
        }

        const anchor = latLonToVector3(location.lat, location.lon, ARTEMIS.EARTH_RADIUS + 0.02);
        const normal = anchor.clone().normalize();
        state.observerMarker.visible = true;
        state.observerMarker.position.copy(anchor);
        state.observerMarker.quaternion.setFromUnitVectors(new THREE.Vector3(0, 1, 0), normal);
    }

    function initObserverLocation() {
        if (!('geolocation' in navigator)) return;

        const onSuccess = (position) => {
            state.observerLocation = {
                lat: position.coords.latitude,
                lon: position.coords.longitude,
                accuracy: position.coords.accuracy
            };
            updateObserverMarker();
        };

        navigator.geolocation.getCurrentPosition(onSuccess, () => { /* optional */ }, {
            enableHighAccuracy: true,
            maximumAge: 60000,
            timeout: 12000
        });

        state.observerWatchId = navigator.geolocation.watchPosition(onSuccess, () => { /* optional */ }, {
            enableHighAccuracy: true,
            maximumAge: 30000,
            timeout: 15000
        });
    }

    function getOrbitElementsFromSatrec(satrec) {
        const eccentricity = Number.isFinite(satrec?.ecco) ? Math.max(0, satrec.ecco) : 0;
        const inclinationDeg = Number.isFinite(satrec?.inclo) ? THREE.MathUtils.radToDeg(satrec.inclo) : 0;
        const meanMotionRadPerMinute = Number.isFinite(satrec?.no_kozai)
            ? satrec.no_kozai
            : Number.isFinite(satrec?.no)
                ? satrec.no
                : NaN;

        if (!Number.isFinite(meanMotionRadPerMinute) || meanMotionRadPerMinute <= 0) {
            return {
                eccentricity,
                inclinationDeg,
                semiMajorAxisKm: NaN,
                periodMinutes: NaN,
                perigeeKm: NaN,
                apogeeKm: NaN
            };
        }

        const meanMotionRadPerSecond = meanMotionRadPerMinute / 60;
        const semiMajorAxisKm = Math.cbrt(EARTH_MU_KM3_S2 / (meanMotionRadPerSecond * meanMotionRadPerSecond));
        const periodMinutes = (Math.PI * 2) / meanMotionRadPerMinute;
        return {
            eccentricity,
            inclinationDeg,
            semiMajorAxisKm,
            periodMinutes,
            perigeeKm: semiMajorAxisKm * (1 - eccentricity) - WGS84_EARTH_RADIUS_KM,
            apogeeKm: semiMajorAxisKm * (1 + eccentricity) - WGS84_EARTH_RADIUS_KM
        };
    }

    function classifyOrbitRegime(orbit, fallbackAltitudeKm = NaN) {
        const eccentricity = Number.isFinite(orbit?.eccentricity) ? orbit.eccentricity : 0;
        const perigeeKm = Number.isFinite(orbit?.perigeeKm) ? orbit.perigeeKm : fallbackAltitudeKm;
        const apogeeKm = Number.isFinite(orbit?.apogeeKm) ? orbit.apogeeKm : fallbackAltitudeKm;
        const meanAltitudeKm = Number.isFinite(perigeeKm) && Number.isFinite(apogeeKm)
            ? (perigeeKm + apogeeKm) / 2
            : fallbackAltitudeKm;
        const periodMinutes = Number.isFinite(orbit?.periodMinutes) ? orbit.periodMinutes : NaN;
        const isGeosynchronous =
            Math.abs(meanAltitudeKm - GEOSTATIONARY_ALTITUDE_KM) <= 1800 &&
            Math.abs(periodMinutes - SIDEREAL_DAY_MINUTES) <= 120 &&
            eccentricity < 0.08;

        if (isGeosynchronous) return 'GEO';
        if (eccentricity >= 0.25 && Number.isFinite(apogeeKm) && apogeeKm >= 2000) return 'HEO';
        if (Number.isFinite(apogeeKm) && apogeeKm < 2000) return 'LEO';
        if (Number.isFinite(meanAltitudeKm) && meanAltitudeKm < GEOSTATIONARY_ALTITUDE_KM) return 'MEO';
        return 'HEO';
    }

    function orbitRegimeActive(regime) {
        return Boolean(state.satelliteFilters[regime]);
    }

    function estimateSatelliteOrbitTotal() {
        return Math.max(SATELLITES_IN_ORBIT_ESTIMATE, state.satelliteCatalog.length + 1);
    }

    function updateSatelliteSearchStatus() {
        if (!dom['satellite-search-status']) return;
        if (!state.satelliteLibraryReady) {
            dom['satellite-search-status'].textContent = state.satelliteLastError || t('sat.libraryMissing');
            return;
        }
        if (!state.satelliteCatalogLoaded) {
            dom['satellite-search-status'].textContent = state.satelliteLastError || t('sat.catalogLoading');
            return;
        }

        if (state.satelliteLastError) {
            dom['satellite-search-status'].textContent = state.satelliteLastError;
            return;
        }

        const enabled = ORBIT_REGIMES.filter((regime) => orbitRegimeActive(regime)).join(', ') || t('common.none');
        dom['satellite-search-status'].textContent = t('sat.catalogReady', {
            total: formatNumber(estimateSatelliteOrbitTotal()),
            trackable: formatNumber(state.satelliteCatalog.length),
            live: formatNumber(state.satelliteLiveCount),
            enabled
        });
    }

    function formatAltitudeKm(value) {
        if (!Number.isFinite(value)) return '--';
        return `${formatNumber(Math.round(value))} km`;
    }

    function formatSatelliteNumber(value, fractionDigits = 0, suffix = '') {
        if (!Number.isFinite(value)) return '--';
        return `${formatNumber(value, {
            minimumFractionDigits: fractionDigits,
            maximumFractionDigits: fractionDigits
        })}${suffix}`;
    }

    function formatSatellitePeriod(minutes) {
        if (!Number.isFinite(minutes)) return '--';
        if (minutes >= 180) return `${formatSatelliteNumber(minutes / 60, 1)} h`;
        return `${formatSatelliteNumber(minutes, 0)} min`;
    }

    function formatSatelliteSize(satellite) {
        if (satellite?.sizeLabel) return satellite.sizeLabel;
        if (Number.isFinite(satellite?.rcsSquareMeters)) {
            return `RCS ${formatSatelliteNumber(satellite.rcsSquareMeters, 2)} m2 (${t('sat.rcsNoSize')})`;
        }
        return '--';
    }

    function formatCoordinate(value, positive, negative) {
        if (!Number.isFinite(value)) return '--';
        const hemisphere = value >= 0 ? positive : negative;
        return `${formatNumber(Math.abs(value), { minimumFractionDigits: 2, maximumFractionDigits: 2 })}° ${hemisphere}`;
    }

    function setText(id, value) {
        if (dom[id]) dom[id].textContent = translateDataLabel(value);
    }

    function currentSatelliteSearchResults() {
        if (!state.satelliteCatalogLoaded) return [];
        const query = state.satelliteSearchQuery.trim().toLowerCase();
        const catalog = state.satelliteCatalog.filter((satellite) => {
            if (!orbitRegimeActive(satellite.regime)) return false;
            if (!query) return true;
            return satellite.name.toLowerCase().includes(query) ||
                String(satellite.satcatObjectId || '').toLowerCase().includes(query) ||
                String(satellite.operator || '').toLowerCase().includes(query) ||
                String(satellite.id).includes(query);
        });

        return catalog
            .sort((a, b) => {
                const aScore = a.name.toLowerCase() === query ? 2 : a.name.toLowerCase().startsWith(query) ? 1 : 0;
                const bScore = b.name.toLowerCase() === query ? 2 : b.name.toLowerCase().startsWith(query) ? 1 : 0;
                if (aScore !== bScore) return bScore - aScore;
                return a.name.localeCompare(b.name, currentLocale());
            })
            .slice(0, SATELLITE_RESULT_LIMIT);
    }

    function renderSatelliteSearchResults() {
        if (!dom['satellite-search-results']) return;
        dom['satellite-search-results'].innerHTML = '';
        updateSatelliteSearchStatus();

        if (!state.satelliteCatalogLoaded) {
            const empty = document.createElement('div');
            empty.className = 'search-empty';
            empty.textContent = t('sat.catalogIntro');
            dom['satellite-search-results'].appendChild(empty);
            return;
        }

        const results = currentSatelliteSearchResults();
        if (!results.length) {
            const empty = document.createElement('div');
            empty.className = 'search-empty';
            empty.textContent = t('sat.noResults');
            dom['satellite-search-results'].appendChild(empty);
            return;
        }

        results.forEach((satellite) => {
            const card = document.createElement('article');
            card.className = 'sat-result-card';
            if (state.followSatelliteId === satellite.id) card.classList.add('active');

            const title = document.createElement('div');
            title.className = 'sat-result-title';
            title.textContent = satellite.name;

            const meta = document.createElement('div');
            meta.className = 'sat-result-meta';
            meta.textContent = `${satellite.regime} | ${translateDataLabel(satellite.type || t('sat.typeFallback'))} | ${translateDataLabel(satellite.operator || t('sat.profilePending'))} | ${t('sat.height')} ${formatAltitudeKm(satellite.altitudeKm)} | NORAD ${satellite.id}`;

            const actions = document.createElement('div');
            actions.className = 'sat-result-actions';

            const focusBtn = document.createElement('button');
            focusBtn.type = 'button';
            focusBtn.className = 'action-btn';
            focusBtn.textContent = t('sat.focus');
            focusBtn.addEventListener('click', () => focusSatelliteById(satellite.id, false));

            const followBtn = document.createElement('button');
            followBtn.type = 'button';
            followBtn.className = 'action-btn';
            followBtn.textContent = state.followSatelliteId === satellite.id ? t('sat.following') : t('sat.follow');
            if (state.followSatelliteId === satellite.id) followBtn.classList.add('active');
            followBtn.addEventListener('click', () => focusSatelliteById(satellite.id, true));

            actions.append(focusBtn, followBtn);
            card.append(title, meta, actions);
            dom['satellite-search-results'].appendChild(card);
        });
    }

    function satelliteColorForName(name) {
        if (/ISS|ZARYA|TIANGONG|CSS/i.test(name)) return new THREE.Color(0xffc768);
        if (/^STARLINK/i.test(name)) return new THREE.Color(0x5fd8ff);
        if (/^ONEWEB/i.test(name)) return new THREE.Color(0xc0b8ff);
        if (/GPS|GALILEO|GLONASS|BEIDOU|NAVSTAR/i.test(name)) return new THREE.Color(0x7fffb2);
        return new THREE.Color(0xe6f2ff);
    }

    function satelliteProfile(type, operator, country, source = 'Name erkannt') {
        return { type, operator, country, profileSource: source };
    }

    function baseSatelliteProfile(regime) {
        return satelliteProfile(
            regime === 'GEO' ? t('sat.typeGeo') : t('sat.typeFallback'),
            t('sat.satcatPending'),
            t('sat.satcatPending'),
            'TLE/NORAD'
        );
    }

    const SATCAT_OWNER_LABELS = {
        AB: 'Arab Satellite Communications Organization',
        ALG: 'Algerien',
        ARGN: 'Argentinien',
        AUS: 'Australien',
        AZER: 'Aserbaidschan',
        BEL: 'Belgien',
        BGR: 'Bulgarien',
        BOL: 'Bolivien',
        BRAZ: 'Brasilien',
        CA: 'Kanada',
        CHBZ: 'China/Brasilien',
        CHLE: 'Chile',
        CIS: 'Russland/GUS',
        COL: 'Kolumbien',
        CZCH: 'Tschechien',
        DEN: 'Daenemark',
        ECU: 'Ecuador',
        EGY: 'Aegypten',
        ESA: 'ESA',
        EUME: 'EUMETSAT',
        EUTE: 'Eutelsat',
        FGER: 'Frankreich/Deutschland',
        FR: 'Frankreich',
        GER: 'Deutschland',
        GLOB: 'Globalstar',
        GREC: 'Griechenland',
        HUN: 'Ungarn',
        IM: 'Inmarsat',
        IND: 'Indien',
        INDO: 'Indonesien',
        IRAN: 'Iran',
        IRAQ: 'Irak',
        ISRA: 'Israel',
        ISS: 'Internationale Raumstation',
        IT: 'Italien',
        JPN: 'Japan',
        KAZ: 'Kasachstan',
        LAOS: 'Laos',
        LTU: 'Litauen',
        LUXE: 'Luxemburg',
        MALA: 'Malaysia',
        MEX: 'Mexiko',
        NATO: 'NATO',
        NETH: 'Niederlande',
        NICO: 'Nicaragua',
        NIG: 'Nigeria',
        NKOR: 'Nordkorea',
        NOR: 'Norwegen',
        NZ: 'Neuseeland',
        O3B: 'O3b/SES',
        PAKI: 'Pakistan',
        PER: 'Peru',
        POL: 'Polen',
        PRC: 'China',
        RASC: 'RascomStar-QAF',
        ROC: 'Taiwan',
        ROM: 'Rumaenien',
        RP: 'Philippinen',
        SAFR: 'Suedafrika',
        SAUD: 'Saudi-Arabien',
        SDN: 'Sudan',
        SES: 'SES',
        SGP: 'Singapur',
        SKOR: 'Suedkorea',
        SPN: 'Spanien',
        SWED: 'Schweden',
        SWTZ: 'Schweiz',
        TBD: 'Nicht bestaetigt',
        THAI: 'Thailand',
        TMMC: 'Turkmenistan/Monaco',
        TURK: 'Tuerkei',
        UAE: 'Vereinigte Arabische Emirate',
        UK: 'Vereinigtes Koenigreich',
        UKR: 'Ukraine',
        UNK: 'Unbekannt',
        US: 'USA',
        USBZ: 'USA/Brasilien',
        VENZ: 'Venezuela',
        VTNM: 'Vietnam'
    };

    const SATCAT_COUNTRY_OVERRIDES = {
        AB: 'Saudi-Arabien',
        EUME: 'Europa',
        EUTE: 'Frankreich',
        GLOB: 'USA',
        IM: 'Vereinigtes Koenigreich',
        INTL: 'USA/Luxemburg',
        ISS: 'International',
        O3B: 'Luxemburg',
        RASC: 'Mauritius',
        SES: 'Luxemburg'
    };

    const SATCAT_OPERATOR_LABELS = {
        AB: 'Arabsat',
        EUME: 'EUMETSAT',
        EUTE: 'Eutelsat',
        GLOB: 'Globalstar',
        IM: 'Inmarsat',
        INTL: 'Intelsat',
        ISS: 'ISS-Partner',
        NATO: 'NATO',
        O3B: 'O3b/SES',
        RASC: 'RascomStar-QAF',
        SES: 'SES'
    };

    const SATELLITE_NAME_OPERATOR_PROFILES = [
        [/^STARLINK\b/, 'SpaceX', 'USA'],
        [/^GLOBALSTAR\b/, 'Globalstar', 'USA'],
        [/^ONEWEB\b/, 'Eutelsat OneWeb', 'Vereinigtes Koenigreich/Frankreich'],
        [/^KUIPER\b|PROJECT KUIPER/, 'Amazon Project Kuiper', 'USA'],
        [/^IRIDIUM\b/, 'Iridium Communications', 'USA'],
        [/^ORBCOMM\b/, 'ORBCOMM', 'USA'],
        [/^O3B\b/, 'O3b/SES', 'Luxemburg'],
        [/^SES\b/, 'SES', 'Luxemburg'],
        [/^LEMUR\b/, 'Spire Global', 'USA/Luxemburg'],
        [/^FLOCK\b|^DOVE\b|^SKYSAT\b/, 'Planet Labs', 'USA'],
        [/^ICEYE\b/, 'ICEYE', 'Finnland'],
        [/^CAPELLA\b/, 'Capella Space', 'USA'],
        [/^HAWK\b/, 'HawkEye 360', 'USA'],
        [/^GPS\b|^NAVSTAR\b/, 'U.S. Space Force', 'USA'],
        [/^GALILEO\b|^GSAT01\b|^GSAT02\b/, 'EU/ESA/EUSPA', 'Europaeische Union'],
        [/^GLONASS\b/, 'Roskosmos', 'Russland'],
        [/^BEIDOU\b|^COMPASS\b/, 'BeiDou/CNSA', 'China'],
        [/^QZSS\b|^MICHIBIKI\b/, 'Cabinet Office/JAXA', 'Japan'],
        [/^IRNSS\b|^NAVIC\b/, 'ISRO', 'Indien'],
        [/^TDRS\b/, 'NASA', 'USA'],
        [/^GOES\b|^NOAA\b|^JPSS\b|^SUOMI NPP\b/, 'NOAA/NASA', 'USA'],
        [/^SENTINEL\b/, 'ESA/Copernicus', 'Europaeische Union'],
        [/^LANDSAT\b/, 'NASA/USGS', 'USA']
    ];

    const SATELLITE_NAME_SIZE_PROFILES = [
        [/^STARLINK\b.*(?:V2|V2 MINI)/, 'ca. 4,1 x 2,7 m'],
        [/^STARLINK\b/, 'ca. 2,8 x 1,4 m'],
        [/^ONEWEB\b/, 'ca. 1,0 x 1,0 x 1,3 m'],
        [/^GLOBALSTAR\b/, 'ca. 2,4 x 2,4 x 1,3 m'],
        [/^IRIDIUM\b/, 'ca. 3,1 x 2,4 x 1,5 m'],
        [/^ORBCOMM\b/, 'ca. 1 m Klasse'],
        [/^O3B\b/, 'mehrere Meter'],
        [/^SES\b/, 'mehrere Meter'],
        [/^KUIPER\b|PROJECT KUIPER/, 'nicht veroeffentlicht'],
        [/^LEMUR\b/, 'ca. 3U/6U CubeSat'],
        [/^FLOCK\b|^DOVE\b/, 'ca. 3U CubeSat'],
        [/^SKYSAT\b/, 'ca. 60 x 60 x 95 cm'],
        [/^ICEYE\b/, 'ca. 1 m Klasse'],
        [/^CAPELLA\b/, 'mehrere Meter entfaltet'],
        [/^HAWK\b/, 'ca. ESPA/CubeSat-Klasse'],
        [/^GPS\b|^NAVSTAR\b/, 'mehrere Meter'],
        [/^GALILEO\b|^GSAT01\b|^GSAT02\b/, 'ca. 2,7 x 1,2 x 1,1 m'],
        [/^GLONASS\b/, 'mehrere Meter'],
        [/^BEIDOU\b|^COMPASS\b/, 'mehrere Meter'],
        [/^QZSS\b|^MICHIBIKI\b/, 'mehrere Meter'],
        [/^TDRS\b/, 'mehrere Meter'],
        [/^GOES\b/, 'mehrere Meter'],
        [/^NOAA\b|^JPSS\b|^SUOMI NPP\b/, 'mehrere Meter'],
        [/^SENTINEL-1\b/, 'ca. 3,4 x 1,3 x 1,3 m'],
        [/^SENTINEL-2\b/, 'ca. 3,4 x 1,8 x 2,4 m'],
        [/^SENTINEL\b/, 'mehrere Meter'],
        [/^LANDSAT\b/, 'mehrere Meter']
    ];

    function satcatOwnerLabel(owner) {
        const code = String(owner || '').trim().toUpperCase();
        return SATCAT_COUNTRY_OVERRIDES[code] || SATCAT_OWNER_LABELS[code] || code || t('sat.notClear');
    }

    function satelliteOperatorFallback(owner) {
        const code = String(owner || '').trim().toUpperCase();
        if (SATCAT_OPERATOR_LABELS[code]) return SATCAT_OPERATOR_LABELS[code];
        return code ? t('sat.notClear') : t('sat.profilePending');
    }

    function satelliteNameOperatorProfile(name) {
        const normalized = String(name || '').trim().toUpperCase();
        if (!normalized) return null;
        const match = SATELLITE_NAME_OPERATOR_PROFILES.find(([pattern]) => pattern.test(normalized));
        return match ? { operator: match[1], country: match[2] } : null;
    }

    function satelliteNameSizeProfile(name) {
        const normalized = String(name || '').trim().toUpperCase();
        if (!normalized) return '';
        const match = SATELLITE_NAME_SIZE_PROFILES.find(([pattern]) => pattern.test(normalized));
        return match ? match[1] : '';
    }

    function satcatTypeLabel(type) {
        const code = String(type || '').trim().toUpperCase();
        if (code === 'PAY') return t('satcat.payload');
        if (code === 'R/B') return t('satcat.rocketBody');
        if (code === 'DEB') return t('satcat.debris');
        if (code === 'UNK') return t('satcat.unknownObject');
        return code || t('sat.typeFallback');
    }

    function parseSatelliteFloat(value) {
        if (value === null || value === undefined) return NaN;
        const parsed = Number.parseFloat(String(value).replace(',', '.'));
        return Number.isFinite(parsed) ? parsed : NaN;
    }

    function dimensionPart(label, value) {
        const parsed = parseSatelliteFloat(value);
        return Number.isFinite(parsed) ? `${label} ${formatSatelliteNumber(parsed, parsed < 10 ? 2 : 1)} m` : '';
    }

    function wikidataProfileFromBindings(bindings) {
        const row = Array.isArray(bindings) && bindings.length ? bindings[0] : null;
        if (!row) return null;
        const length = dimensionPart('L', row.length?.value);
        const width = dimensionPart('B', row.width?.value);
        const height = dimensionPart('H', row.height?.value);
        const diameter = dimensionPart('D', row.diameter?.value);
        const dimensions = [length, width, height].filter(Boolean);
        const sizeLabel = dimensions.length ? dimensions.join(' x ') : diameter;
        return {
            label: row.itemLabel?.value || '',
            operator: row.operatorLabel?.value || row.manufacturerLabel?.value || row.ownerLabel?.value || '',
            sizeLabel
        };
    }

    function applySatelliteProfileData(satellite, payload) {
        if (!satellite || !payload) return;
        const satcat = payload.satcat || null;
        const wikidata = payload.wikidata || null;
        const sources = [];

        if (satcat) {
            const satcatName = satcat.OBJECT_NAME || satcat.objectName || satcat.name;
            const owner = satcat.OWNER || satcat.owner;
            const nameProfile = satelliteNameOperatorProfile(satcatName || satellite.name);
            satellite.name = satcatName || satellite.name;
            satellite.type = satcatTypeLabel(satcat.OBJECT_TYPE || satcat.objectType);
            satellite.country = nameProfile?.country || satcatOwnerLabel(owner);
            satellite.operator = nameProfile?.operator || satelliteOperatorFallback(owner);
            satellite.satcatObjectId = satcat.OBJECT_ID || satcat.objectId || satellite.satcatObjectId;
            const rcs = parseSatelliteFloat(satcat.RCS || satcat.rcs);
            if (Number.isFinite(rcs)) satellite.rcsSquareMeters = rcs;
            sources.push('CelesTrak SATCAT');
        }

        if (wikidata) {
            if (wikidata.operator) satellite.operator = wikidata.operator;
            if (wikidata.sizeLabel) {
                satellite.sizeLabel = wikidata.sizeLabel;
            } else {
                const length = dimensionPart('L', wikidata.lengthM);
                const width = dimensionPart('B', wikidata.widthM);
                const height = dimensionPart('H', wikidata.heightM);
                const diameter = dimensionPart('D', wikidata.diameterM);
                const dimensions = [length, width, height].filter(Boolean);
                if (dimensions.length) satellite.sizeLabel = dimensions.join(' x ');
                else if (diameter) satellite.sizeLabel = diameter;
            }
            if (!satcat && wikidata.label) satellite.name = wikidata.label;
            sources.push('Wikidata');
        }

        if (!satellite.sizeLabel) {
            satellite.sizeLabel = satelliteNameSizeProfile(satellite.name);
        }

        satellite.profileSource = sources.length ? sources.join(' + ') : 'TLE/NORAD';
        satellite.profileLoaded = Boolean(sources.length);
    }

    async function fetchCelestrakSatcatRecord(satellite) {
        const makeUrl = (mode) => {
            const params = new URLSearchParams({ FORMAT: 'JSON' });
            if (mode === 'catnr' && satellite?.id) params.set('CATNR', String(satellite.id));
            if (mode === 'name' && satellite?.name) {
                params.set('NAME', satellite.name);
                params.set('MAX', '1');
            }
            return `${CELESTRAK_SATCAT_RECORDS_URL}?${params.toString()}`;
        };
        const mode = satellite?.id ? 'catnr' : 'name';
        let url = makeUrl(mode);
        if (!url.includes('CATNR=') && !url.includes('NAME=')) return null;
        let response = await fetch(url, {
            cache: 'force-cache',
            headers: { Accept: 'application/json' }
        });
        if (!response.ok) throw new Error(`SATCAT HTTP ${response.status}`);
        let payload = await response.json();
        let records = Array.isArray(payload) ? payload : [];
        if (!records.length && mode === 'catnr' && satellite?.name) {
            response = await fetch(makeUrl('name'), {
                cache: 'force-cache',
                headers: { Accept: 'application/json' }
            });
            if (!response.ok) throw new Error(`SATCAT HTTP ${response.status}`);
            payload = await response.json();
            records = Array.isArray(payload) ? payload : [];
        }
        return records.find((record) => String(record.NORAD_CAT_ID || record.noradCatId || '') === String(satellite?.id)) ||
            records[0] ||
            null;
    }

    async function fetchWikidataSatelliteRecord(satellite) {
        if (!satellite?.id) return null;
        const rawId = String(satellite.id);
        const paddedId = rawId.padStart(5, '0');
        const query = `
SELECT ?item ?itemLabel ?operatorLabel ?manufacturerLabel ?ownerLabel ?length ?width ?height ?diameter WHERE {
  VALUES ?scn { "${rawId}" "${paddedId}" }
  ?item wdt:P377 ?scn.
  OPTIONAL { ?item wdt:P137 ?operator. }
  OPTIONAL { ?item wdt:P176 ?manufacturer. }
  OPTIONAL { ?item wdt:P127 ?owner. }
  OPTIONAL { ?item wdt:P2043 ?length. }
  OPTIONAL { ?item wdt:P2049 ?width. }
  OPTIONAL { ?item wdt:P2048 ?height. }
  OPTIONAL { ?item wdt:P2386 ?diameter. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de,en". }
}
LIMIT 1`;
        const params = new URLSearchParams({ format: 'json', query });
        const response = await fetch(`${WIKIDATA_SPARQL_URL}?${params.toString()}`, {
            cache: 'force-cache',
            headers: { Accept: 'application/sparql-results+json' }
        });
        if (!response.ok) throw new Error(`Wikidata HTTP ${response.status}`);
        const payload = await response.json();
        return wikidataProfileFromBindings(payload?.results?.bindings);
    }

    async function fetchSatelliteProfileData(satellite) {
        const params = new URLSearchParams({
            catnr: String(satellite.id || ''),
            name: satellite.name || '',
            v: SATELLITE_PROFILE_CACHE_VERSION
        });

        try {
            const response = await fetch(`${SATELLITE_PROFILE_API_URL}?${params.toString()}`, {
                cache: 'force-cache',
                headers: { Accept: 'application/json' }
            });
            if (response.ok) return response.json();
        } catch (error) {
            // Static hosting falls back to direct public endpoints below.
        }

        const [satcatResult, wikidataResult] = await Promise.allSettled([
            fetchCelestrakSatcatRecord(satellite),
            fetchWikidataSatelliteRecord(satellite)
        ]);

        return {
            satcat: satcatResult.status === 'fulfilled' ? satcatResult.value : null,
            wikidata: wikidataResult.status === 'fulfilled' ? wikidataResult.value : null
        };
    }

    function ensureSatelliteProfile(satellite) {
        if (!satellite) return null;
        if (satellite.profileLoaded) return Promise.resolve(satellite);
        const key = String(satellite.id || satellite.name || '');
        if (!key) return null;
        if (state.satelliteProfileCache.has(key)) {
            applySatelliteProfileData(satellite, state.satelliteProfileCache.get(key));
            return Promise.resolve(satellite);
        }
        if (state.satelliteProfilePending.has(key)) return state.satelliteProfilePending.get(key);

        satellite.profileSource = t('profile.queryRunning');
        updateSatelliteFocusPanel(satellite);
        const pending = fetchSatelliteProfileData(satellite)
            .then((payload) => {
                state.satelliteProfileCache.set(key, payload);
                applySatelliteProfileData(satellite, payload);
                if (state.followSatelliteId === satellite.id) updateSatelliteFocusPanel(satellite);
                if (document.body.classList.contains('search-open')) renderSatelliteSearchResults();
                return satellite;
            })
            .catch((error) => {
                satellite.profileSource = t('profile.offline', { error: error.message || t('sat.unknownError') });
                if (state.followSatelliteId === satellite.id) updateSatelliteFocusPanel(satellite);
                return satellite;
            })
            .finally(() => {
                state.satelliteProfilePending.delete(key);
            });
        state.satelliteProfilePending.set(key, pending);
        return pending;
    }

    function inferSatelliteProfile(name, regime) {
        const n = name.toUpperCase();
        const fallback = satelliteProfile(
            regime === 'GEO' ? t('sat.typeGeo') : t('sat.typeFallback'),
            t('sat.notClear'),
            t('sat.notClear'),
            t('sat.tleNoOperator')
        );

        if (/\b(DEB|DEBRIS)\b/.test(n)) return satelliteProfile(t('profile.spaceDebris'), t('profile.noActiveOperator'), t('sat.notClear'), t('profile.tleName'));
        if (/\b(R\/B|ROCKET BODY)\b/.test(n)) return satelliteProfile(t('profile.rocketBody'), t('profile.noActiveOperator'), t('sat.notClear'), t('profile.tleName'));
        if (/OBJECT\s+[A-Z0-9]+/.test(n)) return satelliteProfile(t('profile.uncataloged'), t('sat.notClear'), t('sat.notClear'), t('profile.tleName'));

        const knownProfiles = [
            [/ISS|ZARYA|UNITY|ZVEZDA|DESTINY|KIBO|COLUMBUS/, 'Raumstation/ISS-Modul', 'ISS-Partner (NASA, Roskosmos, ESA, JAXA, CSA)', 'International'],
            [/TIANGONG|CSS|TIANHE|WENTIAN|MENGTIAN/, 'Raumstation/Stationsmodul', 'CMSA', 'China'],
            [/TIANZHOU|SHENZHOU/, 'Raumschiff/Versorgung', 'CMSA', 'China'],
            [/^STARLINK/, 'Kommunikationssatellit', 'SpaceX', 'USA'],
            [/^ONEWEB/, 'Kommunikationssatellit', 'Eutelsat OneWeb', 'Vereinigtes Königreich/Frankreich'],
            [/KUIPER/, 'Kommunikationssatellit', 'Amazon Project Kuiper', 'USA'],
            [/IRIDIUM/, 'Kommunikationssatellit', 'Iridium Communications', 'USA'],
            [/GLOBALSTAR/, 'Kommunikationssatellit', 'Globalstar', 'USA'],
            [/ORBCOMM/, 'Kommunikationssatellit', 'ORBCOMM', 'USA'],
            [/O3B|SES/, 'Kommunikationssatellit', 'SES', 'Luxemburg'],
            [/INTELSAT|IS-\d|GALAXY\s?\d|HORIZONS/, 'Kommunikationssatellit', 'Intelsat', 'USA/Luxemburg'],
            [/EUTELSAT|HOTBIRD|HOT BIRD|KONNECT/, 'Kommunikationssatellit', 'Eutelsat', 'Frankreich'],
            [/INMARSAT|VIASAT/, 'Kommunikationssatellit', 'Viasat/Inmarsat', 'USA/Vereinigtes Königreich'],
            [/TELSTAR|ANIK/, 'Kommunikationssatellit', 'Telesat', 'Kanada'],
            [/HISPASAT|AMAZONAS/, 'Kommunikationssatellit', 'Hispasat', 'Spanien'],
            [/TURKSAT/, 'Kommunikationssatellit', 'Turksat', 'Türkei'],
            [/ARABSAT|BADR/, 'Kommunikationssatellit', 'Arabsat', 'Saudi-Arabien'],
            [/NILESAT/, 'Kommunikationssatellit', 'Nilesat', 'Ägypten'],
            [/ECHOSTAR|DISH/, 'Kommunikationssatellit', 'EchoStar/DISH', 'USA'],
            [/TDRS/, 'Kommunikations-/Datenrelaissatellit', 'NASA', 'USA'],
            [/SKYNET/, 'Militärischer Kommunikationssatellit', 'UK Ministry of Defence', 'Vereinigtes Königreich'],
            [/GPS|NAVSTAR/, 'Navigationssatellit', 'U.S. Space Force', 'USA'],
            [/GALILEO|GSAT01|GSAT02/, 'Navigationssatellit', 'EU/ESA/EUSPA', 'Europäische Union'],
            [/GLONASS/, 'Navigationssatellit', 'Roskosmos/Russische Föderation', 'Russland'],
            [/BEIDOU|COMPASS/, 'Navigationssatellit', 'CNSA/BeiDou System', 'China'],
            [/QZSS|MICHIBIKI/, 'Navigationssatellit', 'Cabinet Office/JAXA', 'Japan'],
            [/IRNSS|NAVIC/, 'Navigationssatellit', 'ISRO', 'Indien'],
            [/GOES/, 'Wetter-/Umweltsatellit', 'NOAA/NASA', 'USA'],
            [/NOAA\s?\d|JPSS|SUOMI NPP/, 'Wetter-/Umweltsatellit', 'NOAA/NASA', 'USA'],
            [/METEOSAT|METOP|SENTINEL-6/, 'Wetter-/Umweltsatellit', 'EUMETSAT/ESA', 'Europa'],
            [/HIMAWARI/, 'Wetter-/Umweltsatellit', 'JMA', 'Japan'],
            [/FENGYUN|FY-/, 'Wetter-/Umweltsatellit', 'CMA/CNSA', 'China'],
            [/LANDSAT/, 'Erdbeobachtung', 'NASA/USGS', 'USA'],
            [/SENTINEL/, 'Erdbeobachtung', 'ESA/Copernicus', 'Europäische Union'],
            [/SPOT|PLEIADES/, 'Erdbeobachtung', 'Airbus/CNES', 'Frankreich/Europa'],
            [/WORLDVIEW|GEOEYE|LEGION/, 'Erdbeobachtung', 'Maxar', 'USA'],
            [/PLANET|DOVE|FLOCK|SKYSAT/, 'Erdbeobachtung', 'Planet Labs', 'USA'],
            [/ICEYE/, 'Radar-Erdbeobachtung', 'ICEYE', 'Finnland'],
            [/CAPELLA/, 'Radar-Erdbeobachtung', 'Capella Space', 'USA'],
            [/KOMPSAT|ARIRANG/, 'Erdbeobachtung', 'KARI', 'Südkorea'],
            [/CARTOSAT|RISAT|OCEANSAT|RESOURCESAT|INSAT|GSAT/, 'Satellit (Kommunikation/Erdbeobachtung)', 'ISRO', 'Indien'],
            [/HUBBLE|HST/, 'Weltraumteleskop', 'NASA/ESA', 'USA/Europa'],
            [/CHANDRA|SWIFT|FERMI|TESS|WISE|NEOWISE/, 'Wissenschaftssatellit', 'NASA', 'USA'],
            [/JWST|WEBB/, 'Weltraumteleskop', 'NASA/ESA/CSA', 'International'],
            [/NROL|NOSS|USA\s?\d+/, 'Militär-/Aufklärungssatellit', 'US-Regierung/NRO/DoD', 'USA'],
            [/COSMOS|KOSMOS/, 'Militär-/Regierungssatellit', 'Russische Regierung/Roskosmos', 'Russland'],
            [/YAOGAN|SHIJIAN|SJ-/, 'Regierungs-/Aufklärungssatellit', 'CNSA/Chinesische Regierung', 'China'],
            [/OFEQ/, 'Aufklärungssatellit', 'Israel Ministry of Defense', 'Israel'],
            [/SAR-LUPE|TERRASAR|TANDEM-X/, 'Radar-Erdbeobachtung', 'DLR/Airbus/Bundeswehr', 'Deutschland'],
            [/LEMUR/, 'Wetter-/AIS-Datensatellit', 'Spire Global', 'USA/Luxemburg'],
            [/HAWK/, 'RF-Aufklärungssatellit', 'HawkEye 360', 'USA'],
            [/SWARM/, 'IoT-Kommunikationssatellit', 'Swarm Technologies/SpaceX', 'USA']
        ];

        const match = knownProfiles.find(([pattern]) => pattern.test(n));
        if (!match) return fallback;
        return satelliteProfile(translateDataLabel(match[1]), translateDataLabel(match[2]), translateDataLabel(match[3]), t('profile.fromName'));
    }

    function parseSatelliteCatalog(rawText) {
        const satelliteLib = getSatelliteLib();
        if (!satelliteLib) return [];

        const lines = rawText
            .split(/\r?\n/)
            .map((line) => line.trimEnd())
            .filter(Boolean);
        const entries = [];

        for (let index = 0; index < lines.length - 2; index += 1) {
            const maybeName = lines[index];
            const line1 = lines[index + 1];
            const line2 = lines[index + 2];
            if (!line1?.startsWith('1 ') || !line2?.startsWith('2 ')) continue;

            const name = maybeName.replace(/^0\s+/, '').trim() || `SAT-${entries.length + 1}`;
            try {
                const satrec = satelliteLib.twoline2satrec(line1, line2);
                const now = new Date(earthReferenceTimeMs());
                const samplePosition = satelliteLib.propagate(satrec, now)?.position;
                const geodetic = samplePosition
                    ? satelliteLib.eciToGeodetic(samplePosition, satelliteLib.gstime(now))
                    : null;
                const altitudeKm = Number.isFinite(geodetic?.height) ? Math.max(0, geodetic.height) : 0;
                const orbit = getOrbitElementsFromSatrec(satrec);
                const regime = classifyOrbitRegime(orbit, altitudeKm);
                const profile = baseSatelliteProfile(regime);
                const color = satelliteColorForName(name);
                const id = String(satrec.satnum || line1.slice(2, 7).trim() || entries.length + 1);
                entries.push({
                    id,
                    name,
                    satrec,
                    type: profile.type,
                    operator: profile.operator,
                    country: profile.country,
                    profileSource: profile.profileSource,
                    eccentricity: orbit.eccentricity,
                    inclinationDeg: orbit.inclinationDeg,
                    perigeeKm: orbit.perigeeKm,
                    apogeeKm: orbit.apogeeKm,
                    periodMinutes: orbit.periodMinutes,
                    altitudeKm,
                    regime,
                    orbitSource: 'TLE/SGP4',
                    color: [color.r, color.g, color.b]
                });
            } catch (error) {
                // skip malformed entries
            }
            index += 2;
        }

        return entries;
    }

    function rebuildSatelliteLayer() {
        if (!state.satellitePoints) return;

        const geometry = new THREE.BufferGeometry();
        const positions = new Float32Array(Math.max(1, state.satelliteCatalog.length) * 3);
        const colors = new Float32Array(Math.max(1, state.satelliteCatalog.length) * 3);

        state.satelliteCatalog.forEach((satellite, index) => {
            const [r, g, b] = satellite.color;
            colors[index * 3] = r;
            colors[index * 3 + 1] = g;
            colors[index * 3 + 2] = b;
        });

        geometry.setAttribute('position', new THREE.BufferAttribute(positions, 3));
        geometry.setAttribute('color', new THREE.BufferAttribute(colors, 3));
        geometry.setDrawRange(0, 0);

        state.satellitePoints.geometry.dispose();
        state.satellitePoints.geometry = geometry;
        state.satellitePoints.visible = state.satelliteCatalog.length > 0;
        state.satelliteIndex = new Map(state.satelliteCatalog.map((satellite) => [satellite.id, satellite]));
        if (state.followSatelliteId && !state.satelliteIndex.has(state.followSatelliteId)) {
            state.followSatelliteId = null;
        }
        refreshSatelliteFocusVisuals();
    }

    function refreshSatelliteFocusVisuals() {
        const followedSatellite = state.followSatelliteId
            ? state.satelliteIndex.get(state.followSatelliteId) || null
            : null;
        const isFollowingSatellite = Boolean(followedSatellite);
        updateFocusedSatelliteModel(followedSatellite);
        document.body.classList.toggle('satellite-following', isFollowingSatellite);
        if (dom['satellite-focus-panel']) {
            dom['satellite-focus-panel'].setAttribute('aria-hidden', String(!isFollowingSatellite));
        }
        applyMobilePanelState();

        if (isFollowingSatellite && state.panelVisibility.news) {
            state.satelliteAutoHidNews = true;
            state.panelVisibility.news = false;
            applyPanelVisibility();
            writeUiState();
        } else if (!isFollowingSatellite && state.satelliteAutoHidNews) {
            state.satelliteAutoHidNews = false;
            state.panelVisibility.news = true;
            applyPanelVisibility();
            writeUiState();
        }

        updateSatelliteLayerOpacity(isFollowingSatellite);
        updateSatelliteFocusPanel(followedSatellite);
        updateSatelliteHighlight(performance.now());
        updateSatelliteOrbitPath();
    }

    function updateSatelliteLayerOpacity(isFollowingSatellite = Boolean(state.followSatelliteId)) {
        if (!state.satellitePoints?.material) return;
        const launchTrajectoryActive = Boolean(state.selectedLaunchId && state.launchTrajectoryLine?.visible);
        state.satellitePoints.material.opacity = (isFollowingSatellite || launchTrajectoryActive)
            ? SATELLITE_LAYER_DIMMED_OPACITY
            : SATELLITE_LAYER_OPACITY;
        state.satellitePoints.material.needsUpdate = true;
    }

    function updateSatelliteFocusPanel(satellite) {
        if (!dom['satellite-focus-panel']) return;
        if (!satellite) {
            setText('sat-focus-title', '--');
            setText('sat-focus-subtitle', t('sat.noActiveFollow'));
            [
                'sat-focus-type',
                'sat-focus-operator',
                'sat-focus-country',
                'sat-focus-profile-source',
                'sat-focus-size',
                'sat-focus-regime',
                'sat-focus-altitude',
                'sat-focus-perigee',
                'sat-focus-apogee',
                'sat-focus-inclination',
                'sat-focus-period',
                'sat-focus-eccentricity',
                'sat-focus-latitude',
                'sat-focus-longitude'
            ].forEach((id) => setText(id, '--'));
            return;
        }

        setText('sat-focus-title', satellite.name);
        setText('sat-focus-subtitle', `NORAD ${satellite.id}${satellite.orbitSource ? ` · ${satellite.orbitSource}` : ''}`);
        setText('sat-focus-type', satellite.type || '--');
        setText('sat-focus-operator', satellite.operator || '--');
        setText('sat-focus-country', satellite.country || '--');
        setText('sat-focus-profile-source', satellite.profileSource || '--');
        setText('sat-focus-size', formatSatelliteSize(satellite));
        setText('sat-focus-regime', satellite.regime || '--');
        setText('sat-focus-altitude', formatAltitudeKm(satellite.altitudeKm));
        setText('sat-focus-perigee', formatAltitudeKm(satellite.perigeeKm));
        setText('sat-focus-apogee', formatAltitudeKm(satellite.apogeeKm));
        setText('sat-focus-inclination', formatSatelliteNumber(satellite.inclinationDeg, 2, '°'));
        setText('sat-focus-period', formatSatellitePeriod(satellite.periodMinutes));
        setText('sat-focus-eccentricity', formatSatelliteNumber(satellite.eccentricity, 5));
        setText('sat-focus-latitude', formatCoordinate(satellite.latitudeDeg, 'N', 'S'));
        setText('sat-focus-longitude', formatCoordinate(satellite.longitudeDeg, 'E', 'W'));
    }

    function updateSatelliteHighlight(now = performance.now()) {
        if (!state.satelliteHighlight) return;
        const localPosition = state.followSatelliteId
            ? state.satelliteWorldPositions.get(state.followSatelliteId)
            : null;
        if (!localPosition || !state.earthMesh) {
            state.satelliteHighlight.visible = false;
            return;
        }

        const satellite = state.followSatelliteId
            ? state.satelliteIndex.get(state.followSatelliteId) || null
            : null;
        updateFocusedSatelliteModel(satellite);
        state.satelliteHighlight.position.copy(localPosition);
        state.satelliteHighlight.visible = true;

        const worldPosition = state.earthMesh.localToWorld(localPosition.clone());
        const cameraDistance = state.camera
            ? state.camera.position.distanceTo(worldPosition)
            : 60;
        const scale = THREE.MathUtils.clamp(cameraDistance * 0.018, 0.48, 9);
        state.satelliteHighlight.scale.setScalar(scale);
        orientSatelliteHighlight(localPosition);

        const modelRoot = state.satelliteHighlight.userData.modelRoot;
        if (modelRoot) {
            const mode = normalizeSatelliteSizeMode(state.panelVisibility.satelliteSizeMode);
            let modelScale = mode !== 'default'
                ? THREE.MathUtils.clamp(
                    satellitePointSizeForDistance(cameraDistance) / SATELLITE_POINT_BASE_SIZE,
                    0.00008,
                    1
                )
                : 1;
            if (mode === 'reduced') {
                modelScale *= 1.2;
            }
            modelRoot.scale.setScalar(modelScale);
        }

        const focusRing = state.satelliteHighlight.userData.focusRing;
        if (focusRing) {
            focusRing.scale.setScalar(focusRing.userData.baseScale || 1.65);
            focusRing.material.opacity = 0.74 + 0.14 * (0.5 + 0.5 * Math.sin(now * 0.004));
        }

        const focusLight = state.satelliteHighlight.userData.focusLight;
        if (focusLight) {
            focusLight.intensity = 1.55 + 0.35 * (0.5 + 0.5 * Math.sin(now * 0.003));
        }
    }

    function clearSatelliteOrbitPath() {
        if (state.satelliteOrbitLine) {
            state.satelliteOrbitLine.visible = false;
        }
        if (state.satelliteGroundTrackLine) {
            state.satelliteGroundTrackLine.visible = false;
        }
        state.satelliteOrbitLastKey = '';
    }

    function localVectorToGeodetic(vector) {
        const radius = vector.length();
        if (radius <= 0) return null;
        const latitude = THREE.MathUtils.radToDeg(Math.asin(THREE.MathUtils.clamp(vector.y / radius, -1, 1)));
        const theta = Math.atan2(vector.z, -vector.x);
        const longitude = THREE.MathUtils.euclideanModulo(THREE.MathUtils.radToDeg(theta) + 180, 360) - 180;
        return {
            latitude,
            longitude,
            altitudeKm: Math.max(0, (radius - ARTEMIS.EARTH_RADIUS) * 1000)
        };
    }

    function satellitePositionAt(satellite, dateMs, satelliteLib = getSatelliteLib()) {
        if (!satellite) return null;
        const earthRotation = earthRotationAngleForMs(dateMs);

        if (satellite.id === ISS_NORAD_ID && state.issOemLoaded) {
            const inertialPosition = interpolateIssOemPosition(dateMs);
            if (inertialPosition) {
                const localPosition = inertialPosition.clone().applyAxisAngle(new THREE.Vector3(0, 1, 0), -earthRotation);
                const geodetic = localVectorToGeodetic(localPosition);
                return {
                    localPosition,
                    inertialPosition,
                    latitude: geodetic?.latitude ?? NaN,
                    longitude: geodetic?.longitude ?? NaN,
                    altitudeKm: geodetic?.altitudeKm ?? NaN,
                    source: 'NASA ISS OEM'
                };
            }
        }

        if (!satelliteLib || !satellite.satrec) return null;
        const sampleDate = new Date(dateMs);
        let geodetic;
        try {
            const propagated = satelliteLib.propagate(satellite.satrec, sampleDate);
            const eciPosition = propagated?.position;
            if (!eciPosition ||
                !Number.isFinite(eciPosition.x) ||
                !Number.isFinite(eciPosition.y) ||
                !Number.isFinite(eciPosition.z)) {
                return null;
            }
            geodetic = satelliteLib.eciToGeodetic(eciPosition, satelliteLib.gstime(sampleDate));
        } catch (error) {
            return null;
        }

        const latitude = satelliteLib.degreesLat(geodetic.latitude);
        const longitude = satelliteLib.degreesLong(geodetic.longitude);
        const altitudeKm = Math.max(0, geodetic.height);
        const localPosition = latLonToVector3(latitude, longitude, ARTEMIS.EARTH_RADIUS + altitudeKm / 1000);
        const inertialPosition = localPosition.clone().applyAxisAngle(new THREE.Vector3(0, 1, 0), earthRotation);
        return {
            localPosition,
            inertialPosition,
            latitude,
            longitude,
            altitudeKm,
            source: 'TLE/SGP4'
        };
    }

    function updateSatelliteOrbitPath(force = false) {
        if (!state.satelliteOrbitLine || !state.satelliteGroundTrackLine) return;
        const satellite = state.followSatelliteId
            ? state.satelliteIndex.get(state.followSatelliteId)
            : null;
        const satelliteLib = getSatelliteLib();
        if (!satellite || !satelliteLib || !satellite.satrec || !state.earthGroup) {
            clearSatelliteOrbitPath();
            return;
        }

        const referenceMs = earthReferenceTimeMs();
        const periodMinutes = THREE.MathUtils.clamp(
            Number.isFinite(satellite.periodMinutes) ? satellite.periodMinutes : 96,
            SATELLITE_ORBIT_PERIOD_MIN_MINUTES,
            SATELLITE_ORBIT_PERIOD_MAX_MINUTES
        );
        const key = [
            satellite.id,
            Math.floor(referenceMs / SATELLITE_ORBIT_REFRESH_MS),
            Math.round(periodMinutes * 10),
            clampSatelliteOrbitRevolutions(state.panelVisibility.orbitRevolutions),
            satellite.id === ISS_NORAD_ID && state.issOemLoaded ? 'iss-oem' : 'tle'
        ].join(':');
        if (!force && state.satelliteOrbitLastKey === key) {
            return;
        }

        const orbitPoints = [];
        const groundTrackPoints = [];
        const groundTrackRevolutions = clampSatelliteOrbitRevolutions(state.panelVisibility.orbitRevolutions);
        const groundTrackSampleCount = SATELLITE_ORBIT_SAMPLE_COUNT * groundTrackRevolutions;
        const stepMs = (periodMinutes * 60000) / SATELLITE_ORBIT_SAMPLE_COUNT;
        const groundTrackRadius = ARTEMIS.EARTH_RADIUS * 1.018;
        for (let i = 0; i <= groundTrackSampleCount; i += 1) {
            const sampleMs = referenceMs + i * stepMs;
            const sample = satellitePositionAt(satellite, sampleMs, satelliteLib);
            if (i <= SATELLITE_ORBIT_SAMPLE_COUNT && sample?.inertialPosition) {
                orbitPoints.push(sample.inertialPosition);
            }
            if (sample?.localPosition) {
                groundTrackPoints.push(sample.localPosition.clone().normalize().multiplyScalar(groundTrackRadius));
            }
        }

        if (orbitPoints.length < 2 || groundTrackPoints.length < 2) {
            clearSatelliteOrbitPath();
            return;
        }

        state.satelliteOrbitLine.geometry.dispose();
        state.satelliteOrbitLine.geometry = new THREE.BufferGeometry().setFromPoints(orbitPoints);
        state.satelliteOrbitLine.computeLineDistances();
        state.satelliteOrbitLine.visible = true;

        state.satelliteGroundTrackLine.geometry.dispose();
        state.satelliteGroundTrackLine.geometry = new THREE.BufferGeometry().setFromPoints(groundTrackPoints);
        state.satelliteGroundTrackLine.computeLineDistances();
        state.satelliteGroundTrackLine.visible = true;
        state.satelliteOrbitLastKey = key;
    }

    function frameFollowedSatellite(worldPosition, satellite) {
        if (!state.camera || !state.controls) return;
        const currentOffset = state.camera.position.clone().sub(state.controls.target);
        const viewDirection = currentOffset.lengthSq() > 1e-6
            ? currentOffset.normalize()
            : worldPosition.clone().normalize().multiplyScalar(0.85).add(new THREE.Vector3(0.18, 0.36, 0.22)).normalize();
        const orbitRadius = ARTEMIS.EARTH_RADIUS + ((satellite?.altitudeKm || 0) / 1000);
        const viewDistance = THREE.MathUtils.clamp(orbitRadius * 0.34, 4.2, 34);
        state.controls.target.copy(worldPosition);
        state.camera.position.copy(worldPosition).add(viewDirection.multiplyScalar(viewDistance));
        state.camera.updateProjectionMatrix();
    }

    function updateSatelliteStats() {
        dom['sat-stat-total'].textContent = state.satelliteCatalogLoaded
            ? formatNumber(estimateSatelliteOrbitTotal())
            : '--';
        dom['sat-stat-live'].textContent = state.satelliteCatalogLoaded
            ? formatNumber(state.satelliteLiveCount)
            : '--';
        updateSatelliteSearchStatus();
    }

    function propagateSatellites(force = false) {
        const satelliteLib = getSatelliteLib();
        if (!satelliteLib || !state.satelliteCatalog.length || !state.satellitePoints) {
            state.satelliteLiveCount = 0;
            state.satelliteWorldPositions.clear();
            state.satelliteDrawOrder = [];
            refreshSatelliteFocusVisuals();
            updateSatelliteStats();
            return;
        }

        const referenceMs = earthReferenceTimeMs();
        if (!force && referenceMs - state.satelliteLastPropagationMs < SATELLITE_PROPAGATION_INTERVAL_MS) {
            return;
        }
        state.satelliteLastPropagationMs = referenceMs;

        const geometry = state.satellitePoints.geometry;
        const positionAttr = geometry.getAttribute('position');
        let visibleCount = 0;
        state.satelliteWorldPositions.clear();
        state.satelliteDrawOrder = [];

        state.satelliteCatalog.forEach((satellite) => {
            const propagated = satellitePositionAt(satellite, referenceMs, satelliteLib);
            if (!propagated?.localPosition) return;

            satellite.altitudeKm = propagated.altitudeKm;
            satellite.latitudeDeg = propagated.latitude;
            satellite.longitudeDeg = propagated.longitude;
            satellite.orbitSource = propagated.source;
            if (!orbitRegimeActive(satellite.regime)) {
                return;
            }
            const vector = propagated.localPosition;

            positionAttr.setXYZ(visibleCount, vector.x, vector.y, vector.z);
            state.satelliteWorldPositions.set(satellite.id, vector.clone());
            state.satelliteDrawOrder[visibleCount] = satellite.id;
            visibleCount += 1;
        });

        positionAttr.needsUpdate = true;
        geometry.setDrawRange(0, visibleCount);
        state.satelliteLiveCount = visibleCount;
        state.satellitePoints.visible = visibleCount > 0;
        refreshSatelliteFocusVisuals();
        updateSatelliteStats();
        if (document.body.classList.contains('search-open')) {
            renderSatelliteSearchResults();
        }
    }

    async function fetchSatelliteCatalog() {
        if (!state.satelliteLibraryReady) {
            state.satelliteCatalog = [];
            state.satelliteCatalogLoaded = false;
            state.satelliteLiveCount = 0;
            state.satelliteLastError = state.satelliteLastError || t('sat.libraryUnavailable');
            rebuildSatelliteLayer();
            updateSatelliteStats();
            renderSatelliteSearchResults();
            return;
        }
        try {
            state.satelliteLastError = '';
            const response = await fetch(SATELLITE_TLE_URL, {
                cache: 'no-cache',
                headers: { Accept: 'text/plain' }
            });
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const rawText = await response.text();
            if (!rawText.trim()) throw new Error(t('sat.staticCatalogEmpty'));
            writeSatelliteCache(rawText);
            state.satelliteCatalog = parseSatelliteCatalog(rawText);
            state.satelliteCatalogLoaded = true;
            rebuildSatelliteLayer();
            propagateSatellites(true);
            fetchSatelliteLiveHistory(true);
            renderSatelliteSearchResults();
        } catch (error) {
            const cached = readSatelliteCache();
            if (cached?.rawText) {
                state.satelliteCatalog = parseSatelliteCatalog(cached.rawText);
                state.satelliteCatalogLoaded = true;
                state.satelliteLastError = t('sat.cacheFallback', { error: error.message || t('sat.unknownError') });
                rebuildSatelliteLayer();
                propagateSatellites(true);
                renderSatelliteSearchResults();
                return;
            }

            state.satelliteCatalog = [];
            state.satelliteCatalogLoaded = false;
            state.satelliteLiveCount = 0;
            state.satelliteLastError = t('sat.catalogLoadFailed', { error: error.message || t('sat.unknownError') });
            rebuildSatelliteLayer();
            updateSatelliteStats();
            renderSatelliteSearchResults();
        }
    }

    async function initSatelliteTracking() {
        updateSatelliteStats();
        await ensureSatelliteLibrary();
        loadIssOemData();
        fetchSatelliteLiveHistory();
        fetchSatelliteCatalog();
        if (state.satelliteFetchTimer) clearInterval(state.satelliteFetchTimer);
        state.satelliteFetchTimer = setInterval(() => {
            fetchSatelliteCatalog();
            fetchSatelliteLiveHistory(true);
        }, SATELLITE_FETCH_INTERVAL_MS);
    }

    function rebuildLaunchMarkers() {
        if (!state.launchMarkerRoot) return;
        while (state.launchMarkerRoot.children.length) {
            state.launchMarkerRoot.remove(state.launchMarkerRoot.children[0]);
        }
        state.launchMarkers.clear();

        launchMarkerLaunches().forEach((launch, index) => {
            const lat = launchLatitude(launch);
            const lon = launchLongitude(launch);
            if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;

            const launchId = launchKey(launch);
            const group = new THREE.Group();
            const anchor = latLonToVector3(lat, lon, ARTEMIS.EARTH_RADIUS + 0.12);
            const normal = anchor.clone().normalize();

            const stemHeight = 0.7;
            const stem = new THREE.Mesh(
                new THREE.CylinderGeometry(0.035, 0.035, stemHeight, 10),
                new THREE.MeshBasicMaterial({ color: 0x66d9ff })
            );
            stem.position.copy(anchor.clone().add(normal.clone().multiplyScalar(stemHeight * 0.45)));
            stem.quaternion.setFromUnitVectors(new THREE.Vector3(0, 1, 0), normal);

            const head = new THREE.Mesh(
                new THREE.SphereGeometry(0.16, 12, 12),
                new THREE.MeshBasicMaterial({ color: index === 0 ? 0xffbf54 : 0x20e6ff })
            );
            head.position.copy(anchor.clone().add(normal.clone().multiplyScalar(stemHeight)));
            head.userData.pickKind = 'launch';
            head.userData.launchId = launchId;

            group.add(stem);
            group.add(head);
            group.frustumCulled = false;
            stem.frustumCulled = false;
            head.frustumCulled = false;
            group.userData.active = launchId === state.selectedLaunchId;
            state.launchMarkerRoot.add(group);
            state.launchMarkers.set(launchId, { group, pickMesh: head, normal });
        });

        buildPickableList();
    }

    function updateLaunchMarkers(now) {
        state.launchMarkers.forEach((marker, launchId) => {
            const isActive = marker.group.userData.active;
            const pulse = isActive ? 1 + 0.25 * Math.sin(now * 0.005) : 1 + 0.08 * Math.sin(now * 0.003 + launchId.length);
            marker.pickMesh.scale.setScalar(pulse);
        });
    }

    function getLaunchMarkerWorldData(launchId) {
        const marker = state.launchMarkers.get(launchId);
        if (!marker) return null;
        const position = new THREE.Vector3();
        marker.pickMesh.getWorldPosition(position);
        return {
            position,
            normal: position.clone().normalize()
        };
    }

    function frameLaunchMarker(worldData) {
        if (!state.camera || !state.controls || !worldData?.position) return;
        const target = worldData.position.clone();
        const normal = worldData.normal?.lengthSq() > 1e-6
            ? worldData.normal.clone().normalize()
            : target.clone().normalize();
        const currentOffset = state.camera.position.clone().sub(state.controls.target);
        let side = currentOffset.sub(normal.clone().multiplyScalar(currentOffset.dot(normal)));
        if (side.lengthSq() < 1e-6) {
            side = new THREE.Vector3(0, 1, 0).cross(normal);
        }
        if (side.lengthSq() < 1e-6) {
            side = new THREE.Vector3(1, 0, 0).cross(normal);
        }
        const viewDirection = normal
            .clone()
            .multiplyScalar(0.94)
            .add(side.normalize().multiplyScalar(0.2))
            .normalize();

        state.controls.target.copy(target);
        state.camera.position.copy(target).add(viewDirection.multiplyScalar(LAUNCH_FOCUS_VIEW_DISTANCE));
        state.camera.updateProjectionMatrix();
        state.controls.update();
    }

    function getObserverWorldPosition() {
        if (!state.observerMarker || !state.observerMarker.visible) return null;
        return state.observerMarker.getWorldPosition(new THREE.Vector3());
    }

    function getSatelliteWorldPosition(satelliteId) {
        const local = state.satelliteWorldPositions.get(satelliteId);
        if (!local || !state.earthMesh) return null;
        return state.earthMesh.localToWorld(local.clone());
    }

    function stopSatelliteFollow() {
        if (!state.followSatelliteId) return;
        state.followSatelliteId = null;
        refreshSatelliteFocusVisuals();
        renderSatelliteSearchResults();
    }

    function pickVisibleSatellite() {
        if (!state.satellitePoints?.visible || !state.satelliteLiveCount) return null;
        const previousThreshold = state.raycaster.params.Points?.threshold;
        state.raycaster.params.Points = state.raycaster.params.Points || {};
        state.raycaster.params.Points.threshold = SATELLITE_PICK_THRESHOLD;
        const hits = state.raycaster.intersectObject(state.satellitePoints, false);
        if (previousThreshold === undefined) {
            delete state.raycaster.params.Points.threshold;
        } else {
            state.raycaster.params.Points.threshold = previousThreshold;
        }

        const hit = hits.find((candidate) =>
            Number.isInteger(candidate.index) &&
            candidate.index >= 0 &&
            candidate.index < state.satelliteLiveCount
        );
        if (!hit) return null;
        const satelliteId = state.satelliteDrawOrder[hit.index];
        return satelliteId ? state.satelliteIndex.get(satelliteId) || null : null;
    }

    function focusObserverOnce() {
        const world = getObserverWorldPosition();
        if (!world) return false;
        exitFreeCamera();
        clearFocusModes();
        setFocusTarget(world);
        return true;
    }

    function toggleFollowObserver() {
        const world = getObserverWorldPosition();
        if (!world) return;
        exitFreeCamera();
        const next = !state.followObserver;
        clearFocusModes();
        if (next) {
            state.followObserver = true;
            setFocusTarget(world);
        }
        dom['observer-view-btn']?.classList.toggle('active', state.followObserver);
    }

    function focusSatelliteById(satelliteId, follow) {
        const satellite = state.satelliteIndex.get(satelliteId);
        if (!satellite) return;
        if (follow && state.followSatelliteId === satelliteId) {
            stopSatelliteFollow();
            return;
        }
        if (!orbitRegimeActive(satellite.regime)) {
            state.satelliteFilters[satellite.regime] = true;
            document.querySelector(`[data-sat-filter="${satellite.regime}"]`)?.setAttribute('aria-pressed', 'true');
            propagateSatellites(true);
        }

        const world = getSatelliteWorldPosition(satelliteId);
        if (!world) return;
        exitFreeCamera();
        clearFocusModes();
        setFocusTarget(world);
        if (follow) {
            state.followSatelliteId = satelliteId;
            frameFollowedSatellite(world, satellite);
            ensureSatelliteProfile(satellite);
            if (isMobileViewport()) openMobilePanel('satellite');
        }
        refreshSatelliteFocusVisuals();
        renderSatelliteSearchResults();
    }

    function clearFocusModes() {
        state.followMoon = false;
        state.followOrion = false;
        state.followObserver = false;
        state.followSatelliteId = null;
        state.focusLaunchId = null;
        state.focusedBody = null;
        refreshSatelliteFocusVisuals();
        dom['observer-view-btn']?.classList.remove('active');
        dom['moon-view-btn']?.classList.remove('active');
        dom['follow-artemis']?.classList.remove('active');
        if (document.body.classList.contains('search-open')) {
            renderSatelliteSearchResults();
        }
    }

    function onControlStart() {
        if (state.freeCameraMode) return;
        if (state.followSatelliteId) {
            state.userNavigatingCamera = false;
            return;
        }
        state.userNavigatingCamera = true;
        clearFocusModes();
    }

    function onControlEnd() {
        state.userNavigatingCamera = false;
    }

    function onScenePointerDown(event) {
        if (event.button !== 0) return;
        state.pointerDownScreen = { x: event.clientX, y: event.clientY, dragged: false };
    }

    function onScenePointerMove(event) {
        if (!state.pointerDownScreen) return;
        const dx = event.clientX - state.pointerDownScreen.x;
        const dy = event.clientY - state.pointerDownScreen.y;
        if (Math.hypot(dx, dy) >= SCENE_CLICK_DRAG_TOLERANCE_PX) {
            state.pointerDownScreen.dragged = true;
        }
    }

    function onScenePointerUp() {
        if (state.pointerDownScreen?.dragged) {
            state.sceneClickBlockedUntil = performance.now() + 220;
        }
        state.pointerDownScreen = null;
    }

    function onScenePointerCancel() {
        state.pointerDownScreen = null;
    }

    function focusSelectedLaunch() {
        const launch = getSelectedLaunch();
        if (!launch) return;
        const launchId = launchKey(launch);
        const world = getLaunchMarkerWorldData(launchId);
        if (!world) return;
        exitFreeCamera();
        state.focusLaunchId = launchId;
        state.focusedBody = null;
        state.followObserver = false;
        state.followSatelliteId = null;
        state.followMoon = false;
        state.followOrion = false;
        refreshSatelliteFocusVisuals();
        dom['observer-view-btn']?.classList.remove('active');
        dom['moon-view-btn']?.classList.remove('active');
        dom['follow-artemis']?.classList.remove('active');
        frameLaunchMarker(world);
    }

    function focusFromPick(kind, planetIndex) {
        exitFreeCamera();
        clearFocusModes();
        if (kind === 'sun') {
            setFocusTarget(state.sunScenePos.clone());
            return;
        }
        if (kind === 'moon') {
            setFocusTarget(state.moonMesh.position.clone());
            return;
        }
        if (kind === 'planet') {
            if (planetIndex === 2) {
                setFocusTarget(new THREE.Vector3(0, 0, 0));
            } else if (state.planetMeshes[planetIndex]) {
                setFocusTarget(state.planetMeshes[planetIndex].position.clone());
            }
        }
    }

    function jumpCameraToBody() {
        if (!state.focusedBody) return;
        const target = new THREE.Vector3();

        if (state.focusedBody.kind === 'sun') {
            target.copy(state.sunScenePos);
        } else if (state.focusedBody.kind === 'planet') {
            const idx = state.focusedBody.index;
            if (idx === 2) {
                target.set(0, 0, 0);
            } else {
                const planetMesh = state.planetMeshes[idx];
                if (!planetMesh) return;
                target.copy(planetMesh.position);
            }
        }
        setFocusTarget(target);
    }

    function onSceneClick(event) {
        if (event.button !== 0) return;
        if (performance.now() < state.sceneClickBlockedUntil) return;
        if (state.followSatelliteId) return;
        const rect = state.renderer.domElement.getBoundingClientRect();
        state.pointerNdc.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
        state.pointerNdc.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;
        state.raycaster.setFromCamera(state.pointerNdc, state.camera);

        const pickedSatellite = pickVisibleSatellite();
        if (pickedSatellite) {
            focusSatelliteById(pickedSatellite.id, true);
            return;
        }

        const hits = state.raycaster.intersectObjects(state.pickableMeshes, false);
        if (!hits.length) return;
        const preferredHit = hits.find((hit) => hit.object?.userData?.pickKind === 'launch') || hits[0];
        const data = preferredHit.object.userData || {};
        if (data.pickKind === 'launch') {
            selectLaunch(data.launchId, true);
            return;
        }
        if (data.pickKind === 'sun') focusFromPick('sun');
        if (data.pickKind === 'moon') focusFromPick('moon');
        if (data.pickKind === 'planet' && data.planetIndex !== undefined) focusFromPick('planet', data.planetIndex);
    }

    function formatWarpLabel(value) {
        const absolute = Math.abs(value);
        const core = absolute >= 1000 ? formatNumber(absolute) : String(absolute);
        return `${value < 0 ? '-' : ''}${core}x`;
    }

    function refreshWarpButtons() {
        dom['warp-reset-btn']?.classList.toggle('active', state.timeWarp === 1);
        dom['warp-forward-btn']?.classList.toggle('active', state.timeWarp > 1);
        dom['warp-backward-btn']?.classList.toggle('active', state.timeWarp < 0);
        dom['warp-display'].textContent = formatWarpLabel(state.timeWarp);
    }

    function warpToOne() {
        state.timeWarp = 1;
        state.warpStepMag = 10;
        state.warpTrack = 'idle';
        refreshWarpButtons();
    }

    function cycleWarpForward() {
        state.warpStepMag = state.warpTrack === 'forward' && state.timeWarp >= 10
            ? Math.min(state.warpStepMag * 10, 1e12)
            : 10;
        state.warpTrack = 'forward';
        state.timeWarp = state.warpStepMag;
        refreshWarpButtons();
    }

    function cycleWarpBackward() {
        state.warpStepMag = state.warpTrack === 'backward' && state.timeWarp <= -10
            ? Math.min(state.warpStepMag * 10, 1e12)
            : 10;
        state.warpTrack = 'backward';
        state.timeWarp = -state.warpStepMag;
        refreshWarpButtons();
    }

    function toggleFreeCamera() {
        state.freeCameraMode = !state.freeCameraMode;
        dom['free-cam-btn']?.classList.toggle('active', state.freeCameraMode);
        state.controls.enabled = true;
        state.controls.enablePan = !state.freeCameraMode;
        if (state.freeCameraMode) {
            clearFocusModes();
        }
    }

    function exitFreeCamera() {
        if (!state.freeCameraMode) return;
        state.freeCameraMode = false;
        state.controls.enabled = true;
        state.controls.enablePan = true;
        dom['free-cam-btn']?.classList.remove('active');
        Object.keys(state.flyKeys).forEach((key) => { state.flyKeys[key] = false; });
    }

    function toggleMoonView() {
        exitFreeCamera();
        clearFocusModes();
        setFocusTarget(state.moonMesh.position.clone());
        dom['moon-view-btn']?.classList.add('active');
    }

    function toggleFollowOrion() {
        if (!state.artemisReplayEnabled) {
            setArtemisReplayEnabled(true);
        }
        exitFreeCamera();
        state.followOrion = !state.followOrion;
        if (state.followOrion) {
            state.focusLaunchId = null;
            state.focusedBody = null;
            state.followObserver = false;
            state.followSatelliteId = null;
            state.followMoon = false;
            refreshSatelliteFocusVisuals();
            dom['observer-view-btn']?.classList.remove('active');
            dom['moon-view-btn']?.classList.remove('active');
        }
        dom['follow-artemis']?.classList.toggle('active', state.followOrion);
    }

    function resetView() {
        exitFreeCamera();
        clearFocusModes();
        setFocusTarget(new THREE.Vector3(0, 0, 0));
    }

    function solarSystemView() {
        exitFreeCamera();
        clearFocusModes();
        setFocusTarget(state.sunScenePos.clone());
    }

    function jumpToNow() {
        state.simTime = Date.now();
        warpToOne();
        clearFocusModes();
    }

    function formatMissionMetReadout(metHours) {
        const totalSeconds = Math.max(0, metHours * 3600);
        const days = Math.floor(totalSeconds / 86400);
        const hours = Math.floor((totalSeconds % 86400) / 3600);
        const minutes = Math.floor((totalSeconds % 3600) / 60);
        const seconds = Math.floor(totalSeconds % 60);
        return `T+ ${days}d ${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')} MET`;
    }

    function syncMissionSlider() {
        if (!dom['mission-met-slider'] || state.missionSliderDragging || state.totalMissionHours <= 0) return;
        const met = ARTEMIS.getMET(sceneTimeMs());
        const clamped = THREE.MathUtils.clamp(met, 0, state.totalMissionHours);
        const pct = (clamped / state.totalMissionHours) * 1000;
        dom['mission-met-slider'].value = String(Math.round(pct * 1000) / 1000);
        dom['mission-met-readout'].textContent = formatMissionMetReadout(clamped);
    }

    function onMissionSliderInput() {
        if (!dom['mission-met-slider'] || state.totalMissionHours <= 0) return;
        const t = dom['mission-met-slider'].valueAsNumber / 1000;
        const met = t * state.totalMissionHours;
        if (!state.artemisReplayEnabled) {
            setArtemisReplayEnabled(true);
        }
        state.simTime = simTimeFromMissionMet(met);
        dom['mission-met-readout'].textContent = formatMissionMetReadout(met);
    }

    function jumpToMissionMet(metHours) {
        if (!state.artemisReplayEnabled) {
            setArtemisReplayEnabled(true);
        }
        state.simTime = simTimeFromMissionMet(THREE.MathUtils.clamp(metHours, 0, state.totalMissionHours));
        syncMissionSlider();
    }

    function localizeMissionText(name) {
        const missionMap = new Map([
            ['Vor dem Start', 'mission.beforeStart'],
            ['Start & Erdorbit', 'mission.phase.earthOrbit'],
            ['Hochelliptischer Orbit', 'mission.phase.highOrbit'],
            ['TLI-Burn & Erdabflug', 'mission.phase.tli'],
            ['Mondtransit (Hinflug)', 'mission.phase.outbound'],
            ['Mond-Flyby', 'mission.phase.flyby'],
            ['Rücktransit zur Erde', 'mission.phase.return'],
            ['Wiedereintritt & Landung', 'mission.phase.entry'],
            ['Booster-Abtrennung (SRB)', 'mission.milestone.boosterSep'],
            ['ICPS-Abtrennung', 'mission.milestone.icpsSep'],
            ['Perigäum-Anhebung (PRM)', 'mission.milestone.prm'],
            ['Apogäum-Anhebung (ARB)', 'mission.milestone.arb'],
            ['Proximity-Ops-Test mit ICPS', 'mission.milestone.icpsProximity'],
            ['Perigäum-Korrektur (PRB)', 'mission.milestone.prb'],
            ['TLI-Burn (Mondkurs)', 'mission.milestone.tli'],
            ['Outbound-Kurskorrektur OTC-1', 'mission.milestone.otc1'],
            ['Outbound-Kurskorrektur OTC-2', 'mission.milestone.otc2'],
            ['Annäherung an den Mond', 'mission.milestone.moonApproach'],
            ['Funkverlust (Mondrückseite)', 'mission.milestone.signalLoss'],
            ['Nächste Mondannäherung', 'mission.milestone.closestMoon'],
            ['Funkkontakt wiederhergestellt', 'mission.milestone.signalReturn'],
            ['Mond-Flyby abgeschlossen', 'mission.milestone.flybyComplete'],
            ['Return-Kurskorrektur RTC-1', 'mission.milestone.rtc1'],
            ['Return-Kurskorrektur RTC-2', 'mission.milestone.rtc2'],
            ['Service-Modul-Abtrennung', 'mission.milestone.serviceModuleSep'],
            ['Wiedereintritt (Entry Interface)', 'mission.milestone.reentry'],
            ['Fallschirm-Entfaltung', 'mission.milestone.parachute'],
            ['Splashdown im Pazifik', 'mission.milestone.splashdown']
        ]);
        const key = missionMap.get(name);
        return key ? t(key) : name;
    }

    function buildMissionTimeline() {
        if (!dom['mission-timeline-items']) return;
        dom['mission-timeline-items'].innerHTML = '';
        ARTEMIS.MILESTONES.forEach((milestone, index) => {
            const button = document.createElement('button');
            button.type = 'button';
            button.className = 'mission-step future';
            button.id = `mission-step-${index}`;

            const dot = document.createElement('div');
            dot.className = 'mission-step-dot';

            const content = document.createElement('div');
            const title = document.createElement('strong');
            title.textContent = localizeMissionText(milestone.name);
            const timing = document.createElement('span');
            timing.textContent = `T+ ${formatTimerLabel(milestone.t)}`;

            content.append(title, timing);
            button.append(dot, content);
            button.addEventListener('click', () => {
                jumpToMissionMet(milestone.t);
            });
            dom['mission-timeline-items'].appendChild(button);
        });
    }

    function formatTimerLabel(hours) {
        const totalSeconds = Math.max(0, hours * 3600);
        const days = Math.floor(totalSeconds / 86400);
        const hh = Math.floor((totalSeconds % 86400) / 3600);
        const mm = Math.floor((totalSeconds % 3600) / 60);
        const ss = Math.floor(totalSeconds % 60);
        return `${days}d ${String(hh).padStart(2, '0')}:${String(mm).padStart(2, '0')}:${String(ss).padStart(2, '0')}`;
    }

    function updateMissionTimeline(metHours) {
        let activeIndex = -1;
        for (let i = ARTEMIS.MILESTONES.length - 1; i >= 0; i--) {
            if (metHours >= ARTEMIS.MILESTONES[i].t) {
                activeIndex = i;
                break;
            }
        }

        ARTEMIS.MILESTONES.forEach((milestone, index) => {
            const element = document.getElementById(`mission-step-${index}`);
            if (!element) return;
            element.classList.remove('done', 'active', 'future');
            if (index < activeIndex) element.classList.add('done');
            else if (index === activeIndex) element.classList.add('active');
            else element.classList.add('future');
        });

        if (activeIndex !== state.missionTimelineActiveIndex) {
            state.missionTimelineActiveIndex = activeIndex;
            const activeElement = activeIndex >= 0 ? document.getElementById(`mission-step-${activeIndex}`) : null;
            if (activeElement && document.body.classList.contains('settings-open')) {
                activeElement.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
            }
        }
    }

    function formatDistance(km) {
        if (!Number.isFinite(km)) return '--';
        if (km >= 1000) return `${formatNumber(Math.round(km))} km`;
        return `${km.toFixed(0)} km`;
    }

    function formatVelocity(kms) {
        if (!Number.isFinite(kms)) return '--';
        if (kms >= 10) return `${kms.toFixed(1)} km/s`;
        if (kms >= 1) return `${kms.toFixed(2)} km/s`;
        return `${(kms * 1000).toFixed(0)} m/s`;
    }

    function updateArtemisPanel(metHours) {
        const phase = ARTEMIS.getPhase(metHours);
        const pos = ARTEMIS.interpolatePosition(metHours);
        const moonPos = ARTEMIS.getMoonPosition(metHours);
        const distEarth = Math.sqrt(pos.x ** 2 + pos.y ** 2 + pos.z ** 2) * 1000;
        const distMoon = Math.sqrt(
            (pos.x - moonPos.x) ** 2 +
            (pos.y - moonPos.y) ** 2 +
            (pos.z - moonPos.z) ** 2
        ) * 1000;
        const velocity = ARTEMIS.getVelocity(metHours);

        const totalSeconds = Math.max(0, metHours * 3600);
        const days = Math.floor(totalSeconds / 86400);
        const hours = Math.floor((totalSeconds % 86400) / 3600);
        const minutes = Math.floor((totalSeconds % 3600) / 60);
        const seconds = Math.floor(totalSeconds % 60);

        dom['met-clock'].textContent =
            `T+ ${days}d ${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
        dom['mission-phase'].textContent = localizeMissionText(phase.name);
        dom['mission-date'].textContent = formatLocalDateTime(new Date(sceneTimeMs()));
        dom['dist-earth'].textContent = formatDistance(distEarth);
        dom['dist-moon'].textContent = formatDistance(distMoon);
        dom['velocity'].textContent = formatVelocity(velocity);
        dom['mission-met-readout'].textContent = formatMissionMetReadout(metHours);
        dom['mission-progress-fill'].style.width = `${Math.max(0, Math.min(1, metHours / state.totalMissionHours)) * 100}%`;
        updateMissionTimeline(metHours);
    }

    function updateTrajectoryLines(metHours) {
        if (!state.fullTrajectory.length || !state.artemisReplayEnabled) return;
        const past = [];
        const future = [];
        state.fullTrajectory.forEach((point) => {
            const vector = new THREE.Vector3(point.x, point.y, point.z);
            if (point.t <= metHours) {
                past.push(vector);
            } else {
                if (!future.length && past.length) future.push(past[past.length - 1].clone());
                future.push(vector);
            }
        });

        if (past.length) {
            state.pastLine.geometry.dispose();
            state.pastLine.geometry = new THREE.BufferGeometry().setFromPoints(past);
        }
        if (future.length) {
            state.futureLine.geometry.dispose();
            state.futureLine.geometry = new THREE.BufferGeometry().setFromPoints(future);
            state.futureLine.computeLineDistances();
        }
    }

    function updateSolarSystem(dateMs) {
        const T = ARTEMIS.getJulianCenturies(dateMs);
        const sunPos = ARTEMIS.getSunPosition(T);
        state.sunScenePos.set(sunPos.x, sunPos.y, sunPos.z);

        state.sunMesh.position.copy(state.sunScenePos);
        state.sunGlow.position.copy(state.sunScenePos);
        state.sunPointLight.position.copy(state.sunScenePos);

        const sunDirection = state.sunScenePos.clone().normalize();
        state.sunDirLight.position.copy(sunDirection.clone().multiplyScalar(500));
        state.fillDirLight.position.copy(sunDirection.clone().multiplyScalar(-250));
        if (state.earthNightUniforms) {
            state.earthNightUniforms.earthNightSunDirection.value.copy(sunDirection);
        }

        Object.keys(state.planetMeshes).forEach((key) => {
            const index = Number(key);
            const mesh = state.planetMeshes[index];
            const position = ARTEMIS.getPlanetPosition(index, T);
            mesh.position.set(position.x, position.y, position.z);
            if (mesh.userData.saturnRing) {
                mesh.userData.saturnRing.position.copy(mesh.position);
            }
            refreshOrbitLineGeometry(state.planetOrbits[index], index, T);
        });
        refreshOrbitLineGeometry(state.planetOrbits[2], 2, T);
    }

    function refreshOrbitLineGeometry(line, planetIndex, T) {
        if (!line) return;
        const orbitPoints = ARTEMIS.getPlanetOrbitPoints(planetIndex, T, 256);
        const geometry = line.geometry;
        const positionAttr = geometry.attributes.position;
        if (positionAttr && positionAttr.count === orbitPoints.length) {
            const array = positionAttr.array;
            orbitPoints.forEach((point, index) => {
                array[index * 3] = point.x;
                array[index * 3 + 1] = point.y;
                array[index * 3 + 2] = point.z;
            });
            positionAttr.needsUpdate = true;
            geometry.computeBoundingSphere();
        } else {
            line.geometry.dispose();
            line.geometry = new THREE.BufferGeometry().setFromPoints(
                orbitPoints.map((point) => new THREE.Vector3(point.x, point.y, point.z))
            );
        }
        line.computeLineDistances();
    }

    function earthRotationAngleForMs(dateMs) {
        const jd = (dateMs / 86400000) + 2440587.5;
        const T = (jd - 2451545.0) / 36525;
        const gmstDeg =
            280.46061837 +
            360.98564736629 * (jd - 2451545.0) +
            0.000387933 * T * T -
            (T * T * T) / 38710000;
        // Greenwich starts on the texture's +X meridian, while the J2000
        // reference direction in this display frame points along -Z. GMST
        // advances eastward, so the display rotation must advance with it.
        return THREE.MathUtils.euclideanModulo(
            EARTH_SIDEREAL_REFERENCE_OFFSET_RAD + THREE.MathUtils.degToRad(gmstDeg),
            Math.PI * 2
        );
    }

    function updateEarthRotation(dateMs) {
        if (!state.earthMesh || !state.earthGroup) return;
        state.earthRotationAngle = earthRotationAngleForMs(dateMs);
        state.earthMesh.rotation.y = state.earthRotationAngle;
        if (state.earthCloudMesh) {
            state.earthCloudMesh.rotation.y = state.earthRotationAngle * 1.025;
        }
    }

    function distToSliderValue(distance) {
        const d = Math.max(ZOOM_DIST_MIN, Math.min(ZOOM_DIST_MAX, distance));
        const lo = Math.log(ZOOM_DIST_MIN);
        const hi = Math.log(ZOOM_DIST_MAX);
        return (Math.log(d) - lo) / (hi - lo);
    }

    function sliderValueToDist(t) {
        const lo = Math.log(ZOOM_DIST_MIN);
        const hi = Math.log(ZOOM_DIST_MAX);
        return Math.exp(lo + Math.max(0, Math.min(1, t)) * (hi - lo));
    }

    function applyCameraZoomDistance(distance) {
        const clamped = THREE.MathUtils.clamp(distance, ZOOM_DIST_MIN, ZOOM_DIST_MAX);
        const offset = state.camera.position.clone().sub(state.controls.target);
        if (offset.lengthSq() < 1e-6) {
            offset.set(0.35, 0.25, 1).normalize();
        } else {
            offset.normalize();
        }
        offset.multiplyScalar(clamped);
        state.camera.position.copy(state.controls.target).add(offset);
        state.camera.updateProjectionMatrix();
    }

    function onZoomSliderInput() {
        if (!dom['zoom-slider']) return;
        applyCameraZoomDistance(sliderValueToDist(dom['zoom-slider'].valueAsNumber / 1000));
    }

    function formatZoomReadout(distance) {
        const km = distance * 1000;
        if (km >= 1e6) return t('zoom.millionKm', { value: formatNumber(km / 1e6, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) });
        if (km >= 1000) return `${formatNumber(Math.round(km))} km`;
        return `${Math.round(km)} km`;
    }

    function onResize() {
        state.camera.aspect = window.innerWidth / window.innerHeight;
        state.camera.updateProjectionMatrix();
        state.renderer.setSize(window.innerWidth, window.innerHeight);
        if (isMobileViewport()) closeSettings();
        const limits = mobileSheetLimits();
        Object.keys(state.mobileSheetHeights).forEach((key) => {
            state.mobileSheetHeights[key] = THREE.MathUtils.clamp(state.mobileSheetHeights[key], limits.min, limits.max);
        });
        applyMobilePanelState();
    }

    function onKeyDown(event) {
        if (state.freeCameraMode && Object.prototype.hasOwnProperty.call(state.flyKeys, event.key)) {
            event.preventDefault();
            state.flyKeys[event.key] = true;
        }
        if (event.key === 'Escape') {
            if (state.statsPanelOpen) {
                closeStatsPanel();
                return;
            }
            if (state.mobileActivePanel) {
                closeMobileSheet();
                return;
            }
            if (document.body.classList.contains('search-open')) {
                closeSearch();
                return;
            }
            if (document.body.classList.contains('settings-open')) {
                closeSettings();
                return;
            }
            if (state.freeCameraMode) {
                exitFreeCamera();
                return;
            }
            clearFocusModes();
        }
    }

    function onKeyUp(event) {
        if (state.freeCameraMode && Object.prototype.hasOwnProperty.call(state.flyKeys, event.key)) {
            state.flyKeys[event.key] = false;
        }
    }

    function animate() {
        requestAnimationFrame(animate);

        const now = performance.now();
        const dtReal = (now - state.lastFrameTime) / 1000;
        state.lastFrameTime = now;

        if (state.freeCameraMode) {
            const distance = state.camera.position.distanceTo(state.controls.target);
            const speed = THREE.MathUtils.clamp(distance * 0.12, 40, 120000) * dtReal;
            const forward = new THREE.Vector3();
            state.camera.getWorldDirection(forward);
            const right = new THREE.Vector3().crossVectors(forward, state.camera.up).normalize();
            const move = new THREE.Vector3();
            if (state.flyKeys.ArrowUp) move.addScaledVector(forward, speed);
            if (state.flyKeys.ArrowDown) move.addScaledVector(forward, -speed);
            if (state.flyKeys.ArrowLeft) move.addScaledVector(right, -speed);
            if (state.flyKeys.ArrowRight) move.addScaledVector(right, speed);
            if (move.lengthSq() > 0) {
                state.camera.position.add(move);
                state.controls.target.add(move);
            }
        }

        if (!state.missionSliderDragging) {
            state.simTime += dtReal * 1000 * state.timeWarp;
        }

        updateSolarSystem(sceneTimeMs());
        updateEarthRotation(earthReferenceTimeMs());
        syncLaunchTrajectoryFrame();

        const rawMet = ARTEMIS.getMET(sceneTimeMs());
        const clampedMet = THREE.MathUtils.clamp(rawMet, 0, state.totalMissionHours);
        const moonMet = Number.isFinite(rawMet) ? rawMet : 0;

        const moonPos = ARTEMIS.getMoonPosition(moonMet);
        state.moonMesh.position.set(moonPos.x, moonPos.y, moonPos.z);
        state.moonLabel.position.set(moonPos.x, moonPos.y + ARTEMIS.MOON_RADIUS + 3, moonPos.z);

        if (state.artemisReplayEnabled) {
            const orionPos = ARTEMIS.interpolatePosition(clampedMet);
            state.orionMarker.position.set(orionPos.x, orionPos.y, orionPos.z);
            state.orionGlow.position.set(orionPos.x, orionPos.y, orionPos.z);
            state.orionLabel.position.set(orionPos.x, orionPos.y + 5, orionPos.z);
            state.orionGlow.material.opacity = 0.18 + 0.09 * Math.sin(now * 0.003);
            state.orionGlow.scale.setScalar(1 + 0.18 * Math.sin(now * 0.004));
            if (Math.floor(now / 180) !== Math.floor((now - dtReal * 1000) / 180)) {
                updateTrajectoryLines(clampedMet);
            }
        }

        if (!state.freeCameraMode) {
            if (state.followSatelliteId) {
                const satelliteWorld = getSatelliteWorldPosition(state.followSatelliteId);
                if (satelliteWorld) {
                    const previousTarget = state.controls.target.clone();
                    state.controls.target.lerp(satelliteWorld, 0.32);
                    state.camera.position.add(state.controls.target.clone().sub(previousTarget));
                }
            } else if (!state.userNavigatingCamera && state.focusLaunchId) {
                const launchWorld = getLaunchMarkerWorldData(state.focusLaunchId);
                if (launchWorld) {
                    const previousTarget = state.controls.target.clone();
                    state.controls.target.lerp(launchWorld.position, 0.22);
                    state.camera.position.add(state.controls.target.clone().sub(previousTarget));
                } else {
                    state.focusLaunchId = null;
                }
            } else if (!state.userNavigatingCamera && state.focusedBody) {
                const target = new THREE.Vector3();
                if (state.focusedBody.kind === 'sun') target.copy(state.sunScenePos);
                else if (state.focusedBody.kind === 'planet') {
                    const idx = state.focusedBody.index;
                    if (idx === 2) target.set(0, 0, 0);
                    else target.copy(state.planetMeshes[idx].position);
                }
                state.controls.target.lerp(target, 0.12);
            } else if (!state.userNavigatingCamera && state.followObserver) {
                const observerWorld = getObserverWorldPosition();
                if (observerWorld) state.controls.target.lerp(observerWorld, 0.12);
            } else if (!state.userNavigatingCamera && state.followMoon) {
                state.controls.target.lerp(state.moonMesh.position, 0.1);
            } else if (!state.userNavigatingCamera && state.followOrion && state.artemisReplayEnabled) {
                state.controls.target.lerp(state.orionMarker.position, 0.08);
            }
        }

        const camTargetDist = state.camera.position.distanceTo(state.controls.target);
        updateSatellitePointSizing(camTargetDist);
        const showAllOrbits = camTargetDist >= ORBITS_ALL_DISTANCE;
        state.planetOrbitList.forEach((line) => { line.visible = showAllOrbits; });
        if (state.planetOrbits[2]) state.planetOrbits[2].visible = true;
        if (state.moonOrbitLine) state.moonOrbitLine.visible = true;

        if (state.planetOrbits[2] && state.planetOrbits[2].visible) {
            const dash = THREE.MathUtils.clamp(camTargetDist * 0.07, 18, 16000);
            state.planetOrbits[2].material.dashSize = dash;
            state.planetOrbits[2].material.gapSize = dash * 0.48;
        }

        if (!state.zoomSliderDragging && dom['zoom-slider']) {
            dom['zoom-slider'].value = String(Math.round(distToSliderValue(camTargetDist) * 1000));
        }
        dom['zoom-readout'].textContent = formatZoomReadout(camTargetDist);
        const localNow = new Date();
        if (dom['real-time-zone']) {
            dom['real-time-zone'].textContent = getLocalTimeZoneLabel(localNow);
        }
        dom['real-time-berlin'].textContent = formatLocalTimeOnly(localNow);

        state.dynamicLabels.forEach((label) => {
            const distance = state.camera.position.distanceTo(label.position);
            const scale = distance * 0.08;
            label.scale.set(scale, scale * 0.25, 1);
            if (label._anchor && label._offsetY !== undefined) {
                const offsetY = distance * 0.012;
                label.position.set(label._anchor.x, label._anchor.y + offsetY, label._anchor.z);
            }
        });

        updateLaunchMarkers(now);
        updateObserverMarker();
        if (state.observerPulse) {
            const pulseScale = 1 + 0.22 * Math.sin(now * 0.006);
            state.observerPulse.scale.setScalar(pulseScale);
            state.observerPulse.material.opacity = 0.11 + 0.08 * (0.5 + 0.5 * Math.sin(now * 0.006));
        }
        updateSatelliteHighlight(now);
        propagateSatellites();
        updateArtemisPanel(clampedMet);
        syncMissionSlider();
        state.controls.update();
        state.renderer.render(state.scene, state.camera);
    }

    window.addEventListener('DOMContentLoaded', init);
})();
