import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';

(function () {
    'use strict';

    const ARTEMIS = window.ARTEMIS2;
    if (!ARTEMIS) {
        throw new Error('ARTEMIS2 data source is missing.');
    }

    const UI_STORAGE_KEY = 'artemisobserver-ui-v2';
    const SATELLITE_CACHE_KEY = 'artemisobserver-satellite-cache-v1';
    const LAUNCH_FEED_DATA_URL = 'data/launch-feed.json';
    const LAUNCH_DB_DATA_URL = 'data/launch-db.json';
    const LAUNCH_STATS_DATA_URL = 'data/launch-stats.json';
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
    const SATELLITE_ORBIT_SAMPLE_COUNT = 360;
    const SATELLITE_ORBIT_DEFAULT_REVOLUTIONS = 2;
    const SATELLITE_ORBIT_MIN_REVOLUTIONS = 1;
    const SATELLITE_ORBIT_MAX_REVOLUTIONS = 5;
    const SATELLITE_ORBIT_REFRESH_MS = 30 * 1000;
    const SCENE_CLICK_DRAG_TOLERANCE_PX = 7;

    const localDateTime = new Intl.DateTimeFormat(undefined, {
        weekday: 'short',
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false
    });

    const localShortDateTime = new Intl.DateTimeFormat(undefined, {
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        timeZoneName: 'short'
    });

    const localTimeOnly = new Intl.DateTimeFormat(undefined, {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false
    });

    function getLocalTimeZoneLabel(date = new Date()) {
        try {
            const part = new Intl.DateTimeFormat(undefined, { timeZoneName: 'short' })
                .formatToParts(date)
                .find((entry) => entry.type === 'timeZoneName');
            return part?.value || Intl.DateTimeFormat().resolvedOptions().timeZone || 'Lokal';
        } catch (error) {
            return 'Lokal';
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
        state.satelliteLastError = `Satellitenbibliothek konnte nicht geladen werden (${failedSources.length} CDN-Versuche).`;
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
        observerMarker: null,
        observerPulse: null,
        observerLocation: null,
        observerWatchId: null,
        satellitePoints: null,
        satelliteHighlight: null,
        satelliteFocusedModelKey: '',
        satelliteOrbitLine: null,
        satelliteOrbitLastKey: '',
        satelliteCatalog: [],
        satelliteIndex: new Map(),
        satelliteCatalogLoaded: false,
        satelliteLibraryReady: false,
        satelliteLastError: '',
        satelliteLiveCount: 0,
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
            window.matchMedia('(max-width: 960px)').matches;
    }

    function clampSatelliteOrbitRevolutions(value) {
        return THREE.MathUtils.clamp(
            Math.round(Number(value) || SATELLITE_ORBIT_DEFAULT_REVOLUTIONS),
            SATELLITE_ORBIT_MIN_REVOLUTIONS,
            SATELLITE_ORBIT_MAX_REVOLUTIONS
        );
    }

    function readUiState() {
        const defaults = {
            news: true,
            watch: true,
            controls: true,
            orbitRevolutions: SATELLITE_ORBIT_DEFAULT_REVOLUTIONS
        };
        try {
            const raw = localStorage.getItem(UI_STORAGE_KEY);
            if (!raw) return defaults;
            const parsed = { ...defaults, ...JSON.parse(raw) };
            parsed.orbitRevolutions = clampSatelliteOrbitRevolutions(parsed.orbitRevolutions);
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
        if (!savedAt) return 'noch nicht generiert';
        const ageMs = Math.max(0, Date.now() - savedAt);
        const minutes = Math.max(1, Math.round(ageMs / 60000));
        if (minutes < 60) return `${minutes} min alt`;
        const hours = Math.round(minutes / 60);
        return `${hours} h alt`;
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
            'satellite-orbit-revolutions',
            'satellite-orbit-revolutions-readout',
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
            'toggle-news',
            'toggle-watch',
            'toggle-controls',
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
    }

    function closeLaunchDetailPanel() {
        state.launchDetailActive = false;
        state.selectedLaunchId = null;
        applyLaunchDetailPanelState();
        refreshSelectedLaunchUi();
    }

    function satelliteOrbitRevolutionsLabel(value) {
        return value === 1 ? '1 Umlauf' : `${value} Umlaeufe`;
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

    function bindUi() {
        dom['search-toggle']?.addEventListener('click', openSearch);
        dom['search-close']?.addEventListener('click', closeSearch);
        dom['search-scrim']?.addEventListener('click', closeSearch);
        dom['settings-toggle']?.addEventListener('click', openSettings);
        dom['settings-close']?.addEventListener('click', closeSettings);
        dom['settings-scrim']?.addEventListener('click', closeSettings);

        document.querySelectorAll('[data-ui-toggle]').forEach((button) => {
            button.addEventListener('click', () => {
                const key = button.getAttribute('data-ui-toggle');
                if (!key) return;
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
        dom['toggle-artemis-replay']?.addEventListener('click', () => {
            setArtemisReplayEnabled(!state.artemisReplayEnabled);
        });
        dom['jump-artemis-start']?.addEventListener('click', () => jumpToMissionMet(0));
        dom['jump-artemis-end']?.addEventListener('click', () => jumpToMissionMet(state.totalMissionHours));
        dom['follow-artemis']?.addEventListener('click', toggleFollowOrion);

        dom['satellite-orbit-revolutions']?.addEventListener('input', onSatelliteOrbitRevolutionsInput);

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
        applyPanelVisibility();
        syncSatelliteOrbitSettingsUi();
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
                size: 0.11,
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
        state.satelliteHighlight.quaternion.setFromRotationMatrix(basis);
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
            ? 'Replay-Modus: Artemis II im Raum aktiv'
            : 'Home-Modus: Earth Launch Tracker';
        dom['scene-mode-pill'].classList.toggle('status-live', state.artemisReplayEnabled);
        if (dom['toggle-artemis-replay']) {
            dom['toggle-artemis-replay'].textContent = state.artemisReplayEnabled
                ? 'Replay im Raum an'
                : 'Replay im Raum aus';
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
        if (diff <= 0) return 'Startfenster erreicht';
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
        return 'Unbekannt';
    }

    function launchKey(launch) {
        return String(launch?.id || `${launch?.name || 'launch'}-${launch?.net || launch?.window_start || ''}`);
    }

    function launchPadLabel(launch) {
        if (typeof launch?.pad === 'string' && launch.pad.trim()) {
            return [launch.pad.trim(), launch.padLocation].filter(Boolean).join(' · ');
        }
        const pad = launch?.pad?.name || 'Unbekanntes Pad';
        const location = launch?.pad?.location?.name || '';
        return [pad, location].filter(Boolean).join(' · ');
    }

    function launchRocketName(launch) {
        if (typeof launch?.rocket === 'string' && launch.rocket.trim()) return launch.rocket.trim();
        return launch?.rocket?.configuration?.full_name ||
            launch?.rocket?.configuration?.name ||
            'Rakete unbekannt';
    }

    function launchStatusLabel(launch) {
        if (launch?.statusName) return launch.statusName;
        if (typeof launch?.status === 'string' && launch.status.trim()) return launch.status.trim();
        return launch?.status?.name || 'Status unbekannt';
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
            if (launch.outcome === 'success') return { text: 'Erfolgreich', className: 'status-success' };
            if (launch.outcome === 'failure') return { text: 'Fehlgeschlagen', className: 'status-failure' };
            if (launch.outcome === 'cancelled') return { text: 'Scrubbed / abgesagt', className: 'status-cancelled' };
            if (launch.outcome === 'delayed') return { text: 'Verschoben', className: 'status-delayed' };
            if (launch.outcome === 'go') return { text: 'Go bei T-15', className: 'status-go' };
        }
        const status = classifyLaunchStatus(launch);
        if (status === 'success') return { text: 'Erfolgreich', className: 'status-success' };
        if (status === 'failure') return { text: 'Fehlgeschlagen', className: 'status-failure' };
        if (status === 'cancelled') return { text: 'Scrubbed / abgesagt', className: 'status-cancelled' };
        if (status === 'delayed') return { text: 'Verschoben', className: 'status-delayed' };
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

    function launchStory(launch) {
        if (launch?.missionDescription) return launch.missionDescription;
        if (typeof launch?.mission === 'string' && launch.mission.trim()) return launch.mission.trim();
        return launch?.mission?.description ||
            launch?.mission?.name ||
            'Zu diesem Start liegt im Feed keine Missionsbeschreibung vor.';
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
        if (status === 'cancelled') return 'Start abgesagt - Watch bleibt auf rot.';
        if (status === 'delayed') return 'Start verzoegert oder noch nicht bestaetigt.';
        if (status === 'failure') return 'Start fehlgeschlagen bestaetigt.';
        if (status === 'success' || launch?.postflightStatus === 'success') return 'Start erfolgreich bestaetigt.';
        if (launch?.preflightStatus === 'go' || status === 'live') return 'T-15 vom Worker bestaetigt - Timer ist live-gruen.';
        if (!when) return 'Monitoring wartet auf eine gueltige Startzeit.';

        const diff = when.getTime() - Date.now();
        if (diff > LAUNCH_VERIFY_WINDOW_MS) return 'Worker hat T-15 Check vorgemerkt.';
        if (diff > 0) return launch?.preflightCheckedAt ? 'Preflight-Status aus Worker-Daten geladen.' : 'Worker prueft den T-15 Status.';
        if (diff > -LAUNCH_SUCCESS_CHECK_DELAY_MS) return 'Startfenster erreicht - Worker prueft T+30.';
        return launch?.postflightCheckedAt ? 'Postflight-Status aus Worker-Daten geladen.' : 'Worker holt den T+30 Check nach.';
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
            if (stateLabel) stateLabel.textContent = stream.title || 'Offizieller Stream gefunden.';
            if (link) {
                link.href = streamUrl;
                link.textContent = 'Livestream oeffnen';
                link.classList.remove('is-hidden');
            }
            if (frame) {
                if (frame.src !== (embedUrl || 'about:blank')) frame.src = embedUrl || 'about:blank';
                frame.title = stream.title || 'Launch Livestream';
                frame.classList.toggle('is-hidden', !embedUrl);
            }
        } else {
            if (stateLabel) stateLabel.textContent = 'Noch kein offizieller Stream im Feed - Suche vorbereitet.';
            if (link) {
                link.href = searchUrl;
                link.textContent = 'Auf YouTube suchen';
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
        return { text: `gleich vs ${label}`, className: 'even' };
    }

    function setStatDelta(id, delta) {
        const element = dom[id];
        if (!element) return;
        element.textContent = delta.text;
        element.classList.remove('up', 'down', 'even');
        element.classList.add(delta.className);
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
        setStatDelta('launch-stat-success-week-delta', formatLaunchDelta(stats.week.current, stats.week.previous, 'Vorwoche'));
        setStatDelta('launch-stat-success-month-delta', formatLaunchDelta(stats.month.current, stats.month.previous, 'Vormonat'));
        setStatDelta('launch-stat-success-year-delta', formatLaunchDelta(stats.year.current, stats.year.previous, 'Vorjahr'));
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
            loading.textContent = 'Lade Starthistorie ...';
            dom['launch-feed-items'].appendChild(loading);
            return;
        }

        if (!items.length) {
            const empty = document.createElement('div');
            empty.className = 'launch-empty';
            empty.textContent = state.launchHistoryError || 'Noch keine historischen Starts geladen.';
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
            title.textContent = launch.name || 'Unbenannter Start';

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
            when.textContent = launchDate ? localShortDateTime.format(launchDate) : '--';

            const badgeData = launchStatusBadge(launch);
            const badge = document.createElement('div');
            badge.className = 'launch-history-status';
            if (badgeData.className) badge.classList.add(badgeData.className);
            badge.textContent = badgeData.text;

            item.append(title, meta, rocket, pad, when, badge);
            item.addEventListener('click', () => {
                state.launchWatchList.set(launchKey(launch), launch);
                selectLaunch(launchKey(launch), false);
            });
            dom['launch-feed-items'].appendChild(item);
        });

        if (state.launchHistoryLoading || !state.launchHistoryDone) {
            const footer = document.createElement('div');
            footer.className = 'launch-history-footer';
            footer.textContent = state.launchHistoryLoading ? 'Laedt weitere Historie ...' : 'Weiter scrollen fuer aeltere Starts';
            dom['launch-feed-items'].appendChild(footer);
        }
    }

    function updateLaunchHistoryStatus() {
        if (state.launchFeedMode !== 'history') return;
        if (state.launchHistoryError) {
            setLaunchFeedStatus(`Starthistorie wartet (${state.launchHistoryError})`);
            return;
        }
        setLaunchFeedStatus(`Worker-Historie · ${state.launchHistoryItems.length} beobachtete Starts`);
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
            state.launchHistoryError = error?.message || 'Statische Historie nicht erreichbar';
        } finally {
            state.launchHistoryLoading = false;
            updateLaunchHistoryStatus();
            renderLaunchHistory(state.launchHistoryItems);
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
                ? `Worker-Daten · ${dataAgeLabel(state.launchDataGeneratedAt)} · ${state.launches.length} Starts`
                : 'Lade Startdaten ...');
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
            empty.textContent = 'Keine anstehenden Earth-Launches mit Koordinaten gefunden.';
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
            title.textContent = launch.name || 'Unbenannter Start';

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
            net.textContent = launchDate ? localShortDateTime.format(launchDate) : '--';

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
            if (dom['watch-launch-title']) dom['watch-launch-title'].textContent = 'Ausgewaehlter Start';
            if (dom['watch-launch-subtitle']) dom['watch-launch-subtitle'].textContent = '--';
            if (dom['watch-launch-intel']) dom['watch-launch-intel'].textContent = 'Waehle rechts im Launch Feed einen Start aus.';
            return;
        }

        const when = launchInstant(launch);
        dom['watch-launch-title'].textContent = launch.name || 'Unbenannter Start';
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
        dom['watch-launch-window'].textContent = when ? localShortDateTime.format(when) : '--';
        dom['watch-launch-coords'].textContent = formatCoordinates(launch);
        dom['watch-launch-story'].textContent = launchStory(launch);
        if (dom['watch-launch-intel']) dom['watch-launch-intel'].textContent = launchIntelText(launch);
        updateLaunchStreamUi(launch);

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
        applyLaunchDetailPanelState();
        refreshSelectedLaunchUi();
        if (focus) focusSelectedLaunch();
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
        if (state.launchFeedMode === 'upcoming') setLaunchFeedStatus('Lade Startdaten ...');

        try {
            const payload = await fetchStaticJson(LAUNCH_FEED_DATA_URL);
            const launches = (Array.isArray(payload?.launches) ? payload.launches : [])
                .filter(isEarthLaunch)
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
                setLaunchFeedStatus(`Worker-Daten · ${dataAgeLabel(state.launchDataGeneratedAt)} · ${state.launches.length} Starts`);
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
                setLaunchFeedStatus(`Statische Startdaten nicht erreichbar${error?.message ? ` (${error.message})` : '.'}`);
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
            dom['satellite-search-status'].textContent = state.satelliteLastError || 'Die Satellitenbibliothek konnte nicht geladen werden.';
            return;
        }
        if (!state.satelliteCatalogLoaded) {
            dom['satellite-search-status'].textContent = state.satelliteLastError || 'Lade Satellitenkatalog ...';
            return;
        }

        if (state.satelliteLastError) {
            dom['satellite-search-status'].textContent = state.satelliteLastError;
            return;
        }

        const enabled = ORBIT_REGIMES.filter((regime) => orbitRegimeActive(regime)).join(', ') || 'keine';
        dom['satellite-search-status'].textContent =
            `${estimateSatelliteOrbitTotal().toLocaleString('de-DE')} im Orbit geschaetzt · ${state.satelliteCatalog.length.toLocaleString('de-DE')} oeffentlich trackbar · ${state.satelliteLiveCount.toLocaleString('de-DE')} live sichtbar · Filter: ${enabled}`;
    }

    function formatAltitudeKm(value) {
        if (!Number.isFinite(value)) return '--';
        return `${Math.round(value).toLocaleString('de-DE')} km`;
    }

    function formatSatelliteNumber(value, fractionDigits = 0, suffix = '') {
        if (!Number.isFinite(value)) return '--';
        return `${value.toLocaleString('de-DE', {
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
            return `RCS ${formatSatelliteNumber(satellite.rcsSquareMeters, 2)} m2 (keine Baugröße)`;
        }
        return '--';
    }

    function formatCoordinate(value, positive, negative) {
        if (!Number.isFinite(value)) return '--';
        const hemisphere = value >= 0 ? positive : negative;
        return `${Math.abs(value).toFixed(2).replace('.', ',')}° ${hemisphere}`;
    }

    function setText(id, value) {
        if (dom[id]) dom[id].textContent = value;
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
                return a.name.localeCompare(b.name, 'de');
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
            empty.textContent = 'Sobald der oeffentliche Satellitenkatalog geladen ist, erscheinen hier Treffer.';
            dom['satellite-search-results'].appendChild(empty);
            return;
        }

        const results = currentSatelliteSearchResults();
        if (!results.length) {
            const empty = document.createElement('div');
            empty.className = 'search-empty';
            empty.textContent = 'Kein oeffentlich trackbarer Satellit passt gerade zu deiner Suche.';
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
            meta.textContent = `${satellite.regime} · ${satellite.type || 'Satellit'} · ${satellite.operator || 'Profil ausstehend'} · Hoehe ${formatAltitudeKm(satellite.altitudeKm)} · NORAD ${satellite.id}`;

            const actions = document.createElement('div');
            actions.className = 'sat-result-actions';

            const focusBtn = document.createElement('button');
            focusBtn.type = 'button';
            focusBtn.className = 'action-btn';
            focusBtn.textContent = 'Fokus';
            focusBtn.addEventListener('click', () => focusSatelliteById(satellite.id, false));

            const followBtn = document.createElement('button');
            followBtn.type = 'button';
            followBtn.className = 'action-btn';
            followBtn.textContent = state.followSatelliteId === satellite.id ? 'Verfolgung an' : 'Folgen';
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
            regime === 'GEO' ? 'Satellit (GEO)' : 'Satellit',
            'SATCAT-Abfrage ausstehend',
            'SATCAT-Abfrage ausstehend',
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
        return SATCAT_COUNTRY_OVERRIDES[code] || SATCAT_OWNER_LABELS[code] || code || 'Nicht eindeutig';
    }

    function satelliteOperatorFallback(owner) {
        const code = String(owner || '').trim().toUpperCase();
        if (SATCAT_OPERATOR_LABELS[code]) return SATCAT_OPERATOR_LABELS[code];
        return code ? 'Nicht eindeutig' : 'Profil ausstehend';
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
        if (code === 'PAY') return 'Nutzlast/Satellit';
        if (code === 'R/B') return 'Raketenkörper';
        if (code === 'DEB') return 'Weltraumschrott';
        if (code === 'UNK') return 'Unbekanntes Objekt';
        return code || 'Satellit';
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

        satellite.profileSource = 'SATCAT/Wikidata-Abfrage laeuft';
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
                satellite.profileSource = `TLE/NORAD · Profil offline (${error.message || 'unbekannt'})`;
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
            regime === 'GEO' ? 'Satellit (GEO)' : 'Satellit',
            'Nicht eindeutig',
            'Nicht eindeutig',
            'TLE ohne Betreiberfeld'
        );

        if (/\b(DEB|DEBRIS)\b/.test(n)) return satelliteProfile('Weltraumschrott', 'Kein aktiver Betreiber', 'Nicht eindeutig', 'TLE-Name');
        if (/\b(R\/B|ROCKET BODY)\b/.test(n)) return satelliteProfile('Raketenkörper', 'Kein aktiver Betreiber', 'Nicht eindeutig', 'TLE-Name');
        if (/OBJECT\s+[A-Z0-9]+/.test(n)) return satelliteProfile('Nicht katalogisierte Nutzlast/Objekt', 'Nicht eindeutig', 'Nicht eindeutig', 'TLE-Name');

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
        return satelliteProfile(match[1], match[2], match[3], 'Aus Name abgeleitet');
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

        if (state.satellitePoints?.material) {
            state.satellitePoints.material.opacity = isFollowingSatellite
                ? SATELLITE_LAYER_DIMMED_OPACITY
                : SATELLITE_LAYER_OPACITY;
            state.satellitePoints.material.needsUpdate = true;
        }
        updateSatelliteFocusPanel(followedSatellite);
        updateSatelliteHighlight(performance.now());
        updateSatelliteOrbitPath();
    }

    function updateSatelliteFocusPanel(satellite) {
        if (!dom['satellite-focus-panel']) return;
        if (!satellite) {
            setText('sat-focus-title', '--');
            setText('sat-focus-subtitle', 'Keine Verfolgung aktiv');
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
        if (!state.satelliteOrbitLine) return;
        state.satelliteOrbitLine.visible = false;
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
        if (!state.satelliteOrbitLine) return;
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
            80,
            4320
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

        const points = [];
        const orbitRevolutions = clampSatelliteOrbitRevolutions(state.panelVisibility.orbitRevolutions);
        const totalSampleCount = SATELLITE_ORBIT_SAMPLE_COUNT * orbitRevolutions;
        const stepMs = (periodMinutes * 60000) / SATELLITE_ORBIT_SAMPLE_COUNT;
        for (let i = 0; i <= totalSampleCount; i += 1) {
            const sampleMs = referenceMs + i * stepMs;
            const sample = satellitePositionAt(satellite, sampleMs, satelliteLib);
            if (sample?.inertialPosition) points.push(sample.inertialPosition);
        }

        if (points.length < 2) {
            clearSatelliteOrbitPath();
            return;
        }

        state.satelliteOrbitLine.geometry.dispose();
        state.satelliteOrbitLine.geometry = new THREE.BufferGeometry().setFromPoints(points);
        state.satelliteOrbitLine.computeLineDistances();
        state.satelliteOrbitLine.visible = true;
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
            ? estimateSatelliteOrbitTotal().toLocaleString('de-DE')
            : '--';
        dom['sat-stat-live'].textContent = state.satelliteCatalogLoaded
            ? state.satelliteLiveCount.toLocaleString('de-DE')
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
            state.satelliteLastError = state.satelliteLastError || 'Satellitenbibliothek nicht verfuegbar.';
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
            if (!rawText.trim()) throw new Error('statischer Satellitenkatalog leer');
            writeSatelliteCache(rawText);
            state.satelliteCatalog = parseSatelliteCatalog(rawText);
            state.satelliteCatalogLoaded = true;
            rebuildSatelliteLayer();
            propagateSatellites(true);
            renderSatelliteSearchResults();
        } catch (error) {
            const cached = readSatelliteCache();
            if (cached?.rawText) {
                state.satelliteCatalog = parseSatelliteCatalog(cached.rawText);
                state.satelliteCatalogLoaded = true;
                state.satelliteLastError = `Satelliten aus lokalem Cache · Live-Quelle aktuell nicht erreichbar (${error.message || 'unbekannter Fehler'}).`;
                rebuildSatelliteLayer();
                propagateSatellites(true);
                renderSatelliteSearchResults();
                return;
            }

            state.satelliteCatalog = [];
            state.satelliteCatalogLoaded = false;
            state.satelliteLiveCount = 0;
            state.satelliteLastError = `Satellitenkatalog konnte nicht geladen werden (${error.message || 'unbekannter Fehler'}).`;
            rebuildSatelliteLayer();
            updateSatelliteStats();
            renderSatelliteSearchResults();
        }
    }

    async function initSatelliteTracking() {
        updateSatelliteStats();
        await ensureSatelliteLibrary();
        loadIssOemData();
        fetchSatelliteCatalog();
        if (state.satelliteFetchTimer) clearInterval(state.satelliteFetchTimer);
        state.satelliteFetchTimer = setInterval(fetchSatelliteCatalog, SATELLITE_FETCH_INTERVAL_MS);
    }

    function rebuildLaunchMarkers() {
        if (!state.launchMarkerRoot) return;
        while (state.launchMarkerRoot.children.length) {
            state.launchMarkerRoot.remove(state.launchMarkerRoot.children[0]);
        }
        state.launchMarkers.clear();

        state.launches.forEach((launch, index) => {
            const lat = parseFloat(launch?.pad?.latitude);
            const lon = parseFloat(launch?.pad?.longitude);
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
        state.focusLaunchId = null;
        state.focusedBody = null;
        state.followObserver = false;
        state.followSatelliteId = null;
        state.followMoon = false;
        state.followOrion = false;
        refreshSatelliteFocusVisuals();
        dom['observer-view-btn']?.classList.remove('active');
        dom['moon-view-btn']?.classList.remove('active');
        dom['follow-artemis']?.classList.remove('active');
        setFocusTarget(world.position);
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
        const core = absolute >= 1000 ? absolute.toLocaleString('de-DE') : String(absolute);
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
            title.textContent = milestone.name;
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
        if (km >= 1000) return `${Math.round(km).toLocaleString('de-DE')} km`;
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
        dom['mission-phase'].textContent = phase.name;
        dom['mission-date'].textContent = localDateTime.format(new Date(sceneTimeMs()));
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
        if (km >= 1e6) return `${(km / 1e6).toFixed(2).replace('.', ',')} Mio km`;
        if (km >= 1000) return `${Math.round(km).toLocaleString('de-DE')} km`;
        return `${Math.round(km)} km`;
    }

    function onResize() {
        state.camera.aspect = window.innerWidth / window.innerHeight;
        state.camera.updateProjectionMatrix();
        state.renderer.setSize(window.innerWidth, window.innerHeight);
        if (isMobileViewport()) closeSettings();
    }

    function onKeyDown(event) {
        if (state.freeCameraMode && Object.prototype.hasOwnProperty.call(state.flyKeys, event.key)) {
            event.preventDefault();
            state.flyKeys[event.key] = true;
        }
        if (event.key === 'Escape') {
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
        dom['real-time-berlin'].textContent = localTimeOnly.format(localNow);

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
