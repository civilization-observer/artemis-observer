/*
 * Artemis 2 Mission Observer - 3D Visualization
 * Three.js application with Earth, Moon, Orion tracking + Solar System
 */

import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';

(function () {
    'use strict';

    // ── State ──
    let scene, camera, renderer, controls;
    let earthMesh, moonMesh, orionMarker, orionGlow;
    let sunMesh, sunGlow, sunLabel;
    let earthLabel, moonLabel, orionLabel;
    let pastLine, futureLine;
    let moonOrbitLine;
    let planetOrbits = [];   // orbit lines (excl. Earth) — only in solar system zoom
    let earthOrbitLine;       // Earth orbit — always visible
    let planetOrbitByIndex = {}; // index 0–7 → Line
    let planetMeshByIndex = {};  // index 0–7 → Mesh (excl. duplicate)
    let dynamicLabels = [];   // labels that auto-scale with camera distance
    let pickableMeshes = [];
    let extraPickableMeshes = [];
    const raycaster = new THREE.Raycaster();
    const pointerNdc = new THREE.Vector2();
    /** @type {null | { kind: 'sun' | 'moon' | 'planet', index?: number }} */
    let focusedBody = null;
    let simTime = Date.now();
    /** Mond, Orion, Erde, Planeten: alles um 2 h gegenüber der Simulationsuhr verzögert */
    const SCENE_TIME_OFFSET_MS = 2 * 60 * 60 * 1000;

    function sceneTimeMs() {
        return simTime - SCENE_TIME_OFFSET_MS;
    }

    function simTimeFromMissionMet(metHours) {
        return ARTEMIS2.LAUNCH_UTC + metHours * 3600000 + SCENE_TIME_OFFSET_MS;
    }

    function formatWarpLabel(w) {
        const a = Math.abs(w);
        const s = a >= 1000 ? a.toLocaleString('de-DE') : String(a);
        return (w < 0 ? '−' : '') + s + '×';
    }

    function formatMissionMetReadout(metH) {
        const totalSeconds = Math.max(0, metH * 3600);
        const days = Math.floor(totalSeconds / 86400);
        const hrs = Math.floor((totalSeconds % 86400) / 3600);
        const mins = Math.floor((totalSeconds % 3600) / 60);
        const secs = Math.floor(totalSeconds % 60);
        return `T+ ${days}d ${String(hrs).padStart(2, '0')}:${String(mins).padStart(2, '0')}:${String(secs).padStart(2, '0')} MET`;
    }

    function onMissionMetSliderInput() {
        if (!missionMetSliderEl || totalMET_hours <= 0) return;
        const t = missionMetSliderEl.valueAsNumber / 1000;
        const met = t * totalMET_hours;
        simTime = simTimeFromMissionMet(met);
        if (missionMetReadoutEl) missionMetReadoutEl.textContent = formatMissionMetReadout(met);
    }

    function syncMissionMetSlider() {
        if (!missionMetSliderEl || missionSliderDragging || totalMET_hours <= 0) return;
        const met = ARTEMIS2.getMET(sceneTimeMs());
        const clamped = Math.max(0, Math.min(met, totalMET_hours));
        const pct = (clamped / totalMET_hours) * 1000;
        missionMetSliderEl.value = String(Math.round(pct * 1000) / 1000);
        if (missionMetReadoutEl) missionMetReadoutEl.textContent = formatMissionMetReadout(clamped);
    }

    /** Nur UI: MEZ- und T+-Anzeige gegenüber der Szenenzeit um +2 h */
    const DISPLAY_TIME_OFFSET_MS = 2 * 60 * 60 * 1000;
    const DISPLAY_MET_OFFSET_H = 2;

    let timeWarp = 1;
    /** Zeitraffer: 10× / −10× erneut = ×10 auf die aktuelle Stufe */
    let warpStepMag = 10;
    /** @type { 'idle' | 'forward' | 'backward' } */
    let warpTrack = 'idle';

    let freeCameraMode = false;
    const flyKeys = { ArrowUp: false, ArrowDown: false, ArrowLeft: false, ArrowRight: false };

    let followOrion = false;
    let followMoon = false;
    let lastFrameTime = performance.now();

    /** Ab Kamera–Target-Distanz: alle Planetenbahnen sichtbar (Szeneinheiten ≈ 1000 km) */
    const ORBITS_ALL_DISTANCE = 100000;
    const ZOOM_DIST_MIN = 5;
    const ZOOM_DIST_MAX = 10000000;
    let zoomSliderEl = null;
    let zoomReadoutEl = null;
    let zoomSliderDragging = false;
    let missionMetSliderEl = null;
    let missionMetReadoutEl = null;
    let missionSliderDragging = false;

    const PANEL_UI_STORAGE_KEY = 'artemisobserver-panel-visibility';

    function isMobileViewport() {
        return typeof window.matchMedia === 'function' &&
            window.matchMedia('(max-width: 768px)').matches;
    }

    function defaultPanelVisibility() {
        if (isMobileViewport()) {
            return { info: false, launch: false, controls: false, live: false };
        }
        return { info: true, launch: true, controls: true, live: true };
    }

    let panelVis = defaultPanelVisibility();

    function readPanelState() {
        const def = defaultPanelVisibility();
        try {
            const raw = localStorage.getItem(PANEL_UI_STORAGE_KEY);
            if (raw) {
                const parsed = JSON.parse(raw);
                const merged = { ...def, ...parsed };
                if (merged.live === undefined) merged.live = true;
                return merged;
            }
        } catch (e) { /* ignore */ }
        return def;
    }

    function writePanelState() {
        try {
            localStorage.setItem(PANEL_UI_STORAGE_KEY, JSON.stringify(panelVis));
        } catch (e) { /* ignore */ }
    }

    function applyPanelVisibility() {
        document.body.classList.toggle('ui-hide-info', !panelVis.info);
        document.body.classList.toggle('ui-hide-launch', !panelVis.launch);
        document.body.classList.toggle('ui-hide-controls', !panelVis.controls);
        document.body.classList.toggle('ui-hide-livestream', !panelVis.live);
    }

    function initPanelVisibility() {
        Object.assign(panelVis, readPanelState());
        applyPanelVisibility();
        document.addEventListener('click', (e) => {
            const hideBtn = e.target.closest('.panel-collapse-btn[data-panel]');
            if (hideBtn) {
                const k = hideBtn.dataset.panel;
                if (k !== 'info' && k !== 'launch' && k !== 'controls' && k !== 'live') return;
                panelVis[k] = false;
                applyPanelVisibility();
                writePanelState();
                return;
            }
            const restoreBtn = e.target.closest('.panel-restore-tab[data-panel]');
            if (restoreBtn) {
                const k = restoreBtn.dataset.panel;
                if (k !== 'info' && k !== 'launch' && k !== 'controls' && k !== 'live') return;
                panelVis[k] = true;
                applyPanelVisibility();
                writePanelState();
            }
        });
    }

    function toggleAllPanels() {
        const anyOn = panelVis.info || panelVis.launch || panelVis.controls || panelVis.live;
        if (anyOn) {
            panelVis.info = false;
            panelVis.launch = false;
            panelVis.controls = false;
            panelVis.live = false;
        } else {
            panelVis.info = true;
            panelVis.launch = true;
            panelVis.controls = true;
            panelVis.live = true;
        }
        applyPanelVisibility();
        writePanelState();
    }

    const OBLIQUITY_RAD = 23.4393 * Math.PI / 180;
    const EARTH_POLE = new THREE.Vector3(-Math.sin(OBLIQUITY_RAD), Math.cos(OBLIQUITY_RAD), 0);

    const MEZ_FORMAT = new Intl.DateTimeFormat('de-DE', {
        timeZone: 'Europe/Berlin',
        weekday: 'short',
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false
    });

    let sunScenePos = new THREE.Vector3();
    /** Beleuchtung folgt der Sonnenrichtung (wird mit simTime aktualisiert) */
    let sunDirLight, fillDirLight, sunPointLight;

    let fullTrajectory = [];
    let totalMET_hours = 240;

    // ── Textures ──
    const textureLoader = new THREE.TextureLoader();
    textureLoader.crossOrigin = 'anonymous';
    const EARTH_TEX_URL = 'https://unpkg.com/three-globe/example/img/earth-blue-marble.jpg';
    const MOON_TEX_URL = 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/a8/Solarsystemscope_texture_2k_moon.jpg/1024px-Solarsystemscope_texture_2k_moon.jpg';

    function init() {
        // Renderer with logarithmic depth buffer for huge scale range
        renderer = new THREE.WebGLRenderer({
            antialias: true, alpha: false,
            logarithmicDepthBuffer: true
        });
        renderer.setSize(window.innerWidth, window.innerHeight);
        renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
        renderer.toneMapping = THREE.ACESFilmicToneMapping;
        renderer.toneMappingExposure = 1.2;
        document.getElementById('canvas-container').appendChild(renderer.domElement);

        scene = new THREE.Scene();
        scene.background = new THREE.Color(0x000008);

        // Camera — far plane large enough for Neptune
        camera = new THREE.PerspectiveCamera(50, window.innerWidth / window.innerHeight, 0.1, 20000000);
        camera.position.set(0, 250, 450);

        controls = new OrbitControls(camera, renderer.domElement);
        controls.enableDamping = true;
        controls.dampingFactor = 0.08;
        controls.minDistance = 5;
        controls.maxDistance = 10000000;
        controls.target.set(0, 0, 0);

        const ambientLight = new THREE.AmbientLight(0x333344, 1.5);
        scene.add(ambientLight);

        const T0 = ARTEMIS2.getJulianCenturies(sceneTimeMs());
        const sp0 = ARTEMIS2.getSunPosition(T0);
        sunScenePos.set(sp0.x, sp0.y, sp0.z);
        const sunDir0 = sunScenePos.clone().normalize();
        sunDirLight = new THREE.DirectionalLight(0xffffff, 3);
        sunDirLight.position.copy(sunDir0.clone().multiplyScalar(500));
        scene.add(sunDirLight);
        fillDirLight = new THREE.DirectionalLight(0x4466aa, 0.5);
        fillDirLight.position.copy(sunDir0.clone().multiplyScalar(-200));
        scene.add(fillDirLight);

        createStarField();
        createEarth();
        createMoon();
        createOrion();
        createSun();
        createPlanets();
        createTrajectoryLines();
        createLabels();
        createNorthPoleAxis();
        createMoonOrbit();

        buildPickableList();
        renderer.domElement.addEventListener('click', onSceneClick);
        window.addEventListener('keydown', onKeyDown);
        window.addEventListener('keyup', onKeyUp);

        initControlSectionTabs();

        zoomSliderEl = document.getElementById('zoom-slider');
        zoomReadoutEl = document.getElementById('zoom-readout');
        if (zoomSliderEl) {
            zoomSliderEl.addEventListener('pointerdown', () => { zoomSliderDragging = true; });
            zoomSliderEl.addEventListener('pointerup', () => { zoomSliderDragging = false; });
            zoomSliderEl.addEventListener('pointercancel', () => { zoomSliderDragging = false; });
            zoomSliderEl.addEventListener('input', onZoomSliderInput);
        }

        missionMetSliderEl = document.getElementById('mission-met-slider');
        missionMetReadoutEl = document.getElementById('mission-met-readout');
        if (missionMetSliderEl) {
            missionMetSliderEl.addEventListener('pointerdown', () => { missionSliderDragging = true; });
            missionMetSliderEl.addEventListener('pointerup', () => { missionSliderDragging = false; });
            missionMetSliderEl.addEventListener('pointercancel', () => { missionSliderDragging = false; });
            missionMetSliderEl.addEventListener('input', onMissionMetSliderInput);
        }

        window.addEventListener('resize', onResize);

        simTime = Date.now();

        ARTEMIS2.onDataLoaded(() => {
            fullTrajectory = ARTEMIS2.getFullTrajectoryPoints(0.5);
            const wp = ARTEMIS2.WAYPOINTS;
            if (wp.length > 0) totalMET_hours = wp[wp.length - 1].t;
            updateTrajectoryLines(ARTEMIS2.getMET(sceneTimeMs()));
            syncMissionMetSlider();
        });

        initLaunchFeed();
        initPanelVisibility();

        refreshWarpButtons();

        animate();
    }

    function initControlSectionTabs() {
        const tabSpace = document.getElementById('ctrl-tab-space');
        const tabTime = document.getElementById('ctrl-tab-time');
        const secSpace = document.getElementById('controls-section-space');
        const secTime = document.getElementById('controls-section-time');
        if (!tabSpace || !tabTime || !secSpace || !secTime) return;
        function activateSpace() {
            tabSpace.classList.add('active');
            tabTime.classList.remove('active');
            tabSpace.setAttribute('aria-selected', 'true');
            tabTime.setAttribute('aria-selected', 'false');
            secSpace.style.display = '';
            secTime.style.display = 'none';
        }
        function activateTime() {
            tabTime.classList.add('active');
            tabSpace.classList.remove('active');
            tabTime.setAttribute('aria-selected', 'true');
            tabSpace.setAttribute('aria-selected', 'false');
            secTime.style.display = '';
            secSpace.style.display = 'none';
        }
        tabSpace.addEventListener('click', activateSpace);
        tabTime.addEventListener('click', activateTime);
    }

    function refreshWarpButtons() {
        const w1 = document.querySelector('.warp-btn-w1');
        const wf = document.querySelector('.warp-btn-wf');
        const wb = document.querySelector('.warp-btn-wb');
        [w1, wf, wb].forEach((b) => { if (b) b.classList.remove('active'); });
        if (timeWarp === 1) {
            if (w1) w1.classList.add('active');
        } else if (timeWarp > 0) {
            if (wf) wf.classList.add('active');
        } else {
            if (wb) wb.classList.add('active');
        }
    }

    function warpToOne() {
        timeWarp = 1;
        warpTrack = 'idle';
        warpStepMag = 10;
        refreshWarpButtons();
        const wd = document.getElementById('warp-display');
        if (wd) wd.textContent = formatWarpLabel(timeWarp);
    }

    function cycleWarpForward() {
        if (warpTrack === 'forward' && timeWarp >= 10) {
            warpStepMag = Math.min(warpStepMag * 10, 1e12);
        } else {
            warpStepMag = 10;
        }
        warpTrack = 'forward';
        timeWarp = warpStepMag;
        refreshWarpButtons();
        const wd = document.getElementById('warp-display');
        if (wd) wd.textContent = formatWarpLabel(timeWarp);
    }

    function cycleWarpBackward() {
        if (warpTrack === 'backward' && timeWarp <= -10) {
            warpStepMag = Math.min(warpStepMag * 10, 1e12);
        } else {
            warpStepMag = 10;
        }
        warpTrack = 'backward';
        timeWarp = -warpStepMag;
        refreshWarpButtons();
        const wd = document.getElementById('warp-display');
        if (wd) wd.textContent = formatWarpLabel(timeWarp);
    }

    window.warpToOne = warpToOne;
    window.cycleWarpForward = cycleWarpForward;
    window.cycleWarpBackward = cycleWarpBackward;

    // ── Launch feed (Launch Library 2 / The Space Devs) ──
    const LAUNCH_API = 'https://ll.thespacedevs.com/2.2.0/launch/upcoming/';
    let launchCountdownTimer = null;
    let launchFeedFetchTimer = null;

    function escapeHtml(text) {
        const d = document.createElement('div');
        d.textContent = text == null ? '' : String(text);
        return d.innerHTML;
    }

    function isEarthLaunch(launch) {
        const pad = launch.pad;
        if (!pad) return false;
        const lat = parseFloat(pad.latitude);
        const lon = parseFloat(pad.longitude);
        if (Number.isNaN(lat) || Number.isNaN(lon)) return false;
        return true;
    }

    function launchInstant(launch) {
        const s = launch.net || launch.window_start;
        if (!s) return null;
        const d = new Date(s);
        return Number.isNaN(d.getTime()) ? null : d;
    }

    /** Anbieter / Organisation (z. B. SpaceX, Rocket Lab) — LL2: launch_service_provider */
    function launchOrganization(launch) {
        const lsp = launch.launch_service_provider;
        if (lsp && lsp.name) return String(lsp.name).trim();
        const cfg = launch.rocket && launch.rocket.configuration;
        const mfg = cfg && cfg.manufacturer;
        if (mfg && mfg.name) return String(mfg.name).trim();
        return '';
    }

    function formatLaunchCountdown(target) {
        if (!target) return '—';
        const diff = target.getTime() - Date.now();
        if (diff <= 0) return 'Startfenster erreicht / live';
        const sec = Math.floor(diff / 1000);
        const s = sec % 60;
        const m = Math.floor(sec / 60) % 60;
        const h = Math.floor(sec / 3600) % 24;
        const days = Math.floor(sec / 86400);
        const pad2 = (n) => String(n).padStart(2, '0');
        if (days > 0) return `noch ${days}d ${pad2(h)}:${pad2(m)}:${pad2(s)}`;
        return `noch ${pad2(h)}:${pad2(m)}:${pad2(s)}`;
    }

    function renderLaunchFeed(items) {
        const container = document.getElementById('launch-feed-items');
        if (!container) return;
        container.innerHTML = '';
        if (items.length === 0) {
            container.innerHTML = '<div class="launch-error">Keine anstehenden Starts mit Boden-/Seekoordinaten gefunden.</div>';
            return;
        }
        for (const l of items) {
            const when = launchInstant(l);
            if (!when) continue;
            const org = launchOrganization(l);
            const rocket = l.rocket && l.rocket.configuration
                ? (l.rocket.configuration.full_name || l.rocket.configuration.name || '')
                : '';
            const pad = (l.pad && l.pad.name) ? l.pad.name : '';
            const loc = (l.pad && l.pad.location && l.pad.location.name) ? l.pad.location.name : '';
            const netStr = when.toLocaleString('de-DE', {
                timeZone: 'Europe/Berlin',
                day: '2-digit',
                month: '2-digit',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit',
                timeZoneName: 'short'
            });
            const item = document.createElement('div');
            item.className = 'launch-item';
            const cd = document.createElement('div');
            cd.className = 'launch-countdown';
            cd.setAttribute('data-net', when.toISOString());
            cd.textContent = formatLaunchCountdown(when);
            item.innerHTML =
                '<div class="launch-name">' + escapeHtml(l.name || 'Unbenannt') + '</div>' +
                (org
                    ? '<div class="launch-org">' + escapeHtml(org) + '</div>'
                    : '') +
                '<div class="launch-meta">' +
                escapeHtml(rocket || '—') + '<br>' +
                escapeHtml([pad, loc].filter(Boolean).join(' · ') || '—') +
                '</div>' +
                '<div class="launch-net">Net: ' + escapeHtml(netStr) + '</div>';
            item.appendChild(cd);
            container.appendChild(item);
        }
    }

    async function fetchLaunches() {
        const statusEl = document.getElementById('launch-feed-status');
        const container = document.getElementById('launch-feed-items');
        if (statusEl) statusEl.textContent = 'Lade …';
        try {
            const url = LAUNCH_API + '?limit=40&mode=detailed';
            const res = await fetch(url, { headers: { Accept: 'application/json' } });
            if (!res.ok) throw new Error('HTTP ' + res.status);
            const data = await res.json();
            const list = (data.results || []).filter(isEarthLaunch);
            list.sort((a, b) => {
                const ta = launchInstant(a);
                const tb = launchInstant(b);
                if (!ta || !tb) return 0;
                return ta - tb;
            });
            const top = list.slice(0, 18);
            renderLaunchFeed(top);
            if (statusEl) {
                statusEl.textContent = 'Aktualisiert: ' + new Date().toLocaleTimeString('de-DE') +
                    ' · ' + top.length + ' Termine';
            }
        } catch (e) {
            if (container) {
                container.innerHTML =
                    '<div class="launch-error">Der Start-Feed konnte nicht geladen werden (Netzwerk oder CORS). ' +
                    'Lokal die Seite über einen kleinen Server öffnen oder später erneut versuchen.</div>';
            }
            if (statusEl) statusEl.textContent = 'Fehler';
        }
    }

    function tickLaunchCountdowns() {
        document.querySelectorAll('#launch-feed-items .launch-countdown[data-net]').forEach((el) => {
            const iso = el.getAttribute('data-net');
            if (!iso) return;
            const t = new Date(iso);
            el.textContent = formatLaunchCountdown(t);
        });
    }

    function initLaunchFeed() {
        const btn = document.getElementById('launch-feed-refresh');
        if (btn) {
            btn.addEventListener('click', () => fetchLaunches());
            btn.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); fetchLaunches(); }
            });
        }
        fetchLaunches();
        if (launchFeedFetchTimer) clearInterval(launchFeedFetchTimer);
        launchFeedFetchTimer = setInterval(fetchLaunches, 15 * 60 * 1000);
        if (launchCountdownTimer) clearInterval(launchCountdownTimer);
        launchCountdownTimer = setInterval(tickLaunchCountdowns, 1000);
    }

    window.refreshLaunchFeed = fetchLaunches;

    // ── Stars (all far enough to be outside the solar system) ──
    function createStarField() {
        addStarLayer(10000, 8000000, 16000000, 2000);
        addStarLayer(4000, 10000000, 18000000, 6000);
    }

    function addStarLayer(count, rMin, rMax, size) {
        const geo = new THREE.BufferGeometry();
        const positions = new Float32Array(count * 3);
        for (let i = 0; i < count; i++) {
            const r = rMin + Math.random() * (rMax - rMin);
            const theta = Math.random() * Math.PI * 2;
            const phi = Math.acos(2 * Math.random() - 1);
            positions[i * 3] = r * Math.sin(phi) * Math.cos(theta);
            positions[i * 3 + 1] = r * Math.sin(phi) * Math.sin(theta);
            positions[i * 3 + 2] = r * Math.cos(phi);
        }
        geo.setAttribute('position', new THREE.BufferAttribute(positions, 3));
        const mat = new THREE.PointsMaterial({
            color: 0xffffff, size, sizeAttenuation: true,
            transparent: true, opacity: 0.8
        });
        scene.add(new THREE.Points(geo, mat));
    }

    // ── Earth (tilted by obliquity) ──
    let earthGroup;

    function createEarth() {
        earthGroup = new THREE.Group();
        earthGroup.rotation.z = OBLIQUITY_RAD;
        scene.add(earthGroup);

        const geo = new THREE.SphereGeometry(ARTEMIS2.EARTH_RADIUS, 64, 64);
        const mat = new THREE.MeshPhongMaterial({
            color: 0x4488cc, emissive: 0x112233,
            emissiveIntensity: 0.1, shininess: 25
        });
        textureLoader.load(EARTH_TEX_URL, (tex) => {
            mat.map = tex;
            mat.color.set(0xffffff);
            mat.needsUpdate = true;
        });
        earthMesh = new THREE.Mesh(geo, mat);
        earthMesh.userData.pickKind = 'planet';
        earthMesh.userData.planetIndex = 2;
        planetMeshByIndex[2] = earthMesh;
        earthGroup.add(earthMesh);

        const glowGeo = new THREE.SphereGeometry(ARTEMIS2.EARTH_RADIUS * 1.02, 64, 64);
        const glowMat = new THREE.MeshBasicMaterial({
            color: 0x4488ff, transparent: true, opacity: 0.08, side: THREE.BackSide
        });
        earthGroup.add(new THREE.Mesh(glowGeo, glowMat));
    }

    // ── Moon ──
    function createMoon() {
        const geo = new THREE.SphereGeometry(ARTEMIS2.MOON_RADIUS, 32, 32);
        const mat = new THREE.MeshPhongMaterial({ color: 0xaaaaaa, shininess: 5 });
        textureLoader.load(MOON_TEX_URL, (tex) => {
            mat.map = tex; mat.color.set(0xffffff); mat.needsUpdate = true;
        });
        moonMesh = new THREE.Mesh(geo, mat);
        moonMesh.userData.pickKind = 'moon';
        scene.add(moonMesh);
    }

    // ── Orion capsule marker ──
    function createOrion() {
        const geo = new THREE.SphereGeometry(1.5, 16, 16);
        const mat = new THREE.MeshBasicMaterial({ color: 0x00ffaa });
        orionMarker = new THREE.Mesh(geo, mat);
        scene.add(orionMarker);

        const glowGeo = new THREE.SphereGeometry(3, 16, 16);
        const glowMat = new THREE.MeshBasicMaterial({
            color: 0x00ffaa, transparent: true, opacity: 0.25
        });
        orionGlow = new THREE.Mesh(glowGeo, glowMat);
        scene.add(orionGlow);
    }

    // ── Sun (real distance) ──
    function createSun() {
        const sunVisualRadius = 5000;

        const geo = new THREE.SphereGeometry(sunVisualRadius, 32, 32);
        const mat = new THREE.MeshBasicMaterial({ color: 0xffee88 });
        sunMesh = new THREE.Mesh(geo, mat);
        sunMesh.userData.pickKind = 'sun';
        sunMesh.position.copy(sunScenePos);
        scene.add(sunMesh);

        // Glow sprite
        const canvas = document.createElement('canvas');
        const ctx = canvas.getContext('2d');
        canvas.width = 256; canvas.height = 256;
        const grad = ctx.createRadialGradient(128, 128, 0, 128, 128, 128);
        grad.addColorStop(0, 'rgba(255,255,220,1)');
        grad.addColorStop(0.1, 'rgba(255,230,120,0.8)');
        grad.addColorStop(0.3, 'rgba(255,200,60,0.3)');
        grad.addColorStop(1, 'rgba(255,160,0,0)');
        ctx.fillStyle = grad;
        ctx.fillRect(0, 0, 256, 256);
        const tex = new THREE.CanvasTexture(canvas);
        const spriteMat = new THREE.SpriteMaterial({
            map: tex, transparent: true,
            blending: THREE.AdditiveBlending, depthTest: false
        });
        sunGlow = new THREE.Sprite(spriteMat);
        sunGlow.scale.set(40000, 40000, 1);
        sunGlow.position.copy(sunScenePos);
        scene.add(sunGlow);

        sunLabel = makeTextSprite('Sonne', '#ffdd66');
        sunLabel.position.copy(sunScenePos.clone().add(new THREE.Vector3(0, sunVisualRadius + 3000, 0)));
        scene.add(sunLabel);
        sunLabel._offsetY = sunVisualRadius + 3000;
        sunLabel._anchor = sunScenePos;
        dynamicLabels.push(sunLabel);

        sunPointLight = new THREE.PointLight(0xffffcc, 0.8, ARTEMIS2.AU * 35);
        sunPointLight.position.copy(sunScenePos);
        scene.add(sunPointLight);
    }

    // ── Planets (Positionen & Bahnen folgen simTime über updateSolarSystem) ──
    function createPlanets() {
        const planets = ARTEMIS2.PLANETS;
        const T0 = ARTEMIS2.getJulianCenturies(sceneTimeMs());

        for (let i = 0; i < planets.length; i++) {
            if (i === 2) continue;

            const p = planets[i];
            const pos = ARTEMIS2.getPlanetPosition(i, T0);
            const displayRadius = Math.max(p.radius * 100, 1000);

            const geo = new THREE.SphereGeometry(displayRadius, 24, 24);
            const mat = new THREE.MeshBasicMaterial({ color: new THREE.Color(p.color) });
            const mesh = new THREE.Mesh(geo, mat);
            mesh.position.set(pos.x, pos.y, pos.z);
            mesh.userData.pickKind = 'planet';
            mesh.userData.planetIndex = i;
            planetMeshByIndex[i] = mesh;
            scene.add(mesh);

            if (p.hasRings) {
                const ringGeo = new THREE.RingGeometry(displayRadius * 1.4, displayRadius * 2.3, 64);
                const ringMat = new THREE.MeshBasicMaterial({
                    color: 0xddcc99, transparent: true, opacity: 0.5, side: THREE.DoubleSide
                });
                const ring = new THREE.Mesh(ringGeo, ringMat);
                ring.rotation.x = Math.PI * 0.42;
                ring.position.copy(mesh.position);
                ring.userData.pickKind = 'planet';
                ring.userData.planetIndex = i;
                scene.add(ring);
                extraPickableMeshes.push(ring);
                mesh.userData.saturnRing = ring;
            }

            const label = makeTextSprite(p.name, p.color);
            label.position.set(pos.x, pos.y + displayRadius + 2000, pos.z);
            scene.add(label);
            label._offsetY = displayRadius + 2000;
            label._anchor = mesh.position;
            dynamicLabels.push(label);

            // Orbit — only visible in solar system zoom
            const orbitPts = ARTEMIS2.getPlanetOrbitPoints(i, T0, 256);
            const vecs = orbitPts.map(pt => new THREE.Vector3(pt.x, pt.y, pt.z));
            const orbitGeo = new THREE.BufferGeometry().setFromPoints(vecs);
            const orbitMat = new THREE.LineDashedMaterial({
                color: new THREE.Color(p.color),
                dashSize: p.a * ARTEMIS2.AU * 0.02,
                gapSize: p.a * ARTEMIS2.AU * 0.01,
                transparent: true, opacity: 0.3
            });
            const orbitLine = new THREE.Line(orbitGeo, orbitMat);
            orbitLine.computeLineDistances();
            orbitLine.visible = false;
            scene.add(orbitLine);
            planetOrbits.push(orbitLine);
            planetOrbitByIndex[i] = orbitLine;
        }

        // Earth orbit — always visible (dash sizes updated per frame for readability)
        const earthOrbitPts = ARTEMIS2.getPlanetOrbitPoints(2, T0, 256);
        const earthVecs = earthOrbitPts.map(pt => new THREE.Vector3(pt.x, pt.y, pt.z));
        const earthOrbitGeo = new THREE.BufferGeometry().setFromPoints(earthVecs);
        const earthOrbitMat = new THREE.LineDashedMaterial({
            color: 0x4499ff, dashSize: 400, gapSize: 220,
            transparent: true, opacity: 0.35
        });
        earthOrbitLine = new THREE.Line(earthOrbitGeo, earthOrbitMat);
        earthOrbitLine.computeLineDistances();
        scene.add(earthOrbitLine);
        planetOrbitByIndex[2] = earthOrbitLine;
    }

    // ── Labels (sprite-based) ──
    function makeTextSprite(text, color) {
        const canvas = document.createElement('canvas');
        const ctx = canvas.getContext('2d');
        canvas.width = 256; canvas.height = 64;
        ctx.font = 'bold 28px Segoe UI, Helvetica, sans-serif';
        ctx.fillStyle = color;
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(text, 128, 32);

        const tex = new THREE.CanvasTexture(canvas);
        const mat = new THREE.SpriteMaterial({ map: tex, transparent: true, depthTest: false });
        const sprite = new THREE.Sprite(mat);
        sprite.scale.set(20, 5, 1);
        return sprite;
    }

    function createLabels() {
        earthLabel = makeTextSprite('Erde', '#88bbff');
        earthLabel.position.set(ARTEMIS2.EARTH_RADIUS + 5, 0, 0);
        scene.add(earthLabel);

        moonLabel = makeTextSprite('Mond', '#cccccc');
        scene.add(moonLabel);

        orionLabel = makeTextSprite('ORION', '#00ffaa');
        scene.add(orionLabel);
    }

    // ── North Pole axis (tilted by obliquity) ──
    function createNorthPoleAxis() {
        const axisLen = ARTEMIS2.EARTH_RADIUS * 2.5;
        const north = EARTH_POLE.clone().multiplyScalar(axisLen);
        const south = EARTH_POLE.clone().multiplyScalar(-axisLen);

        const geo = new THREE.BufferGeometry().setFromPoints([south, north]);
        const mat = new THREE.LineBasicMaterial({ color: 0xff4444, transparent: true, opacity: 0.6 });
        scene.add(new THREE.Line(geo, mat));

        const tipGeo = new THREE.SphereGeometry(0.5, 8, 8);
        const tipMat = new THREE.MeshBasicMaterial({ color: 0xff4444 });
        const tip = new THREE.Mesh(tipGeo, tipMat);
        tip.position.copy(north);
        scene.add(tip);

        const nLabel = makeTextSprite('N', '#ff4444');
        nLabel.position.copy(EARTH_POLE.clone().multiplyScalar(axisLen + 2.5));
        nLabel.scale.set(10, 2.5, 1);
        scene.add(nLabel);

        const sLabel = makeTextSprite('S', '#ff4444');
        sLabel.position.copy(EARTH_POLE.clone().multiplyScalar(-axisLen - 2.5));
        sLabel.scale.set(10, 2.5, 1);
        scene.add(sLabel);
    }

    // ── Moon orbit line ──
    function createMoonOrbit() {
        const orbitPts = ARTEMIS2.getMoonOrbitPoints(360);
        const vecs = orbitPts.map(p => new THREE.Vector3(p.x, p.y, p.z));
        const geo = new THREE.BufferGeometry().setFromPoints(vecs);
        const mat = new THREE.LineDashedMaterial({
            color: 0xd8e4f0, dashSize: 5, gapSize: 3,
            transparent: true, opacity: 0.52
        });
        moonOrbitLine = new THREE.Line(geo, mat);
        moonOrbitLine.computeLineDistances();
        scene.add(moonOrbitLine);
    }

    // ── Trajectory lines ──
    function createTrajectoryLines() {
        const pastMat = new THREE.LineBasicMaterial({
            color: 0x00ddff, linewidth: 2, transparent: true, opacity: 0.9
        });
        pastLine = new THREE.Line(new THREE.BufferGeometry(), pastMat);
        scene.add(pastLine);

        const futureMat = new THREE.LineDashedMaterial({
            color: 0xffffff, linewidth: 1, dashSize: 3, gapSize: 2,
            transparent: true, opacity: 0.35
        });
        futureLine = new THREE.Line(new THREE.BufferGeometry(), futureMat);
        scene.add(futureLine);
    }

    function updateTrajectoryLines(metHours) {
        const pastPts = [], futurePts = [];
        for (const pt of fullTrajectory) {
            const v = new THREE.Vector3(pt.x, pt.y, pt.z);
            if (pt.t <= metHours) {
                pastPts.push(v);
            } else {
                if (futurePts.length === 0 && pastPts.length > 0)
                    futurePts.push(pastPts[pastPts.length - 1].clone());
                futurePts.push(v);
            }
        }
        if (pastPts.length > 0) {
            pastLine.geometry.dispose();
            pastLine.geometry = new THREE.BufferGeometry().setFromPoints(pastPts);
        }
        if (futurePts.length > 0) {
            futureLine.geometry.dispose();
            futureLine.geometry = new THREE.BufferGeometry().setFromPoints(futurePts);
            futureLine.computeLineDistances();
        }
    }

    function refreshOrbitLineGeometry(line, planetIdx, T) {
        const orbitPts = ARTEMIS2.getPlanetOrbitPoints(planetIdx, T, 256);
        const n = orbitPts.length;
        const geo = line.geometry;
        const pos = geo.attributes.position;
        if (pos && pos.count === n) {
            const arr = pos.array;
            for (let i = 0; i < n; i++) {
                const p = orbitPts[i];
                arr[i * 3] = p.x;
                arr[i * 3 + 1] = p.y;
                arr[i * 3 + 2] = p.z;
            }
            pos.needsUpdate = true;
            geo.computeBoundingSphere();
        } else {
            const vecs = orbitPts.map(pt => new THREE.Vector3(pt.x, pt.y, pt.z));
            geo.dispose();
            line.geometry = new THREE.BufferGeometry().setFromPoints(vecs);
        }
        line.computeLineDistances();
    }

    /** Sonne, Planeten, Umlaufbahnen und Licht für Simulationszeitpunkt dateMs (UTC ms) */
    function updateSolarSystem(dateMs) {
        const T = ARTEMIS2.getJulianCenturies(dateMs);
        const sp = ARTEMIS2.getSunPosition(T);
        sunScenePos.set(sp.x, sp.y, sp.z);

        if (sunMesh) sunMesh.position.copy(sunScenePos);
        if (sunGlow) sunGlow.position.copy(sunScenePos);
        if (sunPointLight) sunPointLight.position.copy(sunScenePos);

        const sunDir = sunScenePos.clone().normalize();
        if (sunDirLight) sunDirLight.position.copy(sunDir.clone().multiplyScalar(500));
        if (fillDirLight) fillDirLight.position.copy(sunScenePos.clone().normalize().multiplyScalar(-200));

        for (let i = 0; i < 8; i++) {
            if (i === 2) continue;
            const mesh = planetMeshByIndex[i];
            if (!mesh) continue;
            const pos = ARTEMIS2.getPlanetPosition(i, T);
            mesh.position.set(pos.x, pos.y, pos.z);
            if (mesh.userData.saturnRing) {
                mesh.userData.saturnRing.position.copy(mesh.position);
            }
            const orbitLn = planetOrbitByIndex[i];
            if (orbitLn) refreshOrbitLineGeometry(orbitLn, i, T);
        }
        if (earthOrbitLine) refreshOrbitLineGeometry(earthOrbitLine, 2, T);
    }

    // ── UI Updates ──
    function updateUI(metHours) {
        const phase = ARTEMIS2.getPhase(metHours);
        const pos = ARTEMIS2.interpolatePosition(metHours);
        const moonPos = ARTEMIS2.getMoonPosition(metHours);
        const distEarth = Math.sqrt(pos.x ** 2 + pos.y ** 2 + pos.z ** 2) * 1000;
        const distMoon = Math.sqrt(
            (pos.x - moonPos.x) ** 2 + (pos.y - moonPos.y) ** 2 + (pos.z - moonPos.z) ** 2
        ) * 1000;
        const velocity = ARTEMIS2.getVelocity(metHours);

        const metDisplayH = metHours + DISPLAY_MET_OFFSET_H;
        const totalSeconds = Math.max(0, metDisplayH * 3600);
        const days = Math.floor(totalSeconds / 86400);
        const hrs = Math.floor((totalSeconds % 86400) / 3600);
        const mins = Math.floor((totalSeconds % 3600) / 60);
        const secs = Math.floor(totalSeconds % 60);
        const metStr = `T+ ${days}d ${String(hrs).padStart(2, '0')}:${String(mins).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;

        document.getElementById('met-clock').textContent = metStr;
        document.getElementById('phase-name').textContent = phase.name;
        document.getElementById('dist-earth').textContent = formatDistance(distEarth);
        document.getElementById('dist-moon').textContent = formatDistance(distMoon);
        document.getElementById('velocity').textContent = formatVelocity(velocity);

        const KM_TO_MI = 0.621371;
        document.getElementById('dist-earth-mi').textContent = formatDistance(distEarth * KM_TO_MI) + ' (mi)';
        document.getElementById('dist-moon-mi').textContent = formatDistance(distMoon * KM_TO_MI) + ' (mi)';
        document.getElementById('velocity-mi').textContent = Math.round(velocity * 2236.936).toLocaleString('de-DE') + ' mph';

        updateTimeline(metHours);

        document.getElementById('warp-display').textContent = formatWarpLabel(timeWarp);

        const rt = document.getElementById('real-time-mez');
        if (rt) rt.textContent = MEZ_FORMAT.format(new Date(sceneTimeMs() + DISPLAY_TIME_OFFSET_MS));
    }

    function formatDistance(km) {
        if (km >= 1000) return Math.round(km).toLocaleString('de-DE') + ' km';
        return km.toFixed(0) + ' km';
    }

    function formatVelocity(kms) {
        if (kms >= 10) return kms.toFixed(1) + ' km/s';
        if (kms >= 1) return kms.toFixed(2) + ' km/s';
        return (kms * 1000).toFixed(0) + ' m/s';
    }

    // ── Timeline ──
    let timelineBuilt = false;

    function buildTimeline() {
        if (timelineBuilt) return;
        const container = document.getElementById('timeline-items');
        if (!container || !ARTEMIS2.MILESTONES) return;
        timelineBuilt = true;
        container.innerHTML = '';
        ARTEMIS2.MILESTONES.forEach((m, i) => {
            const isLast = i === ARTEMIS2.MILESTONES.length - 1;
            const item = document.createElement('div');
            item.className = 'tl-item future';
            item.id = 'tl-' + i;
            item.innerHTML =
                '<div class="tl-track"><div class="tl-dot"></div>' +
                (isLast ? '' : '<div class="tl-line"></div>') +
                '</div><div class="tl-content"><div class="tl-name">' +
                m.name + '</div><div class="tl-time" id="tl-time-' + i + '"></div></div>';
            container.appendChild(item);
        });
    }

    function formatTimer(hours) {
        if (hours <= 0) return '';
        const d = Math.floor(hours / 24);
        const h = Math.floor(hours % 24);
        const m = Math.floor((hours * 60) % 60);
        const s = Math.floor((hours * 3600) % 60);
        let str = '';
        if (d > 0) str += d + 'd ';
        str += String(h).padStart(2, '0') + ':' + String(m).padStart(2, '0') + ':' + String(s).padStart(2, '0');
        return str;
    }

    let lastActiveIdx = -1;

    function updateTimeline(metHours) {
        buildTimeline();
        const milestones = ARTEMIS2.MILESTONES;
        if (!milestones) return;

        let activeIdx = -1;
        for (let i = milestones.length - 1; i >= 0; i--) {
            if (metHours >= milestones[i].t) { activeIdx = i; break; }
        }
        const nextIdx = activeIdx + 1 < milestones.length ? activeIdx + 1 : -1;

        if (activeIdx !== lastActiveIdx) {
            lastActiveIdx = activeIdx;
            for (let i = 0; i < milestones.length; i++) {
                const el = document.getElementById('tl-' + i);
                if (!el) continue;
                if (i < activeIdx) el.className = 'tl-item done';
                else if (i === activeIdx) el.className = 'tl-item done';
                else if (i === nextIdx) el.className = 'tl-item active';
                else el.className = 'tl-item future';
            }
            if (nextIdx >= 0) {
                const el = document.getElementById('tl-' + nextIdx);
                if (el) el.scrollIntoView({ block: 'center', behavior: 'smooth' });
            }
        }

        for (let i = 0; i < milestones.length; i++) {
            const timeEl = document.getElementById('tl-time-' + i);
            if (!timeEl) continue;
            const diff = milestones[i].t - metHours;
            if (diff <= 0) timeEl.textContent = 'vor ' + formatTimer(-diff);
            else timeEl.textContent = 'in ' + formatTimer(diff);
        }
    }

    // ── Animation loop ──
    function animate() {
        requestAnimationFrame(animate);

        const now = performance.now();
        const dtReal = (now - lastFrameTime) / 1000;
        lastFrameTime = now;

        if (freeCameraMode) {
            const dist = camera.position.distanceTo(controls.target);
            const speed = THREE.MathUtils.clamp(dist * 0.12, 40, 120000) * dtReal;
            const forward = new THREE.Vector3();
            camera.getWorldDirection(forward);
            const right = new THREE.Vector3().crossVectors(forward, camera.up).normalize();
            const move = new THREE.Vector3();
            if (flyKeys.ArrowUp) move.addScaledVector(forward, speed);
            if (flyKeys.ArrowDown) move.addScaledVector(forward, -speed);
            if (flyKeys.ArrowLeft) move.addScaledVector(right, -speed);
            if (flyKeys.ArrowRight) move.addScaledVector(right, speed);
            if (move.lengthSq() > 0) {
                camera.position.add(move);
                controls.target.add(move);
            }
        }

        if (!missionSliderDragging) {
            simTime += dtReal * 1000 * timeWarp;
        }
        updateSolarSystem(sceneTimeMs());

        const metHours = ARTEMIS2.getMET(sceneTimeMs());
        const clampedMET = Math.max(0, Math.min(metHours, totalMET_hours));

        if (earthMesh) {
            earthMesh.rotation.y += (dtReal * timeWarp / 86400) * Math.PI * 2;
        }

        if (moonMesh) {
            const moonPos = ARTEMIS2.getMoonPosition(clampedMET);
            moonMesh.position.set(moonPos.x, moonPos.y, moonPos.z);
            if (moonLabel) moonLabel.position.set(moonPos.x, moonPos.y + ARTEMIS2.MOON_RADIUS + 3, moonPos.z);
        }

        const orionPos = ARTEMIS2.interpolatePosition(clampedMET);
        if (orionMarker) {
            orionMarker.position.set(orionPos.x, orionPos.y, orionPos.z);
            orionGlow.position.set(orionPos.x, orionPos.y, orionPos.z);
            if (orionLabel) orionLabel.position.set(orionPos.x, orionPos.y + 5, orionPos.z);

            const pulse = 0.2 + 0.1 * Math.sin(now * 0.003);
            orionGlow.material.opacity = pulse;
            orionGlow.scale.setScalar(1 + 0.15 * Math.sin(now * 0.004));
        }

        // Focus / camera follow
        if (!freeCameraMode && focusedBody) {
            const p = new THREE.Vector3();
            if (focusedBody.kind === 'sun') p.copy(sunScenePos);
            else if (focusedBody.kind === 'moon') p.copy(moonMesh.position);
            else if (focusedBody.kind === 'planet') {
                if (focusedBody.index === 2) p.set(0, 0, 0);
                else p.copy(planetMeshByIndex[focusedBody.index].position);
            }
            controls.target.lerp(p, 0.12);
        } else if (!freeCameraMode && followMoon && moonMesh) {
            controls.target.lerp(moonMesh.position, 0.08);
        } else if (!freeCameraMode && followOrion) {
            controls.target.lerp(new THREE.Vector3(orionPos.x, orionPos.y, orionPos.z), 0.05);
        }

        // Distance-based visibility (Kamera ↔ OrbitControls-Target, nicht Ursprung)
        const camTargetDist = camera.position.distanceTo(controls.target);
        const showAllOrbits = camTargetDist >= ORBITS_ALL_DISTANCE;

        if (focusedBody && !showAllOrbits) {
            for (const obj of planetOrbits) obj.visible = false;
            if (earthOrbitLine) earthOrbitLine.visible = false;
            if (moonOrbitLine) moonOrbitLine.visible = false;
            if (focusedBody.kind === 'sun') {
                if (earthOrbitLine) earthOrbitLine.visible = true;
            } else if (focusedBody.kind === 'moon') {
                if (moonOrbitLine) moonOrbitLine.visible = true;
            } else if (focusedBody.kind === 'planet') {
                const line = planetOrbitByIndex[focusedBody.index];
                if (line) line.visible = true;
            }
        } else {
            for (const obj of planetOrbits) obj.visible = showAllOrbits;
            if (earthOrbitLine) earthOrbitLine.visible = true;
            if (moonOrbitLine) moonOrbitLine.visible = true;
        }

        if (zoomSliderEl && !zoomSliderDragging) {
            zoomSliderEl.value = String(Math.round(distToSliderValue(camTargetDist) * 1000));
        }
        if (zoomReadoutEl) zoomReadoutEl.textContent = formatZoomReadout(camTargetDist);

        // Earth orbit: finer dashes when zoomed in (dash size tracks view distance)
        if (earthOrbitLine && earthOrbitLine.visible) {
            const ref = camera.position.distanceTo(controls.target);
            const dash = THREE.MathUtils.clamp(ref * 0.07, 18, 16000);
            earthOrbitLine.material.dashSize = dash;
            earthOrbitLine.material.gapSize = dash * 0.48;
        }

        // Scale labels proportionally to camera distance so they stay readable (~2× base)
        for (const label of dynamicLabels) {
            const d = camera.position.distanceTo(label.position);
            const s = d * 0.08;
            label.scale.set(s, s * 0.25, 1);
            if (label._anchor && label._offsetY !== undefined) {
                const oy = d * 0.012;
                label.position.set(label._anchor.x, label._anchor.y + oy, label._anchor.z);
            }
        }

        if (Math.floor(now / 200) !== Math.floor((now - dtReal * 1000) / 200)) {
            updateTrajectoryLines(clampedMET);
        }

        updateUI(clampedMET);
        syncMissionMetSlider();
        controls.update();
        renderer.render(scene, camera);
    }

    function onResize() {
        camera.aspect = window.innerWidth / window.innerHeight;
        camera.updateProjectionMatrix();
        renderer.setSize(window.innerWidth, window.innerHeight);
    }

    function distToSliderValue(dist) {
        const d = Math.max(ZOOM_DIST_MIN, Math.min(ZOOM_DIST_MAX, dist));
        const lo = Math.log(ZOOM_DIST_MIN);
        const hi = Math.log(ZOOM_DIST_MAX);
        return (Math.log(d) - lo) / (hi - lo);
    }

    function sliderValueToDist(t) {
        const lo = Math.log(ZOOM_DIST_MIN);
        const hi = Math.log(ZOOM_DIST_MAX);
        return Math.exp(lo + Math.max(0, Math.min(1, t)) * (hi - lo));
    }

    function applyCameraZoomDistance(dist) {
        const d = THREE.MathUtils.clamp(dist, ZOOM_DIST_MIN, ZOOM_DIST_MAX);
        const target = controls.target;
        const offset = camera.position.clone().sub(target);
        if (offset.lengthSq() < 1e-6) {
            offset.set(0.35, 0.25, 1).normalize();
        } else {
            offset.normalize();
        }
        offset.multiplyScalar(d);
        camera.position.copy(target).add(offset);
        camera.updateProjectionMatrix();
    }

    function onZoomSliderInput() {
        if (!zoomSliderEl) return;
        const t = zoomSliderEl.valueAsNumber / 1000;
        applyCameraZoomDistance(sliderValueToDist(t));
    }

    function formatZoomReadout(dist) {
        const km = dist * 1000;
        if (km >= 1e6) return (km / 1e6).toFixed(2).replace('.', ',') + ' Mio km';
        if (km >= 1000) return Math.round(km).toLocaleString('de-DE') + ' km';
        return Math.round(km) + ' km';
    }

    function buildPickableList() {
        pickableMeshes = [sunMesh, earthMesh, moonMesh];
        for (let i = 0; i < 8; i++) {
            if (planetMeshByIndex[i]) pickableMeshes.push(planetMeshByIndex[i]);
        }
        pickableMeshes.push(...extraPickableMeshes);
    }

    function jumpCameraToBody() {
        if (!focusedBody) return;
        const v = new THREE.Vector3();
        let dist = 14000;
        if (focusedBody.kind === 'sun') {
            v.copy(sunScenePos);
            dist = 220000;
        } else if (focusedBody.kind === 'moon') {
            v.copy(moonMesh.position);
            dist = 9000;
        } else {
            const idx = focusedBody.index;
            if (idx === 2) {
                v.set(0, 0, 0);
                dist = 20000;
            } else {
                const m = planetMeshByIndex[idx];
                v.copy(m.position);
                const r = m.geometry.parameters.radius;
                dist = Math.max(r * 10, 14000);
            }
        }
        controls.target.copy(v);
        const off = new THREE.Vector3(1, 0.45, 0.92).normalize().multiplyScalar(dist);
        camera.position.copy(v.clone().add(off));
    }

    function focusFromPick(kind, planetIndex) {
        exitFreeCamera();
        followOrion = false;
        followMoon = false;
        document.getElementById('follow-btn').classList.remove('active');
        document.getElementById('moon-btn').classList.remove('active');
        if (kind === 'sun') focusedBody = { kind: 'sun' };
        else if (kind === 'moon') focusedBody = { kind: 'moon' };
        else focusedBody = { kind: 'planet', index: planetIndex };
        jumpCameraToBody();
    }

    function onSceneClick(event) {
        if (event.button !== 0) return;
        const rect = renderer.domElement.getBoundingClientRect();
        pointerNdc.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
        pointerNdc.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;
        raycaster.setFromCamera(pointerNdc, camera);
        const hits = raycaster.intersectObjects(pickableMeshes, false);
        if (hits.length === 0) return;
        const u = hits[0].object.userData;
        if (u.pickKind === 'sun') focusFromPick('sun');
        else if (u.pickKind === 'moon') focusFromPick('moon');
        else if (u.pickKind === 'planet' && u.planetIndex !== undefined) focusFromPick('planet', u.planetIndex);
    }

    function onKeyUp(e) {
        if (freeCameraMode && Object.prototype.hasOwnProperty.call(flyKeys, e.key)) {
            flyKeys[e.key] = false;
        }
    }

    function onKeyDown(e) {
        if (freeCameraMode && (e.key === 'ArrowUp' || e.key === 'ArrowDown' || e.key === 'ArrowLeft' || e.key === 'ArrowRight')) {
            e.preventDefault();
            flyKeys[e.key] = true;
        }
        if (e.key === 'Escape') {
            if (freeCameraMode) exitFreeCamera();
            else clearFollowModes();
        }
        if (e.key === 'h' || e.key === 'H') {
            if (e.ctrlKey || e.metaKey || e.altKey) return;
            const t = e.target;
            if (t && (t.closest && (t.closest('input, textarea, select') || t.isContentEditable))) return;
            e.preventDefault();
            toggleAllPanels();
        }
    }

    // ── Controls ──
    function clearFollowModes() {
        followOrion = false;
        followMoon = false;
        focusedBody = null;
        document.getElementById('follow-btn').classList.remove('active');
        document.getElementById('moon-btn').classList.remove('active');
    }

    function exitFreeCamera() {
        if (!freeCameraMode) return;
        freeCameraMode = false;
        controls.enabled = true;
        controls.enablePan = true;
        const fb = document.getElementById('free-cam-btn');
        if (fb) fb.classList.remove('active');
        Object.keys(flyKeys).forEach((k) => { flyKeys[k] = false; });
    }

    window.toggleFreeCamera = function () {
        freeCameraMode = !freeCameraMode;
        const btn = document.getElementById('free-cam-btn');
        if (btn) btn.classList.toggle('active', freeCameraMode);
        controls.enabled = true;
        controls.enablePan = !freeCameraMode;
        if (freeCameraMode) {
            clearFollowModes();
            Object.keys(flyKeys).forEach((k) => { flyKeys[k] = false; });
        } else {
            Object.keys(flyKeys).forEach((k) => { flyKeys[k] = false; });
        }
    };

    window.toggleFollow = function () {
        exitFreeCamera();
        followOrion = !followOrion;
        if (followOrion) {
            focusedBody = null;
            followMoon = false;
            document.getElementById('moon-btn').classList.remove('active');
        }
        document.getElementById('follow-btn').classList.toggle('active', followOrion);
        if (!followOrion) controls.target.set(0, 0, 0);
    };

    window.toggleMoonView = function () {
        exitFreeCamera();
        followMoon = !followMoon;
        if (followMoon) {
            focusedBody = null;
            followOrion = false;
            document.getElementById('follow-btn').classList.remove('active');
        }
        document.getElementById('moon-btn').classList.toggle('active', followMoon);
        if (!followMoon) controls.target.set(0, 0, 0);
    };

    window.resetView = function () {
        exitFreeCamera();
        camera.position.set(0, 250, 450);
        controls.target.set(0, 0, 0);
        clearFollowModes();
    };

    window.solarSystemView = function () {
        exitFreeCamera();
        clearFollowModes();
        controls.target.copy(sunScenePos);
        camera.position.set(sunScenePos.x, 7000000, sunScenePos.z + 100000);
    };

    window.jumpToNow = function () {
        simTime = Date.now();
        warpToOne();
        clearFollowModes();
    };

    window.addEventListener('DOMContentLoaded', init);
})();
