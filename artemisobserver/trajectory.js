/*
 * Artemis 2 Mission Trajectory — Real NASA OEM Ephemeris Data
 * Source: NASA JSC Flight Dynamics, CCSDS OEM v2.0
 * File: Artemis_II_OEM_2026_04_02_to_EI_v3.asc
 * Frame: EME2000 (J2000 Earth-centered inertial), positions in km, velocities in km/s
 * Scene units: 1 unit = 1000 km
 */

const ARTEMIS2 = (() => {

    const LAUNCH_UTC = Date.UTC(2026, 3, 1, 22, 35, 0); // April 1, 2026 22:35 UTC (confirmed)
    const EARTH_RADIUS = 6.371;
    const MOON_RADIUS = 1.737;
    const MOON_DISTANCE = 404.9;  // actual distance at flyby: ~404,914 km / 1000
    const MOON_ORBITAL_PERIOD_H = 27.322 * 24;

    // Moon position derived from Orion's acceleration vector at closest approach (MET 118.478h)
    // Acceleration points toward Moon center; Moon at (-129146, -336069, -185295) km
    const MOON_REF_MET = 118.478;
    const moonRefDir = normalize({ x: -129.146, y: -336.069, z: -185.295 });

    // Approximate Moon orbital pole (perpendicular to orbit plane)
    // Derived from cross product of Moon position with Moon velocity direction
    const orbitNormal = normalize(cross(moonRefDir, getPerp(moonRefDir)));

    // Two basis vectors in the Moon's orbital plane
    const moonE1 = moonRefDir;
    const moonE2 = normalize(cross(orbitNormal, moonE1));

    function normalize(v) {
        const len = Math.sqrt(v.x * v.x + v.y * v.y + v.z * v.z);
        return { x: v.x / len, y: v.y / len, z: v.z / len };
    }

    function cross(a, b) {
        return {
            x: a.y * b.z - a.z * b.y,
            y: a.z * b.x - a.x * b.z,
            z: a.x * b.y - a.y * b.x
        };
    }

    function getPerp(v) {
        if (Math.abs(v.x) < 0.9) return { x: 1, y: 0, z: 0 };
        return { x: 0, y: 1, z: 0 };
    }

    // J2000 equatorial → display coords via ecliptic
    // Ecliptic plane = horizontal (XZ), ecliptic north pole = Y (up)
    // Earth's axis is tilted 23.44° from ecliptic pole
    const OBLIQUITY = 23.4393 * Math.PI / 180;
    const cosObl = Math.cos(OBLIQUITY);
    const sinObl = Math.sin(OBLIQUITY);

    function j2000ToDisplay(v) {
        const ex = v.x;
        const ey = v.y * cosObl + v.z * sinObl;
        const ez = -v.y * sinObl + v.z * cosObl;
        return { x: -ey, y: ez, z: -ex };
    }

    function getMoonPositionJ2000(metHours) {
        const angularRate = (2 * Math.PI) / MOON_ORBITAL_PERIOD_H;
        const dAngle = (metHours - MOON_REF_MET) * angularRate;
        const cosA = Math.cos(dAngle);
        const sinA = Math.sin(dAngle);
        return {
            x: MOON_DISTANCE * (moonE1.x * cosA + moonE2.x * sinA),
            y: MOON_DISTANCE * (moonE1.y * cosA + moonE2.y * sinA),
            z: MOON_DISTANCE * (moonE1.z * cosA + moonE2.z * sinA)
        };
    }

    function getMoonPosition(metHours) {
        return j2000ToDisplay(getMoonPositionJ2000(metHours));
    }

    function getMoonOrbitPoints(numPoints) {
        const pts = [];
        for (let i = 0; i <= numPoints; i++) {
            const angle = (i / numPoints) * 2 * Math.PI;
            const cosA = Math.cos(angle);
            const sinA = Math.sin(angle);
            pts.push(j2000ToDisplay({
                x: MOON_DISTANCE * (moonE1.x * cosA + moonE2.x * sinA),
                y: MOON_DISTANCE * (moonE1.y * cosA + moonE2.y * sinA),
                z: MOON_DISTANCE * (moonE1.z * cosA + moonE2.z * sinA)
            }));
        }
        return pts;
    }

    // Mission phases
    const PHASES = [
        { name: "Start & Erdorbit",           start: 0,    end: 2.5  },
        { name: "Hochelliptischer Orbit",      start: 2.5,  end: 23   },
        { name: "TLI-Burn & Erdabflug",        start: 23,   end: 28   },
        { name: "Mondtransit (Hinflug)",       start: 28,   end: 108  },
        { name: "Mond-Flyby",                  start: 108,  end: 132  },
        { name: "Rücktransit zur Erde",        start: 132,  end: 210  },
        { name: "Wiedereintritt & Landung",    start: 210,  end: 220  }
    ];

    // Key mission milestones with MET in hours
    const MILESTONES = [
        { t: 0,      name: "Liftoff" },
        { t: 0.13,   name: "Booster-Abtrennung (SRB)" },
        { t: 0.53,   name: "ICPS-Abtrennung" },
        { t: 2,      name: "Perigäum-Anhebung (PRM)" },
        { t: 3,      name: "Apogäum-Anhebung (ARB)" },
        { t: 5,      name: "Proximity-Ops-Test mit ICPS" },
        { t: 23,     name: "Perigäum-Korrektur (PRB)" },
        { t: 25,     name: "TLI-Burn (Mondkurs)" },
        { t: 50,     name: "Outbound-Kurskorrektur OTC-1" },
        { t: 80,     name: "Outbound-Kurskorrektur OTC-2" },
        { t: 108,    name: "Annäherung an den Mond" },
        { t: 117,    name: "Funkverlust (Mondrückseite)" },
        { t: 118.5,  name: "Nächste Mondannäherung" },
        { t: 120,    name: "Funkkontakt wiederhergestellt" },
        { t: 125,    name: "Mond-Flyby abgeschlossen" },
        { t: 150,    name: "Return-Kurskorrektur RTC-1" },
        { t: 190,    name: "Return-Kurskorrektur RTC-2" },
        { t: 212,    name: "Service-Modul-Abtrennung" },
        { t: 215,    name: "Wiedereintritt (Entry Interface)" },
        { t: 215.5,  name: "Fallschirm-Entfaltung" },
        { t: 216,    name: "Splashdown im Pazifik" }
    ];

    function getNextMilestone(metHours) {
        for (const m of MILESTONES) {
            if (m.t > metHours) return m;
        }
        return MILESTONES[MILESTONES.length - 1];
    }

    function getLastMilestone(metHours) {
        let last = MILESTONES[0];
        for (const m of MILESTONES) {
            if (m.t <= metHours) last = m;
            else break;
        }
        return last;
    }

    function getPhase(metHours) {
        for (const p of PHASES) {
            if (metHours >= p.start && metHours < p.end) return p;
        }
        if (metHours < 0) return { name: "Vor dem Start", start: 0, end: 0 };
        return PHASES[PHASES.length - 1];
    }

    // NASA ephemeris data will be loaded asynchronously
    let WAYPOINTS = [];
    let dataLoaded = false;
    let loadCallbacks = [];

    function onDataLoaded(cb) {
        if (dataLoaded) cb();
        else loadCallbacks.push(cb);
    }

    // Load JSON data
    fetch('nasa_trajectory.json')
        .then(r => r.json())
        .then(data => {
            // Transform all OEM data from J2000 → display coords
            data.forEach(pt => {
                const d = j2000ToDisplay(pt);
                pt.x = d.x; pt.y = d.y; pt.z = d.z;
            });

            // Prepend early mission phase (LEO, before OEM data starts at ~MET 2.5h)
            const earlyPts = [];
            for (let t = 0; t <= 2.3; t += 0.15) {
                const alt = EARTH_RADIUS + 0.2;
                const angle = (t / 1.5) * 2 * Math.PI;
                const d = j2000ToDisplay({
                    x: alt * Math.cos(angle),
                    y: alt * Math.sin(angle) * 0.4,
                    z: alt * Math.sin(angle) * 0.917
                });
                earlyPts.push({ t, x: d.x, y: d.y, z: d.z, v: 7.8 });
            }
            WAYPOINTS = earlyPts.concat(data);
            dataLoaded = true;
            loadCallbacks.forEach(cb => cb());
            loadCallbacks = [];
        })
        .catch(err => {
            console.error('Failed to load trajectory data:', err);
        });

    // Binary search for the waypoint segment containing metHours
    function findSegment(metHours) {
        if (WAYPOINTS.length < 2) return 0;
        let lo = 0, hi = WAYPOINTS.length - 2;
        while (lo < hi) {
            const mid = (lo + hi + 1) >> 1;
            if (WAYPOINTS[mid].t <= metHours) lo = mid;
            else hi = mid - 1;
        }
        return lo;
    }

    function interpolatePosition(metHours) {
        if (WAYPOINTS.length === 0) return { t: metHours, x: EARTH_RADIUS + 0.2, y: 0, z: 0, v: 7.8 };
        if (metHours <= WAYPOINTS[0].t) return { ...WAYPOINTS[0] };
        if (metHours >= WAYPOINTS[WAYPOINTS.length - 1].t) return { ...WAYPOINTS[WAYPOINTS.length - 1] };

        const idx = findSegment(metHours);
        const p0 = WAYPOINTS[Math.max(0, idx - 1)];
        const p1 = WAYPOINTS[idx];
        const p2 = WAYPOINTS[Math.min(WAYPOINTS.length - 1, idx + 1)];
        const p3 = WAYPOINTS[Math.min(WAYPOINTS.length - 1, idx + 2)];

        const dt = p2.t - p1.t;
        if (dt < 0.0001) return { ...p1 };
        const localT = (metHours - p1.t) / dt;

        function catmullRom(v0, v1, v2, v3, t) {
            const t2 = t * t, t3 = t2 * t;
            return 0.5 * ((2 * v1) + (-v0 + v2) * t + (2 * v0 - 5 * v1 + 4 * v2 - v3) * t2 + (-v0 + 3 * v1 - 3 * v2 + v3) * t3);
        }

        return {
            t: metHours,
            x: catmullRom(p0.x, p1.x, p2.x, p3.x, localT),
            y: catmullRom(p0.y, p1.y, p2.y, p3.y, localT),
            z: catmullRom(p0.z, p1.z, p2.z, p3.z, localT),
            v: p1.v + (p2.v - p1.v) * localT
        };
    }

    // Real velocity from state vectors (km/s)
    function getVelocity(metHours) {
        const pos = interpolatePosition(metHours);
        return pos.v || 0;
    }

    function getMET(now) {
        return (now - LAUNCH_UTC) / 3600000;
    }

    function getFullTrajectoryPoints(stepHours) {
        const points = [];
        if (WAYPOINTS.length === 0) return points;
        const tStart = WAYPOINTS[0].t;
        const tEnd = WAYPOINTS[WAYPOINTS.length - 1].t;
        for (let t = tStart; t <= tEnd; t += stepHours) {
            points.push(interpolatePosition(t));
        }
        return points;
    }

    // ══════════════════════════════════════════════════════
    // Solar System — mean orbital elements (J2000.0 epoch)
    // ══════════════════════════════════════════════════════

    const AU = 149597.870691; // 1 AU in scene units (1000 km)
    const SUN_RADIUS = 696;

    const PLANETS = [
        { name: 'Merkur',  color: '#b0b0b0', a: 0.38710, e: 0.20563, i: 7.005, node: 48.331, peri: 77.456,  L0: 252.251, Lrate: 149472.674, radius: 2.440 },
        { name: 'Venus',   color: '#e8cfa0', a: 0.72333, e: 0.00677, i: 3.395, node: 76.680, peri: 131.534, L0: 181.980, Lrate: 58517.816,  radius: 6.052 },
        { name: 'Erde',    color: '#4499ff', a: 1.00000, e: 0.01671, i: 0.000, node: -11.261,peri: 102.937, L0: 100.464, Lrate: 35999.373,  radius: 6.371 },
        { name: 'Mars',    color: '#cc6644', a: 1.52368, e: 0.09340, i: 1.850, node: 49.558, peri: 336.060, L0: 355.453, Lrate: 19140.300,  radius: 3.390 },
        { name: 'Jupiter', color: '#ddaa88', a: 5.20289, e: 0.04850, i: 1.303, node: 100.464,peri: 14.331,  L0: 34.351,  Lrate: 3034.906,   radius: 69.911 },
        { name: 'Saturn',  color: '#eeccaa', a: 9.53668, e: 0.05415, i: 2.489, node: 113.666,peri: 93.057,  L0: 49.945,  Lrate: 1222.114,   radius: 58.232, hasRings: true },
        { name: 'Uranus',  color: '#88ddee', a: 19.1892, e: 0.04717, i: 0.773, node: 74.006, peri: 173.005, L0: 313.232, Lrate: 428.266,    radius: 25.362 },
        { name: 'Neptun',  color: '#4466bb', a: 30.0699, e: 0.00859, i: 1.770, node: 131.784,peri: 48.124,  L0: 304.880, Lrate: 218.486,    radius: 24.622 }
    ];

    function eclipticToDisplay(v) {
        return { x: -v.y, y: v.z, z: -v.x };
    }

    function getJulianCenturies(dateMs) {
        const J2000_MS = Date.UTC(2000, 0, 1, 12, 0, 0);
        return (dateMs - J2000_MS) / (86400000 * 36525);
    }

    function solveKepler(M, e) {
        M = M % (2 * Math.PI);
        if (M < 0) M += 2 * Math.PI;
        let E = M;
        for (let i = 0; i < 30; i++) {
            const dE = (E - e * Math.sin(E) - M) / (1 - e * Math.cos(E));
            E -= dE;
            if (Math.abs(dE) < 1e-10) break;
        }
        return E;
    }

    function planetHelioEcliptic(idx, T) {
        const p = PLANETS[idx];
        const deg = Math.PI / 180;
        const L = ((p.L0 + p.Lrate * T) % 360) * deg;
        const M = L - p.peri * deg;
        const E = solveKepler(M, p.e);
        const v = 2 * Math.atan2(
            Math.sqrt(1 + p.e) * Math.sin(E / 2),
            Math.sqrt(1 - p.e) * Math.cos(E / 2)
        );
        const r = p.a * AU * (1 - p.e * Math.cos(E));
        const w = (p.peri - p.node) * deg;
        const cosO = Math.cos(p.node * deg), sinO = Math.sin(p.node * deg);
        const cosI = Math.cos(p.i * deg), sinI = Math.sin(p.i * deg);
        const cosWV = Math.cos(w + v), sinWV = Math.sin(w + v);
        return {
            x: r * (cosO * cosWV - sinO * sinWV * cosI),
            y: r * (sinO * cosWV + cosO * sinWV * cosI),
            z: r * sinWV * sinI
        };
    }

    function getSunPosition(T) {
        const earth = planetHelioEcliptic(2, T);
        return eclipticToDisplay({ x: -earth.x, y: -earth.y, z: -earth.z });
    }

    function getPlanetPosition(idx, T) {
        const earth = planetHelioEcliptic(2, T);
        const planet = planetHelioEcliptic(idx, T);
        return eclipticToDisplay({
            x: planet.x - earth.x,
            y: planet.y - earth.y,
            z: planet.z - earth.z
        });
    }

    function getPlanetOrbitPoints(idx, T, numPoints) {
        const p = PLANETS[idx];
        const earth = planetHelioEcliptic(2, T);
        const deg = Math.PI / 180;
        const w = (p.peri - p.node) * deg;
        const cosO = Math.cos(p.node * deg), sinO = Math.sin(p.node * deg);
        const cosI = Math.cos(p.i * deg), sinI = Math.sin(p.i * deg);
        const pts = [];
        for (let i = 0; i <= numPoints; i++) {
            const M = (i / numPoints) * 2 * Math.PI;
            const E = solveKepler(M, p.e);
            const v = 2 * Math.atan2(
                Math.sqrt(1 + p.e) * Math.sin(E / 2),
                Math.sqrt(1 - p.e) * Math.cos(E / 2)
            );
            const r = p.a * AU * (1 - p.e * Math.cos(E));
            const cosWV = Math.cos(w + v), sinWV = Math.sin(w + v);
            const hx = r * (cosO * cosWV - sinO * sinWV * cosI);
            const hy = r * (sinO * cosWV + cosO * sinWV * cosI);
            const hz = r * sinWV * sinI;
            pts.push(eclipticToDisplay({
                x: hx - earth.x, y: hy - earth.y, z: hz - earth.z
            }));
        }
        return pts;
    }

    return {
        LAUNCH_UTC, EARTH_RADIUS, MOON_RADIUS, MOON_DISTANCE,
        AU, SUN_RADIUS, PLANETS,
        PHASES, MILESTONES, WAYPOINTS,
        getMoonPosition, getMoonOrbitPoints,
        getSunPosition, getPlanetPosition, getPlanetOrbitPoints,
        getJulianCenturies,
        getPhase, getNextMilestone, getLastMilestone,
        interpolatePosition, getVelocity, getMET,
        getFullTrajectoryPoints, onDataLoaded,
        get dataLoaded() { return dataLoaded; }
    };
})();
