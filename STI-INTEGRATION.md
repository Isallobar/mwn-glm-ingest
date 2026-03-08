# MWN STI Ingest — Integration Guide

## Add to your existing GLM Render service

### 1. Add the dependency

```bash
npm install nexrad-level-3-data
```

### 2. Copy `sti-ingest.js` into your project root (same folder as your GLM server file)

### 3. Add two lines to your existing server file

In your main server file (wherever you have the GLM Express app), add:

```javascript
// At the top with other requires
const { startSTIIngest } = require('./sti-ingest');

// After your existing app.listen() or wherever you start the server
startSTIIngest(app);
```

That's it. Your existing endpoints stay exactly the same:
- `/glm` — GLM flash clusters (unchanged)
- `/status` — GLM service health (unchanged)

New endpoints:
- `/sti` — All active NEXRAD storm cells CONUS-wide (GeoJSON)
- `/sti?bounds=28.93,-94.04,33.02,-88.82` — Filter to Louisiana bounds
- `/sti/status` — STI service health

### 4. Deploy to Render

```bash
git add sti-ingest.js package.json
git commit -m "Add NEXRAD Level III STI storm cell tracking"
git push
```

Render will auto-deploy.

## How it works

- Polls AWS S3 `unidata-nexrad-level3` bucket every 2 minutes
- Checks all ~160 NEXRAD sites for latest STI (Product 58) files
- Parses binary Level III format using `nexrad-level-3-data`
- Extracts storm cell positions, motion vectors, max dBZ, VIL, echo tops
- Serves clean GeoJSON with `motionDir` and `motionSpeedMph` per cell
- Drops cells older than 10 minutes

## Weather Bug integration

In your Weather Bug, replace the GLM-based storm motion with:

```javascript
// Fetch SCIT storm cells filtered to state bounds
const resp = await fetch('https://mwn-glm-ingest-1.onrender.com/sti?bounds=28.93,-94.04,33.02,-88.82');
const data = await resp.json();

// Each feature has:
// properties.motionDir      — degrees (0-360)
// properties.motionSpeedMph — mph
// properties.maxDbz         — max reflectivity
// properties.cellId         — SCIT cell ID (e.g. "F4")
```

## Tuning

In `sti-ingest.js`, adjust:
- `pollIntervalMs: 120000` — How often to check S3 (2 min default)
- `maxAgeMs: 600000` — Drop cells older than this (10 min default)
- `batchSize: 20` — Concurrent S3 requests per batch
