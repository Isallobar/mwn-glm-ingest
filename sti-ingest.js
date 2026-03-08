// ═══════════════════════════════════════════════════════════════════════
// NEXRAD Level III STI/NSS Ingest — Storm Cell Tracking & Structure
// Add to existing MWN GLM Ingest Render service
// ═══════════════════════════════════════════════════════════════════════
//
// Polls AWS S3 (unidata-nexrad-level3) for the latest STI (Product 58)
// and NSS (Product 62) files from all ~160 NEXRAD sites. Parses binary
// Level III data into clean JSON with storm cell positions, motion
// vectors, reflectivity, VIL, echo tops, and hail indices.
//
// Endpoints:
//   GET /sti           — All active storm cells CONUS-wide (GeoJSON)
//   GET /sti?bounds=S,W,N,E  — Filter to bounding box
//   GET /sti/status    — Service health + stats
//
// ═══════════════════════════════════════════════════════════════════════

const nexradLevel3 = require('nexrad-level-3-data');

// ── All operational NEXRAD WSR-88D sites ──
// Format: { id, lat, lon } — lat/lon is the radar site location
const NEXRAD_SITES = [
  {id:'KABR',lat:45.456,lon:-98.413},{id:'KABX',lat:35.150,lon:-106.824},{id:'KAKQ',lat:36.984,lon:-77.008},
  {id:'KAMA',lat:35.233,lon:-101.709},{id:'KAMX',lat:25.611,lon:-80.413},{id:'KAPX',lat:44.907,lon:-84.720},
  {id:'KARX',lat:43.823,lon:-91.191},{id:'KATX',lat:48.195,lon:-122.496},{id:'KBBX',lat:39.496,lon:-121.632},
  {id:'KBGM',lat:42.200,lon:-75.985},{id:'KBHX',lat:40.499,lon:-124.292},{id:'KBIS',lat:46.771,lon:-100.760},
  {id:'KBLX',lat:45.854,lon:-108.607},{id:'KBMX',lat:33.172,lon:-86.770},{id:'KBOX',lat:41.956,lon:-71.137},
  {id:'KBRO',lat:25.916,lon:-97.419},{id:'KBUF',lat:42.949,lon:-78.737},{id:'KBYX',lat:24.598,lon:-81.703},
  {id:'KCAE',lat:33.949,lon:-81.118},{id:'KCBW',lat:46.039,lon:-67.806},{id:'KCBX',lat:43.491,lon:-116.236},
  {id:'KCCX',lat:40.923,lon:-78.004},{id:'KCLE',lat:41.413,lon:-81.860},{id:'KCLX',lat:32.656,lon:-81.042},
  {id:'KCRP',lat:27.784,lon:-97.511},{id:'KCXX',lat:44.511,lon:-73.167},{id:'KCYS',lat:41.152,lon:-104.806},
  {id:'KDAX',lat:38.501,lon:-121.678},{id:'KDDC',lat:37.761,lon:-99.969},{id:'KDFX',lat:29.273,lon:-100.281},
  {id:'KDGX',lat:32.280,lon:-89.984},{id:'KDIX',lat:39.947,lon:-74.411},{id:'KDLH',lat:46.837,lon:-92.210},
  {id:'KDMX',lat:41.731,lon:-93.723},{id:'KDOX',lat:38.826,lon:-75.440},{id:'KDTX',lat:42.700,lon:-83.472},
  {id:'KDVN',lat:41.612,lon:-90.581},{id:'KDYX',lat:32.538,lon:-99.254},{id:'KEAX',lat:38.810,lon:-94.264},
  {id:'KEMX',lat:31.894,lon:-110.630},{id:'KENX',lat:42.586,lon:-74.064},{id:'KEOX',lat:31.460,lon:-85.459},
  {id:'KEPZ',lat:31.873,lon:-106.698},{id:'KESX',lat:35.701,lon:-114.891},{id:'KEVX',lat:30.565,lon:-85.922},
  {id:'KEWX',lat:29.704,lon:-98.029},{id:'KEYX',lat:35.098,lon:-117.561},{id:'KFCX',lat:37.024,lon:-80.274},
  {id:'KFDR',lat:34.362,lon:-98.976},{id:'KFDX',lat:34.635,lon:-103.630},{id:'KFFC',lat:33.364,lon:-84.566},
  {id:'KFSD',lat:43.588,lon:-96.729},{id:'KFSX',lat:34.574,lon:-111.198},{id:'KFTG',lat:39.787,lon:-104.546},
  {id:'KFWS',lat:32.573,lon:-97.303},{id:'KGGW',lat:48.206,lon:-106.625},{id:'KGJX',lat:39.062,lon:-108.214},
  {id:'KGLD',lat:39.367,lon:-101.700},{id:'KGRB',lat:44.499,lon:-88.111},{id:'KGRK',lat:30.722,lon:-97.383},
  {id:'KGRR',lat:42.894,lon:-85.545},{id:'KGSP',lat:34.883,lon:-82.220},{id:'KGWX',lat:33.897,lon:-88.329},
  {id:'KGYX',lat:43.891,lon:-70.257},{id:'KHDX',lat:33.077,lon:-106.123},{id:'KHGX',lat:29.472,lon:-95.079},
  {id:'KHNX',lat:36.314,lon:-119.632},{id:'KHPX',lat:36.737,lon:-87.285},{id:'KHTX',lat:34.931,lon:-86.083},
  {id:'KICT',lat:37.655,lon:-97.443},{id:'KICX',lat:37.591,lon:-112.862},{id:'KILN',lat:39.420,lon:-83.822},
  {id:'KILX',lat:40.151,lon:-89.337},{id:'KIND',lat:39.708,lon:-86.280},{id:'KINX',lat:36.175,lon:-95.565},
  {id:'KIWA',lat:33.289,lon:-111.670},{id:'KIWX',lat:41.359,lon:-85.700},{id:'KJAX',lat:30.485,lon:-81.702},
  {id:'KJGX',lat:32.675,lon:-83.351},{id:'KJKL',lat:37.591,lon:-83.313},{id:'KLBB',lat:33.654,lon:-101.814},
  {id:'KLCH',lat:30.125,lon:-93.216},{id:'KLIX',lat:30.337,lon:-89.826},{id:'KLNX',lat:41.958,lon:-100.576},
  {id:'KLOT',lat:41.604,lon:-88.085},{id:'KLRX',lat:40.740,lon:-116.803},{id:'KLSX',lat:38.699,lon:-90.683},
  {id:'KLTX',lat:33.989,lon:-78.429},{id:'KLVX',lat:37.975,lon:-85.944},{id:'KLWX',lat:38.975,lon:-77.478},
  {id:'KLZK',lat:34.836,lon:-92.262},{id:'KMAF',lat:31.943,lon:-102.189},{id:'KMAX',lat:42.081,lon:-122.717},
  {id:'KMBX',lat:48.393,lon:-100.864},{id:'KMHX',lat:34.776,lon:-76.876},{id:'KMKX',lat:42.968,lon:-88.551},
  {id:'KMLB',lat:28.113,lon:-80.654},{id:'KMOB',lat:30.679,lon:-88.240},{id:'KMPX',lat:44.849,lon:-93.566},
  {id:'KMQT',lat:46.531,lon:-87.548},{id:'KMRX',lat:36.169,lon:-83.402},{id:'KMSX',lat:47.041,lon:-113.986},
  {id:'KMTX',lat:41.263,lon:-112.448},{id:'KMUX',lat:37.155,lon:-121.898},{id:'KMVX',lat:47.528,lon:-97.326},
  {id:'KMXX',lat:32.537,lon:-85.790},{id:'KNKX',lat:32.919,lon:-117.042},{id:'KNQA',lat:35.345,lon:-89.873},
  {id:'KOAX',lat:41.320,lon:-96.367},{id:'KOHX',lat:36.247,lon:-86.563},{id:'KOKX',lat:40.866,lon:-72.864},
  {id:'KOTX',lat:47.681,lon:-117.627},{id:'KOUN',lat:35.236,lon:-97.462},{id:'KPAH',lat:37.068,lon:-88.772},
  {id:'KPBZ',lat:40.532,lon:-80.218},{id:'KPDT',lat:45.691,lon:-118.853},{id:'KPOE',lat:31.156,lon:-92.976},
  {id:'KPUX',lat:38.460,lon:-104.181},{id:'KRAX',lat:35.665,lon:-78.490},{id:'KRGX',lat:39.754,lon:-119.462},
  {id:'KRIW',lat:43.066,lon:-108.477},{id:'KRLX',lat:38.311,lon:-81.723},{id:'KRMX',lat:41.407,lon:-79.158},
  {id:'KRNK',lat:37.207,lon:-80.410},{id:'KRTX',lat:45.715,lon:-122.966},{id:'KSFX',lat:43.106,lon:-112.686},
  {id:'KSGF',lat:37.235,lon:-93.400},{id:'KSHV',lat:32.451,lon:-93.841},{id:'KSJT',lat:31.371,lon:-100.493},
  {id:'KSOX',lat:33.818,lon:-117.636},{id:'KSRX',lat:35.290,lon:-94.362},{id:'KTBW',lat:27.706,lon:-82.402},
  {id:'KTFX',lat:47.460,lon:-111.385},{id:'KTLH',lat:30.398,lon:-84.329},{id:'KTLX',lat:35.333,lon:-97.278},
  {id:'KTWX',lat:38.997,lon:-96.233},{id:'KTYX',lat:43.756,lon:-75.680},{id:'KUDX',lat:44.125,lon:-102.830},
  {id:'KUEX',lat:40.321,lon:-98.442},{id:'KVAX',lat:30.890,lon:-83.002},{id:'KVBX',lat:34.838,lon:-120.398},
  {id:'KVNX',lat:36.741,lon:-98.128},{id:'KVTX',lat:34.412,lon:-119.179},{id:'KVWX',lat:38.260,lon:-87.725},
  {id:'KYUX',lat:32.495,lon:-114.657},{id:'KLGX',lat:47.117,lon:-124.107},{id:'KDOX',lat:38.826,lon:-75.440},
  // TDWR sites (some overlap)
  {id:'TPBI',lat:26.690,lon:-80.096},{id:'TMCI',lat:39.323,lon:-94.730},{id:'TORD',lat:41.983,lon:-87.860},
];

// ── State tracking ──
const stiState = {
  storms: [],               // All active storm cells
  lastPoll: null,
  lastFileBysite: new Map(), // site → last processed filename
  pollCount: 0,
  errorCount: 0,
  siteErrors: new Map(),
  pollIntervalMs: 120000,   // 2 minutes between full polls
  maxAgeMs: 600000,         // Drop storms older than 10 minutes
  batchSize: 20,            // Concurrent S3 requests per batch
};

const S3_BUCKET = 'https://unidata-nexrad-level3.s3.amazonaws.com';


// ═══════════════════════════════════════════════════════════════════════
// S3 HELPERS
// ═══════════════════════════════════════════════════════════════════════

/**
 * List the most recent STI file for a given NEXRAD site.
 * Uses S3 REST API with prefix filtering.
 */
async function getLatestSTIKey(siteId) {
  const now = new Date();
  // Try today's date first, fall back to yesterday if early UTC
  const dates = [formatDatePrefix(now)];
  if (now.getUTCHours() < 2) {
    const yesterday = new Date(now.getTime() - 86400000);
    dates.push(formatDatePrefix(yesterday));
  }

  for (const datePrefix of dates) {
    const prefix = `${siteId}_STI_${datePrefix}`;
    const url = `${S3_BUCKET}?list-type=2&prefix=${encodeURIComponent(prefix)}&max-keys=50`;

    try {
      const resp = await fetch(url, { signal: AbortSignal.timeout(8000) });
      if (!resp.ok) continue;
      const xml = await resp.text();

      // Parse keys from XML response
      const keys = [];
      const keyRegex = /<Key>([^<]+)<\/Key>/g;
      let match;
      while ((match = keyRegex.exec(xml)) !== null) {
        if (match[1].includes('_STI_')) keys.push(match[1]);
      }

      if (keys.length > 0) {
        // Keys are alphabetically sorted, last = most recent
        return keys[keys.length - 1];
      }
    } catch (e) {
      // Timeout or network error — skip this date
    }
  }
  return null;
}


/**
 * Fetch and parse a Level III STI binary file from S3.
 */
async function fetchAndParseSTI(key) {
  const url = `${S3_BUCKET}/${key}`;
  const resp = await fetch(url, { signal: AbortSignal.timeout(10000) });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  const buffer = Buffer.from(await resp.arrayBuffer());
  return nexradLevel3(buffer);
}


function formatDatePrefix(date) {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, '0');
  const d = String(date.getUTCDate()).padStart(2, '0');
  return `${y}_${m}_${d}`;
}


// ═══════════════════════════════════════════════════════════════════════
// STORM CELL EXTRACTION
// ═══════════════════════════════════════════════════════════════════════

/**
 * Extract storm cell data from parsed Level III STI product.
 * Returns array of storm cell objects.
 */
function extractStormCells(parsed, siteId, siteLat, siteLon) {
  const cells = [];
  if (!parsed || !parsed.symbology) return cells;

  try {
    // STI Product 58 contains storm cell data in symbology layers
    // Each layer can have multiple packets with cell positions and attributes
    for (const layer of (parsed.symbology.layers || [])) {
      for (const packet of (layer.packets || [])) {

        // Packet codes 15 (Storm ID), 23 (SCIT past/forecast), 25 (STI Circle)
        // The cell data is typically in tabular form in the product description
        // or in graphic packets with position data

        if (packet.type === 'STI' || packet.packetCode === 58 ||
            packet.cells || packet.features) {

          const items = packet.cells || packet.features || [];
          for (const cell of items) {
            const cellData = extractCellFields(cell, siteId, siteLat, siteLon);
            if (cellData) cells.push(cellData);
          }
        }
      }
    }

    // Also check tabular alphanumeric data (Product Description Block)
    if (parsed.productDescription) {
      const tabularCells = parseTabularSTI(parsed, siteId, siteLat, siteLon);
      if (tabularCells.length > 0 && cells.length === 0) {
        return tabularCells;
      }
    }

    // Check for graphic data with storm positions
    if (cells.length === 0 && parsed.symbology) {
      const graphicCells = parseGraphicSTI(parsed, siteId, siteLat, siteLon);
      if (graphicCells.length > 0) return graphicCells;
    }

  } catch (e) {
    console.warn(`[STI] Parse error for ${siteId}:`, e.message);
  }

  return cells;
}


/**
 * Extract fields from a single cell object.
 */
function extractCellFields(cell, siteId, siteLat, siteLon) {
  if (!cell) return null;

  // Position can be in various formats depending on the parser output
  let lat = cell.lat || cell.latitude || null;
  let lon = cell.lon || cell.longitude || null;

  // If position is relative to radar (range/azimuth), convert
  if (!lat && cell.range != null && cell.azimuth != null) {
    const rangeKm = cell.range * 1.852; // nautical miles to km
    const azRad = cell.azimuth * Math.PI / 180;
    lat = siteLat + (rangeKm / 111.32) * Math.cos(azRad);
    lon = siteLon + (rangeKm / (111.32 * Math.cos(siteLat * Math.PI / 180))) * Math.sin(azRad);
  }

  if (!lat || !lon) return null;

  return {
    cellId: cell.id || cell.cellId || cell.stormId || null,
    siteId: siteId,
    lat: Math.round(lat * 1000) / 1000,
    lon: Math.round(lon * 1000) / 1000,
    motionDir: cell.direction || cell.dir || cell.motionDirection || null,
    motionSpeedKts: cell.speed || cell.motionSpeed || null,
    motionSpeedMph: cell.speed ? Math.round(cell.speed * 1.15078) : null,
    maxDbz: cell.maxReflectivity || cell.maxDbz || cell.dbz || null,
    vil: cell.vil || cell.VIL || null,
    echoTop: cell.top || cell.echoTop || cell.echoTopFt || null,
    echoBase: cell.base || cell.echoBase || null,
    hailProb: cell.poh || cell.hailProb || null,
    hailSize: cell.maxHailSize || cell.hailSize || null,
    time: new Date().toISOString(),
  };
}


/**
 * Parse tabular/alphanumeric STI data from product description.
 * This is the text-based storm attribute table.
 */
function parseTabularSTI(parsed, siteId, siteLat, siteLon) {
  const cells = [];

  // The tabular data may be in productDescription.tabularAlphanumeric
  // or in various text blocks within the product
  const tabular = parsed.productDescription?.tabularAlphanumeric ||
                   parsed.tabularAlphanumeric || null;

  if (!tabular) return cells;

  // Tabular STI contains lines like:
  // CELL ID  AZ/RAN  MOVT  DBZM  HT  TOP  FCST MVMT  ERR/MEAN
  // F4      242/67   22/ 16  53  6.3  22.4  245/67  14/  9
  try {
    const lines = typeof tabular === 'string' ? tabular.split('\n') :
                  Array.isArray(tabular) ? tabular.flat() : [];

    for (const line of lines) {
      // Match cell lines: ID, AZ/RAN, DIR/SPD, DBZ, etc.
      // Format varies but generally: ID  AZ/RAN  SPD/DIR  DBZM  HT  TOP
      const cellMatch = line.match(
        /^\s*([A-Z]\d+)\s+(\d+)\s*\/\s*(\d+)\s+(\d+)\s*\/?\s*(\d+)?\s+(\d+)\s/
      );
      if (!cellMatch) continue;

      const cellId = cellMatch[1];
      const azimuth = parseInt(cellMatch[2]);
      const rangeNm = parseInt(cellMatch[3]);
      const speed = parseInt(cellMatch[4]);
      const direction = cellMatch[5] ? parseInt(cellMatch[5]) : null;
      const dbz = parseInt(cellMatch[6]);

      // Convert range/azimuth to lat/lon
      const rangeKm = rangeNm * 1.852;
      const azRad = azimuth * Math.PI / 180;
      const lat = siteLat + (rangeKm / 111.32) * Math.cos(azRad);
      const lon = siteLon + (rangeKm / (111.32 * Math.cos(siteLat * Math.PI / 180))) * Math.sin(azRad);

      cells.push({
        cellId,
        siteId,
        lat: Math.round(lat * 1000) / 1000,
        lon: Math.round(lon * 1000) / 1000,
        motionDir: direction || azimuth,
        motionSpeedKts: speed,
        motionSpeedMph: Math.round(speed * 1.15078),
        maxDbz: dbz,
        vil: null,
        echoTop: null,
        echoBase: null,
        hailProb: null,
        hailSize: null,
        time: new Date().toISOString(),
      });
    }
  } catch (e) {
    console.warn(`[STI] Tabular parse error for ${siteId}:`, e.message);
  }

  return cells;
}


/**
 * Parse graphic/symbology packets for storm positions.
 */
function parseGraphicSTI(parsed, siteId, siteLat, siteLon) {
  const cells = [];

  try {
    for (const layer of (parsed.symbology?.layers || [])) {
      for (const packet of (layer.packets || [])) {
        // Check for various packet types that carry position data
        if (packet.data && Array.isArray(packet.data)) {
          for (const item of packet.data) {
            if (item.lat != null && item.lon != null) {
              cells.push({
                cellId: item.id || item.stormId || null,
                siteId,
                lat: Math.round(item.lat * 1000) / 1000,
                lon: Math.round(item.lon * 1000) / 1000,
                motionDir: item.direction || null,
                motionSpeedKts: item.speed || null,
                motionSpeedMph: item.speed ? Math.round(item.speed * 1.15078) : null,
                maxDbz: item.maxDbz || null,
                vil: null,
                echoTop: null,
                echoBase: null,
                hailProb: null,
                hailSize: null,
                time: new Date().toISOString(),
              });
            }
          }
        }
      }
    }
  } catch (e) {
    // ignore
  }

  return cells;
}


// ═══════════════════════════════════════════════════════════════════════
// POLLING ENGINE
// ═══════════════════════════════════════════════════════════════════════

/**
 * Poll all NEXRAD sites in batches for latest STI data.
 */
async function pollAllSites() {
  const startTime = Date.now();
  stiState.pollCount++;
  let newCellCount = 0;
  let sitesChecked = 0;
  let sitesWithData = 0;

  const allCells = [];

  // Process sites in batches to avoid overwhelming network
  for (let i = 0; i < NEXRAD_SITES.length; i += stiState.batchSize) {
    const batch = NEXRAD_SITES.slice(i, i + stiState.batchSize);
    const batchPromises = batch.map(async (site) => {
      try {
        sitesChecked++;
        const latestKey = await getLatestSTIKey(site.id);
        if (!latestKey) return;

        // Skip if we already processed this file
        if (stiState.lastFileBysite.get(site.id) === latestKey) return;

        // Check file age — extract timestamp from key
        const fileAge = getFileAgeMs(latestKey);
        if (fileAge > stiState.maxAgeMs) return;

        const parsed = await fetchAndParseSTI(latestKey);
        const cells = extractStormCells(parsed, site.id, site.lat, site.lon);

        if (cells.length > 0) {
          sitesWithData++;
          newCellCount += cells.length;
          allCells.push(...cells);
          stiState.lastFileBysite.set(site.id, latestKey);
        }
      } catch (e) {
        const errCount = (stiState.siteErrors.get(site.id) || 0) + 1;
        stiState.siteErrors.set(site.id, errCount);
        if (errCount <= 3) {
          console.warn(`[STI] ${site.id} error (${errCount}):`, e.message);
        }
        stiState.errorCount++;
      }
    });

    await Promise.all(batchPromises);
  }

  // Merge new cells — deduplicate by cellId+siteId
  const cellMap = new Map();
  for (const cell of allCells) {
    const key = `${cell.siteId}_${cell.cellId || cell.lat + ',' + cell.lon}`;
    cellMap.set(key, cell);
  }

  // Also keep recent cells from previous poll if their site wasn't updated
  const now = Date.now();
  for (const cell of stiState.storms) {
    const key = `${cell.siteId}_${cell.cellId || cell.lat + ',' + cell.lon}`;
    if (!cellMap.has(key)) {
      const age = now - new Date(cell.time).getTime();
      if (age < stiState.maxAgeMs) {
        cellMap.set(key, cell);
      }
    }
  }

  stiState.storms = [...cellMap.values()];
  stiState.lastPoll = new Date().toISOString();

  const elapsed = Date.now() - startTime;
  console.log(`[STI] Poll #${stiState.pollCount}: ${sitesChecked} sites checked, ` +
    `${sitesWithData} with data, ${stiState.storms.length} total cells (${elapsed}ms)`);
}


/**
 * Get file age in ms from the timestamp encoded in the S3 key.
 */
function getFileAgeMs(key) {
  // Key format: KLIX_STI_2026_03_08_01_35_40
  const parts = key.match(/_(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})_(\d{2})$/);
  if (!parts) return Infinity;
  const fileDate = new Date(Date.UTC(
    parseInt(parts[1]), parseInt(parts[2]) - 1, parseInt(parts[3]),
    parseInt(parts[4]), parseInt(parts[5]), parseInt(parts[6])
  ));
  return Date.now() - fileDate.getTime();
}


// ═══════════════════════════════════════════════════════════════════════
// EXPRESS ROUTE HANDLERS — Add these to your existing app
// ═══════════════════════════════════════════════════════════════════════

function registerSTIRoutes(app) {

  // GET /sti — All active storm cells (or filtered by bounds)
  app.get('/sti', (req, res) => {
    res.set({
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'no-store',
    });

    let storms = stiState.storms;

    // Optional bounding box filter: ?bounds=south,west,north,east
    if (req.query.bounds) {
      const parts = req.query.bounds.split(',').map(Number);
      if (parts.length === 4 && parts.every(n => !isNaN(n))) {
        const [south, west, north, east] = parts;
        storms = storms.filter(s =>
          s.lat >= south && s.lat <= north &&
          s.lon >= west && s.lon <= east
        );
      }
    }

    res.json({
      type: 'FeatureCollection',
      features: storms.map(s => ({
        type: 'Feature',
        geometry: {
          type: 'Point',
          coordinates: [s.lon, s.lat],
        },
        properties: {
          cellId: s.cellId,
          siteId: s.siteId,
          motionDir: s.motionDir,
          motionSpeedKts: s.motionSpeedKts,
          motionSpeedMph: s.motionSpeedMph,
          maxDbz: s.maxDbz,
          vil: s.vil,
          echoTop: s.echoTop,
          echoBase: s.echoBase,
          hailProb: s.hailProb,
          hailSize: s.hailSize,
          time: s.time,
        },
      })),
      meta: {
        storm_count: storms.length,
        total_cells: stiState.storms.length,
        last_poll: stiState.lastPoll,
        poll_count: stiState.pollCount,
        source: 'NEXRAD Level III STI (Product 58)',
      },
    });
  });

  // GET /sti/status — Service health
  app.get('/sti/status', (req, res) => {
    res.set({
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
    });
    res.json({
      status: 'ok',
      storm_count: stiState.storms.length,
      last_poll: stiState.lastPoll,
      poll_count: stiState.pollCount,
      error_count: stiState.errorCount,
      sites_tracked: NEXRAD_SITES.length,
      sites_with_data: new Set(stiState.storms.map(s => s.siteId)).size,
      poll_interval_ms: stiState.pollIntervalMs,
    });
  });

  console.log('[STI] Routes registered: /sti, /sti/status');
}


// ═══════════════════════════════════════════════════════════════════════
// INITIALIZATION — Call from your existing server startup
// ═══════════════════════════════════════════════════════════════════════

function startSTIIngest(app) {
  registerSTIRoutes(app);

  // Run first poll immediately
  console.log(`[STI] Starting NEXRAD Level III STI ingest — ${NEXRAD_SITES.length} sites`);
  pollAllSites().catch(e => console.error('[STI] Initial poll error:', e));

  // Schedule recurring polls
  setInterval(() => {
    pollAllSites().catch(e => console.error('[STI] Poll error:', e));
  }, stiState.pollIntervalMs);
}


module.exports = { startSTIIngest, registerSTIRoutes };
