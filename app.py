#!/usr/bin/env python3
"""
GLM Ingest Service — MWN Lightning Jump Detector
================================================
Polls NOAA GOES-East (GOES-19, falls back to GOES-16) GLM-L2-LCFA files
from the public AWS S3 bucket every 20 seconds, parses the NetCDF flash
data, clusters flashes into storm cells, and serves the result as GeoJSON
on http://localhost:8801/glm

Your lightning-jump-detector.html polls this endpoint instead of running
synthetic data when the service is running.

Requirements
------------
    pip install boto3 xarray netCDF4 scipy flask flask-cors

Run
---
    python glm-ingest.py

The server starts on port 8801. Add to OBS/XSplit startup scripts or run
in a terminal before launching your stream.

GOES Satellite Notes
--------------------
- GOES-19: Primary GOES-East as of April 4, 2025 (bucket: noaa-goes19)
- GOES-16: Legacy GOES-East through April 2025 (bucket: noaa-goes16)
- GOES-18: GOES-West (bucket: noaa-goes18)
- GOES-East covers CONUS + most of Western Hemisphere
- GLM files are ~20 second windows of optical lightning detection
- Each file: flash_lat, flash_lon, flash_energy, flash_area, flash_time_offset

S3 path format:
    s3://noaa-goes19/GLM-L2-LCFA/YYYY/DOY/HH/
    OR_GLM-L2-LCFA_G19_sYYYYDOYHHMMSS_eYYYYDOYHHMMSS_cYYYYDOYHHMMSS.nc
"""

import os
import io
import json
import time
import math
import logging
import threading
from datetime import datetime, timezone, timedelta
from collections import defaultdict

import boto3
import xarray as xr
import numpy as np
from flask import Flask, jsonify
from flask_cors import CORS
from botocore import UNSIGNED
from botocore.config import Config

# ── CONFIG ────────────────────────────────────────────────────────────────────

PORT           = int(os.environ.get("PORT", 8801))
POLL_INTERVAL  = 20          # seconds between S3 polls
CLUSTER_RADIUS = 0.5         # degrees — flash clustering radius
MIN_FLASHES    = 1           # minimum flashes to report a cluster
MAX_AGE_SEC    = 60          # discard files older than this
CONUS_BOUNDS   = dict(lat_min=24.0, lat_max=50.0, lon_min=-125.0, lon_max=-66.0)

# Satellite buckets in priority order (GOES-East first)
GOES_BUCKETS = ['noaa-goes19', 'noaa-goes16']
GLM_PREFIX   = 'GLM-L2-LCFA'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('glm-ingest')

# ── GLOBAL STATE ──────────────────────────────────────────────────────────────

_state = {
    'clusters':    [],       # list of cluster dicts for /glm endpoint
    'file_time':   None,     # datetime of the last successfully parsed file
    'fetch_time':  None,     # wall-clock time of last successful fetch
    'source':      None,     # which S3 bucket we used
    'file_count':  0,        # total files parsed this session
    'flash_count': 0,        # total flashes parsed this session
    'error':       None,     # last error string, or None
}
_lock = threading.Lock()

# ── S3 CLIENT (unsigned — public bucket) ──────────────────────────────────────

s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED), region_name='us-east-1')

# ── GLM PARSING ───────────────────────────────────────────────────────────────

def latest_glm_key(bucket: str, dt: datetime) -> str | None:
    """
    List the GLM-L2-LCFA prefix for the given hour and the hour before,
    return the key of the most recent file.
    """
    keys = []
    for offset_h in (0, 1):
        t = dt - timedelta(hours=offset_h)
        prefix = f"{GLM_PREFIX}/{t.year}/{t.timetuple().tm_yday:03d}/{t.hour:02d}/"
        try:
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            for obj in resp.get('Contents', []):
                keys.append((obj['LastModified'], obj['Key']))
        except Exception as e:
            log.debug(f"list {bucket}/{prefix}: {e}")

    if not keys:
        return None
    keys.sort(key=lambda x: x[0], reverse=True)
    return keys[0][1]


def parse_glm_file(bucket: str, key: str) -> list[dict]:
    """
    Download one GLM NetCDF file from S3 and return a list of flash dicts:
        { lat, lon, energy, area }
    Filters to CONUS bounds.
    GLM files are NetCDF4 — must use engine='netcdf4', which requires a real
    file path (not BytesIO), so we write to a temp file first.
    """
    import tempfile
    log.info(f"Fetching s3://{bucket}/{key}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj['Body'].read()

    # netcdf4 engine requires a real path, not a BytesIO buffer
    with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp:
        tmp.write(data)
        tmp_path = tmp.name

    try:
        ds = xr.open_dataset(tmp_path, engine='netcdf4')

        # GLM variables
        lats    = ds['flash_lat'].values.astype(float)
        lons    = ds['flash_lon'].values.astype(float)
        energy  = ds['flash_energy'].values.astype(float)   # Joules (radiant)
        area    = ds['flash_area'].values.astype(float)     # km²

        ds.close()
    finally:
        os.unlink(tmp_path)  # always clean up temp file

    flashes = []
    b = CONUS_BOUNDS
    for i in range(len(lats)):
        la, lo = lats[i], lons[i]
        if math.isnan(la) or math.isnan(lo):
            continue
        if not (b['lat_min'] <= la <= b['lat_max'] and b['lon_min'] <= lo <= b['lon_max']):
            continue
        flashes.append({
            'lat':    round(float(la), 4),
            'lon':    round(float(lo), 4),
            'energy': float(energy[i]) if not math.isnan(energy[i]) else 0.0,
            'area':   float(area[i])   if not math.isnan(area[i])   else 0.0,
        })

    log.info(f"  → {len(flashes)} CONUS flashes from {len(lats)} total")
    return flashes


def cluster_flashes(flashes: list[dict]) -> list[dict]:
    """
    Simple greedy spatial clustering.
    Groups flashes within CLUSTER_RADIUS degrees of each other.
    Returns list of cluster dicts:
        { lat, lon, count, area, total_energy }
    """
    if not flashes:
        return []

    assigned = [False] * len(flashes)
    clusters = []

    for i, f in enumerate(flashes):
        if assigned[i]:
            continue
        # Start a new cluster at this flash
        members = [f]
        assigned[i] = True
        for j in range(i + 1, len(flashes)):
            if assigned[j]:
                continue
            g = flashes[j]
            if abs(f['lat'] - g['lat']) > CLUSTER_RADIUS * 2:
                continue  # fast reject
            dist = math.hypot(f['lat'] - g['lat'], f['lon'] - g['lon'])
            if dist <= CLUSTER_RADIUS:
                members.append(g)
                assigned[j] = True

        if len(members) < MIN_FLASHES:
            continue

        c_lat   = sum(m['lat']    for m in members) / len(members)
        c_lon   = sum(m['lon']    for m in members) / len(members)
        t_nrg   = sum(m['energy'] for m in members)
        t_area  = sum(m['area']   for m in members)

        clusters.append({
            'lat':          round(c_lat, 4),
            'lon':          round(c_lon, 4),
            'count':        len(members),
            'area':         round(max(t_area / 1e3, 0.01), 4),  # rough normalize
            'total_energy': round(t_nrg, 2),
        })

    # Sort by count descending
    clusters.sort(key=lambda c: c['count'], reverse=True)
    return clusters


# ── POLL LOOP ─────────────────────────────────────────────────────────────────

def poll_loop():
    """Background thread — polls S3 every POLL_INTERVAL seconds."""
    log.info("GLM poll loop started")
    while True:
        try:
            dt = datetime.now(timezone.utc)
            flashes = None
            used_bucket = None

            for bucket in GOES_BUCKETS:
                key = latest_glm_key(bucket, dt)
                if key is None:
                    log.warning(f"No GLM files found in {bucket}")
                    continue

                # Parse file timestamp from key name (sYYYYDOYHHMMSS)
                fname = key.split('/')[-1]
                # e.g. OR_GLM-L2-LCFA_G19_s20260670000000_e...nc
                try:
                    s_part = [p for p in fname.split('_') if p.startswith('s')][0]
                    year   = int(s_part[1:5])
                    doy    = int(s_part[5:8])
                    hour   = int(s_part[8:10])
                    minute = int(s_part[10:12])
                    second = int(s_part[12:14])
                    file_dt = datetime(year, 1, 1, hour, minute, second,
                                       tzinfo=timezone.utc) + timedelta(days=doy - 1)
                    age = (dt - file_dt).total_seconds()
                    if age > MAX_AGE_SEC:
                        log.info(f"Most recent file is {age:.0f}s old — skipping")
                        continue
                except Exception:
                    file_dt = dt  # fallback

                flashes = parse_glm_file(bucket, key)
                used_bucket = bucket
                break

            if flashes is not None:
                clusters = cluster_flashes(flashes)
                with _lock:
                    _state['clusters']    = clusters
                    _state['file_time']   = file_dt
                    _state['fetch_time']  = dt
                    _state['source']      = used_bucket
                    _state['file_count'] += 1
                    _state['flash_count'] += len(flashes)
                    _state['error']       = None
                log.info(f"Updated: {len(clusters)} clusters from {len(flashes)} flashes")
            else:
                with _lock:
                    _state['error'] = 'No recent GLM file found'

        except Exception as e:
            log.error(f"Poll error: {e}", exc_info=True)
            with _lock:
                _state['error'] = str(e)

        time.sleep(POLL_INTERVAL)


# ── FLASK API ─────────────────────────────────────────────────────────────────

app = Flask(__name__)
CORS(app)  # Allow requests from file:// and any localhost origin


@app.route('/glm')
def glm_endpoint():
    """
    Returns current GLM cluster data as GeoJSON FeatureCollection.

    Each Feature:
        geometry.type        = "Point"
        geometry.coordinates = [lon, lat]
        properties:
            count        — flash count in this cluster
            area         — normalized area proxy
            total_energy — sum of flash radiant energy (J)
    """
    with _lock:
        clusters   = list(_state['clusters'])
        file_time  = _state['file_time']
        fetch_time = _state['fetch_time']
        source     = _state['source']
        error      = _state['error']

    features = []
    for c in clusters:
        features.append({
            'type': 'Feature',
            'geometry': {
                'type':        'Point',
                'coordinates': [c['lon'], c['lat']],
            },
            'properties': {
                'count':        c['count'],
                'area':         c['area'],
                'total_energy': c.get('total_energy', 0),
            },
        })

    resp = {
        'type':     'FeatureCollection',
        'features': features,
        'meta': {
            'source':      source,
            'file_time':   file_time.isoformat()  if file_time  else None,
            'fetch_time':  fetch_time.isoformat() if fetch_time else None,
            'flash_total': _state['flash_count'],
            'file_total':  _state['file_count'],
            'error':       error,
        },
    }
    return jsonify(resp)


@app.route('/status')
def status():
    """Quick health check — shows current state."""
    with _lock:
        s = dict(_state)
        s['clusters'] = len(s['clusters'])
        if s['file_time']:  s['file_time']  = s['file_time'].isoformat()
        if s['fetch_time']: s['fetch_time'] = s['fetch_time'].isoformat()
    return jsonify(s)


@app.route('/')
def root():
    return (
        '<h2>GLM Ingest Service — MWN</h2>'
        '<p><a href="/glm">/glm</a> — GeoJSON flash clusters</p>'
        '<p><a href="/status">/status</a> — service health</p>'
    )


# ── ENTRY POINT ───────────────────────────────────────────────────────────────
# When running locally (python app.py), start the poll thread here.
# Under gunicorn, the thread is started by post_fork in gunicorn.conf.py
# because threads don't survive gunicorn's fork() into worker processes.

if __name__ == '__main__':
    _poll_thread = threading.Thread(target=poll_loop, daemon=True)
    _poll_thread.start()
    log.info(f"GLM poll thread started (PORT={PORT})")
    log.info(f"GLM Ingest Service starting on 0.0.0.0:{PORT}")
    log.info(f"Watching buckets: {GOES_BUCKETS}")
    log.info(f"CONUS bounds: {CONUS_BOUNDS}")
    app.run(host='0.0.0.0', port=PORT, debug=False, use_reloader=False)
