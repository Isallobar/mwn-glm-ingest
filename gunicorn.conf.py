# gunicorn.conf.py — MWN GLM Ingest Service
# Starts the S3 poll thread inside each worker process after fork.
# This is necessary because threads don't survive gunicorn's fork().

import threading

def post_fork(server, worker):
    """Called in each worker process after forking from master."""
    from app import poll_loop
    t = threading.Thread(target=poll_loop, daemon=True)
    t.start()
    server.log.info(f"GLM poll thread started in worker {worker.pid}")
