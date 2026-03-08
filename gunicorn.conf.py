"""Gunicorn config for Render deployment."""
import multiprocessing

bind = "0.0.0.0:8801"
workers = 1          # single worker — poll thread shares state
threads = 2
timeout = 120
accesslog = "-"
errorlog = "-"
loglevel = "info"

def post_fork(server, worker):
    """Start the GLM poll thread after gunicorn forks the worker."""
    from app import poll_loop
    import threading
    t = threading.Thread(target=poll_loop, daemon=True)
    t.start()
