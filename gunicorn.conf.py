"""Gunicorn config for Render deployment."""
import multiprocessing

bind = "0.0.0.0:8801"
workers = 1          # single worker — both poll threads share state
threads = 2
timeout = 120
accesslog = "-"
errorlog = "-"
loglevel = "info"

def post_fork(server, worker):
    """Start the GLM + STI poll threads after gunicorn forks the worker."""
    from app import start_threads
    start_threads()
