"""
Gunicorn configuration for production deployment on t3.small
Optimized for 2 GB RAM, 2 vCPU
"""
import multiprocessing
import os

# Server socket
bind = "0.0.0.0:8000"
backlog = 2048

# Worker processes
# For t3.small (2 vCPU): Use 2 workers
# Formula: (2 * CPU cores) + 1 = 5, but we limit to 2 for memory
workers = int(os.getenv("GUNICORN_WORKERS", 2))
worker_class = "uvicorn.workers.UvicornWorker"
worker_connections = 100

# Worker lifecycle
max_requests = 1000  # Restart worker after 1000 requests (prevent memory leaks)
max_requests_jitter = 100  # Add randomness to prevent all workers restarting at once
max_worker_memory = 400  # MB - restart worker if memory exceeds this

# Timeouts
timeout = 30  # 30 seconds for request timeout
keepalive = 5  # Keep connections alive for 5 seconds
graceful_timeout = 30  # Wait 30 seconds for workers to finish during shutdown

# Process naming
proc_name = "placement_api"

# Server mechanics
daemon = False  # Don't run as daemon (Docker handles this)
pidfile = None
umask = 0
user = None
group = None
tmp_upload_dir = None

# Logging
accesslog = "-"  # Log to stdout
errorlog = "-"   # Log to stderr
loglevel = os.getenv("LOG_LEVEL", "info")
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# Server hooks
def on_starting(server):
    """Called just before the master process is initialized."""
    server.log.info("Starting Gunicorn server")

def on_reload(server):
    """Called to recycle workers during a reload via SIGHUP."""
    server.log.info("Reloading Gunicorn workers")

def when_ready(server):
    """Called just after the server is started."""
    server.log.info("Gunicorn server is ready. Spawning workers")

def worker_int(worker):
    """Called when a worker receives the SIGINT or SIGQUIT signal."""
    worker.log.info("Worker received INT or QUIT signal")

def worker_abort(worker):
    """Called when a worker is aborted."""
    worker.log.info("Worker received SIGABRT signal")
