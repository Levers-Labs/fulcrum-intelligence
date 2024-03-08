"""
Gunicorn configuration file
"""

import json
import multiprocessing
import os

use_loglevel = os.getenv("LOG_LEVEL", "info")
accesslog_var = os.getenv("ACCESS_LOG", "-")
use_accesslog = accesslog_var or None
errorlog_var = os.getenv("ERROR_LOG", "-")
use_errorlog = errorlog_var or None
graceful_timeout_str = os.getenv("GRACEFUL_TIMEOUT", "60")
timeout_str = os.getenv("TIMEOUT", "60")
keepalive_str = os.getenv("KEEP_ALIVE", "5")

# Gunicorn config variables
bind = "0.0.0.0:8000"
worker_class = "uvicorn.workers.UvicornWorker"
workers = multiprocessing.cpu_count() * 2 + 1 if multiprocessing.cpu_count() > 0 else 3

max_requests = 1000
max_requests_jitter = 50

log_file = "-"
loglevel = use_loglevel
errorlog = use_errorlog
accesslog = use_accesslog
graceful_timeout = int(graceful_timeout_str)
timeout = int(timeout_str)
keepalive = int(keepalive_str)

# For debugging and testing
log_data = {
    "loglevel": loglevel,
    "workers": workers,
    "bind": bind,
    "graceful_timeout": graceful_timeout,
    "timeout": timeout,
    "keepalive": keepalive,
    "errorlog": errorlog,
    "accesslog": accesslog,
}

print(json.dumps(log_data))
