import os
import logging
import sys
from flask_caching.backends.rediscache import RedisCache
from celery.schedules import crontab

SUPERSET_ENV = os.getenv("SUPERSET_ENV", "development")
APP_ROOT = os.getenv("SUPERSET_APP_ROOT", "/")
BASEPATH = os.getenv("BASEPATH", "/")
LOG_LEVEL = getattr(logging, os.getenv("SUPERSET_LOG_LEVEL", "INFO").upper(), logging.INFO)
SUPERSET_POSTGRES_USER = os.getenv("SUPERSET_POSTGRES_USER") or "superset"
SUPERSET_POSTGRES_PASSWORD = os.getenv("SUPERSET_POSTGRES_PASSWORD") or "superset"
SUPERSET_POSTGRES_HOST = os.getenv("SUPERSET_POSTGRES_HOST") or "superset_postgres"
SUPERSET_POSTGRES_PORT = os.getenv("SUPERSET_POSTGRES_PORT") or "5432"
SUPERSET_POSTGRES_DB = os.getenv("SUPERSET_POSTGRES_DB") or "superset"
REDIS_HOST = os.getenv("SUPERSET_REDIS_HOST", "redis")
REDIS_PORT = os.getenv("SUPERSET_REDIS_PORT", "6379")
REDIS_CELERY_DB = os.getenv("SUPERSET_REDIS_CELERY_DB", "0")
REDIS_RESULTS_DB = os.getenv("SUPERSET_REDIS_RESULTS_DB", "1")

# FAB Rate limiting: this is a security feature for preventing DDOS attacks. The
# feature is on by default to make Superset secure by default, but you should
# fine tune the limits to your needs. You can read more about the different
# parameters here: https://flask-limiter.readthedocs.io/en/stable/configuration.html
RATELIMIT_ENABLED = SUPERSET_ENV == "production"
RATELIMIT_APPLICATION = "50 per second"
AUTH_RATE_LIMITED = SUPERSET_ENV == "production"
AUTH_RATE_LIMIT = "5 per second"

# # Import all settings from the main config first
# from flask_caching.backends.filesystemcache import FileSystemCache
# from superset_config import *  # noqa: F403

# Superset specific config
ROW_LIMIT = 5000

# # Override caching to use simple in-memory cache instead of Redis
# RESULTS_BACKEND = FileSystemCache("/app/superset_home/sqllab")

# On Redis
RESULTS_BACKEND = RedisCache(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_RESULTS_DB, key_prefix='superset_results_')

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True
# Add endpoints that need to be exempt from CSRF protection
WTF_CSRF_EXEMPT_LIST = []
# A CSRF token that expires in 1 year
WTF_CSRF_TIME_LIMIT = 60 * 60 * 24 * 365

# The SQLAlchemy connection string to your database backend
# This connection defines the path to the database that stores your
# superset metadata (slices, connections, tables, dashboards, ...).
# Note that the connection information to connect to the datasources
# you want to explore are managed directly in the web UI
# The check_same_thread=false property ensures the sqlite client does not attempt
# to enforce single-threaded access, which may be problematic in some edge cases
SQLALCHEMY_DATABASE_URI = (
    f"postgresql://"
    f"{SUPERSET_POSTGRES_USER}:{SUPERSET_POSTGRES_PASSWORD}@"
    f"{SUPERSET_POSTGRES_HOST}:{SUPERSET_POSTGRES_PORT}/{SUPERSET_POSTGRES_DB}"
)
SQLALCHEMY_EXAMPLES_URI = None

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": REDIS_RESULTS_DB,
}
DATA_CACHE_CONFIG = CACHE_CONFIG
THUMBNAIL_CACHE_CONFIG = CACHE_CONFIG


class CeleryConfig:
    broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_CELERY_DB}"
    imports = (
        "superset.sql_lab",
        "superset.tasks.scheduler",
        "superset.tasks.thumbnails",
        "superset.tasks.cache",
    )
    result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_RESULTS_DB}"
    worker_prefetch_multiplier = 1
    task_acks_late = False
    beat_schedule = {
        "reports.scheduler": {
            "task": "reports.scheduler",
            "schedule": crontab(minute="*", hour="*"),
        },
        "reports.prune_log": {
            "task": "reports.prune_log",
            "schedule": crontab(minute=10, hour=0),
        },
    }


CELERY_CONFIG = CeleryConfig

FEATURE_FLAGS = {"ALERT_REPORTS": True}
ALERT_REPORTS_NOTIFICATION_DRY_RUN = True
# When using docker compose baseurl should be http://superset_nginx{ENV{BASEPATH}}/  # noqa: E501
WEBDRIVER_BASEURL = f"http://superset_app{APP_ROOT}/"  
# The base URL for the email report hyperlinks.
WEBDRIVER_BASEURL_USER_FRIENDLY = f"http://localhost:8888/{BASEPATH}/"
SQLLAB_CTAS_NO_LIMIT = True


# Flask App Builder configuration
# Your App secret key will be used for securely signing the session cookie
# and encrypting sensitive information on the database
# Make sure you are changing this key for your deployment with a strong key.
# Alternatively you can set it with `SUPERSET_SECRET_KEY` environment variable.
# You MUST set this for production environments or the server will refuse
# to start and you will see an error in the logs accordingly.
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY") or "YOUR_OWN_RANDOM_GENERATED_SECRET_KEY"