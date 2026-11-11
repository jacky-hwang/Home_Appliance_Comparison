import os
from pathlib import Path

WORKER_ACCOUNT = os.environ.get("WORKER_ACCOUNT", "worker")
WORKER_PASSWORD = os.environ.get("WORKER_PASSWORD", "worker")
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "127.0.0.1")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", 5673))
MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", 3307))
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "tibame")
MYSQL_USER = os.environ.get("MYSQL_USER", "tibame")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "local_dev_password")
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CSV_OUTPUT_DIR = Path(os.environ.get("CSV_OUTPUT_DIR", "data"))
if not CSV_OUTPUT_DIR.is_absolute():
    CSV_OUTPUT_DIR = PROJECT_ROOT / CSV_OUTPUT_DIR
