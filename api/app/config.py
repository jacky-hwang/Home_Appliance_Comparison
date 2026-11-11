import os

from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine

DATABASE_URL = URL.create(
    "mysql+pymysql",
    username=os.environ.get("MYSQL_USER", "tibame"),
    password=os.environ.get("MYSQL_PASSWORD", ""),
    host=os.environ.get("DB_HOST", os.environ.get("MYSQL_HOST", "127.0.0.1")),
    port=int(os.environ.get("DB_PORT", os.environ.get("MYSQL_PORT", "3307"))),
    database=os.environ.get("MYSQL_DATABASE", "tibame"),
)

engine: Engine = create_engine(DATABASE_URL, pool_pre_ping=True)
