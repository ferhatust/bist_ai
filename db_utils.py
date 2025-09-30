import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

USER = os.getenv("MYSQL_USER")
PASS = os.getenv("MYSQL_PASS")
HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
PORT = os.getenv("MYSQL_PORT", "3306")
DB   = os.getenv("MYSQL_DB")

URL = f"mysql+pymysql://{USER}:{PASS}@{HOST}:{PORT}/{DB}?charset=utf8mb4"
engine = create_engine(URL, pool_size=10, max_overflow=20, future=True)

def execute(sql, params=None):
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})
