from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

@dataclass
class MySQLConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    charset: str = "utf8mb4"

def make_engine(cfg: Dict[str, Any]) -> Engine:
    mc = MySQLConfig(
        host=cfg["host"],
        port=int(cfg.get("port", 3306)),
        user=cfg["user"],
        password=str(cfg.get("password", "")),
        database=cfg["database"],
        charset=cfg.get("charset", "utf8mb4"),
    )
    # sqlalchemy + pymysql
    url = f"mysql+pymysql://{mc.user}:{mc.password}@{mc.host}:{mc.port}/{mc.database}?charset={mc.charset}"
    engine = create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=3600,
        future=True,
    )
    return engine

def healthcheck(engine: Engine) -> None:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
