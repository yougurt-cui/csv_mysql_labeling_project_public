from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from rich.console import Console
from sqlalchemy import text
from sqlalchemy.engine import Engine

console = Console()

CANONICAL_COLS = [
    "external_id",
    "title",
    "comment_text",
    "author",
    "created_at",
    "like_count",
    "raw_json",
]

def _apply_column_map(df: pd.DataFrame, colmap: Dict[str, str]) -> pd.DataFrame:
    out = pd.DataFrame()
    for c in CANONICAL_COLS:
        src = (colmap or {}).get(c, "")
        if src and src in df.columns:
            out[c] = df[src]
        else:
            out[c] = None

    # comment_text is mandatory
    if out["comment_text"].isna().all():
        raise ValueError("Mapped `comment_text` is empty. Check config/column_map.yaml")
    return out

def ingest_csv(engine: Engine, csv_path: Path, source_name: str, encoding: str = "utf-8", chunksize: int = 2000) -> str:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    ingest_batch_id = uuid.uuid4().hex[:12]
    console.print(f"[cyan]Ingest batch:[/cyan] {ingest_batch_id}")

    # read once (simple). For very large CSV, you can switch to iterator chunks.
    df = pd.read_csv(csv_path, encoding=encoding)
    console.print(f"[cyan]CSV rows:[/cyan] {len(df)}")

    # Column mapping is handled in caller (CLI). This function expects df already canonicalized.
    return ingest_batch_id

def write_raw_comments(engine: Engine, df_canonical: pd.DataFrame, source_name: str, ingest_batch_id: str) -> int:
    df = df_canonical.copy()
    df["source_name"] = source_name
    df["ingest_batch_id"] = ingest_batch_id
    df["label_status"] = "PENDING"

    # normalize types
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    # Insert with ON DUPLICATE KEY for idempotency
    sql = """
    INSERT INTO raw_comments(
      source_name, external_id, title, comment_text, author, created_at, like_count, raw_json,
      ingest_batch_id, label_status
    )
    VALUES(
      :source_name, :external_id, :title, :comment_text, :author, :created_at, :like_count, :raw_json,
      :ingest_batch_id, :label_status
    )
    ON DUPLICATE KEY UPDATE
      title=VALUES(title),
      comment_text=VALUES(comment_text),
      author=VALUES(author),
      created_at=VALUES(created_at),
      like_count=VALUES(like_count),
      raw_json=VALUES(raw_json),
      ingest_batch_id=VALUES(ingest_batch_id),
      label_status=IF(label_status='DONE', 'DONE', 'PENDING');
    """

    rows = df.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(text(sql), rows)
    return len(rows)

def canonicalize_csv(csv_path: Path, colmap: Dict[str, str], encoding: str = "utf-8") -> pd.DataFrame:
    df = pd.read_csv(csv_path, encoding=encoding)
    return _apply_column_map(df, colmap)
