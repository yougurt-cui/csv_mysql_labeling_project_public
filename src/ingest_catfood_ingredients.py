# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
import re
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
from rich.console import Console
from sqlalchemy import text
from sqlalchemy.engine import Engine

console = Console()
TABLE_RE = re.compile(r"^[A-Za-z0-9_]+$")

BRAND_COLS = ["brand", "品牌", "品牌名", "品牌名称"]
PRODUCT_COLS = ["product_name", "商品名", "产品名", "猫粮名", "title", "name", "商品标题"]
INGREDIENT_COLS = [
    "ingredient_text",
    "ingredients",
    "ingredient",
    "原料",
    "原料组成",
    "配料",
    "配方",
    "成分",
    "主要成分",
    "原材料",
    "原料表",
    "配料表",
]
URL_COLS = ["source_url", "url", "链接", "商品链接"]


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    if pd.isna(value):
        return ""
    return str(value).strip()


def _pick_first(row: pd.Series, candidates: Sequence[str]) -> str:
    for col in candidates:
        if col in row.index:
            v = _safe_text(row[col])
            if v:
                return v
    return ""


def _chunked(seq: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def ensure_catfood_ingredient_table(
    engine: Engine,
    table_name: str = "catfood_ingredient_raw",
) -> None:
    if not TABLE_RE.fullmatch(table_name):
        raise ValueError(f"Invalid table name: {table_name}")
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
      id BIGINT PRIMARY KEY AUTO_INCREMENT,
      external_id CHAR(32) NOT NULL,
      brand VARCHAR(255) NULL,
      product_name VARCHAR(512) NULL,
      ingredient_text LONGTEXT NOT NULL,
      source_url TEXT NULL,
      source_file VARCHAR(255) NOT NULL,
      row_no INT NOT NULL,
      raw_json LONGTEXT NULL,
      ingest_batch_id VARCHAR(32) NOT NULL,
      ingest_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uq_external_id (external_id),
      KEY idx_brand (brand),
      KEY idx_product_name (product_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _build_records_from_csv(csv_path: Path) -> List[Dict[str, Any]]:
    last_err: Optional[Exception] = None
    for enc in ("utf-8", "utf-8-sig", "gbk", "gb18030"):
        try:
            df = pd.read_csv(csv_path, dtype=str, encoding=enc)
            break
        except Exception as exc:
            last_err = exc
    else:
        raise ValueError(f"read csv failed: {csv_path}, err={last_err}")

    if df.empty:
        return []

    rows: List[Dict[str, Any]] = []
    for idx, row in df.iterrows():
        brand = _pick_first(row, BRAND_COLS)
        product_name = _pick_first(row, PRODUCT_COLS)
        ingredient_text = _pick_first(row, INGREDIENT_COLS)
        source_url = _pick_first(row, URL_COLS)
        if not ingredient_text:
            continue

        row_no = int(idx) + 1
        dedup_seed = f"{brand}|{product_name}|{ingredient_text}"
        external_id = hashlib.md5(dedup_seed.encode("utf-8")).hexdigest()

        raw_obj = {
            str(k): (None if pd.isna(v) else v)
            for k, v in row.items()
        }
        rows.append(
            {
                "external_id": external_id,
                "brand": brand or None,
                "product_name": product_name or None,
                "ingredient_text": ingredient_text,
                "source_url": source_url or None,
                "source_file": csv_path.name,
                "row_no": row_no,
                "raw_json": json.dumps(raw_obj, ensure_ascii=False),
            }
        )
    return rows


def ingest_catfood_ingredient_dir(
    engine: Engine,
    input_dir: Path,
    file_pattern: str = "*.csv",
    table_name: str = "catfood_ingredient_raw",
    batch_size: int = 500,
) -> Tuple[int, int, str]:
    if not TABLE_RE.fullmatch(table_name):
        raise ValueError(f"Invalid table name: {table_name}")
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    files = sorted([p for p in input_dir.glob(file_pattern) if p.is_file()])
    if not files:
        return 0, 0, ""

    ensure_catfood_ingredient_table(engine, table_name=table_name)
    ingest_batch_id = uuid.uuid4().hex[:12]
    console.print(f"[cyan]Catfood ingredient ingest batch:[/cyan] {ingest_batch_id}")

    all_rows: List[Dict[str, Any]] = []
    for f in files:
        all_rows.extend(_build_records_from_csv(f))

    if not all_rows:
        return len(files), 0, ingest_batch_id

    sql = f"""
    INSERT INTO `{table_name}` (
      external_id, brand, product_name, ingredient_text, source_url,
      source_file, row_no, raw_json, ingest_batch_id
    )
    VALUES (
      :external_id, :brand, :product_name, :ingredient_text, :source_url,
      :source_file, :row_no, :raw_json, :ingest_batch_id
    )
    ON DUPLICATE KEY UPDATE
      brand=VALUES(brand),
      product_name=VALUES(product_name),
      ingredient_text=VALUES(ingredient_text),
      source_url=VALUES(source_url),
      source_file=VALUES(source_file),
      row_no=VALUES(row_no),
      raw_json=VALUES(raw_json),
      ingest_batch_id=VALUES(ingest_batch_id)
    """

    total = 0
    for part in _chunked(all_rows, max(1, int(batch_size))):
        payload = [{**item, "ingest_batch_id": ingest_batch_id} for item in part]
        with engine.begin() as conn:
            conn.execute(text(sql), payload)
        total += len(payload)

    return len(files), total, ingest_batch_id
