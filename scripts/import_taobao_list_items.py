#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Import taobao detail_items_raw_*.json files into MySQL table taobao_catfood_list_items.

Handles noisy files where:
1) terminal logs / ANSI escape sequences are mixed into file content;
2) JSON payload is wrapped as a quoted string.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, urlparse

import pymysql

try:
    import yaml
except Exception:
    yaml = None

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.db import make_engine
from src.ingest_taobao import cleanup_taobao_table


DEFAULT_MYSQL = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "csv_labeling",
    "charset": "utf8mb4",
}

ANSI_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
TABLE_RE = re.compile(r"^[A-Za-z0-9_]+$")
FILE_RE = re.compile(r"^(?:list_items_raw|detail_items_raw)_(.+)_(\d{8}_\d{6})\.json$")


def load_mysql_defaults() -> Dict[str, Any]:
    merged = dict(DEFAULT_MYSQL)
    if yaml is None:
        return merged

    base_dir = Path(__file__).resolve().parents[1]
    candidates = [
        base_dir / "config" / "config.yaml",
        base_dir / "config" / "config.example.yaml",
    ]
    for p in candidates:
        if not p.exists():
            continue
        try:
            with p.open("r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            mysql_cfg = cfg.get("mysql") or {}
            if isinstance(mysql_cfg, dict):
                for k in ("host", "port", "user", "password", "database", "charset"):
                    v = mysql_cfg.get(k)
                    if v not in (None, ""):
                        merged[k] = v
                return merged
        except Exception:
            continue
    return merged


def parse_args() -> argparse.Namespace:
    mysql_defaults = load_mysql_defaults()
    parser = argparse.ArgumentParser(description="Import taobao detail_items_raw json files into MySQL")
    parser.add_argument("--host", default=mysql_defaults["host"])
    parser.add_argument("--port", type=int, default=int(mysql_defaults["port"]))
    parser.add_argument("--user", default=mysql_defaults["user"])
    parser.add_argument("--password", default=mysql_defaults["password"])
    parser.add_argument("--database", default=mysql_defaults["database"])
    parser.add_argument("--charset", default=mysql_defaults.get("charset", "utf8mb4"))
    parser.add_argument(
        "--input-dir",
        default=os.getenv("TAOBAO_INPUT_DIR", str(BASE_DIR / "data" / "taobao")),
    )
    parser.add_argument("--pattern", default="detail_items_raw_*.json")
    parser.add_argument("--table", default="taobao_catfood_list_items")
    parser.add_argument("--mode", choices=["insert", "upsert"], default="upsert")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def ensure_table_name(table: str) -> str:
    if not TABLE_RE.match(table):
        raise ValueError(f"invalid table name: {table}")
    return table


def strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text)


def decode_json_maybe_nested(obj: Any) -> Any:
    out = obj
    for _ in range(3):
        if isinstance(out, str):
            s = out.strip()
            if not s:
                break
            try:
                out = json.loads(s)
            except Exception:
                break
        else:
            break
    return out


def extract_json_payload(raw_text: str) -> Any:
    cleaned = strip_ansi(raw_text)
    decoder = json.JSONDecoder()

    direct = cleaned.strip()
    if direct:
        try:
            return decode_json_maybe_nested(json.loads(direct))
        except Exception:
            pass

    for match in re.finditer(r'[{"\[]', cleaned):
        fragment = cleaned[match.start() :].lstrip()
        try:
            obj, _ = decoder.raw_decode(fragment)
        except Exception:
            continue
        obj = decode_json_maybe_nested(obj)
        if isinstance(obj, (dict, list)):
            return obj

    raise ValueError("no valid JSON payload found")


def load_items(path: Path) -> List[Dict[str, Any]]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    payload = extract_json_payload(text)

    if isinstance(payload, dict):
        items = payload.get("items")
        if isinstance(items, list):
            return [x for x in items if isinstance(x, dict)]
        if "title" in payload and "url" in payload:
            return [payload]
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    raise ValueError(f"unrecognized payload shape in {path}")


def parse_file_meta(path: Path) -> Tuple[str, Optional[datetime]]:
    match = FILE_RE.match(path.name)
    if not match:
        return "unknown", None
    keyword = match.group(1)
    ts_str = match.group(2)
    try:
        crawl_ts = datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
    except ValueError:
        crawl_ts = None
    return keyword, crawl_ts


def parse_decimal_like(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).replace("¥", "").replace("￥", "").strip()
    s = re.sub(r"\s+", "", s)
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return m.group(1) if m else None


def parse_pay_count(title: str) -> Tuple[Optional[str], Optional[int]]:
    m = re.search(r"(\d+(?:\.\d+)?)(万)?\+?人付款", title or "")
    if not m:
        return None, None
    txt = m.group(0)
    v = float(m.group(1))
    if m.group(2):
        v *= 10000
    return txt, int(v)


def parse_sold_count(value: Any) -> Tuple[Optional[str], Optional[int]]:
    text_value = str(value or "").strip()
    if not text_value:
        return None, None

    m = re.search(r"(已售\s*(\d+(?:\.\d+)?)(万)?\+?)", text_value)
    if not m:
        m = re.search(r"((\d+(?:\.\d+)?)(万)?\+?\s*(?:件已售|人付款|人购买))", text_value)
    if not m:
        m = re.search(r"^((\d+(?:\.\d+)?)(万)?\+?)$", text_value)
    if not m:
        return text_value, None

    v = float(m.group(2))
    if m.group(3):
        v *= 10000
    return text_value, int(v)


def parse_item_id(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    try:
        q = parse_qs(urlparse(url).query)
        ids = q.get("id")
        return ids[0] if ids else None
    except Exception:
        return None


def parse_site(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    host = (urlparse(url).netloc or "").lower()
    if "tmall.com" in host:
        return "tmall"
    if "taobao.com" in host:
        return "taobao"
    return host or None


def normalize_title(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def normalize_text(value: Any) -> Optional[str]:
    text_value = re.sub(r"\s+", " ", str(value or "").strip())
    return text_value or None


def parse_rank_no(item: Dict[str, Any], fallback_rank: int) -> int:
    raw_rank = item.get("index")
    if raw_rank is None:
        return fallback_rank
    try:
        return max(1, int(str(raw_rank).strip()))
    except Exception:
        return fallback_rank


def build_records(path: Path) -> List[Dict[str, Any]]:
    keyword, crawl_ts = parse_file_meta(path)
    items = load_items(path)

    out: List[Dict[str, Any]] = []
    for idx, item in enumerate(items, start=1):
        title = normalize_title(item.get("title"))
        product_url = str(item.get("url") or "").strip() or None
        item_id = parse_item_id(product_url)
        ext_seed = f"{keyword}|{item_id or product_url or title}"
        external_id = hashlib.md5(ext_seed.encode("utf-8")).hexdigest()
        pay_count_text, pay_count = parse_pay_count(title)
        sold_text, sold_count = parse_sold_count(item.get("sold"))
        if pay_count is None and sold_count is not None:
            pay_count_text, pay_count = sold_text, sold_count

        out.append(
            {
                "external_id": external_id,
                "keyword": keyword,
                "crawl_ts": crawl_ts,
                "source_file": path.name,
                "rank_no": parse_rank_no(item, idx),
                "title": title,
                "price": parse_decimal_like(item.get("price")),
                "pay_count_text": pay_count_text,
                "pay_count": pay_count,
                "ship_from": normalize_text(item.get("ship_from")),
                "product_url": product_url,
                "item_id": item_id,
                "site": parse_site(product_url),
                "food_taste": normalize_text(item.get("food_taste")),
                "net_content": normalize_text(item.get("net_content")),
                "sold_text": sold_text,
                "sold_count": sold_count,
                "raw_json": json.dumps(item, ensure_ascii=False),
            }
        )
    return out


def chunked(seq: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def create_table(cursor, table: str) -> None:
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{table}` (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          external_id CHAR(32) NOT NULL,
          keyword VARCHAR(255) NOT NULL,
          crawl_ts DATETIME NULL,
          source_file VARCHAR(255) NOT NULL,
          rank_no INT NOT NULL,
          title TEXT NULL,
          price DECIMAL(10,2) NULL,
          pay_count_text VARCHAR(64) NULL,
          pay_count INT NULL,
          ship_from VARCHAR(128) NULL,
          product_url TEXT NULL,
          item_id VARCHAR(64) NULL,
          site VARCHAR(32) NULL,
          food_taste VARCHAR(255) NULL,
          net_content VARCHAR(255) NULL,
          sold_text VARCHAR(64) NULL,
          sold_count INT NULL,
          raw_json LONGTEXT NULL,
          ingest_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          UNIQUE KEY uq_external_id (external_id),
          KEY idx_keyword (keyword),
          KEY idx_crawl_ts (crawl_ts),
          KEY idx_item_id (item_id),
          KEY idx_sold_count (sold_count)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def ensure_columns(cursor, table: str) -> None:
    cursor.execute(f"SHOW COLUMNS FROM `{table}`")
    col_set = {row[0] for row in cursor.fetchall()}
    if "food_taste" not in col_set:
        cursor.execute(f"ALTER TABLE `{table}` ADD COLUMN food_taste VARCHAR(255) NULL AFTER site")
    if "net_content" not in col_set:
        cursor.execute(f"ALTER TABLE `{table}` ADD COLUMN net_content VARCHAR(255) NULL AFTER food_taste")
    if "sold_text" not in col_set:
        cursor.execute(f"ALTER TABLE `{table}` ADD COLUMN sold_text VARCHAR(64) NULL AFTER net_content")
    if "sold_count" not in col_set:
        cursor.execute(f"ALTER TABLE `{table}` ADD COLUMN sold_count INT NULL AFTER sold_text")

    cursor.execute(f"SHOW INDEX FROM `{table}`")
    idx_names = {row[2] for row in cursor.fetchall()}
    if "idx_sold_count" not in idx_names:
        cursor.execute(f"ALTER TABLE `{table}` ADD INDEX idx_sold_count (sold_count)")


def build_sql(table: str, mode: str) -> str:
    cols = [
        "external_id",
        "keyword",
        "crawl_ts",
        "source_file",
        "rank_no",
        "title",
        "price",
        "pay_count_text",
        "pay_count",
        "ship_from",
        "product_url",
        "item_id",
        "site",
        "food_taste",
        "net_content",
        "sold_text",
        "sold_count",
        "raw_json",
    ]
    col_sql = ", ".join(f"`{c}`" for c in cols)
    val_sql = ", ".join(["%s"] * len(cols))
    base = f"INSERT INTO `{table}` ({col_sql}) VALUES ({val_sql})"
    if mode == "upsert":
        update_cols = [c for c in cols if c != "external_id"]
        upd = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in update_cols)
        return f"{base} ON DUPLICATE KEY UPDATE {upd}"
    return base


def main() -> None:
    args = parse_args()
    table = ensure_table_name(args.table)
    input_dir = Path(os.path.expanduser(args.input_dir))
    if not input_dir.exists():
        raise FileNotFoundError(f"input directory not found: {input_dir}")

    files = sorted(input_dir.glob(args.pattern))
    if not files:
        print(f"[INFO] no files matched: {input_dir}/{args.pattern}")
        return

    all_records: List[Dict[str, Any]] = []
    for p in files:
        rows = build_records(p)
        all_records.extend(rows)

    print(f"[INFO] files: {len(files)}")
    print(f"[INFO] rows parsed: {len(all_records)}")
    if all_records:
        print(f"[INFO] sample keyword={all_records[0]['keyword']}, rank={all_records[0]['rank_no']}")

    if args.dry_run:
        print("[DRY-RUN] no database write")
        return

    sql = build_sql(table, args.mode)
    cols = [
        "external_id",
        "keyword",
        "crawl_ts",
        "source_file",
        "rank_no",
        "title",
        "price",
        "pay_count_text",
        "pay_count",
        "ship_from",
        "product_url",
        "item_id",
        "site",
        "food_taste",
        "net_content",
        "sold_text",
        "sold_count",
        "raw_json",
    ]
    conn = pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        charset=args.charset,
    )
    try:
        with conn.cursor() as cur:
            create_table(cur, table)
            ensure_columns(cur, table)
            conn.commit()

            params = [tuple(r.get(c) for c in cols) for r in all_records]
            done = 0
            for part in chunked(params, max(1, args.batch_size)):
                cur.executemany(sql, part)
                conn.commit()
                done += len(part)
                print(f"[INFO] processed: {done}")
    finally:
        conn.close()

    print(f"[DONE] imported: {done}")
    cleanup = cleanup_taobao_table(
        engine=make_engine(
            {
                "host": args.host,
                "port": args.port,
                "user": args.user,
                "password": args.password,
                "database": args.database,
                "charset": args.charset,
            }
        ),
        table_name=table,
    )
    print(
        "[DONE] cleaned: "
        f"kept {cleanup.rows_after}/{cleanup.rows_before}, "
        f"dropped={cleanup.total_dropped}, "
        f"low_pay={cleanup.low_pay_dropped}, dup={cleanup.duplicate_dropped}"
    )
    if cleanup.backup_table:
        print(f"[INFO] cleanup backup table: {cleanup.backup_table}")


if __name__ == "__main__":
    main()
