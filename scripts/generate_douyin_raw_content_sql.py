#!/usr/bin/env python3
"""Clean Douyin search_contents CSVs and emit a SQL load script.

Outputs:
  - data/douyin_raw_content_clean.csv : deduped/cleaned rows
  - sql/douyin_raw_content.sql        : CREATE TABLE + INSERT statements

The SQL uses INSERT IGNORE on aweme_id so it is idempotent.
"""
from __future__ import annotations

import argparse
import csv
import glob
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = BASE_DIR / "data"
OUT_CSV = BASE_DIR / "data" / "douyin_raw_content_clean.csv"
OUT_SQL = BASE_DIR / "sql" / "douyin_raw_content.sql"


def _clean_text(raw: str) -> str:
    if raw is None:
        return ""
    return " ".join(str(raw).replace("\r", " ").replace("\n", " ").split())


def _to_int(raw: str) -> Optional[int]:
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return None


def _parse_ts(raw: str) -> Optional[str]:
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        ts = float(raw)
    except ValueError:
        return None
    # detect milliseconds
    if ts > 1e12:
        ts = ts / 1000.0
    try:
        return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except (OSError, OverflowError, ValueError):
        return None


def _hash_record(title: str, desc: str, create_time: Optional[str]) -> str:
    key = "|".join([title, desc, create_time or ""])
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def load_records(data_dir: Path = DEFAULT_DATA_DIR) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    seen = set()
    pattern = str(data_dir / "search_contents_*.csv")
    for path in sorted(glob.glob(pattern)):
        csv_path = Path(path)
        with csv_path.open(encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                aweme_id = (row.get("aweme_id") or "").strip()
                title = _clean_text(row.get("title", ""))
                desc = _clean_text(row.get("desc", ""))
                create_time = _parse_ts(row.get("create_time", ""))
                if not aweme_id:
                    aweme_id = _hash_record(title, desc, create_time)
                if aweme_id in seen:
                    continue
                seen.add(aweme_id)

                records.append(
                    {
                        "aweme_id": aweme_id,
                        "aweme_type": _to_int(row.get("aweme_type", "")),
                        "title": title,
                        "content": desc,
                        "create_time": create_time,
                        "user_id": _clean_text(row.get("user_id", "")),
                        "sec_uid": _clean_text(row.get("sec_uid", "")),
                        "short_user_id": _clean_text(row.get("short_user_id", "")),
                        "user_unique_id": _clean_text(row.get("user_unique_id", "")),
                        "user_signature": _clean_text(row.get("user_signature", "")),
                        "nickname": _clean_text(row.get("nickname", "")),
                        "avatar": _clean_text(row.get("avatar", "")),
                        "liked_count": _to_int(row.get("liked_count", "")),
                        "collected_count": _to_int(row.get("collected_count", "")),
                        "comment_count": _to_int(row.get("comment_count", "")),
                        "share_count": _to_int(row.get("share_count", "")),
                        "ip_location": _clean_text(row.get("ip_location", "")),
                        "last_modify_ts": _parse_ts(row.get("last_modify_ts", "")),
                        "aweme_url": _clean_text(row.get("aweme_url", "")),
                        "cover_url": _clean_text(row.get("cover_url", "")),
                        "video_download_url": _clean_text(row.get("video_download_url", "")),
                        "music_download_url": _clean_text(row.get("music_download_url", "")),
                        "note_download_url": _clean_text(row.get("note_download_url", "")),
                        "source_keyword": _clean_text(row.get("source_keyword", "")),
                        "source_file": csv_path.name,
                    }
                )
    return records


def write_clean_csv(records: Iterable[Dict[str, object]], out_csv: Path = OUT_CSV) -> None:
    fieldnames = [
        "aweme_id",
        "aweme_type",
        "title",
        "content",
        "create_time",
        "user_id",
        "sec_uid",
        "short_user_id",
        "user_unique_id",
        "user_signature",
        "nickname",
        "avatar",
        "liked_count",
        "collected_count",
        "comment_count",
        "share_count",
        "ip_location",
        "last_modify_ts",
        "aweme_url",
        "cover_url",
        "video_download_url",
        "music_download_url",
        "note_download_url",
        "source_keyword",
        "source_file",
    ]
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            writer.writerow(rec)


def _sql_escape(val: Optional[object]) -> str:
    if val is None:
        return "NULL"
    if isinstance(val, int):
        return str(val)
    text = str(val)
    text = text.replace("\\", "\\\\").replace("'", "''")
    return f"'{text}'"


def write_sql(
    records: List[Dict[str, object]],
    chunk_size: int = 500,
    out_sql: Path = OUT_SQL,
    table_name: str = "douyin_raw_content",
) -> None:
    out_sql.parent.mkdir(parents=True, exist_ok=True)
    with out_sql.open("w", encoding="utf-8") as f:
        f.write(
            f"""CREATE TABLE IF NOT EXISTS {table_name} (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  aweme_id VARCHAR(64) NOT NULL,
  aweme_type INT NULL,
  title TEXT NULL,
  content LONGTEXT NULL,
  create_time DATETIME NULL,
  user_id VARCHAR(64) NULL,
  sec_uid VARCHAR(128) NULL,
  short_user_id VARCHAR(64) NULL,
  user_unique_id VARCHAR(64) NULL,
  user_signature TEXT NULL,
  nickname VARCHAR(255) NULL,
  avatar TEXT NULL,
  liked_count INT NULL,
  collected_count INT NULL,
  comment_count INT NULL,
  share_count INT NULL,
  ip_location VARCHAR(128) NULL,
  last_modify_ts DATETIME NULL,
  aweme_url TEXT NULL,
  cover_url TEXT NULL,
  video_download_url TEXT NULL,
  music_download_url TEXT NULL,
  note_download_url TEXT NULL,
  source_keyword VARCHAR(255) NULL,
  source_file VARCHAR(255) NULL,
  ingest_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_aweme_id (aweme_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

"""
        )

        cols = [
            "aweme_id",
            "aweme_type",
            "title",
            "content",
            "create_time",
            "user_id",
            "sec_uid",
            "short_user_id",
            "user_unique_id",
            "user_signature",
            "nickname",
            "avatar",
            "liked_count",
            "collected_count",
            "comment_count",
            "share_count",
            "ip_location",
            "last_modify_ts",
            "aweme_url",
            "cover_url",
            "video_download_url",
            "music_download_url",
            "note_download_url",
            "source_keyword",
            "source_file",
            "ingest_ts",
        ]
        for i in range(0, len(records), chunk_size):
            chunk = records[i : i + chunk_size]
            f.write(
                f"INSERT IGNORE INTO {table_name}\n  ("
                + ", ".join(cols)
                + ")\nVALUES\n"
            )
            values_sql = []
            for rec in chunk:
                values = []
                for col in cols:
                    if col == "ingest_ts":
                        values.append("NOW()")
                    else:
                        values.append(_sql_escape(rec.get(col)))
                values_sql.append("  (" + ", ".join(values) + ")")
            f.write(",\n".join(values_sql))
            f.write(";\n\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate SQL for douyin raw content")
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR), help="Directory containing search_contents_*.csv")
    parser.add_argument("--table-name", default="douyin_raw_content", help="Target table name")
    args = parser.parse_args()

    data_dir = Path(args.data_dir).resolve()
    records = load_records(data_dir=data_dir)
    write_clean_csv(records, OUT_CSV)
    write_sql(records, out_sql=OUT_SQL, table_name=args.table_name)
    print(f"Rows prepared: {len(records)}")
    print(f"Clean CSV : {OUT_CSV}")
    print(f"SQL script: {OUT_SQL}")


if __name__ == "__main__":
    main()
