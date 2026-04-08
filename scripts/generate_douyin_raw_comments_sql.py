#!/usr/bin/env python3
"""Clean Douyin search_comments CSVs and emit a SQL load script.

Outputs:
  - data/douyin_raw_comments_clean.csv : deduped/cleaned rows
  - sql/douyin_raw_comments.sql        : CREATE TABLE + INSERT statements

The SQL uses INSERT IGNORE with a unique hash so it is idempotent.
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
OUT_CSV = BASE_DIR / "data" / "douyin_raw_comments_clean.csv"
OUT_SQL = BASE_DIR / "sql" / "douyin_raw_comments.sql"


def _parse_date(raw: str) -> Optional[str]:
    raw = (raw or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _clean_text(raw: str) -> str:
    if raw is None:
        return ""
    # Collapse newlines to spaces to keep single-line SQL literals.
    return " ".join(str(raw).replace("\r", " ").replace("\n", " ").split())


def _hash_record(title: str, comment: str, comment_date: Optional[str], keyword: str, comment_ip: str) -> str:
    parts = [title, comment, comment_date or "", keyword, comment_ip]
    key = "|".join(parts)
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def load_records(data_dir: Path = DEFAULT_DATA_DIR) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    seen = set()
    pattern = str(data_dir / "search_comments_*.csv")
    for path in sorted(glob.glob(pattern)):
        csv_path = Path(path)
        with csv_path.open(encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                comment = _clean_text(row.get("评论", ""))
                if not comment or comment.lower() == "none":
                    continue

                title = _clean_text(row.get("标题", ""))
                content = _clean_text(row.get("内容", ""))
                like_raw = (row.get("帖子点赞量") or "").strip()
                try:
                    post_like_count = int(like_raw)
                except ValueError:
                    post_like_count = None

                comment_date = _parse_date(row.get("评论时间", ""))
                keyword = _clean_text(row.get("检索词", ""))
                comment_ip = _clean_text(row.get("评论IP地址", ""))

                external_id = _hash_record(title, comment, comment_date, keyword, comment_ip)
                if external_id in seen:
                    continue
                seen.add(external_id)

                records.append(
                    {
                        "external_id": external_id,
                        "post_title": title,
                        "post_content": content,
                        "post_like_count": post_like_count,
                        "comment_text": comment,
                        "comment_date": comment_date,
                        "search_keyword": keyword,
                        "comment_ip": comment_ip,
                        "source_file": csv_path.name,
                    }
                )
    return records


def write_clean_csv(records: Iterable[Dict[str, object]], out_csv: Path = OUT_CSV) -> None:
    fieldnames = [
        "external_id",
        "post_title",
        "post_content",
        "post_like_count",
        "comment_text",
        "comment_date",
        "search_keyword",
        "comment_ip",
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
    chunk_size: int = 1000,
    out_sql: Path = OUT_SQL,
    table_name: str = "douyin_raw_comments",
) -> None:
    out_sql.parent.mkdir(parents=True, exist_ok=True)
    with out_sql.open("w", encoding="utf-8") as f:
        f.write(
            f"""CREATE TABLE IF NOT EXISTS {table_name} (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  external_id CHAR(32) NOT NULL,
  post_title TEXT NULL,
  post_content LONGTEXT NULL,
  post_like_count INT NULL,
  comment_text LONGTEXT NOT NULL,
  comment_date DATE NULL,
  search_keyword VARCHAR(255) NULL,
  comment_ip VARCHAR(64) NULL,
  source_file VARCHAR(255) NULL,
  ingest_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_external_id (external_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

"""
        )

        cols = [
            "external_id",
            "post_title",
            "post_content",
            "post_like_count",
            "comment_text",
            "comment_date",
            "search_keyword",
            "comment_ip",
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
    parser = argparse.ArgumentParser(description="Generate SQL for douyin raw comments")
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR), help="Directory containing search_comments_*.csv")
    parser.add_argument("--table-name", default="douyin_raw_comments", help="Target table name")
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
