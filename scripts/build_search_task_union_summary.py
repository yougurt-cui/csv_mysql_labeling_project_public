#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Merge three flattened tables, dedupe, and group by search_words + task_type to a summary table.

- Input tables default to:
  douyin_raw_comments_last_info_flat_temp
  douyin_raw_comments_pre_info_flat
  douyin_raw_comments_pre_info_flat_temp
- Output table default: douyin_search_task_union_summary

The task_type is taken from product_entities_tags[0].task_type; missing values fall back to ''.
"""

import argparse
from typing import Iterable

import pymysql

DEFAULT_TABLES = [
    "douyin_raw_comments_last_info_flat_temp",
    "douyin_raw_comments_pre_info_flat",
    "douyin_raw_comments_pre_info_flat_temp",
]
DEFAULT_OUT = "douyin_search_task_union_summary"


def quote_ident(name: str) -> str:
    """Naive identifier quoting to tolerate custom table names."""
    return f"`{name.replace('`', '``')}`"


def build_insert_sql(input_tables: Iterable[str], out_table: str) -> str:
    unions = []
    for tbl in input_tables:
        unions.append(
            f"""
        SELECT search_words,
               COALESCE(JSON_UNQUOTE(JSON_EXTRACT(product_entities_tags, '$[0].task_type')), '') AS task_type,
               post_content,
               purchase_intent
        FROM {quote_ident(tbl)}
        """
        )
    union_sql = "\n        UNION ALL\n".join(unions)
    return f"""
INSERT INTO {quote_ident(out_table)} (search_words, task_type, total_count, distinct_post_content, purchase_intent_cnt)
SELECT search_words, task_type,
       COUNT(*) AS total_count,
       COUNT(DISTINCT post_content) AS distinct_post_content,
       SUM(purchase_intent = '明确购买意愿') AS purchase_intent_cnt
FROM (
    SELECT DISTINCT search_words, task_type, post_content, purchase_intent
    FROM (
        {union_sql}
    ) AS all_rows
) AS dedup
GROUP BY search_words, task_type
"""


def main():
    ap = argparse.ArgumentParser(description="Build grouped summary across three info_flat tables.")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", default="root")
    ap.add_argument("--password", default="")
    ap.add_argument("--database", default="csv_labeling")
    ap.add_argument(
        "--tables",
        nargs="+",
        default=DEFAULT_TABLES,
        help="Input tables to union (default: %(default)s)",
    )
    ap.add_argument(
        "--out-table",
        default=DEFAULT_OUT,
        help="Destination summary table (default: %(default)s)",
    )
    args = ap.parse_args()

    conn = pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        charset="utf8mb4",
        autocommit=True,
    )

    drop_sql = f"DROP TABLE IF EXISTS {quote_ident(args.out_table)}"
    create_sql = f"""
CREATE TABLE {quote_ident(args.out_table)} (
  search_words VARCHAR(255) NOT NULL DEFAULT '',
  task_type VARCHAR(255) NOT NULL DEFAULT '',
  total_count BIGINT NOT NULL,
  distinct_post_content BIGINT NOT NULL,
  purchase_intent_cnt BIGINT NOT NULL,
  PRIMARY KEY (search_words, task_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""
    insert_sql = build_insert_sql(args.tables, args.out_table)

    with conn.cursor() as cur:
        cur.execute(drop_sql)
        cur.execute(create_sql)
        cur.execute(insert_sql)
        cur.execute(f"SELECT COUNT(*) FROM {quote_ident(args.out_table)}")
        total_rows = cur.fetchone()[0]
    conn.close()
    print(f"[done] {args.out_table} rows: {total_rows}")


if __name__ == "__main__":
    main()
