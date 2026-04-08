#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
清洗规则：
- 仅针对 product_entities_tags 中 task_type == "锅具收纳" 的实体。
- 若该实体的原始名称不包含“架”或“收纳”任一关键词，则清空其 task_type（置为 ""）。
- 含关键词的实体保留 "锅具收纳"。

使用：
  python3 scripts/enforce_pot_storage_task.py        # 落库
  python3 scripts/enforce_pot_storage_task.py --dry-run  # 只看统计
可加 --limit 控制处理行数。
"""

import argparse
import json
from typing import Any, Dict, List

import pymysql

TABLE_DEFAULT = "douyin_raw_comments_pre_info_flat"
TARGET_TASK = "锅具收纳"
ALLOW_KEYWORDS = ["架", "收纳"]


def parse_tags(raw) -> List[Dict[str, Any]]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    return data if isinstance(data, list) else []


def fetch_rows(conn, table: str, limit: int | None) -> List[tuple]:
    sql = f"""
        SELECT pre_product_id, product_entities_tags
        FROM {table}
        WHERE JSON_SEARCH(product_entities_tags, 'one', '%%{TARGET_TASK}%%') IS NOT NULL
        ORDER BY pre_product_id
    """
    params = ()
    if limit:
        sql += " LIMIT %s"
        params = (limit,)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def should_keep(entity: str) -> bool:
    return any(kw in entity for kw in ALLOW_KEYWORDS)


def clean_tags(tags: List[Dict[str, Any]]) -> bool:
    changed = False
    for item in tags:
        if item.get("task_type") == TARGET_TASK:
            ent = str(item.get("entity", ""))
            if not should_keep(ent):
                item["task_type"] = ""
                changed = True
    return changed


def main():
    ap = argparse.ArgumentParser(description="限定锅具收纳仅在含“架/收纳”实体上保留")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", default="root")
    ap.add_argument("--password", default="")
    ap.add_argument("--database", default="csv_labeling")
    ap.add_argument("--table", default=TABLE_DEFAULT)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        charset="utf8mb4",
        autocommit=False,
    )

    try:
        rows = fetch_rows(conn, args.table, args.limit)
        total = len(rows)
        updated_rows = 0
        cleared_entities = 0

        for pre_product_id, tags_raw in rows:
            tags = parse_tags(tags_raw)
            before = json.dumps(tags, ensure_ascii=False, sort_keys=True)
            if not tags:
                continue
            changed = clean_tags(tags)
            after = json.dumps(tags, ensure_ascii=False, sort_keys=True)
            if not changed or before == after:
                continue
            updated_rows += 1
            cleared_entities += sum(1 for t in tags if t.get("task_type") == "" and TARGET_TASK in before)
            if args.dry_run:
                continue
            payload = json.dumps(tags, ensure_ascii=False)
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {args.table} SET product_entities_tags=%s WHERE pre_product_id=%s",
                    (payload, pre_product_id),
                )

        if not args.dry_run:
            conn.commit()
        print(f"rows_scanned={total}, rows_updated={updated_rows}, entities_cleared={cleared_entities}, dry_run={args.dry_run}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
