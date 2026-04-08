#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将表中 product_entities 含以下关键词的实体更新 task_type：
- “拉篮”/“沥水” -> 碗碟收纳
- “洗碗机” -> 洗碗机收纳
- “架子”/“微波炉”/“置物架” -> 电器收纳

仅修改命中的实体；其他字段保持不变。支持 --dry-run / --limit。
"""

import argparse
import json
from typing import Any, Dict, List

import pymysql

TABLE_DEFAULT = "douyin_raw_comments_pre_info_flat"

RULES = [
    (["拉篮", "沥水"], "碗碟收纳"),
    (["洗碗机"], "洗碗机收纳"),
    (["架子", "微波炉", "置物架"], "电器收纳"),
]


def parse_entities(raw) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return [str(x) for x in raw if str(x).strip()]
    try:
        arr = json.loads(raw)
    except Exception:
        return []
    if isinstance(arr, list):
        return [str(x) for x in arr if str(x).strip()]
    return []


def parse_tags(raw) -> List[Dict[str, Any]]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    return data if isinstance(data, list) else []


def ensure_tags(entities: List[str], existing: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_entity = {str(item.get("entity", "")): item for item in existing}
    tags: List[Dict[str, Any]] = []
    for ent in entities:
        ent = (ent or "").strip()
        if not ent:
            continue
        if ent in by_entity:
            tags.append(by_entity[ent])
        else:
            tags.append({"entity": ent, "sku_or_accessory": "", "task_type": ""})
    return tags


def mark(tags: List[Dict[str, Any]]) -> bool:
    changed = False
    for item in tags:
        ent = str(item.get("entity", ""))
        for keywords, target in RULES:
            if any(kw in ent for kw in keywords):
                if item.get("task_type") != target:
                    item["task_type"] = target
                    changed = True
                break
    return changed


def fetch_rows(conn, table: str, limit: int | None) -> List[tuple]:
    conds = []
    for keywords, _ in RULES:
        for kw in keywords:
            conds.append(f"JSON_SEARCH(product_entities, 'one', '%%{kw}%%') IS NOT NULL")
    where_sql = " OR ".join(conds)
    sql = f"""
        SELECT pre_product_id, product_entities, product_entities_tags
        FROM {table}
        WHERE {where_sql}
        ORDER BY pre_product_id
    """
    params = ()
    if limit:
        sql += " LIMIT %s"
        params = (limit,)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def main():
    ap = argparse.ArgumentParser(description="按关键词规则批量修正 task_type")
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
        updated_entities = 0

        for pre_product_id, ents_raw, tags_raw in rows:
            entities = parse_entities(ents_raw)
            tags = ensure_tags(entities, parse_tags(tags_raw))
            before = json.dumps(tags, ensure_ascii=False, sort_keys=True)
            changed = mark(tags)
            after = json.dumps(tags, ensure_ascii=False, sort_keys=True)
            if not changed or before == after:
                continue
            updated_rows += 1
            updated_entities += sum(1 for t in tags if any(kw in t.get("entity", "") for kw in KEYWORDS) and t.get("task_type") == TARGET_TASK)
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
        print(f"rows_scanned={total}, rows_updated={updated_rows}, entities_marked={updated_entities}, dry_run={args.dry_run}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
