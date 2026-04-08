#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将 douyin_raw_comments_pre_info_flat 中 product_entities 含 “架子/微波炉/置物架”
的对应实体 task_type 统一填充为“电器收纳”。

仅修改命中的实体；其余字段保持不变。默认落库，可用 --dry-run 只看统计。
"""

import argparse
import json
from typing import Any, Dict, List

import pymysql

TABLE_DEFAULT = "douyin_raw_comments_pre_info_flat"
KEYWORDS = ["架子", "微波炉", "置物架"]
TARGET_TASK = "电器收纳"


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
    """确保每个实体都有 tag 对象，保留已有字段。"""
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


def mark_appliance(tags: List[Dict[str, Any]]) -> bool:
    """为命中关键词的实体赋予 TARGET_TASK。返回是否有变更。"""
    changed = False
    for item in tags:
        ent = str(item.get("entity", ""))
        if any(kw in ent for kw in KEYWORDS):
            if item.get("task_type") != TARGET_TASK:
                item["task_type"] = TARGET_TASK
                changed = True
    return changed


def fetch_rows(conn, table: str, limit: int | None) -> List[tuple]:
    sql = f"""
        SELECT pre_product_id, product_entities, product_entities_tags
        FROM {table}
        WHERE JSON_SEARCH(product_entities, 'one', '%%架子%%') IS NOT NULL
           OR JSON_SEARCH(product_entities, 'one', '%%微波炉%%') IS NOT NULL
           OR JSON_SEARCH(product_entities, 'one', '%%置物架%%') IS NOT NULL
        ORDER BY pre_product_id
    """
    if limit:
        sql += " LIMIT %s"
        params = (limit,)
    else:
        params = ()
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def main():
    ap = argparse.ArgumentParser(description="将含架子/微波炉/置物架的 task_type 填为 电器收纳")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", default="root")
    ap.add_argument("--password", default="")
    ap.add_argument("--database", default="csv_labeling")
    ap.add_argument("--table", default=TABLE_DEFAULT)
    ap.add_argument("--limit", type=int, default=None, help="最多处理多少行（默认不限）")
    ap.add_argument("--dry-run", action="store_true", help="只打印统计，不落库")
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
            changed = mark_appliance(tags)
            after = json.dumps(tags, ensure_ascii=False, sort_keys=True)
            if not changed or before == after:
                continue
            updated_rows += 1
            updated_entities += sum(1 for t in tags if t.get("task_type") == TARGET_TASK and any(kw in t.get("entity", "") for kw in KEYWORDS))
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
