#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
给 douyin_raw_comments_pre_info_flat 表的 product_entities 打两类标签：
1) sku_or_accessory: "SKU" 或 "配件"（根据是否为配件/辅材进行简单规则判断）
2) task_type: 厨房收纳任务枚举之一（锅具收纳、调料收纳、菜刀收纳、菜板收纳、碗碟收纳、食品收纳、菜品收纳），匹配不到则为空字符串

只处理 product_entities 长度为 1 或 2 的行，结果写入新列 product_entities_tags(JSON)。
"""

import argparse
import json
from typing import Any, List, Dict

import pymysql


# ---- 规则配置 ----
ACCESSORY_KEYWORDS = [
    "配件",
    "支架",
    "托盘",
    "滑轨",
    "导轨",
    "轨道",
    "挂钩",
    "粘钩",
    "胶",
    "免钉",
    "纳米胶",
    "双面胶",
    "胶条",
    "螺丝",
    "螺杆",
    "螺钉",
    "螺母",
    "膨胀管",
    "角码",
    "卡扣",
    "贴纸",
    "贴片",
    "封边",
    "材料包",
    "挡板",
    "固定件",
    "垫片",
    "防撞条",
]

TASK_RULES = [
    ("菜刀收纳", ["菜刀", "刀具", "刀架", "刀座", "刀筷"]),
    ("菜板收纳", ["菜板", "砧板", "案板", "切菜板", "砧子"]),
    ("锅具收纳", ["锅具", "锅盖", "锅", "汤锅", "炒锅", "蒸锅", "平底锅", "奶锅"]),
    ("碗碟收纳", ["碗", "碗架", "碗篮", "碗柜", "碗盘", "碗碟", "盘", "盘子", "碟"]),
    ("调料收纳", ["调料", "调味", "佐料", "酱油", "生抽", "老抽", "醋", "料酒", "香料", "味精", "鸡精", "胡椒", "辣椒", "花椒", "孜然", "油壶", "油瓶", "喷油", "调料瓶", "调料罐", "调料盒", "调料架", "盐罐", "酱料", "油罐", "食用油", "橄榄油"]),
    ("菜品收纳", ["熟食", "熟菜", "剩菜", "剩饭", "菜品", "菜肴", "饭菜", "菜盘", "熟食盘", "饭盒", "便当"]),
    ("食品收纳", ["食材", "食品", "蔬菜", "水果", "生鲜", "肉", "肉类", "鸡蛋", "粮食", "大米", "米", "面粉", "零食", "饼干", "糖果"]),
]


def classify_role(entity: str) -> str:
    e = entity or ""
    if any(kw in e for kw in ACCESSORY_KEYWORDS):
        return "配件"
    return "SKU"


def classify_task(entity: str) -> str:
    e = entity or ""
    for task, keywords in TASK_RULES:
        if any(kw in e for kw in keywords):
            return task
    return ""


def tag_entities(entities: List[str]) -> List[Dict[str, Any]]:
    tagged = []
    for ent in entities:
        ent = (ent or "").strip()
        if not ent:
            continue
        tagged.append(
            {
                "entity": ent,
                "sku_or_accessory": classify_role(ent),
                "task_type": classify_task(ent),
            }
        )
    return tagged


def ensure_tag_column(conn, table: str) -> None:
    """如果没有 product_entities_tags 列则新增。"""
    with conn.cursor() as cur:
        cur.execute(f"SHOW COLUMNS FROM {table} LIKE 'product_entities_tags'")
        exists = cur.fetchone()
        if exists:
            return
        cur.execute(
            f"ALTER TABLE {table} "
            "ADD COLUMN product_entities_tags JSON NULL"
        )
    conn.commit()


def fetch_rows(conn, table: str, limit: int | None = None, skip_filled: bool = True):
    sql = [
        f"SELECT pre_product_id, product_entities, product_entities_tags",
        f"FROM {table}",
        "WHERE JSON_LENGTH(product_entities) IN (1, 2)",
    ]
    params: List[Any] = []
    if skip_filled:
        sql.append("AND (product_entities_tags IS NULL OR product_entities_tags = 'null')")
    sql.append("ORDER BY pre_product_id")
    if limit:
        sql.append("LIMIT %s")
        params.append(limit)
    full_sql = "\n".join(sql)
    with conn.cursor() as cur:
        cur.execute(full_sql, params)
        return cur.fetchall()


def parse_entities(raw) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return [str(x) for x in raw if str(x).strip()]
    try:
        arr = json.loads(raw)
        if isinstance(arr, list):
            return [str(x) for x in arr if str(x).strip()]
    except Exception:
        return []
    return []


def update_tags(conn, table: str, pre_product_id: int, tags: List[Dict[str, Any]]) -> None:
    payload = json.dumps(tags, ensure_ascii=False)
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {table} SET product_entities_tags=%s WHERE pre_product_id=%s",
            (payload, pre_product_id),
        )


def main():
    parser = argparse.ArgumentParser(description="为 product_entities 打标签")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--database", default="csv_labeling")
    parser.add_argument("--table", default="douyin_raw_comments_pre_info_flat")
    parser.add_argument("--limit", type=int, default=None, help="最多处理多少行（默认全部）")
    parser.add_argument("--force", action="store_true", help="即便 product_entities_tags 已有值也重写")
    parser.add_argument("--dry-run", action="store_true", help="只打印统计不落库")
    args = parser.parse_args()

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
        ensure_tag_column(conn, args.table)
        rows = fetch_rows(conn, args.table, limit=args.limit, skip_filled=not args.force)
        total = len(rows)
        updated = 0
        for pre_product_id, entities_raw, existing_tags in rows:
            entities = parse_entities(entities_raw)
            tags = tag_entities(entities)
            if not tags:
                continue
            updated += 1
            if args.dry_run:
                continue
            update_tags(conn, args.table, pre_product_id, tags)
        if not args.dry_run:
            conn.commit()
        print(f"found rows: {total}, updated: {updated}, dry_run={args.dry_run}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
