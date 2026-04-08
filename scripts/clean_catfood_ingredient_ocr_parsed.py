#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
根据 image_name 回填 catfood_ingredient_ocr_parsed.brand / product_name。

默认只做 dry-run 预览；传 --apply 才会：
1. 先完整备份原表到一个时间戳备份表
2. 再在同一个事务里更新可安全解析的空值行
"""

from __future__ import annotations

import argparse
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pymysql
import yaml

DEFAULT_TABLE = "catfood_ingredient_ocr_parsed"
TABLE_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")
MULTISPACE_RE = re.compile(r"\s+")
LEADING_SEP_RE = re.compile(r"^[\s_\-:：]+")
TRAILING_PATTERNS = [
    re.compile(r"\s*20\d{2}-\d{2}-\d{2}\s+\d{2}\.\d{2}\.\d{2}$"),
    re.compile(r"\s*\d{2}\.\d{2}\.\d{2}$"),
]

BRAND_ALIASES = [
    ("草本魔力", "草本魔力"),
    ("天衡宝", "天衡宝"),
    ("汤普森", "汤普森"),
    ("纽翠斯", "纽翠斯"),
    ("绿福摩", "绿福摩"),
    ("金素力高", "素力高"),
    ("希尔斯", "希尔斯"),
    ("麦富迪", "麦富迪"),
    ("钻石", "钻石"),
    ("冠能", "冠能"),
    ("甄萃", "甄萃"),
    ("星益", "星益"),
    ("澳龙", "澳龙"),
    ("皇家", "皇家"),
    ("荒野", "荒野"),
    ("巅峰", "巅峰"),
    ("素力高", "素力高"),
    ("帕特", "帕特"),
    ("诺乐", "诺乐"),
    ("恩萃", "恩萃"),
    ("美士", "美士"),
    ("GO!", "go"),
    ("GO", "go"),
    ("go!", "go"),
    ("go", "go"),
]
BRAND_ALIASES.sort(key=lambda item: len(item[0]), reverse=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="根据 image_name 清洗猫粮 OCR 解析表的品牌和产品名")
    parser.add_argument("--config", default="config/config.yaml", help="MySQL 配置文件")
    parser.add_argument("--table", default=DEFAULT_TABLE, help="要更新的表名")
    parser.add_argument("--limit", type=int, default=None, help="仅处理前 N 条候选记录")
    parser.add_argument("--apply", action="store_true", help="真正执行备份和更新；默认仅预览")
    return parser.parse_args()


def load_mysql_config(config_path: str) -> Dict[str, object]:
    data = yaml.safe_load(Path(config_path).read_text(encoding="utf-8")) or {}
    mysql = data.get("mysql") or {}
    if not mysql:
        raise ValueError("config.yaml missing mysql config")
    return {
        "host": mysql.get("host", "127.0.0.1"),
        "port": int(mysql.get("port", 3306)),
        "user": mysql.get("user", "root"),
        "password": mysql.get("password", ""),
        "database": mysql.get("database", ""),
        "charset": mysql.get("charset", "utf8mb4"),
    }


def validate_table_name(table: str) -> str:
    if not TABLE_NAME_RE.match(table):
        raise ValueError("unsafe table name")
    return table


def normalize_stem(image_name: Optional[str]) -> str:
    name = os.path.basename((image_name or "").strip())
    stem, _ = os.path.splitext(name)
    stem = stem.strip()
    for pattern in TRAILING_PATTERNS:
        updated = pattern.sub("", stem).strip()
        if updated != stem:
            stem = updated
    return MULTISPACE_RE.sub(" ", stem).strip()


def extract_brand_product(image_name: Optional[str]) -> Dict[str, Optional[str]]:
    cleaned = normalize_stem(image_name)
    if not cleaned:
        return {"cleaned_image_name": cleaned, "brand": None, "product_name": None}

    lowered = cleaned.casefold()
    for alias, canonical_brand in BRAND_ALIASES:
        if not lowered.startswith(alias.casefold()):
            continue
        remainder = cleaned[len(alias):].strip()
        remainder = LEADING_SEP_RE.sub("", remainder).strip()
        remainder = MULTISPACE_RE.sub(" ", remainder).strip()
        if canonical_brand and remainder:
            return {
                "cleaned_image_name": cleaned,
                "brand": canonical_brand,
                "product_name": remainder,
            }
    return {"cleaned_image_name": cleaned, "brand": None, "product_name": None}


def fetch_candidates(conn: pymysql.connections.Connection, table: str, limit: Optional[int]) -> List[Dict[str, object]]:
    sql = f"""
        SELECT id, source_id, image_name, brand, product_name
        FROM `{table}`
        WHERE image_name IS NOT NULL
          AND TRIM(image_name) <> ''
          AND (
            brand IS NULL OR TRIM(brand) = '' OR
            product_name IS NULL OR TRIM(product_name) = ''
          )
        ORDER BY id
    """
    params: List[object] = []
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def build_preview_rows(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    preview_rows: List[Dict[str, object]] = []
    for row in rows:
        parsed = extract_brand_product(row.get("image_name"))
        preview_rows.append(
            {
                "id": row["id"],
                "source_id": row["source_id"],
                "image_name": row.get("image_name"),
                "cleaned_image_name": parsed["cleaned_image_name"],
                "brand": parsed["brand"],
                "product_name": parsed["product_name"],
            }
        )
    return preview_rows


def create_backup_table(conn: pymysql.connections.Connection, table: str) -> str:
    backup_table = f"{table}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE `{backup_table}` LIKE `{table}`")
        cur.execute(f"INSERT INTO `{backup_table}` SELECT * FROM `{table}`")
    return backup_table


def apply_updates(conn: pymysql.connections.Connection, table: str, resolved_rows: List[Dict[str, object]]) -> int:
    updated = 0
    sql = f"""
        UPDATE `{table}`
        SET brand = %s,
            product_name = %s
        WHERE id = %s
          AND image_name = %s
          AND (brand IS NULL OR TRIM(brand) = '')
          AND (product_name IS NULL OR TRIM(product_name) = '')
    """
    with conn.cursor() as cur:
        for row in resolved_rows:
            cur.execute(
                sql,
                (
                    row["brand"],
                    row["product_name"],
                    row["id"],
                    row["image_name"],
                ),
            )
            updated += cur.rowcount
    return updated


def main() -> None:
    args = parse_args()
    table = validate_table_name(args.table)
    mysql_cfg = load_mysql_config(args.config)

    conn = pymysql.connect(
        host=mysql_cfg["host"],
        port=mysql_cfg["port"],
        user=mysql_cfg["user"],
        password=mysql_cfg["password"],
        database=mysql_cfg["database"],
        charset=mysql_cfg["charset"],
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )

    try:
        candidates = fetch_candidates(conn, table, args.limit)
        preview_rows = build_preview_rows(candidates)
        resolved_rows = [row for row in preview_rows if row["brand"] and row["product_name"]]
        unresolved_rows = [row for row in preview_rows if not row["brand"] or not row["product_name"]]

        print("candidates={0}, resolved={1}, unresolved={2}, apply={3}".format(
            len(candidates), len(resolved_rows), len(unresolved_rows), args.apply
        ))

        for row in preview_rows:
            print(
                "{id}\t{source_id}\t{image_name}\t=>\t{brand}\t{product_name}".format(
                    id=row["id"],
                    source_id=row["source_id"],
                    image_name=row["image_name"],
                    brand=row["brand"] or "<unresolved>",
                    product_name=row["product_name"] or "<unresolved>",
                )
            )

        if not args.apply:
            return

        if unresolved_rows:
            raise RuntimeError("found unresolved rows; aborting without backup or update")

        backup_table = create_backup_table(conn, table)
        updated = apply_updates(conn, table, resolved_rows)
        conn.commit()

        print("backup_table={0}".format(backup_table))
        print("rows_updated={0}".format(updated))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
