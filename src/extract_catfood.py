# -*- coding: utf-8 -*-
from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

from rich.console import Console
from sqlalchemy import text
from sqlalchemy.engine import Engine

console = Console()

TABLE_RE = re.compile(r"^[A-Za-z0-9_]+$")

DEFAULT_CAT_CTX = "猫粮|猫咪|猫猫|幼猫|成猫|布偶|英短|缅因|狸花|主子|喵|处方粮"
DEFAULT_BRAND_RE = (
    "皇家|冠能|渴望|巅峰|百利|麦富迪|弗列加特|鲜朗|蓝氏|"
    "诚实[[:space:]]*一口|csyk|简创|布兰德|自然光环|帕特|顽皮|伊莱恩|"
    "好主人|卡妮|星速|明仔团|喵梵思"
)
DEFAULT_HEALTH_RE = (
    "软便|拉稀|腹泻|便秘|黑下巴|呕吐|吐毛|泪痕|掉毛|毛发|过敏|玻璃胃|"
    "肠胃|消化|尿闭|尿血|泌尿|肾|口炎|胰腺|上火|分泌物|结石"
)


@dataclass
class ExtractState:
    last_douyin_ingest_ts: Optional[datetime]
    last_xhs_ingest_ts: Optional[datetime]


@dataclass
class ExtractResult:
    batch_id: str
    inserted_rows: int
    scanned_douyin_rows: int
    scanned_xhs_rows: int
    current_douyin_max_ts: Optional[datetime]
    current_xhs_max_ts: Optional[datetime]


def _safe_table(name: str) -> str:
    if not TABLE_RE.fullmatch(name):
        raise ValueError(f"Invalid table name: {name}")
    return name


def ensure_extract_tables(
    engine: Engine,
    target_table: str = "catfood_brand_health_candidates",
    state_table: str = "catfood_brand_health_extract_state",
) -> None:
    target_table = _safe_table(target_table)
    state_table = _safe_table(state_table)

    target_ddl = f"""
    CREATE TABLE IF NOT EXISTS `{target_table}` (
      id BIGINT PRIMARY KEY AUTO_INCREMENT,
      platform VARCHAR(32) NOT NULL,
      raw_id BIGINT NULL,
      external_id VARCHAR(128) NOT NULL,
      event_date DATE NULL,
      keyword VARCHAR(255) NULL,
      title TEXT NULL,
      content LONGTEXT NULL,
      comment_text LONGTEXT NULL,
      brand_ctx_hit TINYINT(1) NOT NULL DEFAULT 0,
      health_ctx_hit TINYINT(1) NOT NULL DEFAULT 0,
      brand_comment_hit TINYINT(1) NOT NULL DEFAULT 0,
      health_comment_hit TINYINT(1) NOT NULL DEFAULT 0,
      confidence_bucket VARCHAR(64) NOT NULL,
      source_ingest_ts DATETIME NULL,
      extract_batch_id VARCHAR(32) NOT NULL,
      extract_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      KEY idx_platform_external (platform, external_id),
      KEY idx_event_date (event_date),
      KEY idx_extract_ts (extract_ts)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    state_ddl = f"""
    CREATE TABLE IF NOT EXISTS `{state_table}` (
      id TINYINT PRIMARY KEY,
      last_douyin_ingest_ts DATETIME NULL,
      last_xhs_ingest_ts DATETIME NULL,
      last_run_ts DATETIME NULL,
      updated_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    with engine.begin() as conn:
        conn.execute(text(target_ddl))
        conn.execute(text(state_ddl))
        conn.execute(
            text(
                f"""
                INSERT INTO `{state_table}` (id, last_douyin_ingest_ts, last_xhs_ingest_ts, last_run_ts)
                VALUES (1, NULL, NULL, NULL)
                ON DUPLICATE KEY UPDATE id = id
                """
            )
        )

    _ensure_target_columns(engine=engine, target_table=target_table)
    _ensure_target_indexes(engine=engine, target_table=target_table)


def _fetch_table_columns(engine: Engine, table_name: str) -> set[str]:
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT COLUMN_NAME
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = :table_name
                """
            ),
            {"table_name": table_name},
        ).fetchall()
    return {r[0] for r in rows}


def _ensure_target_columns(engine: Engine, target_table: str) -> None:
    target_table = _safe_table(target_table)
    cols = _fetch_table_columns(engine, target_table)
    required_cols = {
        "platform": "VARCHAR(32) NOT NULL DEFAULT ''",
        "raw_id": "BIGINT NULL",
        "external_id": "VARCHAR(128) NOT NULL DEFAULT ''",
        "event_date": "DATE NULL",
        "keyword": "VARCHAR(255) NULL",
        "title": "TEXT NULL",
        "content": "LONGTEXT NULL",
        "comment_text": "LONGTEXT NULL",
        "brand_ctx_hit": "TINYINT(1) NOT NULL DEFAULT 0",
        "health_ctx_hit": "TINYINT(1) NOT NULL DEFAULT 0",
        "brand_comment_hit": "TINYINT(1) NOT NULL DEFAULT 0",
        "health_comment_hit": "TINYINT(1) NOT NULL DEFAULT 0",
        "confidence_bucket": "VARCHAR(64) NOT NULL DEFAULT 'C_仅上下文命中'",
        "source_ingest_ts": "DATETIME NULL",
        "extract_batch_id": "VARCHAR(32) NULL",
        "extract_ts": "DATETIME NULL",
    }
    with engine.begin() as conn:
        for col, ddl in required_cols.items():
            if col in cols:
                continue
            conn.execute(text(f"ALTER TABLE `{target_table}` ADD COLUMN `{col}` {ddl}"))


def _ensure_target_indexes(engine: Engine, target_table: str) -> None:
    target_table = _safe_table(target_table)
    with engine.begin() as conn:
        idx_rows = conn.execute(text(f"SHOW INDEX FROM `{target_table}`")).fetchall()
        index_names = {r[2] for r in idx_rows}
        if "idx_platform_external" not in index_names:
            conn.execute(text(f"ALTER TABLE `{target_table}` ADD INDEX idx_platform_external (platform, external_id)"))
        if "idx_event_date" not in index_names:
            conn.execute(text(f"ALTER TABLE `{target_table}` ADD INDEX idx_event_date (event_date)"))
        if "idx_extract_ts" not in index_names:
            conn.execute(text(f"ALTER TABLE `{target_table}` ADD INDEX idx_extract_ts (extract_ts)"))


def _get_state(engine: Engine, state_table: str) -> ExtractState:
    state_table = _safe_table(state_table)
    with engine.begin() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT last_douyin_ingest_ts, last_xhs_ingest_ts
                FROM `{state_table}`
                WHERE id = 1
                """
            )
        ).mappings().first()
    if not row:
        return ExtractState(last_douyin_ingest_ts=None, last_xhs_ingest_ts=None)
    return ExtractState(
        last_douyin_ingest_ts=row.get("last_douyin_ingest_ts"),
        last_xhs_ingest_ts=row.get("last_xhs_ingest_ts"),
    )


def _get_current_max_ingest_ts(engine: Engine) -> Dict[str, Optional[datetime]]:
    with engine.begin() as conn:
        d = conn.execute(text("SELECT MAX(ingest_ts) AS v FROM douyin_raw_comments")).mappings().first()
        x = conn.execute(text("SELECT MAX(ingest_ts) AS v FROM xiaohongshu_raw_comments")).mappings().first()
    return {
        "douyin": d.get("v") if d else None,
        "xhs": x.get("v") if x else None,
    }


def _count_scanned_rows(
    engine: Engine,
    last_douyin_ts: Optional[datetime],
    current_douyin_ts: Optional[datetime],
    last_xhs_ts: Optional[datetime],
    current_xhs_ts: Optional[datetime],
) -> Dict[str, int]:
    with engine.begin() as conn:
        d = conn.execute(
            text(
                """
                SELECT COUNT(*) AS cnt
                FROM douyin_raw_comments
                WHERE (:last_douyin_ts IS NULL OR ingest_ts > :last_douyin_ts)
                  AND (:current_douyin_ts IS NULL OR ingest_ts <= :current_douyin_ts)
                """
            ),
            {
                "last_douyin_ts": last_douyin_ts,
                "current_douyin_ts": current_douyin_ts,
            },
        ).mappings().first()
        x = conn.execute(
            text(
                """
                SELECT COUNT(*) AS cnt
                FROM xiaohongshu_raw_comments
                WHERE (:last_xhs_ts IS NULL OR ingest_ts > :last_xhs_ts)
                  AND (:current_xhs_ts IS NULL OR ingest_ts <= :current_xhs_ts)
                  AND created_at IS NOT NULL
                  AND TRIM(created_at) <> ''
                  AND TRIM(created_at) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}( .*)?$'
                """
            ),
            {
                "last_xhs_ts": last_xhs_ts,
                "current_xhs_ts": current_xhs_ts,
            },
        ).mappings().first()
    return {
        "douyin": int(d.get("cnt") or 0),
        "xhs": int(x.get("cnt") or 0),
    }


def run_catfood_extraction_incremental(
    engine: Engine,
    target_table: str = "catfood_brand_health_candidates",
    state_table: str = "catfood_brand_health_extract_state",
    cat_ctx_re: str = DEFAULT_CAT_CTX,
    brand_re: str = DEFAULT_BRAND_RE,
    health_re: str = DEFAULT_HEALTH_RE,
) -> ExtractResult:
    target_table = _safe_table(target_table)
    state_table = _safe_table(state_table)
    ensure_extract_tables(engine, target_table=target_table, state_table=state_table)

    state = _get_state(engine, state_table=state_table)
    max_ts = _get_current_max_ingest_ts(engine)
    current_douyin_ts = max_ts["douyin"]
    current_xhs_ts = max_ts["xhs"]

    scanned = _count_scanned_rows(
        engine=engine,
        last_douyin_ts=state.last_douyin_ingest_ts,
        current_douyin_ts=current_douyin_ts,
        last_xhs_ts=state.last_xhs_ingest_ts,
        current_xhs_ts=current_xhs_ts,
    )

    batch_id = uuid.uuid4().hex[:12]

    insert_sql = f"""
    INSERT INTO `{target_table}` (
      platform, raw_id, external_id, event_date, keyword, title, content, comment_text,
      brand_ctx_hit, health_ctx_hit, brand_comment_hit, health_comment_hit, confidence_bucket,
      source_ingest_ts, extract_batch_id, extract_ts
    )
    SELECT
      s.platform,
      s.raw_id,
      s.external_id,
      s.event_date,
      s.keyword,
      s.title,
      s.content,
      s.comment_text,
      s.brand_ctx_hit,
      s.health_ctx_hit,
      s.brand_comment_hit,
      s.health_comment_hit,
      CASE
        WHEN s.brand_comment_hit = 1 AND s.health_comment_hit = 1 THEN 'A_评论内品牌+健康'
        WHEN s.brand_comment_hit = 1 OR s.health_comment_hit = 1 THEN 'B_评论内单命中'
        ELSE 'C_仅上下文命中'
      END AS confidence_bucket,
      s.source_ingest_ts,
      :batch_id,
      NOW()
      FROM (
        SELECT
          b.platform,
          b.raw_id,
          b.external_id,
          b.event_date,
          b.keyword,
          b.title,
          b.content,
          b.comment_text,
          b.source_ingest_ts,
          (IFNULL(b.comment_text, '') REGEXP :cat_ctx_re) AS cat_ctx_hit,
          (IFNULL(b.comment_text, '') REGEXP :brand_re) AS brand_ctx_hit,
          (IFNULL(b.comment_text, '') REGEXP :health_re) AS health_ctx_hit,
          (IFNULL(b.comment_text, '') REGEXP :brand_re) AS brand_comment_hit,
          (IFNULL(b.comment_text, '') REGEXP :health_re) AS health_comment_hit
      FROM (
        SELECT
          'douyin' AS platform,
          id AS raw_id,
          external_id,
          comment_date AS event_date,
          search_keyword AS keyword,
          post_title AS title,
          post_content AS content,
          comment_text,
          ingest_ts AS source_ingest_ts
        FROM douyin_raw_comments
        WHERE (:last_douyin_ts IS NULL OR ingest_ts > :last_douyin_ts)
          AND (:current_douyin_ts IS NULL OR ingest_ts <= :current_douyin_ts)

        UNION ALL

        SELECT
          'xiaohongshu' AS platform,
          id AS raw_id,
          external_id,
          STR_TO_DATE(SUBSTRING(TRIM(created_at), 1, 10), '%Y-%m-%d') AS event_date,
          query_keyword AS keyword,
          title,
          content,
          comment_text,
          ingest_ts AS source_ingest_ts
        FROM xiaohongshu_raw_comments
        WHERE (:last_xhs_ts IS NULL OR ingest_ts > :last_xhs_ts)
          AND (:current_xhs_ts IS NULL OR ingest_ts <= :current_xhs_ts)
          AND created_at IS NOT NULL
          AND TRIM(created_at) <> ''
          AND TRIM(created_at) REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}( .*)?$'
      ) b
    ) s
    LEFT JOIN `{target_table}` t
      ON t.platform = s.platform
     AND t.external_id = s.external_id
    WHERE (s.brand_comment_hit = 1 OR s.health_comment_hit = 1)
      AND t.external_id IS NULL
    """

    with engine.begin() as conn:
        result = conn.execute(
            text(insert_sql),
            {
                "last_douyin_ts": state.last_douyin_ingest_ts,
                "current_douyin_ts": current_douyin_ts,
                "last_xhs_ts": state.last_xhs_ingest_ts,
                "current_xhs_ts": current_xhs_ts,
                "cat_ctx_re": cat_ctx_re,
                "brand_re": brand_re,
                "health_re": health_re,
                "batch_id": batch_id,
            },
        )
        inserted_rows = int(result.rowcount or 0)

        conn.execute(
            text(
                f"""
                UPDATE `{state_table}`
                SET
                  last_douyin_ingest_ts = COALESCE(:current_douyin_ts, last_douyin_ingest_ts),
                  last_xhs_ingest_ts = COALESCE(:current_xhs_ts, last_xhs_ingest_ts),
                  last_run_ts = NOW()
                WHERE id = 1
                """
            ),
            {
                "current_douyin_ts": current_douyin_ts,
                "current_xhs_ts": current_xhs_ts,
            },
        )

    console.print(
        f"[green]猫粮数据抽取完成[/green] batch={batch_id}, inserted={inserted_rows}, "
        f"scanned(douyin={scanned['douyin']}, xhs={scanned['xhs']})"
    )
    return ExtractResult(
        batch_id=batch_id,
        inserted_rows=inserted_rows,
        scanned_douyin_rows=scanned["douyin"],
        scanned_xhs_rows=scanned["xhs"],
        current_douyin_max_ts=current_douyin_ts,
        current_xhs_max_ts=current_xhs_ts,
    )
