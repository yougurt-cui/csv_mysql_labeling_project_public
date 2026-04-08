# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional

from rich.console import Console
from sqlalchemy import text
from sqlalchemy.engine import Engine

console = Console()

TABLE_RE = re.compile(r"^[A-Za-z0-9_]+$")

DEFAULT_CAT_CTX_RE = r"猫|猫粮|幼猫|成猫|布偶|英短|美短|主子|喵|罐头|冻干|泌尿|发腮|黑下巴|软便|泪痕"
DEFAULT_BRAND_PREFILTER_RE = (
    r"皇家|K36|BK34|BK36|BS34|F32|阿皇|"
    r"渴望|爱肯拿|安肯拿|弗列加特|麦富迪|蓝氏|鲜朗|鲜郎|"
    r"素力高|福摩|绿福摩|百利|巅峰|冠能|网易严选|法米娜|"
    r"纽翠斯|纽顿|玫斯|自然光环|纯福|领先|顽皮|比瑞吉|"
    r"有鱼|狂野盛宴|帕特|好主人|卫仕|卫士|百丽"
)

HARD_STOP_CHARS = "。！？!?；;\n"

NEGATION_PREFIX_RE = re.compile(r"(没有|没|无|未|不|并非|不是|不会|别)[^，。！？!?；;\n]{0,2}$")


@dataclass
class ExtractState:
    last_douyin_ingest_ts: Optional[datetime]
    last_xhs_ingest_ts: Optional[datetime]


@dataclass
class BrandRelationExtractResult:
    batch_id: str
    scanned_rows: int
    matched_rows: int
    upserted_rows: int
    current_douyin_max_ts: Optional[datetime]
    current_xhs_max_ts: Optional[datetime]


@dataclass
class BrandMention:
    canonical: str
    matched_text: str
    start: int
    end: int


def _safe_table(name: str) -> str:
    if not TABLE_RE.fullmatch(name):
        raise ValueError(f"Invalid table name: {name}")
    return name


def _compile_brand_patterns() -> list[tuple[str, re.Pattern[str], int]]:
    brand_aliases: dict[str, list[str]] = {
        "皇家": [
            r"皇家猫粮",
            r"皇家处方(?:粮)?",
            r"皇家(?:奶糕|室内|英短|泌尿|高纤维|减肥粮|布偶粮)?",
            r"阿皇",
            r"(?<![A-Za-z])hj(?![A-Za-z])",
            r"(?<![A-Za-z])(k36|bk34|bk36|bs34|f32)(?![A-Za-z])",
        ],
        "渴望": [r"渴望(?:原味鸡|八重守护|苔原)?", r"(?<![A-Za-z])orijen(?![A-Za-z])"],
        "爱肯拿": [r"爱肯拿(?:海洋盛宴|农场盛宴|鸡肉|鱼肉)?", r"安肯拿", r"(?<![A-Za-z])acana(?![A-Za-z])"],
        "弗列加特": [r"弗列加特"],
        "麦富迪": [r"麦富迪(?:barf|霸服|冻干|N5)?", r"(?<![A-Za-z])barf(?![A-Za-z])"],
        "蓝氏": [r"蓝氏(?:奶盾|猎鸟|乳鸽|猫粮)?"],
        "鲜朗": [r"鲜朗", r"鲜郎"],
        "素力高": [r"金装?素力高", r"素力高", r"(?<![A-Za-z])solid\s*gold(?![A-Za-z])"],
        "福摩": [r"绿福摩", r"福摩", r"(?<![A-Za-z])fromm(?![A-Za-z])"],
        "百利": [r"百利(?:幼)?", r"百丽猫粮", r"百丽", r"(?<![A-Za-z])instinct(?![A-Za-z])"],
        "巅峰": [r"巅峰", r"(?<![A-Za-z])ziwi(?![A-Za-z])"],
        "冠能": [r"冠能(?:幼猫|鸡肉三文鱼|猫粮)?", r"(?<![A-Za-z])pro\s*plan(?![A-Za-z])"],
        "网易严选": [r"网易严选", r"网易家(?:的)?"],
        "自然光环": [r"自然光环", r"自然光猫粮", r"(?<![A-Za-z])halo(?![A-Za-z])"],
        "法米娜": [r"法米娜(?:无谷|鸡肉|猫粮)?", r"(?<![A-Za-z])n&d(?![A-Za-z])"],
        "纽翠斯": [r"纽翠斯"],
        "纽顿": [r"纽顿"],
        "玫斯": [r"玫斯"],
        "纯福": [r"纯福"],
        "领先": [r"领先"],
        "顽皮": [r"顽皮"],
        "比瑞吉": [r"比瑞吉"],
        "有鱼": [r"有鱼"],
        "狂野盛宴": [r"狂野盛宴"],
        "帕特": [r"帕特"],
        "好主人": [r"好主人(?:金装)?"],
        "卫仕": [r"卫仕", r"卫士"],
        "GO!": [r"(?<![A-Za-z])go!?(?![A-Za-z])"],
        "NOW": [r"(?<![A-Za-z])now(?![A-Za-z])"],
    }

    compiled: list[tuple[str, re.Pattern[str], int]] = []
    for canonical, patterns in brand_aliases.items():
        for pattern in patterns:
            compiled.append((canonical, re.compile(pattern, re.IGNORECASE), len(pattern)))
    compiled.sort(key=lambda item: item[2], reverse=True)
    return compiled


def _compile_symptom_patterns() -> list[tuple[str, re.Pattern[str]]]:
    symptom_map: list[tuple[str, str]] = [
        ("软便", r"软便|稀便"),
        ("拉稀", r"拉稀|腹泻|窜稀|蹿稀|窜肚|蹿肚"),
        ("便秘", r"便秘|拉不出屎|拉不出粑粑|拉不出来"),
        ("便血", r"便血|血便|有血丝|带血|便里有血|便便有血"),
        ("黑下巴", r"黑下巴"),
        ("泪痕", r"泪痕|眼泪巨多|眼泪多|流泪|眼屎也多|眼屎多"),
        ("呕吐", r"呕吐|吐毛|总是吐|爱吐|吐了"),
        ("肠胃炎", r"肠胃炎|肠胃病|肠胃出问题"),
        ("肠胃不适", r"玻璃胃|肠胃不好|肠胃差|肠胃弱|消化不好|消化不良"),
        ("毛囊炎", r"毛囊炎"),
        ("脱毛", r"脱毛|掉毛"),
        ("放屁", r"疯狂放屁|放屁"),
        ("臭便", r"巨臭|很臭|臭味"),
        ("后腿无力", r"后腿无力|走路不太稳当|爬不上楼梯"),
        ("抽搐", r"抽搐"),
        ("应激", r"应激"),
        ("泌尿问题", r"泌尿|尿闭|尿血|尿结石|肾结石"),
    ]
    return [(name, re.compile(pattern, re.IGNORECASE)) for name, pattern in symptom_map]


BRAND_PATTERNS = _compile_brand_patterns()
SYMPTOM_PATTERNS = _compile_symptom_patterns()


def ensure_brand_relation_tables(
    engine: Engine,
    target_table: str = "catfood_brand_relation_comments",
    state_table: str = "catfood_brand_relation_extract_state",
) -> None:
    target_table = _safe_table(target_table)
    state_table = _safe_table(state_table)

    target_sql = f"""
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
      brand_mention_count INT NOT NULL DEFAULT 0,
      brand_coverage_count INT NOT NULL DEFAULT 0,
      covered_brands_json LONGTEXT NULL,
      relation_count INT NOT NULL DEFAULT 0,
      relations_json LONGTEXT NULL,
      comment_symptom_flag TINYINT(1) NOT NULL DEFAULT 0,
      comment_symptom_terms_json LONGTEXT NULL,
      has_symptom_relation TINYINT(1) NOT NULL DEFAULT 0,
      source_ingest_ts DATETIME NULL,
      extract_batch_id VARCHAR(32) NOT NULL,
      extract_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uq_platform_external (platform, external_id),
      KEY idx_keyword (keyword),
      KEY idx_event_date (event_date),
      KEY idx_brand_coverage_count (brand_coverage_count),
      KEY idx_relation_count (relation_count),
      KEY idx_comment_symptom_flag (comment_symptom_flag),
      KEY idx_has_symptom_relation (has_symptom_relation),
      KEY idx_extract_ts (extract_ts)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    state_sql = f"""
    CREATE TABLE IF NOT EXISTS `{state_table}` (
      id TINYINT PRIMARY KEY,
      last_douyin_ingest_ts DATETIME NULL,
      last_xhs_ingest_ts DATETIME NULL,
      last_run_ts DATETIME NULL,
      updated_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    with engine.begin() as conn:
        conn.execute(text(target_sql))
        conn.execute(text(state_sql))
        conn.execute(
            text(
                f"""
                INSERT INTO `{state_table}` (id, last_douyin_ingest_ts, last_xhs_ingest_ts, last_run_ts)
                VALUES (1, NULL, NULL, NULL)
                ON DUPLICATE KEY UPDATE id = id
                """
            )
        )


def _get_state(engine: Engine, state_table: str) -> ExtractState:
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


def _get_current_max_ingest_ts(engine: Engine) -> dict[str, Optional[datetime]]:
    with engine.begin() as conn:
        douyin = conn.execute(text("SELECT MAX(ingest_ts) AS v FROM douyin_raw_comments")).mappings().first()
        xhs = conn.execute(text("SELECT MAX(ingest_ts) AS v FROM xiaohongshu_raw_comments")).mappings().first()
    return {
        "douyin": douyin.get("v") if douyin else None,
        "xhs": xhs.get("v") if xhs else None,
    }


def _iter_source_rows(
    engine: Engine,
    last_douyin_ts: Optional[datetime],
    current_douyin_ts: Optional[datetime],
    last_xhs_ts: Optional[datetime],
    current_xhs_ts: Optional[datetime],
    brand_prefilter_re: str,
    cat_ctx_re: str,
    fetch_size: int = 2000,
) -> Iterable[dict]:
    union_sql = text(
        """
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
          AND IFNULL(comment_text, '') REGEXP :brand_prefilter_re
          AND CONCAT_WS(' ', IFNULL(search_keyword, ''), IFNULL(post_title, ''), IFNULL(post_content, ''), IFNULL(comment_text, ''))
              REGEXP :cat_ctx_re

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
          AND IFNULL(comment_text, '') REGEXP :brand_prefilter_re
          AND CONCAT_WS(' ', IFNULL(query_keyword, ''), IFNULL(title, ''), IFNULL(content, ''), IFNULL(comment_text, ''))
              REGEXP :cat_ctx_re
        """
    )

    params = {
        "last_douyin_ts": last_douyin_ts,
        "current_douyin_ts": current_douyin_ts,
        "last_xhs_ts": last_xhs_ts,
        "current_xhs_ts": current_xhs_ts,
        "brand_prefilter_re": brand_prefilter_re,
        "cat_ctx_re": cat_ctx_re,
    }

    with engine.connect().execution_options(stream_results=True) as conn:
        result = conn.execute(union_sql, params)
        while True:
            rows = result.mappings().fetchmany(fetch_size)
            if not rows:
                break
            for row in rows:
                yield dict(row)


def _find_brand_mentions(text_value: Optional[str]) -> list[BrandMention]:
    text_in = str(text_value or "")
    if not text_in.strip():
        return []

    candidates: list[BrandMention] = []
    for canonical, pattern, _ in BRAND_PATTERNS:
        for match in pattern.finditer(text_in):
            candidates.append(
                BrandMention(
                    canonical=canonical,
                    matched_text=match.group(0),
                    start=match.start(),
                    end=match.end(),
                )
            )

    candidates.sort(key=lambda item: (item.start, -(item.end - item.start), item.canonical))

    accepted: list[BrandMention] = []
    for cand in candidates:
        overlap = False
        for prev in accepted:
            if cand.end <= prev.start or cand.start >= prev.end:
                continue
            overlap = True
            break
        if not overlap:
            accepted.append(cand)

    accepted.sort(key=lambda item: (item.start, item.end, item.canonical))
    return accepted


def _ordered_unique_brands(mentions: list[BrandMention]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for mention in mentions:
        if mention.canonical in seen:
            continue
        seen.add(mention.canonical)
        out.append(mention.canonical)
    return out


def _is_negated(text_value: str, start: int) -> bool:
    prefix = text_value[max(0, start - 6):start]
    return bool(NEGATION_PREFIX_RE.search(prefix))


def _find_symptom_terms(text_value: Optional[str]) -> list[str]:
    text_in = str(text_value or "")
    if not text_in.strip():
        return []

    out: list[str] = []
    seen: set[str] = set()
    for label, pattern in SYMPTOM_PATTERNS:
        for match in pattern.finditer(text_in):
            if _is_negated(text_in, match.start()):
                continue
            if label in seen:
                continue
            seen.add(label)
            out.append(label)
    return out


def _extract_connector(text_value: str, left: BrandMention, right: BrandMention) -> Optional[str]:
    between = text_value[left.end:right.start]
    prefix = text_value[max(0, left.start - 4):left.start]
    compact_between = re.sub(r"\s+", "", between)
    max_bridge_len = 24

    if ("从" in prefix or "由" in prefix) and "到" in compact_between and len(compact_between) <= 18:
        if "由" in prefix and "从" not in prefix:
            return "由...到"
        return "从...到"

    if len(compact_between) > max_bridge_len:
        return None

    connector_re = re.compile(
        r"(换成|换到|换了|换着吃|换粮|换|改成|改吃|改到|转粮|转成|转到|混着吃|混着|混粮|混|掺着吃|掺着|掺|搭配|后面换了|后面换成|后来换了|后来换成|后来吃|现在吃|准备换)"
    )
    match = connector_re.search(compact_between)
    if match:
        return match.group(1)
    return None


def _relation_segment(text_value: str, mentions: list[BrandMention], index: int) -> str:
    left = mentions[index]
    right = mentions[index + 1]

    start = max(0, left.start - 4)
    hard_limit = min(len(text_value), right.end + 100)

    stop = hard_limit
    for pos in range(right.end, hard_limit):
        ch = text_value[pos]
        if ch in HARD_STOP_CHARS:
            stop = pos
            break
        if ch in "，,":
            after = text_value[pos + 1: min(len(text_value), pos + 26)].strip()
            if re.search(r"^(从|由|后面|后来|然后|再|现在)", after):
                stop = pos
                break
            after_short = after[:16]
            if _find_brand_mentions(after_short) and re.search(r"(到|换成|换到|换了|换|改成|改吃|改到|转到|转成)", after_short):
                stop = pos
                break

    return text_value[start:stop].strip()


def _extract_relations(text_value: Optional[str], mentions: list[BrandMention]) -> list[dict]:
    text_in = str(text_value or "")
    if len(mentions) < 2 or not text_in.strip():
        return []

    relations: list[dict] = []
    for idx in range(len(mentions) - 1):
        left = mentions[idx]
        right = mentions[idx + 1]
        if left.canonical == right.canonical:
            continue

        connector = _extract_connector(text_in, left, right)
        if not connector:
            continue

        evidence = _relation_segment(text_in, mentions, idx)
        symptom_terms = _find_symptom_terms(evidence)
        relations.append(
            {
                "from_brand": left.canonical,
                "to_brand": right.canonical,
                "from_text": left.matched_text,
                "to_text": right.matched_text,
                "connector": connector,
                "symptom_flag": 1 if symptom_terms else 0,
                "symptom_terms": symptom_terms,
                "evidence_text": evidence,
            }
        )
    return relations


def _build_upsert_rows(rows: Iterable[dict], batch_id: str) -> tuple[list[dict], int]:
    out: list[dict] = []
    matched_rows = 0

    for row in rows:
        comment_text = str(row.get("comment_text") or "")
        mentions = _find_brand_mentions(comment_text)
        if not mentions:
            continue

        matched_rows += 1
        covered_brands = _ordered_unique_brands(mentions)
        relations = _extract_relations(comment_text, mentions)
        comment_symptom_terms = _find_symptom_terms(comment_text)

        out.append(
            {
                "platform": row.get("platform"),
                "raw_id": row.get("raw_id"),
                "external_id": row.get("external_id"),
                "event_date": row.get("event_date"),
                "keyword": row.get("keyword"),
                "title": row.get("title"),
                "content": row.get("content"),
                "comment_text": comment_text,
                "brand_mention_count": len(mentions),
                "brand_coverage_count": len(covered_brands),
                "covered_brands_json": json.dumps(covered_brands, ensure_ascii=False),
                "relation_count": len(relations),
                "relations_json": json.dumps(relations, ensure_ascii=False),
                "comment_symptom_flag": 1 if comment_symptom_terms else 0,
                "comment_symptom_terms_json": json.dumps(comment_symptom_terms, ensure_ascii=False),
                "has_symptom_relation": 1 if any(item["symptom_flag"] == 1 for item in relations) else 0,
                "source_ingest_ts": row.get("source_ingest_ts"),
                "extract_batch_id": batch_id,
            }
        )

    return out, matched_rows


def run_brand_relation_extraction(
    engine: Engine,
    target_table: str = "catfood_brand_relation_comments",
    state_table: str = "catfood_brand_relation_extract_state",
    cat_ctx_re: str = DEFAULT_CAT_CTX_RE,
    brand_prefilter_re: str = DEFAULT_BRAND_PREFILTER_RE,
    batch_size: int = 500,
) -> BrandRelationExtractResult:
    target_table = _safe_table(target_table)
    state_table = _safe_table(state_table)
    ensure_brand_relation_tables(engine, target_table=target_table, state_table=state_table)

    state = _get_state(engine, state_table=state_table)
    max_ts = _get_current_max_ingest_ts(engine)
    current_douyin_ts = max_ts["douyin"]
    current_xhs_ts = max_ts["xhs"]

    source_rows = _iter_source_rows(
        engine=engine,
        last_douyin_ts=state.last_douyin_ingest_ts,
        current_douyin_ts=current_douyin_ts,
        last_xhs_ts=state.last_xhs_ingest_ts,
        current_xhs_ts=current_xhs_ts,
        brand_prefilter_re=brand_prefilter_re,
        cat_ctx_re=cat_ctx_re,
    )

    batch_id = uuid.uuid4().hex[:12]
    scanned_rows = 0
    matched_rows = 0
    upserted_rows = 0
    buffer: list[dict] = []

    upsert_sql = text(
        f"""
        INSERT INTO `{target_table}` (
          platform, raw_id, external_id, event_date, keyword, title, content, comment_text,
          brand_mention_count, brand_coverage_count, covered_brands_json,
          relation_count, relations_json, comment_symptom_flag, comment_symptom_terms_json,
          has_symptom_relation, source_ingest_ts, extract_batch_id, extract_ts
        )
        VALUES (
          :platform, :raw_id, :external_id, :event_date, :keyword, :title, :content, :comment_text,
          :brand_mention_count, :brand_coverage_count, :covered_brands_json,
          :relation_count, :relations_json, :comment_symptom_flag, :comment_symptom_terms_json,
          :has_symptom_relation, :source_ingest_ts, :extract_batch_id, NOW()
        )
        ON DUPLICATE KEY UPDATE
          raw_id=VALUES(raw_id),
          event_date=VALUES(event_date),
          keyword=VALUES(keyword),
          title=VALUES(title),
          content=VALUES(content),
          comment_text=VALUES(comment_text),
          brand_mention_count=VALUES(brand_mention_count),
          brand_coverage_count=VALUES(brand_coverage_count),
          covered_brands_json=VALUES(covered_brands_json),
          relation_count=VALUES(relation_count),
          relations_json=VALUES(relations_json),
          comment_symptom_flag=VALUES(comment_symptom_flag),
          comment_symptom_terms_json=VALUES(comment_symptom_terms_json),
          has_symptom_relation=VALUES(has_symptom_relation),
          source_ingest_ts=VALUES(source_ingest_ts),
          extract_batch_id=VALUES(extract_batch_id),
          extract_ts=NOW()
        """
    )

    def flush() -> None:
        nonlocal matched_rows, upserted_rows, buffer
        if not buffer:
            return
        upsert_rows, local_matched = _build_upsert_rows(buffer, batch_id=batch_id)
        matched_rows += local_matched
        if upsert_rows:
            with engine.begin() as conn:
                conn.execute(upsert_sql, upsert_rows)
            upserted_rows += len(upsert_rows)
        buffer = []

    for row in source_rows:
        scanned_rows += 1
        buffer.append(row)
        if len(buffer) >= batch_size:
            flush()

    flush()

    with engine.begin() as conn:
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
        f"[green]品牌关系抽取完成[/green] batch={batch_id}, scanned={scanned_rows}, "
        f"matched={matched_rows}, upserted={upserted_rows}"
    )

    return BrandRelationExtractResult(
        batch_id=batch_id,
        scanned_rows=scanned_rows,
        matched_rows=matched_rows,
        upserted_rows=upserted_rows,
        current_douyin_max_ts=current_douyin_ts,
        current_xhs_max_ts=current_xhs_ts,
    )
