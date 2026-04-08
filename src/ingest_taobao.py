# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
import hashlib
import json
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, urlparse

from rich.console import Console
from sqlalchemy import text
from sqlalchemy.engine import Engine

console = Console()

ANSI_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
FILE_RE = re.compile(r"^(?:list_items_raw|detail_items_raw)_(.+)_(\d{8}_\d{6})\.json$")
TABLE_RE = re.compile(r"^[A-Za-z0-9_]+$")
DEFAULT_CLEANUP_MIN_PAY_COUNT = 100
DEFAULT_FOOD_TASTE_SIMILARITY_THRESHOLD = 0.75


@dataclass
class TaobaoCleanupSummary:
    rows_before: int
    rows_after: int
    low_pay_dropped: int
    duplicate_dropped: int
    total_dropped: int
    min_pay_count: int
    similarity_threshold: float
    backup_table: Optional[str] = None


@dataclass
class TaobaoIngestSummary:
    files: int
    rows: int
    ingest_batch_id: str
    cleanup: TaobaoCleanupSummary


def _strip_ansi(value: str) -> str:
    return ANSI_RE.sub("", value)


def _decode_json_maybe_nested(obj: Any) -> Any:
    out = obj
    for _ in range(3):
        if isinstance(out, str):
            s = out.strip()
            if not s:
                break
            try:
                out = json.loads(s)
            except Exception:
                break
        else:
            break
    return out


def _extract_json_payload(raw_text: str) -> Any:
    cleaned = _strip_ansi(raw_text)
    decoder = json.JSONDecoder()

    direct = cleaned.strip()
    if direct:
        try:
            return _decode_json_maybe_nested(json.loads(direct))
        except Exception:
            pass

    for match in re.finditer(r'[{"\[]', cleaned):
        fragment = cleaned[match.start() :].lstrip()
        try:
            obj, _ = decoder.raw_decode(fragment)
        except Exception:
            continue
        obj = _decode_json_maybe_nested(obj)
        if isinstance(obj, (dict, list)):
            return obj

    raise ValueError("No valid JSON payload found in file.")


def _load_items(json_path: Path) -> List[Dict[str, Any]]:
    raw_text = json_path.read_text(encoding="utf-8", errors="ignore")
    payload = _extract_json_payload(raw_text)

    if isinstance(payload, dict):
        items = payload.get("items")
        if isinstance(items, list):
            return [x for x in items if isinstance(x, dict)]
        if "title" in payload and "url" in payload:
            return [payload]
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    raise ValueError(f"Unrecognized payload in {json_path}")


def _parse_file_meta(path: Path) -> Tuple[str, Optional[datetime]]:
    match = FILE_RE.match(path.name)
    if not match:
        return "unknown", None
    keyword = match.group(1)
    ts_str = match.group(2)
    try:
        crawl_ts = datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
    except ValueError:
        crawl_ts = None
    return keyword, crawl_ts


def _parse_decimal_like(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).replace("¥", "").replace("￥", "").strip()
    s = re.sub(r"\s+", "", s)
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return m.group(1) if m else None


def _parse_pay_count(title: str) -> Tuple[Optional[str], Optional[int]]:
    m = re.search(r"(\d+(?:\.\d+)?)(万)?\+?人付款", title or "")
    if not m:
        return None, None
    txt = m.group(0)
    n = float(m.group(1))
    if m.group(2):
        n *= 10000
    return txt, int(n)


def _parse_sold_count(value: Any) -> Tuple[Optional[str], Optional[int]]:
    text_value = str(value or "").strip()
    if not text_value:
        return None, None

    m = re.search(r"(已售\s*(\d+(?:\.\d+)?)(万)?\+?)", text_value)
    if not m:
        m = re.search(r"((\d+(?:\.\d+)?)(万)?\+?\s*(?:件已售|人付款|人购买))", text_value)
    if not m:
        m = re.search(r"^((\d+(?:\.\d+)?)(万)?\+?)$", text_value)
    if not m:
        return text_value, None

    n = float(m.group(2))
    if m.group(3):
        n *= 10000
    return text_value, int(n)


def _parse_item_id(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    try:
        q = parse_qs(urlparse(url).query)
        vals = q.get("id")
        return vals[0] if vals else None
    except Exception:
        return None


def _parse_site(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    host = (urlparse(url).netloc or "").lower()
    if "tmall.com" in host:
        return "tmall"
    if "taobao.com" in host:
        return "taobao"
    return host or None


def _normalize_title(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _normalize_text(value: Any) -> Optional[str]:
    text_value = re.sub(r"\s+", " ", str(value or "").strip())
    return text_value or None


def _normalize_food_taste_key(value: Any) -> str:
    text_value = str(value or "").strip().lower()
    text_value = re.sub(r"\s+", "", text_value)
    text_value = re.sub(r"[【】\[\]()（）:：,，。/\\+]+", "", text_value)
    return text_value


def _parse_rank_no(item: Dict[str, Any], fallback_rank: int) -> int:
    raw_rank = item.get("index")
    if raw_rank is None:
        return fallback_rank
    try:
        return max(1, int(str(raw_rank).strip()))
    except Exception:
        return fallback_rank


def _build_records(path: Path) -> List[Dict[str, Any]]:
    keyword, crawl_ts = _parse_file_meta(path)
    items = _load_items(path)

    out: List[Dict[str, Any]] = []
    for idx, item in enumerate(items, start=1):
        title = _normalize_title(item.get("title"))
        product_url = str(item.get("url") or "").strip() or None
        item_id = _parse_item_id(product_url)
        # Stable id to make upsert idempotent across multiple crawls of same keyword.
        ext_seed = f"{keyword}|{item_id or product_url or title}"
        external_id = hashlib.md5(ext_seed.encode("utf-8")).hexdigest()
        pay_count_text, pay_count = _parse_pay_count(title)
        sold_text, sold_count = _parse_sold_count(item.get("sold"))
        if pay_count is None and sold_count is not None:
            pay_count_text, pay_count = sold_text, sold_count

        out.append(
            {
                "external_id": external_id,
                "keyword": keyword,
                "crawl_ts": crawl_ts,
                "source_file": path.name,
                "rank_no": _parse_rank_no(item, idx),
                "title": title,
                "price": _parse_decimal_like(item.get("price")),
                "pay_count_text": pay_count_text,
                "pay_count": pay_count,
                "ship_from": _normalize_text(item.get("ship_from")),
                "product_url": product_url,
                "item_id": item_id,
                "site": _parse_site(product_url),
                "food_taste": _normalize_text(item.get("food_taste")),
                "net_content": _normalize_text(item.get("net_content")),
                "sold_text": sold_text,
                "sold_count": sold_count,
                "raw_json": json.dumps(item, ensure_ascii=False),
            }
        )
    return out


def _chunked(seq: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _make_backup_table_name(table_name: str, suffix: str) -> str:
    safe_suffix = re.sub(r"[^A-Za-z0-9_]+", "_", str(suffix or "")).strip("_") or datetime.now().strftime("%Y%m%d_%H%M%S")
    budget = 64 - len("_backup_") - len(safe_suffix)
    base_name = table_name[: max(1, budget)]
    return f"{base_name}_backup_{safe_suffix}"


def _empty_cleanup_summary(
    min_pay_count: int = DEFAULT_CLEANUP_MIN_PAY_COUNT,
    similarity_threshold: float = DEFAULT_FOOD_TASTE_SIMILARITY_THRESHOLD,
) -> TaobaoCleanupSummary:
    return TaobaoCleanupSummary(
        rows_before=0,
        rows_after=0,
        low_pay_dropped=0,
        duplicate_dropped=0,
        total_dropped=0,
        min_pay_count=int(min_pay_count),
        similarity_threshold=float(similarity_threshold),
        backup_table=None,
    )


def ensure_taobao_table(engine: Engine, table_name: str = "taobao_catfood_list_items") -> None:
    if not TABLE_RE.match(table_name):
        raise ValueError(f"Invalid table name: {table_name}")
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
      id BIGINT PRIMARY KEY AUTO_INCREMENT,
      external_id CHAR(32) NOT NULL,
      keyword VARCHAR(255) NOT NULL,
      crawl_ts DATETIME NULL,
      source_file VARCHAR(255) NOT NULL,
      rank_no INT NOT NULL,
      title TEXT NULL,
      price DECIMAL(10,2) NULL,
      pay_count_text VARCHAR(64) NULL,
      pay_count INT NULL,
      ship_from VARCHAR(128) NULL,
      product_url TEXT NULL,
      item_id VARCHAR(64) NULL,
      site VARCHAR(32) NULL,
      food_taste VARCHAR(255) NULL,
      net_content VARCHAR(255) NULL,
      sold_text VARCHAR(64) NULL,
      sold_count INT NULL,
      raw_json LONGTEXT NULL,
      ingest_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uq_external_id (external_id),
      KEY idx_keyword (keyword),
      KEY idx_crawl_ts (crawl_ts),
      KEY idx_item_id (item_id),
      KEY idx_sold_count (sold_count)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
        cols = conn.execute(
            text(
                """
                SELECT COLUMN_NAME
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = :table_name
                """
            ),
            {"table_name": table_name},
        ).fetchall()
        col_set = {r[0] for r in cols}
        if "food_taste" not in col_set:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD COLUMN food_taste VARCHAR(255) NULL AFTER site"))
        if "net_content" not in col_set:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD COLUMN net_content VARCHAR(255) NULL AFTER food_taste"))
        if "sold_text" not in col_set:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD COLUMN sold_text VARCHAR(64) NULL AFTER net_content"))
        if "sold_count" not in col_set:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD COLUMN sold_count INT NULL AFTER sold_text"))

        idx_rows = conn.execute(text(f"SHOW INDEX FROM `{table_name}`")).fetchall()
        idx_names = {r[2] for r in idx_rows}
        if "idx_sold_count" not in idx_names:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD INDEX idx_sold_count (sold_count)"))


def cleanup_taobao_table(
    engine: Engine,
    table_name: str = "taobao_catfood_list_items",
    min_pay_count: int = DEFAULT_CLEANUP_MIN_PAY_COUNT,
    food_taste_similarity_threshold: float = DEFAULT_FOOD_TASTE_SIMILARITY_THRESHOLD,
    backup_suffix: Optional[str] = None,
) -> TaobaoCleanupSummary:
    if not TABLE_RE.match(table_name):
        raise ValueError(f"Invalid table name: {table_name}")

    min_pay_count = int(min_pay_count)
    food_taste_similarity_threshold = float(food_taste_similarity_threshold)
    if min_pay_count < 0:
        raise ValueError(f"Invalid min_pay_count: {min_pay_count}")
    if not 0 <= food_taste_similarity_threshold <= 1:
        raise ValueError(f"Invalid food_taste_similarity_threshold: {food_taste_similarity_threshold}")

    with engine.begin() as conn:
        rows = [
            dict(row)
            for row in conn.execute(
                text(
                    f"""
                    SELECT id, keyword, title, pay_count, food_taste, net_content, sold_text, product_url
                    FROM `{table_name}`
                    ORDER BY id
                    """
                )
            ).mappings().all()
        ]

        rows_before = len(rows)
        if not rows:
            return _empty_cleanup_summary(
                min_pay_count=min_pay_count,
                similarity_threshold=food_taste_similarity_threshold,
            )

        keep_pool = [row for row in rows if int(row.get("pay_count") or 0) >= min_pay_count]
        low_pay_dropped = rows_before - len(keep_pool)
        keep_ids: set[int] = set()
        duplicate_dropped = 0

        if keep_pool:
            parent = {int(row["id"]): int(row["id"]) for row in keep_pool}

            def find(node_id: int) -> int:
                while parent[node_id] != node_id:
                    parent[node_id] = parent[parent[node_id]]
                    node_id = parent[node_id]
                return node_id

            def union(left_id: int, right_id: int) -> None:
                left_root = find(left_id)
                right_root = find(right_id)
                if left_root != right_root:
                    parent[right_root] = left_root

            for i in range(len(keep_pool)):
                for j in range(i + 1, len(keep_pool)):
                    left = keep_pool[i]
                    right = keep_pool[j]
                    if left["keyword"] != right["keyword"]:
                        continue
                    left_key = _normalize_food_taste_key(left.get("food_taste"))
                    right_key = _normalize_food_taste_key(right.get("food_taste"))
                    if not left_key or not right_key:
                        continue
                    if SequenceMatcher(None, left_key, right_key).ratio() >= food_taste_similarity_threshold:
                        union(int(left["id"]), int(right["id"]))

            clusters: Dict[int, List[Dict[str, Any]]] = {}
            for row in keep_pool:
                clusters.setdefault(find(int(row["id"])), []).append(row)

            for items in clusters.values():
                def score_key(item: Dict[str, Any]) -> Tuple[int, int, int]:
                    completeness = sum(
                        1
                        for field in ("food_taste", "net_content", "sold_text", "title", "product_url")
                        if item.get(field)
                    )
                    return (
                        int(item.get("pay_count") or 0),
                        completeness,
                        int(item["id"]),
                    )

                winner = max(items, key=score_key)
                keep_ids.add(int(winner["id"]))
                duplicate_dropped += sum(1 for item in items if int(item["id"]) != int(winner["id"]))

        all_ids = {int(row["id"]) for row in rows}
        final_drop_ids = sorted(all_ids - keep_ids)
        total_dropped = len(final_drop_ids)
        backup_table = None

        if final_drop_ids:
            backup_table = _make_backup_table_name(table_name, backup_suffix or datetime.now().strftime("%Y%m%d_%H%M%S"))
            conn.execute(text(f"CREATE TABLE `{backup_table}` LIKE `{table_name}`"))
            conn.execute(text(f"INSERT INTO `{backup_table}` SELECT * FROM `{table_name}`"))
            conn.execute(text(f"DELETE FROM `{table_name}` WHERE id IN ({', '.join(str(i) for i in final_drop_ids)})"))

        rows_after = int(conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`")).scalar() or 0)

    return TaobaoCleanupSummary(
        rows_before=rows_before,
        rows_after=rows_after,
        low_pay_dropped=low_pay_dropped,
        duplicate_dropped=duplicate_dropped,
        total_dropped=total_dropped,
        min_pay_count=min_pay_count,
        similarity_threshold=food_taste_similarity_threshold,
        backup_table=backup_table,
    )


def ingest_taobao_list_dir(
    engine: Engine,
    input_dir: Path,
    file_pattern: str = "detail_items_raw_*.json",
    table_name: str = "taobao_catfood_list_items",
    batch_size: int = 500,
) -> TaobaoIngestSummary:
    if not TABLE_RE.match(table_name):
        raise ValueError(f"Invalid table name: {table_name}")
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    files = sorted(input_dir.glob(file_pattern))
    if not files:
        return TaobaoIngestSummary(
            files=0,
            rows=0,
            ingest_batch_id="",
            cleanup=_empty_cleanup_summary(),
        )

    ensure_taobao_table(engine, table_name=table_name)
    ingest_batch_id = uuid.uuid4().hex[:12]
    console.print(f"[cyan]Ingest batch:[/cyan] {ingest_batch_id}")

    all_rows: List[Dict[str, Any]] = []
    for path in files:
        all_rows.extend(_build_records(path))

    sql = f"""
    INSERT INTO `{table_name}` (
      external_id, keyword, crawl_ts, source_file, rank_no, title, price,
      pay_count_text, pay_count, ship_from, product_url, item_id, site,
      food_taste, net_content, sold_text, sold_count, raw_json
    )
    VALUES (
      :external_id, :keyword, :crawl_ts, :source_file, :rank_no, :title, :price,
      :pay_count_text, :pay_count, :ship_from, :product_url, :item_id, :site,
      :food_taste, :net_content, :sold_text, :sold_count, :raw_json
    )
    ON DUPLICATE KEY UPDATE
      keyword=VALUES(keyword),
      crawl_ts=VALUES(crawl_ts),
      source_file=VALUES(source_file),
      rank_no=VALUES(rank_no),
      title=VALUES(title),
      price=VALUES(price),
      pay_count_text=VALUES(pay_count_text),
      pay_count=VALUES(pay_count),
      ship_from=VALUES(ship_from),
      product_url=VALUES(product_url),
      item_id=VALUES(item_id),
      site=VALUES(site),
      food_taste=VALUES(food_taste),
      net_content=VALUES(net_content),
      sold_text=VALUES(sold_text),
      sold_count=VALUES(sold_count),
      raw_json=VALUES(raw_json);
    """

    total = 0
    for part in _chunked(all_rows, max(1, batch_size)):
        with engine.begin() as conn:
            conn.execute(text(sql), list(part))
        total += len(part)
        console.print(f"[cyan]Processed:[/cyan] {total}")

    cleanup = cleanup_taobao_table(
        engine=engine,
        table_name=table_name,
        min_pay_count=DEFAULT_CLEANUP_MIN_PAY_COUNT,
        food_taste_similarity_threshold=DEFAULT_FOOD_TASTE_SIMILARITY_THRESHOLD,
        backup_suffix=ingest_batch_id,
    )
    console.print(
        f"[cyan]Cleanup:[/cyan] kept {cleanup.rows_after}/{cleanup.rows_before} "
        f"(dropped={cleanup.total_dropped}, low_pay={cleanup.low_pay_dropped}, dup={cleanup.duplicate_dropped})"
    )
    if cleanup.backup_table:
        console.print(f"[cyan]Cleanup backup:[/cyan] {cleanup.backup_table}")

    return TaobaoIngestSummary(
        files=len(files),
        rows=total,
        ingest_batch_id=ingest_batch_id,
        cleanup=cleanup,
    )
