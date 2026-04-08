import os
import re
import json
import shutil
import fnmatch
import time
import threading
import uuid
from pathlib import Path
from typing import Any, Dict, List, Tuple

from flask import Flask, jsonify, request, render_template_string
import pymysql

from scripts import generate_douyin_raw_comments_sql as gen_comments
from scripts import generate_douyin_raw_content_sql as gen_contents
from src.db import make_engine
from src.settings import load_settings
from src.ingest_xhs import ingest_xhs_csv
from src.ingest_brand_issue_ctq import ingest_brand_issue_ctq_dir
from src.ingest_taobao import ingest_taobao_list_dir
from src.extract_catfood import run_catfood_extraction_incremental
from src.ocr_image import run_ocr_image, update_ocr_image_record_path
from src.parse_catfood_guarantee import parse_catfood_guarantee_values
from src.parse_catfood_ocr import parse_catfood_ingredient_ocr_json
from src.parse_catfood_ingredient_types import (
    backfill_catfood_biotic_labels,
    backfill_catfood_fiber_carb_labels,
    backfill_catfood_protein_labels,
    parse_catfood_ingredient_composition_types,
)
from src.parse_taobao_title import parse_taobao_title_standardized

app = Flask(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_A_PATH = str(PROJECT_ROOT / "data" / "incoming")
DEFAULT_B_PATH = str(PROJECT_ROOT / "data")
DEFAULT_DB = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "csv_labeling",
}
DEFAULT_TABLE_COMMENTS = "douyin_raw_comments"
DEFAULT_TABLE_CONTENT = "douyin_raw_content"
DEFAULT_CLUSTER_TABLE = "douyin_raw_comments_last_info_flat_temp"
DEFAULT_TABLE_XHS = "xiaohongshu_raw_comments"
DEFAULT_TABLE_TAOBAO = "taobao_catfood_list_items"
DEFAULT_TABLE_TAOBAO_TITLE_PARSED = "taobao_catfood_title_parsed"
DEFAULT_TABLE_CATFOOD = "catfood_brand_health_candidates"
DEFAULT_TABLE_CATFOOD_STATE = "catfood_brand_health_extract_state"
DEFAULT_TABLE_BRAND_ISSUE_CTQ = "catfood_brand_issue_ctq_cases"
DEFAULT_TABLE_CATFOOD_INGREDIENT = "catfood_ingredient_ocr_results"
DEFAULT_TABLE_CATFOOD_INGREDIENT_PARSED = "catfood_ingredient_ocr_parsed"
DEFAULT_TABLE_PRODUCT_INFO = "product_info"
DEFAULT_TABLE_PRODUCT_GUARANTEE = "product_guarantee"
DEFAULT_TABLE_CATFOOD_FEATURE_SUMMARY = "catfood_feature_summary"
DEFAULT_TABLE_CATFOOD_FEATURE_PROTEIN = "catfood_feature_protein_labels"
DEFAULT_TABLE_CATFOOD_FEATURE_FIBER_CARB = "catfood_feature_fiber_carb_labels"
DEFAULT_TABLE_CATFOOD_FEATURE_BIOTIC = "catfood_feature_biotic_labels"
DEFAULT_CATFOOD_INGREDIENT_HISTORY_DIR = str(PROJECT_ROOT / "history" / "catfood_ingredient_images")
DEFAULT_CATFOOD_GUARANTEE_HISTORY_DIR = str(PROJECT_ROOT / "history" / "catfood_ingredient_images" / "guarantee")
DEFAULT_BRAND_ISSUE_CTQ_DIR = str(PROJECT_ROOT / "data" / "brand_issue_ctq")
DEFAULT_OCR_IMAGE_DIR = str(PROJECT_ROOT / "data")
DEFAULT_OCR_IMAGE_GLOB = "*.jpg,*.jpeg,*.png,*.bmp,*.webp,*.heic,*.heif,*.tif,*.tiff,*.jfif"
DEFAULT_OCR_TABLE = "ocr_image_results"
DEFAULT_CATFOOD_LABEL_ENGINEERING_LIMIT = "0"
DEFAULT_CATFOOD_LABEL_ENGINEERING_CONCURRENCY = "4"

_CATFOOD_LABEL_ENGINEERING_JOBS: Dict[str, Dict[str, Any]] = {}
_CATFOOD_LABEL_ENGINEERING_LOCK = threading.Lock()
_CATFOOD_GUARANTEE_JOBS: Dict[str, Dict[str, Any]] = {}
_CATFOOD_GUARANTEE_LOCK = threading.Lock()

PAGE = """
<!doctype html>
<html lang="zh">
  <head>
    <meta charset="utf-8">
    <title>抖音数据导入工具</title>
    <style>
      body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 32px; color: #111; }
      h1 { margin-bottom: 12px; }
      form { border: 1px solid #ccc; padding: 16px; border-radius: 8px; margin-bottom: 20px; }
      label { display: block; margin: 6px 0 2px; font-weight: 600; }
      input { width: 100%; padding: 8px; box-sizing: border-box; }
      button { margin-top: 12px; padding: 8px 16px; cursor: pointer; }
      .msg { padding: 12px; border-radius: 6px; background: #f6f8fa; margin-bottom: 16px; }
      .error { background: #ffe6e6; color: #900; }
      .success { background: #e6ffed; color: #085b27; }
    </style>
  </head>
  <body>
    <h1>抖音数据导入</h1>
    <p><a href="/cluster">→ 去聚类页面</a> | <a href="/xhs">→ 小红书数据导入</a> | <a href="/catfood_ingredients">→ 猫粮原材料导入</a> | <a href="/taobao">→ 淘宝数据导入</a> | <a href="/catfood_extract">→ 猫粮数据抽取</a> | <a href="/catfood_label_engineering">→ 猫粮标签工程</a></p>
    {% if message %}
      <div class="msg {{ 'error' if is_error else 'success' }}">{{ message }}</div>
    {% endif %}

    <form method="post">
      <input type="hidden" name="action" value="move">
      <h3>功能1：移动 CSV</h3>
      <label>A 路径（源，包含 CSV）</label>
      <input name="a_path" value="{{ a_path }}">
      <label>B 路径（目标）</label>
      <input name="b_path" value="{{ b_path }}">
      <button type="submit">把 A 路径下的 CSV 移到 B 路径</button>
    </form>

    <form method="post">
      <input type="hidden" name="action" value="import">
      <h3>功能2：B 路径数据入库</h3>
      <label>B 路径（search_comments_* / search_contents_* 所在目录）</label>
      <input name="b_path" value="{{ b_path }}">
      <label>评论目标表名</label>
      <input name="comment_table" value="{{ comment_table }}">
      <label>内容目标表名</label>
      <input name="content_table" value="{{ content_table }}">
      <label>MySQL Host</label>
      <input name="db_host" value="{{ db.host }}">
      <label>MySQL Port</label>
      <input name="db_port" value="{{ db.port }}">
      <label>MySQL User</label>
      <input name="db_user" value="{{ db.user }}">
      <label>MySQL Password</label>
      <input name="db_password" value="{{ db.password }}">
      <label>MySQL Database</label>
      <input name="db_name" value="{{ db.database }}">
      <button type="submit">导入 B 路径数据到数据库</button>
    </form>

    <form method="post">
      <input type="hidden" name="action" value="filter_comments">
      <h3>功能3：按关键词过滤 douyin_raw_comments 生成临时表</h3>
      <label>search_keyword（多个用逗号分隔）</label>
      <input name="filter_keyword" value="{{ filter_keyword }}">
      <label>临时表名</label>
      <input name="filter_tmp_table" value="{{ filter_tmp_table }}">
      <label>MySQL Host</label>
      <input name="db_host" value="{{ db.host }}">
      <label>MySQL Port</label>
      <input name="db_port" value="{{ db.port }}">
      <label>MySQL User</label>
      <input name="db_user" value="{{ db.user }}">
      <label>MySQL Password</label>
      <input name="db_password" value="{{ db.password }}">
      <label>MySQL Database</label>
      <input name="db_name" value="{{ db.database }}">
      <button type="submit">生成临时表</button>
    </form>

    <form method="post">
      <input type="hidden" name="action" value="ocr_images_batch">
      <h3>功能4：目录图片批量 OCR 并写入数据库</h3>
      <label>图片目录地址</label>
      <input name="ocr_image_dir" value="{{ ocr_image_dir }}">
      <label>图片匹配规则（逗号分隔）</label>
      <input name="ocr_image_glob" value="{{ ocr_image_glob }}">
      <div style="color:#555;font-size:13px;margin-top:6px;">会递归扫描子目录中的图片。</div>
      <label>OCR 结果表名</label>
      <input name="ocr_table" value="{{ ocr_table }}">
      <label>MySQL Host</label>
      <input name="db_host" value="{{ db.host }}">
      <label>MySQL Port</label>
      <input name="db_port" value="{{ db.port }}">
      <label>MySQL User</label>
      <input name="db_user" value="{{ db.user }}">
      <label>MySQL Password</label>
      <input name="db_password" value="{{ db.password }}">
      <label>MySQL Database</label>
      <input name="db_name" value="{{ db.database }}">
      <button type="submit">开始批量 OCR 入库</button>
    </form>
  </body>
</html>
"""


def _run_sql_file(conn, sql_path: Path) -> List[str]:
    logs: List[str] = []
    text = sql_path.read_text(encoding="utf-8")
    statements = []
    buf = []
    for line in text.splitlines():
        buf.append(line)
        if line.strip().endswith(";"):
            stmt = "\n".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
    if buf:
        stmt = "\n".join(buf).strip()
        if stmt:
            statements.append(stmt)

    with conn.cursor() as cur:
        for idx, stmt in enumerate(statements, 1):
            cur.execute(stmt)
            logs.append(f"Executed {idx}/{len(statements)}")
    return logs


def _run_sql_text(conn, sql_text: str) -> List[str]:
    """执行包含多个语句的 SQL 文本。"""
    logs: List[str] = []
    statements = []
    buf = []
    for line in sql_text.splitlines():
        buf.append(line)
        if line.strip().endswith(";"):
            stmt = "\n".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
    if buf:
        stmt = "\n".join(buf).strip()
        if stmt:
            statements.append(stmt)
    with conn.cursor() as cur:
        for idx, stmt in enumerate(statements, 1):
            cur.execute(stmt)
            logs.append(f"Executed {idx}/{len(statements)}")
    return logs


def _move_csv(a_path: Path, b_path: Path) -> int:
    if not a_path.exists():
        raise FileNotFoundError(f"源路径不存在: {a_path}")
    project_root = Path(__file__).resolve().parent
    data_path = project_root / "data"
    history_path = project_root / "history"
    if data_path.exists():
        history_path.mkdir(parents=True, exist_ok=True)
        for existing_file in data_path.iterdir():
            if not existing_file.is_file():
                continue
            target = history_path / existing_file.name
            if target.exists():
                target.unlink()
            shutil.move(str(existing_file), str(target))

    b_path.mkdir(parents=True, exist_ok=True)
    moved = 0
    for csv_file in a_path.glob("*.csv"):
        target = b_path / csv_file.name
        if target.exists():
            target.unlink()
        shutil.move(str(csv_file), str(target))
        moved += 1
    return moved


def _import_data(b_path: Path, db_cfg: dict, comment_table: str, content_table: str) -> str:
    # 生成 comments SQL
    comments_records = gen_comments.load_records(data_dir=b_path)
    comments_sql_path = Path("sql") / f"{comment_table}.sql"
    gen_comments.write_clean_csv(comments_records)
    gen_comments.write_sql(comments_records, out_sql=comments_sql_path, table_name=comment_table)

    # 生成 contents SQL
    content_records = gen_contents.load_records(data_dir=b_path)
    contents_sql_path = Path("sql") / f"{content_table}.sql"
    gen_contents.write_clean_csv(content_records)
    gen_contents.write_sql(content_records, out_sql=contents_sql_path, table_name=content_table)

    conn = pymysql.connect(
        host=db_cfg["host"],
        port=int(db_cfg["port"]),
        user=db_cfg["user"],
        password=db_cfg["password"],
        database=db_cfg["database"],
        charset="utf8mb4",
        autocommit=True,
    )
    try:
        logs = []
        logs += _run_sql_file(conn, comments_sql_path)
        logs += _run_sql_file(conn, contents_sql_path)
    finally:
        conn.close()
    return f"导入完成：comments {len(comments_records)} 行，content {len(content_records)} 行；SQL 执行 {len(logs)} 次。"


def _run_sql_text(conn, sql: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql)


def _create_filtered_comments_table(db_cfg: dict, search_keyword: str, tmp_table: str) -> str:
    """按固定条件过滤 douyin_raw_comments，生成临时表。"""
    if not search_keyword:
        raise ValueError("search_keyword 不能为空")
    keywords = [kw.strip() for kw in re.split(r"[,，\n\r]+", search_keyword) if kw.strip()]
    if not keywords:
        raise ValueError("search_keyword 不能为空")
    tmp_table = _safe_table_name(tmp_table)
    base_table = _safe_table_name(DEFAULT_TABLE_COMMENTS)

    kw_placeholders = ", ".join(["%s"] * len(keywords))
    conditions = [
        f"search_keyword IN ({kw_placeholders})",
        "CHAR_LENGTH(comment_text) > 6",
        "CHAR_LENGTH(comment_text) < 30",
        "comment_text NOT LIKE %s",
        "comment_text NOT REGEXP %s",
        "comment_text NOT LIKE %s",
        "comment_text NOT LIKE %s",
        "comment_text NOT LIKE %s",
        "comment_text NOT LIKE %s",
        "comment_text NOT LIKE %s",
        "comment_text NOT LIKE %s",
        "comment_text NOT LIKE %s",
        "comment_date > %s",
        "search_keyword NOT IN (%s, %s)",
        "comment_text NOT LIKE %s",
        "comment_text NOT LIKE %s",
        "comment_text NOT LIKE %s",
        "comment_text NOT LIKE %s",
    ]
    params = [
        *keywords,
        "%@%",
        r"^\[[^\]]+\](\[[^\]]+\])*$",
        "%谢%",
        "%同款%",
        "%感谢支持[比心]%",
        "%哈哈%",
        "%链接%",
        "%下单%",
        "%想要%",
        "2024-01-01",
        "小户型灶台收纳",
        "厨房台面收纳",
        "%关注%",
        "%材料包%",
        "%哪里%",
        "%买%",
    ]

    conn = pymysql.connect(
        host=db_cfg["host"],
        port=int(db_cfg["port"]),
        user=db_cfg["user"],
        password=db_cfg["password"],
        database=db_cfg["database"],
        charset="utf8mb4",
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {tmp_table}")
            sql = f"""
                CREATE TABLE {tmp_table} AS
                SELECT *
                FROM {base_table}
                WHERE {" AND ".join(conditions)}
            """
            cur.execute(sql, params)
            cur.execute(
                f"""
                UPDATE {tmp_table}
                SET ingest_ts = NOW()
                WHERE ingest_ts IS NULL OR ingest_ts < '1970-01-02 00:00:00'
                """
            )
            cur.execute(f"SELECT COUNT(*) FROM {tmp_table}")
            total = cur.fetchone()[0]
        return f"已创建临时表 {tmp_table}，共 {total} 条"
    finally:
        conn.close()


def _safe_table_name(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        raise ValueError("表名仅允许字母、数字和下划线")
    return name


def _inspect_table_columns(db_cfg: dict, table: str):
    """返回 (id_column, content_column)，content 优先 extracted_json > product_entities_tags > product_entities > comment_text。"""
    table = _safe_table_name(table)
    conn = pymysql.connect(
        host=db_cfg["host"],
        port=int(db_cfg["port"]),
        user=db_cfg["user"],
        password=db_cfg["password"],
        database=db_cfg["database"],
        charset="utf8mb4",
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COLUMN_NAME
                FROM information_schema.columns
                WHERE table_schema=%s AND table_name=%s
                """,
                (db_cfg["database"], table),
            )
            cols = {row[0] for row in cur.fetchall()}
    finally:
        conn.close()

    id_col = "pre_product_id" if "pre_product_id" in cols else "id"
    content_col = None
    for c in ["extracted_json", "product_entities_tags", "product_entities", "comment_text"]:
        if c in cols:
            content_col = c
            break
    if content_col is None:
        content_col = "comment_text"
    return id_col, content_col


def _fetch_extracted(
    db_cfg: dict,
    table: str,
    limit: int,
    search_words: str = "",
    comment_ip: str = "",
) -> List[Tuple[int, str]]:
    table = _safe_table_name(table)
    id_col, content_col = _inspect_table_columns(db_cfg, table)
    conn = pymysql.connect(
        host=db_cfg["host"],
        port=int(db_cfg["port"]),
        user=db_cfg["user"],
        password=db_cfg["password"],
        database=db_cfg["database"],
        charset="utf8mb4",
        autocommit=True,
    )
    rows: List[Tuple[int, str]] = []
    try:
        with conn.cursor() as cur:
            where = []
            params = []
            if search_words:
                where.append("search_words = %s")
                params.append(search_words)
            if comment_ip:
                where.append("comment_ip = %s")
                params.append(comment_ip)
            where_sql = ("WHERE " + " AND ".join(where)) if where else ""
            sql = f"""
                SELECT {id_col} AS row_id, {content_col} AS payload
                FROM {table}
                {where_sql}
                ORDER BY {id_col} DESC
                LIMIT %s
            """
            params.append(limit)
            cur.execute(sql, params)
            for rid, payload in cur.fetchall():
                text = ""
                if payload is None:
                    text = ""
                else:
                    try:
                        obj = json.loads(payload)
                        if isinstance(obj, dict):
                            text = json.dumps(obj, ensure_ascii=False)
                        elif isinstance(obj, list):
                            text = json.dumps(obj, ensure_ascii=False)
                        else:
                            text = str(obj)
                    except Exception:
                        text = str(payload)
                if text:
                    rows.append((rid, text))
    finally:
        conn.close()
    return rows


def _tokenize(text: str) -> set:
    # crude tokenization: words/numbers or single CJK chars
    words = re.findall(r"[A-Za-z0-9]+", text.lower())
    chars = [ch for ch in text if "\u4e00" <= ch <= "\u9fff"]
    return set(words + chars)


def _jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def cluster_texts(items: List[Tuple[int, str]], threshold: float = 0.35) -> List[dict]:
    clusters: List[dict] = []
    for rid, text in items:
        toks = _tokenize(text)
        if not toks:
            continue
        best_idx = -1
        best_sim = 0.0
        for idx, cl in enumerate(clusters):
            sim = _jaccard(toks, cl["tokens"])
            if sim > best_sim:
                best_sim = sim
                best_idx = idx
        if best_sim >= threshold and best_idx >= 0:
            clusters[best_idx]["items"].append((rid, text))
        else:
            clusters.append({"tokens": toks, "items": [(rid, text)]})
    clusters.sort(key=lambda c: len(c["items"]), reverse=True)
    # Prepare summary
    summary = []
    for cl in clusters:
        summary.append(
            {
                "size": len(cl["items"]),
                "samples": cl["items"][:5],
            }
        )
    return summary


def _ingest_xhs_data(
    b_path: Path,
    db_cfg: dict,
    source_name: str,
    csv_pattern: str,
    fallback_keyword: str,
    encoding: str,
    batch_size: int,
) -> str:
    if not b_path.exists():
        raise FileNotFoundError(f"路径不存在: {b_path}")
    csv_files = sorted(b_path.glob(csv_pattern))
    if not csv_files:
        raise FileNotFoundError(f"未找到匹配的 CSV: {csv_pattern}")

    engine = make_engine(db_cfg)
    total_rows = 0
    batches = []
    for csv_file in csv_files:
        n, ingest_batch_id = ingest_xhs_csv(
            engine,
            csv_path=csv_file,
            source_name=source_name,
            fallback_keyword=fallback_keyword or None,
            encoding=encoding,
            batch_size=batch_size,
        )
        total_rows += n
        batches.append(f"{csv_file.name}:{ingest_batch_id}")
    detail = "; ".join(batches)
    return f"导入完成：{len(csv_files)} 个文件，共 {total_rows} 行。batch: {detail}"

def _ingest_taobao_data(
    input_dir: Path,
    db_cfg: dict,
    file_pattern: str,
    table_name: str,
    batch_size: int,
) -> str:
    if not input_dir.exists():
        raise FileNotFoundError(f"路径不存在: {input_dir}")
    engine = make_engine(db_cfg)
    result = ingest_taobao_list_dir(
        engine=engine,
        input_dir=input_dir,
        file_pattern=file_pattern,
        table_name=table_name,
        batch_size=batch_size,
    )
    if result.files == 0:
        return f"未找到匹配文件：{file_pattern}"
    parts = [
        f"导入完成：{result.files} 个文件，共 {result.rows} 行。batch: {result.ingest_batch_id}",
        (
            f"清洗后保留 {result.cleanup.rows_after}/{result.cleanup.rows_before} 行，"
            f"删除 {result.cleanup.total_dropped} 行"
            f"（pay_count<{result.cleanup.min_pay_count}: {result.cleanup.low_pay_dropped}，"
            f"food_taste 去重: {result.cleanup.duplicate_dropped}）"
        ),
    ]
    if result.cleanup.backup_table:
        parts.append(f"备份表：{result.cleanup.backup_table}")
    return "；".join(parts)

def _ingest_brand_issue_ctq_data(
    input_dir: Path,
    db_cfg: dict,
    file_pattern: str,
    table_name: str,
    parsed_table: str,
    batch_size: int,
) -> str:
    if not input_dir.exists():
        raise FileNotFoundError(f"路径不存在: {input_dir}")
    engine = make_engine(db_cfg)
    result = ingest_brand_issue_ctq_dir(
        engine=engine,
        input_dir=input_dir,
        file_pattern=file_pattern,
        table_name=table_name,
        parsed_table=parsed_table,
        batch_size=batch_size,
    )
    if result.files == 0:
        return f"未找到匹配文件：{file_pattern}"
    return (
        f"导入完成：{result.files} 个文件，扫描 {result.sheets} 个 sheet，"
        f"写入 {result.rows} 行；table={table_name}；"
        f"品牌对齐源表={parsed_table}；batch={result.batch_id}"
    )

def _extract_catfood_data(
    db_cfg: dict,
    target_table: str,
    state_table: str,
) -> str:
    engine = make_engine(db_cfg)
    res = run_catfood_extraction_incremental(
        engine=engine,
        target_table=target_table,
        state_table=state_table,
    )
    return (
        f"抽取完成：新增 {res.inserted_rows} 行；"
        f"扫描 douyin={res.scanned_douyin_rows}, xhs={res.scanned_xhs_rows}；"
        f"batch={res.batch_id}"
    )


def _parse_taobao_title_data(
    db_cfg: dict,
    source_table: str,
    target_table: str,
    limit: int,
) -> str:
    engine = make_engine(db_cfg)
    res = parse_taobao_title_standardized(
        engine=engine,
        source_table=source_table,
        target_table=target_table,
        limit=limit,
    )
    return (
        f"标准化抽取完成：扫描 {res.scanned} 条，写入 {res.upserted} 条；"
        f"source={res.source_table} -> target={res.target_table}；batch={res.batch_id}"
    )


def _collect_image_files(image_dir: Path, patterns_text: str) -> List[Path]:
    if not image_dir.exists():
        raise FileNotFoundError(f"图片目录不存在: {image_dir}")
    if not image_dir.is_dir():
        raise NotADirectoryError(f"路径不是目录: {image_dir}")

    patterns = [p.strip() for p in re.split(r"[,，\n\r]+", patterns_text or "") if p.strip()]
    if not patterns:
        patterns = ["*.jpg", "*.jpeg", "*.png", "*.bmp", "*.webp", "*.heic", "*.heif", "*.tif", "*.tiff", "*.jfif"]
    patterns_lower = [p.lower() for p in patterns]

    uniq = {}
    for f in image_dir.rglob("*"):
        if not f.is_file():
            continue
        rel = str(f.relative_to(image_dir)).replace("\\", "/").lower()
        name = f.name.lower()
        matched = any(
            fnmatch.fnmatch(name, p) or fnmatch.fnmatch(rel, p)
            for p in patterns_lower
        )
        if matched:
            uniq[str(f.resolve())] = f
    files = [uniq[k] for k in sorted(uniq.keys())]
    return files


def _unique_target_path(path: Path) -> Path:
    if not path.exists():
        return path
    parent = path.parent
    stem = path.stem
    suffix = path.suffix
    idx = 1
    while True:
        cand = parent / f"{stem}_{idx}{suffix}"
        if not cand.exists():
            return cand
        idx += 1


def _move_to_history(image_path: Path, source_root: Path, history_root: Path) -> Path:
    history_root.mkdir(parents=True, exist_ok=True)
    try:
        rel = image_path.resolve().relative_to(source_root.resolve())
        target = history_root / rel
    except Exception:
        target = history_root / image_path.name
    target.parent.mkdir(parents=True, exist_ok=True)
    target = _unique_target_path(target)
    shutil.move(str(image_path), str(target))
    return target


def _batch_ocr_images(
    image_dir: Path,
    db_cfg: dict,
    image_glob: str,
    table_name: str,
) -> str:
    files = _collect_image_files(image_dir, image_glob)
    if not files:
        raise FileNotFoundError(f"目录中未找到匹配图片: {image_glob}")

    st = load_settings()
    ocr_cfg = st.ocr
    openai_cfg = st.openai
    engine = make_engine(db_cfg)
    history_dir = Path(DEFAULT_CATFOOD_INGREDIENT_HISTORY_DIR)

    ok = 0
    failed: List[str] = []
    for idx, image_path in enumerate(files, start=1):
        if idx > 1:
            time.sleep(1.5)
        try:
            result = run_ocr_image(
                engine=engine,
                image_path=image_path,
                out_json_path=None,
                table_name=table_name,
                ocr_cfg=ocr_cfg,
                openai_cfg=openai_cfg,
            )
            moved_path = _move_to_history(image_path=image_path, source_root=image_dir, history_root=history_dir)
            update_ocr_image_record_path(
                engine=engine,
                table_name=table_name,
                file_sha256=result["file_sha256"],
                image_path=moved_path,
            )
            ok += 1
        except Exception as exc:
            failed.append(f"{image_path.name}: {exc}")

    if not failed:
        return (
            f"批量 OCR 完成：共 {len(files)} 张，成功 {ok} 张，全部已写入 {table_name}；"
            f"图片已移动到 {history_dir}"
        )

    head = "；".join(failed[:5])
    extra = f"；其余 {len(failed) - 5} 张失败" if len(failed) > 5 else ""
    moved_note = f"；成功图片已移动到 {history_dir}" if ok > 0 else ""
    return (
        f"批量 OCR 完成：共 {len(files)} 张，成功 {ok} 张，失败 {len(failed)} 张。"
        f"失败示例：{head}{extra}{moved_note}"
    )


def _parse_catfood_ingredient_ocr(
    db_cfg: dict,
    source_table: str,
    target_table: str,
    limit: int,
) -> str:
    engine = make_engine(db_cfg)
    res = parse_catfood_ingredient_ocr_json(
        engine=engine,
        source_table=source_table,
        target_table=target_table,
        limit=limit,
    )
    return (
        f"解析完成：扫描 {res.scanned} 条，写入 {res.upserted} 条；"
        f"source={res.source_table} -> target={res.target_table}；batch={res.batch_id}"
    )


def _parse_catfood_guarantee_data(
    db_cfg: dict,
    limit: int,
) -> str:
    st = load_settings()
    engine = make_engine(db_cfg)
    res = parse_catfood_guarantee_values(
        engine=engine,
        openai_cfg=st.openai,
        ocr_cfg=st.ocr,
        source_table=DEFAULT_TABLE_CATFOOD_INGREDIENT,
        parsed_table=DEFAULT_TABLE_CATFOOD_INGREDIENT_PARSED,
        info_table=DEFAULT_TABLE_PRODUCT_INFO,
        guarantee_table=DEFAULT_TABLE_PRODUCT_GUARANTEE,
        limit=limit,
        processed_dir=Path(DEFAULT_CATFOOD_GUARANTEE_HISTORY_DIR),
    )
    return _format_catfood_guarantee_message(res)


def _format_catfood_guarantee_message(res: Any) -> str:
    error_note = f"；示例错误：{' | '.join(res.error_samples)}" if getattr(res, "error_samples", None) else ""
    return (
        f"保证值解析完成：扫描 {res.scanned} 条，成功 {res.succeeded} 条，"
        f"空保证值 {res.empty_guarantees} 条，写入 guarantee {res.guarantee_rows} 条，失败 {res.failed} 条；"
        f"处理成功图片已移动到 {Path(DEFAULT_CATFOOD_GUARANTEE_HISTORY_DIR)}；"
        f"源表={res.source_table}；parsed 表={res.parsed_table}；写入表={res.guarantee_table}；"
        f"batch={res.batch_id}"
        f"{error_note}"
    )


def _parse_catfood_ingredient_types_data(
    db_cfg: dict,
    source_table: str,
    summary_table: str,
    protein_table: str,
    fiber_carb_table: str,
    biotic_table: str,
    limit: int,
) -> str:
    st = load_settings()
    engine = make_engine(db_cfg)
    res = parse_catfood_ingredient_composition_types(
        engine=engine,
        openai_cfg=st.openai,
        source_table=source_table,
        target_table=summary_table,
        protein_table=protein_table,
        fiber_carb_table=fiber_carb_table,
        biotic_table=biotic_table,
        limit=limit,
    )
    return (
        f"成分特征拆分完成：扫描 {res.scanned} 条，汇总写入 {res.upserted} 条，"
        f"蛋白明细 {res.protein_rows} 条，纤维/碳水明细 {res.fiber_carb_rows} 条，益生元/益生菌明细 {res.biotic_rows} 条；"
        f"source={res.source_table} -> summary={res.target_table}；batch={res.batch_id}"
    )


def _set_catfood_guarantee_job(job_id: str, **payload: Any) -> Dict[str, Any]:
    with _CATFOOD_GUARANTEE_LOCK:
        job = _CATFOOD_GUARANTEE_JOBS.setdefault(job_id, {"job_id": job_id})
        job.update(payload)
        return dict(job)


def _get_catfood_guarantee_job(job_id: str) -> Dict[str, Any] | None:
    with _CATFOOD_GUARANTEE_LOCK:
        job = _CATFOOD_GUARANTEE_JOBS.get(job_id)
        return dict(job) if job else None


def _run_catfood_guarantee_job(
    job_id: str,
    db_cfg: dict,
    source_table: str,
    parsed_table: str,
    info_table: str,
    guarantee_table: str,
    limit: int,
) -> None:
    started_at = time.time()
    _set_catfood_guarantee_job(
        job_id,
        status="running",
        started_at=started_at,
        finished_at=None,
        source_table=source_table,
        parsed_table=parsed_table,
        info_table=info_table,
        guarantee_table=guarantee_table,
        limit=limit,
        total_rows=0,
        processed_rows=0,
        succeeded=0,
        failed=0,
        empty_guarantees=0,
        guarantee_rows=0,
        current_source_id=None,
        current_image_name=None,
        batch_id=None,
        error=None,
        message="保证值解析已启动。",
    )
    engine = None
    try:
        st = load_settings()
        engine = make_engine(db_cfg)

        def _progress(payload: Dict[str, Any]) -> None:
            _set_catfood_guarantee_job(job_id, **payload)

        res = parse_catfood_guarantee_values(
            engine=engine,
            openai_cfg=st.openai,
            ocr_cfg=st.ocr,
            source_table=source_table,
            parsed_table=parsed_table,
            info_table=info_table,
            guarantee_table=guarantee_table,
            limit=limit,
            processed_dir=Path(DEFAULT_CATFOOD_GUARANTEE_HISTORY_DIR),
            progress_callback=_progress,
        )
        _set_catfood_guarantee_job(
            job_id,
            status="completed",
            finished_at=time.time(),
            scanned=res.scanned,
            total_rows=res.scanned,
            processed_rows=res.scanned,
            succeeded=res.succeeded,
            failed=res.failed,
            empty_guarantees=res.empty_guarantees,
            guarantee_rows=res.guarantee_rows,
            batch_id=res.batch_id,
            error=" | ".join(res.error_samples) if res.error_samples else None,
            message=_format_catfood_guarantee_message(res),
        )
    except Exception as exc:
        _set_catfood_guarantee_job(
            job_id,
            status="error",
            finished_at=time.time(),
            error=str(exc),
            message=f"保证值解析失败：{exc}",
        )
    finally:
        if engine is not None:
            engine.dispose()


def _set_catfood_label_job(job_id: str, **payload: Any) -> Dict[str, Any]:
    with _CATFOOD_LABEL_ENGINEERING_LOCK:
        job = _CATFOOD_LABEL_ENGINEERING_JOBS.setdefault(job_id, {"job_id": job_id})
        job.update(payload)
        return dict(job)


def _get_catfood_label_job(job_id: str) -> Dict[str, Any] | None:
    with _CATFOOD_LABEL_ENGINEERING_LOCK:
        job = _CATFOOD_LABEL_ENGINEERING_JOBS.get(job_id)
        return dict(job) if job else None


def _run_catfood_label_engineering_job(
    job_id: str,
    db_cfg: dict,
    feature_type: str,
    source_table: str,
    target_table: str,
    limit: int,
    concurrency: int,
) -> None:
    feature_label_map = {
        "protein": "蛋白标签工程",
        "fiber_carb": "纤维/碳水标签工程",
        "biotic": "益生元/益生菌标签工程",
    }
    feature_label = feature_label_map.get(feature_type, "猫粮标签工程")
    started_at = time.time()
    _set_catfood_label_job(
        job_id,
        status="running",
        started_at=started_at,
        finished_at=None,
        feature_type=feature_type,
        feature_label=feature_label,
        source_table=source_table,
        target_table=target_table,
        limit=limit,
        concurrency=concurrency,
        message=f"{feature_label}已启动。",
        unique_total=0,
        unique_done=0,
        written=0,
        batch_id=None,
        error=None,
    )
    engine = None
    try:
        st = load_settings()
        engine = make_engine(db_cfg)

        def _progress(payload: Dict[str, Any]) -> None:
            _set_catfood_label_job(job_id, **payload)

        if feature_type == "protein":
            res = backfill_catfood_protein_labels(
                engine=engine,
                openai_cfg=st.openai,
                source_table=source_table,
                protein_table=target_table,
                limit=limit,
                concurrency=concurrency,
                progress_callback=_progress,
            )
        elif feature_type == "fiber_carb":
            res = backfill_catfood_fiber_carb_labels(
                engine=engine,
                openai_cfg=st.openai,
                source_table=source_table,
                fiber_carb_table=target_table,
                limit=limit,
                concurrency=concurrency,
                progress_callback=_progress,
            )
        elif feature_type == "biotic":
            res = backfill_catfood_biotic_labels(
                engine=engine,
                openai_cfg=st.openai,
                source_table=source_table,
                biotic_table=target_table,
                limit=limit,
                concurrency=concurrency,
                progress_callback=_progress,
            )
        else:
            raise ValueError(f"未知标签工程类型: {feature_type}")
        _set_catfood_label_job(
            job_id,
            status="completed",
            finished_at=time.time(),
            scanned=res.scanned,
            upserted=res.upserted,
            batch_id=res.batch_id,
            message=f"{feature_label}完成：扫描 {res.scanned} 条，写入 {res.upserted} 条；batch={res.batch_id}",
        )
    except Exception as exc:
        _set_catfood_label_job(
            job_id,
            status="error",
            finished_at=time.time(),
            error=str(exc),
            message=f"{feature_label}失败：{exc}",
        )
    finally:
        if engine is not None:
            engine.dispose()


@app.route("/", methods=["GET", "POST"])
def home():
    message = ""
    is_error = False
    a_path = request.form.get("a_path", DEFAULT_A_PATH)
    b_path = request.form.get("b_path", DEFAULT_B_PATH)
    db = {
        "host": request.form.get("db_host", DEFAULT_DB["host"]),
        "port": request.form.get("db_port", DEFAULT_DB["port"]),
        "user": request.form.get("db_user", DEFAULT_DB["user"]),
        "password": request.form.get("db_password", DEFAULT_DB["password"]),
        "database": request.form.get("db_name", DEFAULT_DB["database"]),
    }
    comment_table = request.form.get("comment_table", DEFAULT_TABLE_COMMENTS)
    content_table = request.form.get("content_table", DEFAULT_TABLE_CONTENT)
    filter_keyword = request.form.get("filter_keyword", "")
    filter_tmp_table = request.form.get("filter_tmp_table", "douyin_raw_comments_tmp")
    ocr_image_dir = request.form.get("ocr_image_dir", DEFAULT_OCR_IMAGE_DIR)
    ocr_image_glob = request.form.get("ocr_image_glob", DEFAULT_OCR_IMAGE_GLOB)
    ocr_table = request.form.get("ocr_table", DEFAULT_OCR_TABLE)

    if request.method == "POST":
        action = request.form.get("action")
        try:
            if action == "move":
                moved = _move_csv(Path(a_path), Path(b_path))
                message = f"已移动 {moved} 个 CSV 文件到 {b_path}"
            elif action == "import":
                message = _import_data(Path(b_path), db, comment_table, content_table)
            elif action == "filter_comments":
                message = _create_filtered_comments_table(db, filter_keyword, filter_tmp_table)
            elif action == "ocr_images_batch":
                message = _batch_ocr_images(
                    image_dir=Path(ocr_image_dir),
                    db_cfg=db,
                    image_glob=ocr_image_glob,
                    table_name=ocr_table,
                )
            else:
                message = "未知操作"
        except Exception as exc:
            is_error = True
            message = f"操作失败：{exc}"

    return render_template_string(
        PAGE,
        message=message,
        is_error=is_error,
        a_path=a_path,
        b_path=b_path,
        db=db,
        comment_table=comment_table,
        content_table=content_table,
        filter_keyword=filter_keyword,
        filter_tmp_table=filter_tmp_table,
        ocr_image_dir=ocr_image_dir,
        ocr_image_glob=ocr_image_glob,
        ocr_table=ocr_table,
    )


XHS_PAGE = """
<!doctype html>
<html lang="zh">
  <head>
    <meta charset="utf-8">
    <title>小红书数据导入</title>
    <style>
      body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 32px; color: #111; }
      h1 { margin-bottom: 12px; }
      form { border: 1px solid #ccc; padding: 16px; border-radius: 8px; margin-bottom: 20px; }
      label { display: block; margin: 6px 0 2px; font-weight: 600; }
      input { width: 100%; padding: 8px; box-sizing: border-box; }
      button { margin-top: 12px; padding: 8px 16px; cursor: pointer; }
      .msg { padding: 12px; border-radius: 6px; background: #f6f8fa; margin-bottom: 16px; }
      .error { background: #ffe6e6; color: #900; }
      .success { background: #e6ffed; color: #085b27; }
      .note { color: #555; font-size: 13px; margin-top: 6px; }
    </style>
  </head>
  <body>
    <h1>小红书数据导入</h1>
    <p><a href="/">← 返回抖音导入</a> | <a href="/cluster">→ 去聚类页面</a> | <a href="/catfood_ingredients">→ 猫粮原材料导入</a> | <a href="/taobao">→ 淘宝数据导入</a> | <a href="/catfood_extract">→ 猫粮数据抽取</a></p>
    {% if message %}
      <div class="msg {{ 'error' if is_error else 'success' }}">{{ message }}</div>
    {% endif %}

    <form method="post">
      <h3>功能：导入 B 路径 CSV 到 {{ xhs_table }}</h3>
      <label>B 路径（CSV 目录）</label>
      <input name="b_path" value="{{ b_path }}">
      <label>CSV 匹配规则</label>
      <input name="csv_pattern" value="{{ csv_pattern }}">
      <div class="note">默认匹配 xiaohongshu_*_data.csv</div>
      <label>source_name</label>
      <input name="source_name" value="{{ source_name }}">
      <label>检索词兜底（可选）</label>
      <input name="keyword" value="{{ keyword }}">
      <label>CSV 编码</label>
      <input name="encoding" value="{{ encoding }}">
      <label>批量写入大小</label>
      <input name="batch_size" value="{{ batch_size }}">
      <label>MySQL Host</label>
      <input name="db_host" value="{{ db.host }}">
      <label>MySQL Port</label>
      <input name="db_port" value="{{ db.port }}">
      <label>MySQL User</label>
      <input name="db_user" value="{{ db.user }}">
      <label>MySQL Password</label>
      <input name="db_password" value="{{ db.password }}">
      <label>MySQL Database</label>
      <input name="db_name" value="{{ db.database }}">
      <button type="submit">导入小红书数据</button>
    </form>
  </body>
</html>
"""


@app.route("/xhs", methods=["GET", "POST"])
def xhs_view():
    message = ""
    is_error = False
    b_path = request.form.get("b_path", DEFAULT_B_PATH)
    csv_pattern = request.form.get("csv_pattern", "xiaohongshu_*_data.csv")
    source_name = request.form.get("source_name", "xiaohongshu")
    keyword = request.form.get("keyword", "")
    encoding = request.form.get("encoding", "utf-8-sig")
    batch_size = request.form.get("batch_size", "2000")
    db = {
        "host": request.form.get("db_host", DEFAULT_DB["host"]),
        "port": request.form.get("db_port", DEFAULT_DB["port"]),
        "user": request.form.get("db_user", DEFAULT_DB["user"]),
        "password": request.form.get("db_password", DEFAULT_DB["password"]),
        "database": request.form.get("db_name", DEFAULT_DB["database"]),
    }

    if request.method == "POST":
        try:
            message = _ingest_xhs_data(
                Path(b_path),
                db,
                source_name=source_name,
                csv_pattern=csv_pattern,
                fallback_keyword=keyword,
                encoding=encoding,
                batch_size=int(batch_size),
            )
        except Exception as exc:
            is_error = True
            message = f"操作失败：{exc}"

    return render_template_string(
        XHS_PAGE,
        message=message,
        is_error=is_error,
        b_path=b_path,
        csv_pattern=csv_pattern,
        source_name=source_name,
        keyword=keyword,
        encoding=encoding,
        batch_size=batch_size,
        db=db,
        xhs_table=DEFAULT_TABLE_XHS,
    )


CATFOOD_INGREDIENT_PAGE = """
<!doctype html>
<html lang="zh">
  <head>
    <meta charset="utf-8">
    <title>猫粮原材料导入</title>
    <style>
      body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 32px; color: #111; }
      h1 { margin-bottom: 12px; }
      form { border: 1px solid #ccc; padding: 16px; border-radius: 8px; margin-bottom: 20px; }
      label { display: block; margin: 6px 0 2px; font-weight: 600; }
      input { width: 100%; padding: 8px; box-sizing: border-box; }
      button { margin-top: 12px; padding: 8px 16px; cursor: pointer; }
      .msg { padding: 12px; border-radius: 6px; background: #f6f8fa; margin-bottom: 16px; }
      .error { background: #ffe6e6; color: #900; }
      .success { background: #e6ffed; color: #085b27; }
      .note { color: #555; font-size: 13px; margin-top: 6px; }
      .job { border: 1px solid #ddd; padding: 16px; border-radius: 8px; background: #fafafa; }
      .job-row { margin: 6px 0; }
      progress { width: 100%; height: 20px; margin-top: 8px; }
      code { background: #f3f4f6; padding: 2px 4px; border-radius: 4px; }
    </style>
  </head>
  <body>
    <h1>猫粮原材料导入</h1>
    <p><a href="/">← 返回抖音导入</a> | <a href="/xhs">→ 小红书数据导入</a> | <a href="/taobao">→ 淘宝数据导入</a> | <a href="/catfood_extract">→ 猫粮数据抽取</a></p>
    {% if message %}
      <div class="msg {{ 'error' if is_error else 'success' }}">{{ message }}</div>
    {% endif %}

    <form method="post">
      <input type="hidden" name="action" value="ocr_import">
      <h3>功能：导入原材料图片并 OCR 入库到 {{ ingredient_table }}</h3>
      <label>输入目录（猫粮原材料图片所在目录）</label>
      <input name="input_dir" value="{{ input_dir }}">
      <label>图片匹配规则（逗号分隔）</label>
      <input name="image_glob" value="{{ image_glob }}">
      <div class="note">默认 *.jpg,*.jpeg,*.png,*.bmp,*.webp,*.heic,*.heif,*.tif,*.tiff,*.jfif；会递归扫描子目录，且大小写不敏感。</div>
      <div class="note">OCR 成功后，图片会自动移动到：history/catfood_ingredient_images</div>
      <label>目标表名</label>
      <input name="table_name" value="{{ table_name }}">
      <label>MySQL Host</label>
      <input name="db_host" value="{{ db.host }}">
      <label>MySQL Port</label>
      <input name="db_port" value="{{ db.port }}">
      <label>MySQL User</label>
      <input name="db_user" value="{{ db.user }}">
      <label>MySQL Password</label>
      <input name="db_password" value="{{ db.password }}">
      <label>MySQL Database</label>
      <input name="db_name" value="{{ db.database }}">
      <button type="submit">导入猫粮原材料</button>
    </form>

    <form method="post">
      <input type="hidden" name="action" value="parse_ocr_json">
      <h3>功能：解析 OCR JSON 结构化入库</h3>
      <label>源表（含 ocr_json）</label>
      <input name="parse_source_table" value="{{ parse_source_table }}">
      <label>目标表（解析结果）</label>
      <input name="parse_target_table" value="{{ parse_target_table }}">
      <label>本次解析条数</label>
      <input name="parse_limit" value="{{ parse_limit }}">
      <div class="note">解析字段：名称、原料组成、电话、进口商。默认严格增量：仅解析目标表中不存在的 source_id；字段缺失时自动写 NULL。</div>
      <label>MySQL Host</label>
      <input name="db_host" value="{{ db.host }}">
      <label>MySQL Port</label>
      <input name="db_port" value="{{ db.port }}">
      <label>MySQL User</label>
      <input name="db_user" value="{{ db.user }}">
      <label>MySQL Password</label>
      <input name="db_password" value="{{ db.password }}">
      <label>MySQL Database</label>
      <input name="db_name" value="{{ db.database }}">
      <button type="submit">开始解析 OCR JSON</button>
    </form>

    <form method="post">
      <input type="hidden" name="action" value="parse_guarantee">
      <h3>功能：源表 {{ guarantee_parsed_table }}，写入 {{ guarantee_table }}</h3>
      <div class="note">系统会从 {{ guarantee_parsed_table }} 读取图片信息和产品名，识别后的保证值明细写入 {{ guarantee_table }}。</div>
      <div class="note">处理成功后，图片会自动移动到：{{ guarantee_history_dir }}</div>
      <label>本次处理条数</label>
      <input name="guarantee_limit" value="{{ guarantee_limit }}">
      <div class="note">会按 source_id 关联 {{ guarantee_parsed_table }} 与 {{ guarantee_table }}，并写入 source_id、parsed_row_id、image_name、file_sha256。</div>
      <div class="note">该功能依赖千问兼容接口：优先读取 QWEN_API_KEY / DASHSCOPE_API_KEY；也支持 config.yaml 中 openai.base_url/model 指向 DashScope/Qwen。</div>
      <label>MySQL Host</label>
      <input name="db_host" value="{{ db.host }}">
      <label>MySQL Port</label>
      <input name="db_port" value="{{ db.port }}">
      <label>MySQL User</label>
      <input name="db_user" value="{{ db.user }}">
      <label>MySQL Password</label>
      <input name="db_password" value="{{ db.password }}">
      <label>MySQL Database</label>
      <input name="db_name" value="{{ db.database }}">
      <button type="submit">开始识别保证值</button>
    </form>

    <div class="job" id="guarantee-job-panel" {% if not guarantee_job_id %}style="display:none"{% endif %}>
      <h3>保证值任务进度</h3>
      <div class="job-row">任务 ID：<code id="guarantee-job-id">{{ guarantee_job_id or '' }}</code></div>
      <div class="job-row">状态：<span id="guarantee-job-status">{{ guarantee_job.status if guarantee_job else '未开始' }}</span></div>
      <div class="job-row">源表：<code id="guarantee-job-source-table">{{ guarantee_job.source_table if guarantee_job and guarantee_job.source_table else guarantee_source_table }}</code></div>
      <div class="job-row">解析表：<code id="guarantee-job-parsed-table">{{ guarantee_job.parsed_table if guarantee_job and guarantee_job.parsed_table else guarantee_parsed_table }}</code></div>
      <div class="job-row">写入表：<code id="guarantee-job-target-table">{{ guarantee_job.guarantee_table if guarantee_job and guarantee_job.guarantee_table else guarantee_table }}</code></div>
      <div class="job-row">批次：<code id="guarantee-job-batch">{{ guarantee_job.batch_id if guarantee_job and guarantee_job.batch_id else '-' }}</code></div>
      <div class="job-row">当前 source_id：<span id="guarantee-job-current-source">{{ guarantee_job.current_source_id if guarantee_job and guarantee_job.current_source_id else '-' }}</span></div>
      <div class="job-row">当前图片：<span id="guarantee-job-current-image">{{ guarantee_job.current_image_name if guarantee_job and guarantee_job.current_image_name else '-' }}</span></div>
      <div class="job-row">消息：<span id="guarantee-job-message">{{ guarantee_job.message if guarantee_job and guarantee_job.message else '-' }}</span></div>
      <div class="job-row">进度：<span id="guarantee-job-progress-text">0 / 0</span></div>
      <progress id="guarantee-job-progress-bar" max="100" value="0"></progress>
      <div class="job-row">成功：<span id="guarantee-job-succeeded">0</span></div>
      <div class="job-row">空保证值：<span id="guarantee-job-empty">0</span></div>
      <div class="job-row">写入 guarantee：<span id="guarantee-job-guarantee-rows">0</span></div>
      <div class="job-row">失败：<span id="guarantee-job-failed">0</span></div>
      <div class="job-row">错误：<span id="guarantee-job-error">{{ guarantee_job.error if guarantee_job and guarantee_job.error else '-' }}</span></div>
    </div>

    <script>
      const guaranteeJobId = {{ guarantee_job_id|tojson }};

      function setGuaranteeText(id, text) {
        const el = document.getElementById(id);
        if (el) el.textContent = text;
      }

      function setGuaranteeProgress(percent, current, total) {
        const bar = document.getElementById("guarantee-job-progress-bar");
        if (bar) bar.value = percent;
        setGuaranteeText("guarantee-job-progress-text", `${current} / ${total}`);
      }

      async function pollGuaranteeJobStatus() {
        if (!guaranteeJobId) return;
        const panel = document.getElementById("guarantee-job-panel");
        if (panel) panel.style.display = "block";
        try {
          const resp = await fetch(`/catfood_ingredients/guarantee_status?job_id=${encodeURIComponent(guaranteeJobId)}`, { cache: "no-store" });
          if (!resp.ok) {
            setGuaranteeText("guarantee-job-status", "error");
            setGuaranteeText("guarantee-job-error", `状态查询失败: HTTP ${resp.status}`);
            return;
          }
          const data = await resp.json();
          setGuaranteeText("guarantee-job-id", data.job_id || guaranteeJobId);
          setGuaranteeText("guarantee-job-status", data.status || "-");
          setGuaranteeText("guarantee-job-source-table", data.source_table || "-");
          setGuaranteeText("guarantee-job-parsed-table", data.parsed_table || "-");
          setGuaranteeText("guarantee-job-target-table", data.guarantee_table || "-");
          setGuaranteeText("guarantee-job-batch", data.batch_id || "-");
          setGuaranteeText("guarantee-job-current-source", data.current_source_id || "-");
          setGuaranteeText("guarantee-job-current-image", data.current_image_name || "-");
          setGuaranteeText("guarantee-job-message", data.message || "-");
          setGuaranteeText("guarantee-job-succeeded", String(data.succeeded || 0));
          setGuaranteeText("guarantee-job-empty", String(data.empty_guarantees || 0));
          setGuaranteeText("guarantee-job-guarantee-rows", String(data.guarantee_rows || 0));
          setGuaranteeText("guarantee-job-failed", String(data.failed || 0));
          setGuaranteeText("guarantee-job-error", data.error || "-");
          setGuaranteeProgress(data.progress_percent || 0, data.processed_rows || 0, data.total_rows || 0);
          if (data.status === "running" || data.status === "queued") {
            window.setTimeout(pollGuaranteeJobStatus, 2000);
          }
        } catch (err) {
          setGuaranteeText("guarantee-job-status", "error");
          setGuaranteeText("guarantee-job-error", String(err));
        }
      }

      if (guaranteeJobId) {
        pollGuaranteeJobStatus();
      }
    </script>
  </body>
</html>
"""


@app.route("/catfood_ingredients", methods=["GET", "POST"])
def catfood_ingredients_view():
    message = ""
    is_error = False
    input_dir = request.form.get("input_dir", DEFAULT_B_PATH)
    image_glob = request.form.get("image_glob", DEFAULT_OCR_IMAGE_GLOB)
    table_name = request.form.get("table_name", DEFAULT_TABLE_CATFOOD_INGREDIENT)
    parse_source_table = request.form.get("parse_source_table", DEFAULT_TABLE_CATFOOD_INGREDIENT)
    parse_target_table = request.form.get("parse_target_table", DEFAULT_TABLE_CATFOOD_INGREDIENT_PARSED)
    parse_limit = request.form.get("parse_limit", "500")
    guarantee_source_table = DEFAULT_TABLE_CATFOOD_INGREDIENT
    guarantee_parsed_table = DEFAULT_TABLE_CATFOOD_INGREDIENT_PARSED
    guarantee_table = DEFAULT_TABLE_PRODUCT_GUARANTEE
    guarantee_limit = request.form.get("guarantee_limit", "200")
    guarantee_job_id = request.values.get("guarantee_job_id", "").strip()
    db = {
        "host": request.form.get("db_host", DEFAULT_DB["host"]),
        "port": request.form.get("db_port", DEFAULT_DB["port"]),
        "user": request.form.get("db_user", DEFAULT_DB["user"]),
        "password": request.form.get("db_password", DEFAULT_DB["password"]),
        "database": request.form.get("db_name", DEFAULT_DB["database"]),
    }

    if request.method == "POST":
        try:
            action = request.form.get("action", "ocr_import")
            if action == "parse_ocr_json":
                message = _parse_catfood_ingredient_ocr(
                    db_cfg=db,
                    source_table=parse_source_table,
                    target_table=parse_target_table,
                    limit=int(parse_limit),
                )
            elif action == "parse_guarantee":
                guarantee_job_id = uuid.uuid4().hex[:12]
                _set_catfood_guarantee_job(
                    guarantee_job_id,
                    status="queued",
                    started_at=None,
                    finished_at=None,
                    source_table=guarantee_source_table,
                    parsed_table=guarantee_parsed_table,
                    info_table=DEFAULT_TABLE_PRODUCT_INFO,
                    guarantee_table=guarantee_table,
                    limit=int(guarantee_limit),
                    total_rows=0,
                    processed_rows=0,
                    succeeded=0,
                    failed=0,
                    empty_guarantees=0,
                    guarantee_rows=0,
                    current_source_id=None,
                    current_image_name=None,
                    batch_id=None,
                    error=None,
                    message="任务已创建，等待启动。",
                )
                worker = threading.Thread(
                    target=_run_catfood_guarantee_job,
                    kwargs={
                        "job_id": guarantee_job_id,
                        "db_cfg": db,
                        "source_table": guarantee_source_table,
                        "parsed_table": guarantee_parsed_table,
                        "info_table": DEFAULT_TABLE_PRODUCT_INFO,
                        "guarantee_table": guarantee_table,
                        "limit": int(guarantee_limit),
                    },
                    daemon=True,
                )
                worker.start()
                message = f"保证值解析任务已启动：job_id={guarantee_job_id}"
            else:
                message = _batch_ocr_images(
                    image_dir=Path(input_dir),
                    db_cfg=db,
                    image_glob=image_glob,
                    table_name=table_name,
                )
        except Exception as exc:
            is_error = True
            message = f"导入失败：{exc}"

    guarantee_job = _get_catfood_guarantee_job(guarantee_job_id) if guarantee_job_id else None
    return render_template_string(
        CATFOOD_INGREDIENT_PAGE,
        message=message,
        is_error=is_error,
        input_dir=input_dir,
        image_glob=image_glob,
        table_name=table_name,
        parse_source_table=parse_source_table,
        parse_target_table=parse_target_table,
        parse_limit=parse_limit,
        guarantee_history_dir=DEFAULT_CATFOOD_GUARANTEE_HISTORY_DIR,
        guarantee_source_table=guarantee_source_table,
        guarantee_parsed_table=guarantee_parsed_table,
        guarantee_table=guarantee_table,
        guarantee_limit=guarantee_limit,
        guarantee_job_id=guarantee_job_id,
        guarantee_job=guarantee_job,
        db=db,
        ingredient_table=DEFAULT_TABLE_CATFOOD_INGREDIENT,
    )


@app.route("/catfood_ingredients/guarantee_status", methods=["GET"])
def catfood_ingredients_guarantee_status():
    job_id = request.args.get("job_id", "").strip()
    if not job_id:
        return jsonify({"error": "missing job_id"}), 400
    job = _get_catfood_guarantee_job(job_id)
    if not job:
        return jsonify({"error": "job not found", "job_id": job_id}), 404

    total_rows = int(job.get("total_rows") or 0)
    processed_rows = int(job.get("processed_rows") or 0)
    progress_percent = 100 if job.get("status") == "completed" else 0
    if total_rows > 0:
        progress_percent = int(processed_rows * 100 / total_rows)
    payload = dict(job)
    payload["progress_percent"] = progress_percent
    return jsonify(payload)


TAOBAO_PAGE = """
<!doctype html>
<html lang="zh">
  <head>
    <meta charset="utf-8">
    <title>淘宝数据导入</title>
    <style>
      body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 32px; color: #111; }
      h1 { margin-bottom: 12px; }
      form { border: 1px solid #ccc; padding: 16px; border-radius: 8px; margin-bottom: 20px; }
      label { display: block; margin: 6px 0 2px; font-weight: 600; }
      input { width: 100%; padding: 8px; box-sizing: border-box; }
      button { margin-top: 12px; padding: 8px 16px; cursor: pointer; }
      .msg { padding: 12px; border-radius: 6px; background: #f6f8fa; margin-bottom: 16px; }
      .error { background: #ffe6e6; color: #900; }
      .success { background: #e6ffed; color: #085b27; }
      .note { color: #555; font-size: 13px; margin-top: 6px; }
    </style>
  </head>
  <body>
    <h1>淘宝数据导入</h1>
    <p><a href="/">← 返回抖音导入</a> | <a href="/xhs">→ 小红书数据导入</a> | <a href="/catfood_ingredients">→ 猫粮原材料导入</a> | <a href="/cluster">→ 去聚类页面</a> | <a href="/catfood_extract">→ 猫粮数据抽取</a></p>
    {% if message %}
      <div class="msg {{ 'error' if is_error else 'success' }}">{{ message }}</div>
    {% endif %}

    <form method="post">
      <input type="hidden" name="action" value="import_taobao">
      <h3>功能：导入 detail_items_raw_*.json 到 {{ taobao_table }}</h3>
      <label>输入目录（detail_items_raw_*.json 所在目录）</label>
      <input name="input_dir" value="{{ input_dir }}">
      <label>文件匹配规则</label>
      <input name="file_pattern" value="{{ file_pattern }}">
      <div class="note">默认匹配 detail_items_raw_*.json，也兼容手动填写 list_items_raw_*.json。导入后会自动清洗：仅保留 pay_count&gt;=100，并按 food_taste 高相似去重，保留 pay_count 最高的一条；若有删除会自动备份原表。</div>
      <label>目标表名</label>
      <input name="table_name" value="{{ table_name }}">
      <label>批量写入大小</label>
      <input name="batch_size" value="{{ batch_size }}">
      <label>MySQL Host</label>
      <input name="db_host" value="{{ db.host }}">
      <label>MySQL Port</label>
      <input name="db_port" value="{{ db.port }}">
      <label>MySQL User</label>
      <input name="db_user" value="{{ db.user }}">
      <label>MySQL Password</label>
      <input name="db_password" value="{{ db.password }}">
      <label>MySQL Database</label>
      <input name="db_name" value="{{ db.database }}">
      <button type="submit">导入淘宝数据</button>
    </form>

    <form method="post">
      <input type="hidden" name="action" value="parse_title">
      <h3>功能：标准化抽取 title 字段（关键词/付款人数/问题类型/配方类型/适合猫/猫年龄/每kg单价）</h3>
      <label>源表（淘宝导入表）</label>
      <input name="parse_source_table" value="{{ parse_source_table }}">
      <label>目标表（标准化结果）</label>
      <input name="parse_target_table" value="{{ parse_target_table }}">
      <label>本次抽取条数</label>
      <input name="parse_limit" value="{{ parse_limit }}">
      <div class="note">输出字段：keywords、付款人数、问题类型、formula_type、suitable_cat、cat_age_stage、net_weight_kg、unit_price_per_kg（每1kgX元）。</div>
      <label>MySQL Host</label>
      <input name="db_host" value="{{ db.host }}">
      <label>MySQL Port</label>
      <input name="db_port" value="{{ db.port }}">
      <label>MySQL User</label>
      <input name="db_user" value="{{ db.user }}">
      <label>MySQL Password</label>
      <input name="db_password" value="{{ db.password }}">
      <label>MySQL Database</label>
      <input name="db_name" value="{{ db.database }}">
      <button type="submit">开始标准化抽取</button>
    </form>
  </body>
</html>
"""


@app.route("/taobao", methods=["GET", "POST"])
def taobao_view():
    message = ""
    is_error = False
    input_dir = request.form.get(
        "input_dir",
        str(PROJECT_ROOT / "data" / "taobao"),
    )
    file_pattern = request.form.get("file_pattern", "detail_items_raw_*.json")
    table_name = request.form.get("table_name", DEFAULT_TABLE_TAOBAO)
    batch_size = request.form.get("batch_size", "500")
    parse_source_table = request.form.get("parse_source_table", DEFAULT_TABLE_TAOBAO)
    parse_target_table = request.form.get("parse_target_table", DEFAULT_TABLE_TAOBAO_TITLE_PARSED)
    parse_limit = request.form.get("parse_limit", "1000")
    db = {
        "host": request.form.get("db_host", DEFAULT_DB["host"]),
        "port": request.form.get("db_port", DEFAULT_DB["port"]),
        "user": request.form.get("db_user", DEFAULT_DB["user"]),
        "password": request.form.get("db_password", DEFAULT_DB["password"]),
        "database": request.form.get("db_name", DEFAULT_DB["database"]),
    }

    if request.method == "POST":
        try:
            action = request.form.get("action", "import_taobao")
            if action == "parse_title":
                message = _parse_taobao_title_data(
                    db_cfg=db,
                    source_table=parse_source_table,
                    target_table=parse_target_table,
                    limit=int(parse_limit),
                )
            else:
                message = _ingest_taobao_data(
                    input_dir=Path(input_dir),
                    db_cfg=db,
                    file_pattern=file_pattern,
                    table_name=table_name,
                    batch_size=int(batch_size),
                )
        except Exception as exc:
            is_error = True
            message = f"导入失败：{exc}"

    return render_template_string(
        TAOBAO_PAGE,
        message=message,
        is_error=is_error,
        input_dir=input_dir,
        file_pattern=file_pattern,
        table_name=table_name,
        batch_size=batch_size,
        parse_source_table=parse_source_table,
        parse_target_table=parse_target_table,
        parse_limit=parse_limit,
        db=db,
        taobao_table=DEFAULT_TABLE_TAOBAO,
    )


CATFOOD_EXTRACT_PAGE = """
<!doctype html>
<html lang="zh">
  <head>
    <meta charset="utf-8">
    <title>猫粮数据抽取</title>
    <style>
      body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 32px; color: #111; }
      h1 { margin-bottom: 12px; }
      form { border: 1px solid #ccc; padding: 16px; border-radius: 8px; margin-bottom: 20px; }
      label { display: block; margin: 6px 0 2px; font-weight: 600; }
      input { width: 100%; padding: 8px; box-sizing: border-box; }
      button { margin-top: 12px; padding: 8px 16px; cursor: pointer; }
      .msg { padding: 12px; border-radius: 6px; background: #f6f8fa; margin-bottom: 16px; }
      .error { background: #ffe6e6; color: #900; }
      .success { background: #e6ffed; color: #085b27; }
      .note { color: #555; font-size: 13px; margin-top: 6px; }
    </style>
  </head>
  <body>
    <h1>猫粮数据抽取</h1>
    <p><a href="/">← 返回抖音导入</a> | <a href="/xhs">→ 小红书数据导入</a> | <a href="/catfood_ingredients">→ 猫粮原材料导入</a> | <a href="/taobao">→ 淘宝数据导入</a> | <a href="/cluster">→ 去聚类页面</a> | <a href="/catfood_label_engineering">→ 猫粮标签工程</a></p>
    {% if message %}
      <div class="msg {{ 'error' if is_error else 'success' }}">{{ message }}</div>
    {% endif %}

    <form method="post">
      <input type="hidden" name="action" value="extract_catfood">
      <h3>功能：从 douyin_raw_comments / xiaohongshu_raw_comments 增量抽取猫粮候选数据</h3>
      <label>候选结果表</label>
      <input name="target_table" value="{{ target_table }}">
      <label>增量状态表</label>
      <input name="state_table" value="{{ state_table }}">
      <div class="note">每次点击只处理新增数据（按 ingest_ts 水位增量）；抽取 comment_text 命中“品牌词”或“健康词”（同时命中也保留）的记录。</div>
      <label>MySQL Host</label>
      <input name="db_host" value="{{ db.host }}">
      <label>MySQL Port</label>
      <input name="db_port" value="{{ db.port }}">
      <label>MySQL User</label>
      <input name="db_user" value="{{ db.user }}">
      <label>MySQL Password</label>
      <input name="db_password" value="{{ db.password }}">
      <label>MySQL Database</label>
      <input name="db_name" value="{{ db.database }}">
      <button type="submit">开始猫粮数据抽取</button>
    </form>

    <form method="post">
      <input type="hidden" name="action" value="import_brand_issue_ctq">
      <h3>功能：导入品牌问题/CTQ Excel，并按 {{ brand_issue_parsed_table }} 对齐品牌</h3>
      <label>输入目录（brand_issue_ctq Excel 所在目录）</label>
      <input name="brand_issue_input_dir" value="{{ brand_issue_input_dir }}">
      <label>文件匹配规则</label>
      <input name="brand_issue_pattern" value="{{ brand_issue_pattern }}">
      <label>目标表</label>
      <input name="brand_issue_table" value="{{ brand_issue_table }}">
      <label>品牌对齐源表</label>
      <input name="brand_issue_parsed_table" value="{{ brand_issue_parsed_table }}">
      <label>批量写入大小</label>
      <input name="brand_issue_batch_size" value="{{ brand_issue_batch_size }}">
      <div class="note">默认只导入包含评论案例的 sheet；像占比/核心因素这类汇总 sheet 会跳过。若 Excel 内没有品牌列，会优先用文件名推断品牌，再与 {{ brand_issue_parsed_table }} 的 brand 做对齐。</div>
      <label>MySQL Host</label>
      <input name="db_host" value="{{ db.host }}">
      <label>MySQL Port</label>
      <input name="db_port" value="{{ db.port }}">
      <label>MySQL User</label>
      <input name="db_user" value="{{ db.user }}">
      <label>MySQL Password</label>
      <input name="db_password" value="{{ db.password }}">
      <label>MySQL Database</label>
      <input name="db_name" value="{{ db.database }}">
      <button type="submit">导入品牌问题/CTQ Excel</button>
    </form>

    <form method="post">
      <input type="hidden" name="action" value="parse_ingredient_types">
      <h3>功能：用大模型拆分原料特征（3 张明细表 + 1 张汇总表）</h3>
      <label>源表（含 ingredient_composition）</label>
      <input name="parse_ing_source_table" value="{{ parse_ing_source_table }}">
      <label>汇总表</label>
      <input name="parse_ing_summary_table" value="{{ parse_ing_summary_table }}">
      <label>蛋白质明细表</label>
      <input name="parse_ing_protein_table" value="{{ parse_ing_protein_table }}">
      <label>纤维/碳水标签表</label>
      <input name="parse_ing_fiber_carb_table" value="{{ parse_ing_fiber_carb_table }}">
      <label>益生元/益生菌明细表</label>
      <input name="parse_ing_biotic_table" value="{{ parse_ing_biotic_table }}">
      <label>本次解析条数</label>
      <input name="parse_ing_limit" value="{{ parse_ing_limit }}">
      <div class="note">默认严格增量：仅处理汇总表中不存在的 source_id。会产出 1 张蛋白质标签表、1 张纤维/碳水标签表、1 张益生元/益生菌明细表，并同步生成 1 张整体特征汇总表。</div>
      <label>MySQL Host</label>
      <input name="db_host" value="{{ db.host }}">
      <label>MySQL Port</label>
      <input name="db_port" value="{{ db.port }}">
      <label>MySQL User</label>
      <input name="db_user" value="{{ db.user }}">
      <label>MySQL Password</label>
      <input name="db_password" value="{{ db.password }}">
      <label>MySQL Database</label>
      <input name="db_name" value="{{ db.database }}">
      <button type="submit">开始成分类型拆分</button>
    </form>
  </body>
</html>
"""


@app.route("/catfood_extract", methods=["GET", "POST"])
def catfood_extract_view():
    message = ""
    is_error = False
    target_table = request.form.get("target_table", DEFAULT_TABLE_CATFOOD)
    state_table = request.form.get("state_table", DEFAULT_TABLE_CATFOOD_STATE)
    brand_issue_input_dir = request.form.get("brand_issue_input_dir", DEFAULT_BRAND_ISSUE_CTQ_DIR)
    brand_issue_pattern = request.form.get("brand_issue_pattern", "*.xlsx")
    brand_issue_table = request.form.get("brand_issue_table", DEFAULT_TABLE_BRAND_ISSUE_CTQ)
    brand_issue_parsed_table = request.form.get("brand_issue_parsed_table", DEFAULT_TABLE_CATFOOD_INGREDIENT_PARSED)
    brand_issue_batch_size = request.form.get("brand_issue_batch_size", "200")
    parse_ing_source_table = request.form.get("parse_ing_source_table", DEFAULT_TABLE_CATFOOD_INGREDIENT_PARSED)
    parse_ing_summary_table = request.form.get("parse_ing_summary_table", DEFAULT_TABLE_CATFOOD_FEATURE_SUMMARY)
    parse_ing_protein_table = request.form.get("parse_ing_protein_table", DEFAULT_TABLE_CATFOOD_FEATURE_PROTEIN)
    parse_ing_fiber_carb_table = request.form.get("parse_ing_fiber_carb_table", DEFAULT_TABLE_CATFOOD_FEATURE_FIBER_CARB)
    parse_ing_biotic_table = request.form.get("parse_ing_biotic_table", DEFAULT_TABLE_CATFOOD_FEATURE_BIOTIC)
    parse_ing_limit = request.form.get("parse_ing_limit", "500")
    db = {
        "host": request.form.get("db_host", DEFAULT_DB["host"]),
        "port": request.form.get("db_port", DEFAULT_DB["port"]),
        "user": request.form.get("db_user", DEFAULT_DB["user"]),
        "password": request.form.get("db_password", DEFAULT_DB["password"]),
        "database": request.form.get("db_name", DEFAULT_DB["database"]),
    }

    if request.method == "POST":
        try:
            action = request.form.get("action", "extract_catfood")
            if action == "parse_ingredient_types":
                message = _parse_catfood_ingredient_types_data(
                    db_cfg=db,
                    source_table=parse_ing_source_table,
                    summary_table=parse_ing_summary_table,
                    protein_table=parse_ing_protein_table,
                    fiber_carb_table=parse_ing_fiber_carb_table,
                    biotic_table=parse_ing_biotic_table,
                    limit=int(parse_ing_limit),
                )
            elif action == "import_brand_issue_ctq":
                message = _ingest_brand_issue_ctq_data(
                    input_dir=Path(brand_issue_input_dir),
                    db_cfg=db,
                    file_pattern=brand_issue_pattern,
                    table_name=brand_issue_table,
                    parsed_table=brand_issue_parsed_table,
                    batch_size=int(brand_issue_batch_size),
                )
            else:
                message = _extract_catfood_data(
                    db_cfg=db,
                    target_table=target_table,
                    state_table=state_table,
                )
        except Exception as exc:
            is_error = True
            message = f"抽取失败：{exc}"

    return render_template_string(
        CATFOOD_EXTRACT_PAGE,
        message=message,
        is_error=is_error,
        target_table=target_table,
        state_table=state_table,
        brand_issue_input_dir=brand_issue_input_dir,
        brand_issue_pattern=brand_issue_pattern,
        brand_issue_table=brand_issue_table,
        brand_issue_parsed_table=brand_issue_parsed_table,
        brand_issue_batch_size=brand_issue_batch_size,
        parse_ing_source_table=parse_ing_source_table,
        parse_ing_summary_table=parse_ing_summary_table,
        parse_ing_protein_table=parse_ing_protein_table,
        parse_ing_fiber_carb_table=parse_ing_fiber_carb_table,
        parse_ing_biotic_table=parse_ing_biotic_table,
        parse_ing_limit=parse_ing_limit,
        db=db,
    )


CATFOOD_LABEL_ENGINEERING_PAGE = """
<!doctype html>
<html lang="zh">
  <head>
    <meta charset="utf-8">
    <title>猫粮标签工程</title>
    <style>
      body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 32px; color: #111; }
      h1 { margin-bottom: 12px; }
      form { border: 1px solid #ccc; padding: 16px; border-radius: 8px; margin-bottom: 20px; }
      label { display: block; margin: 6px 0 2px; font-weight: 600; }
      input { width: 100%; padding: 8px; box-sizing: border-box; }
      button { margin-top: 12px; padding: 8px 16px; cursor: pointer; }
      .msg { padding: 12px; border-radius: 6px; background: #f6f8fa; margin-bottom: 16px; }
      .error { background: #ffe6e6; color: #900; }
      .success { background: #e6ffed; color: #085b27; }
      .note { color: #555; font-size: 13px; margin-top: 6px; }
      .job { border: 1px solid #ddd; padding: 16px; border-radius: 8px; background: #fafafa; }
      .job-row { margin: 6px 0; }
      progress { width: 100%; height: 20px; margin-top: 8px; }
      code { background: #f3f4f6; padding: 2px 4px; border-radius: 4px; }
    </style>
  </head>
  <body>
    <h1>猫粮标签工程</h1>
    <p><a href="/">← 返回抖音导入</a> | <a href="/catfood_extract">→ 猫粮数据抽取</a> | <a href="/catfood_ingredients">→ 猫粮原材料导入</a> | <a href="/taobao">→ 淘宝数据导入</a></p>
    {% if message %}
      <div class="msg {{ 'error' if is_error else 'success' }}">{{ message }}</div>
    {% endif %}

    <form method="post">
      <input type="hidden" name="action" value="start_protein_label_engineering">
      <h3>功能：回填猫粮蛋白标签表</h3>
      <label>源表（含 ingredient_composition）</label>
      <input name="protein_source_table" value="{{ protein_source_table }}">
      <label>目标表</label>
      <input name="protein_table" value="{{ protein_table }}">
      <div class="note">当前页面只会处理 <code>catfood_feature_protein_labels</code>，不会改动汇总表、纤维/碳水表、益生元/益生菌表。</div>
      <button type="submit">开始蛋白标签工程</button>
    </form>

    <form method="post">
      <input type="hidden" name="action" value="start_fiber_carb_label_engineering">
      <h3>功能：回填猫粮纤维/碳水标签表</h3>
      <label>源表（含 ingredient_composition）</label>
      <input name="fiber_source_table" value="{{ fiber_source_table }}">
      <label>目标表</label>
      <input name="fiber_carb_table" value="{{ fiber_carb_table }}">
      <div class="note">当前页面只会处理 <code>catfood_feature_fiber_carb_labels</code>，不会改动蛋白表、汇总表、益生元/益生菌表。</div>
      <button type="submit">开始纤维/碳水标签工程</button>
    </form>

    <form method="post">
      <input type="hidden" name="action" value="start_biotic_label_engineering">
      <h3>功能：回填猫粮益生元/益生菌标签表</h3>
      <label>源表（含 ingredient_composition）</label>
      <input name="biotic_source_table" value="{{ biotic_source_table }}">
      <label>目标表</label>
      <input name="biotic_table" value="{{ biotic_table }}">
      <div class="note">当前页面只会处理 <code>catfood_feature_biotic_labels</code>，不会改动蛋白表、纤维/碳水表、汇总表。</div>
      <button type="submit">开始益生元/益生菌标签工程</button>
    </form>

    <div class="job" id="job-panel" {% if not job_id %}style="display:none"{% endif %}>
      <h3>任务进度</h3>
      <div class="job-row">任务 ID：<code id="job-id">{{ job_id or '' }}</code></div>
      <div class="job-row">功能：<span id="job-feature-label">{{ job.feature_label if job and job.feature_label else '-' }}</span></div>
      <div class="job-row">状态：<span id="job-status">{{ job.status if job else '未开始' }}</span></div>
      <div class="job-row">目标表：<code id="job-target-table">{{ job.target_table if job and job.target_table else '-' }}</code></div>
      <div class="job-row">批次：<code id="job-batch">{{ job.batch_id if job and job.batch_id else '-' }}</code></div>
      <div class="job-row">消息：<span id="job-message">{{ job.message if job and job.message else '-' }}</span></div>
      <div class="job-row">进度：<span id="job-progress-text">0 / 0</span></div>
      <progress id="job-progress-bar" max="100" value="0"></progress>
      <div class="job-row">已写入：<span id="job-written">0</span></div>
      <div class="job-row">错误：<span id="job-error">{{ job.error if job and job.error else '-' }}</span></div>
    </div>

    <script>
      const jobId = {{ job_id|tojson }};

      function setText(id, text) {
        const el = document.getElementById(id);
        if (el) el.textContent = text;
      }

      function setProgress(percent, current, total) {
        const bar = document.getElementById("job-progress-bar");
        if (bar) bar.value = percent;
        setText("job-progress-text", `${current} / ${total}`);
      }

      async function pollJobStatus() {
        if (!jobId) return;
        const panel = document.getElementById("job-panel");
        if (panel) panel.style.display = "block";
        try {
          const resp = await fetch(`/catfood_label_engineering/status?job_id=${encodeURIComponent(jobId)}`, { cache: "no-store" });
          if (!resp.ok) {
            setText("job-status", "error");
            setText("job-error", `状态查询失败: HTTP ${resp.status}`);
            return;
          }
          const data = await resp.json();
          setText("job-id", data.job_id || jobId);
          setText("job-feature-label", data.feature_label || "-");
          setText("job-status", data.status || "-");
          setText("job-target-table", data.target_table || "-");
          setText("job-batch", data.batch_id || "-");
          setText("job-message", data.message || "-");
          setText("job-written", String(data.written || 0));
          setText("job-error", data.error || "-");
          setProgress(data.progress_percent || 0, data.unique_done || 0, data.unique_total || 0);
          if (data.status === "running" || data.status === "queued") {
            window.setTimeout(pollJobStatus, 2000);
          }
        } catch (err) {
          setText("job-status", "error");
          setText("job-error", String(err));
        }
      }

      if (jobId) {
        pollJobStatus();
      }
    </script>
  </body>
</html>
"""


@app.route("/catfood_label_engineering", methods=["GET", "POST"])
def catfood_label_engineering_view():
    message = ""
    is_error = False
    protein_source_table = request.form.get(
        "protein_source_table",
        request.args.get("protein_source_table", DEFAULT_TABLE_CATFOOD_INGREDIENT_PARSED),
    )
    protein_table = request.form.get("protein_table", request.args.get("protein_table", DEFAULT_TABLE_CATFOOD_FEATURE_PROTEIN))
    fiber_source_table = request.form.get(
        "fiber_source_table",
        request.args.get("fiber_source_table", DEFAULT_TABLE_CATFOOD_INGREDIENT_PARSED),
    )
    fiber_carb_table = request.form.get(
        "fiber_carb_table",
        request.args.get("fiber_carb_table", DEFAULT_TABLE_CATFOOD_FEATURE_FIBER_CARB),
    )
    biotic_source_table = request.form.get(
        "biotic_source_table",
        request.args.get("biotic_source_table", DEFAULT_TABLE_CATFOOD_INGREDIENT_PARSED),
    )
    biotic_table = request.form.get(
        "biotic_table",
        request.args.get("biotic_table", DEFAULT_TABLE_CATFOOD_FEATURE_BIOTIC),
    )
    limit = request.form.get("limit", request.args.get("limit", DEFAULT_CATFOOD_LABEL_ENGINEERING_LIMIT))
    concurrency = request.form.get("concurrency", request.args.get("concurrency", DEFAULT_CATFOOD_LABEL_ENGINEERING_CONCURRENCY))
    job_id = request.values.get("job_id", "")
    db = {
        "host": request.form.get("db_host", request.args.get("db_host", DEFAULT_DB["host"])),
        "port": request.form.get("db_port", request.args.get("db_port", DEFAULT_DB["port"])),
        "user": request.form.get("db_user", request.args.get("db_user", DEFAULT_DB["user"])),
        "password": request.form.get("db_password", request.args.get("db_password", DEFAULT_DB["password"])),
        "database": request.form.get("db_name", request.args.get("db_name", DEFAULT_DB["database"])),
    }

    if request.method == "POST":
        try:
            action = request.form.get("action", "start_protein_label_engineering")
            if action == "start_protein_label_engineering":
                job_id = uuid.uuid4().hex[:12]
                _set_catfood_label_job(
                    job_id,
                    status="queued",
                    message="任务已创建，等待启动。",
                    feature_type="protein",
                    feature_label="蛋白标签工程",
                    source_table=protein_source_table,
                    target_table=protein_table,
                    limit=int(limit),
                    concurrency=int(concurrency),
                    unique_total=0,
                    unique_done=0,
                    written=0,
                    batch_id=None,
                    error=None,
                    started_at=None,
                    finished_at=None,
                )
                worker = threading.Thread(
                    target=_run_catfood_label_engineering_job,
                    kwargs={
                        "job_id": job_id,
                        "db_cfg": db,
                        "feature_type": "protein",
                        "source_table": protein_source_table,
                        "target_table": protein_table,
                        "limit": int(limit),
                        "concurrency": int(concurrency),
                    },
                    daemon=True,
                )
                worker.start()
                message = f"蛋白标签工程任务已启动：job_id={job_id}"
            elif action == "start_fiber_carb_label_engineering":
                job_id = uuid.uuid4().hex[:12]
                _set_catfood_label_job(
                    job_id,
                    status="queued",
                    message="任务已创建，等待启动。",
                    feature_type="fiber_carb",
                    feature_label="纤维/碳水标签工程",
                    source_table=fiber_source_table,
                    target_table=fiber_carb_table,
                    limit=int(limit),
                    concurrency=int(concurrency),
                    unique_total=0,
                    unique_done=0,
                    written=0,
                    batch_id=None,
                    error=None,
                    started_at=None,
                    finished_at=None,
                )
                worker = threading.Thread(
                    target=_run_catfood_label_engineering_job,
                    kwargs={
                        "job_id": job_id,
                        "db_cfg": db,
                        "feature_type": "fiber_carb",
                        "source_table": fiber_source_table,
                        "target_table": fiber_carb_table,
                        "limit": int(limit),
                        "concurrency": int(concurrency),
                    },
                    daemon=True,
                )
                worker.start()
                message = f"纤维/碳水标签工程任务已启动：job_id={job_id}"
            elif action == "start_biotic_label_engineering":
                job_id = uuid.uuid4().hex[:12]
                _set_catfood_label_job(
                    job_id,
                    status="queued",
                    message="任务已创建，等待启动。",
                    feature_type="biotic",
                    feature_label="益生元/益生菌标签工程",
                    source_table=biotic_source_table,
                    target_table=biotic_table,
                    limit=int(limit),
                    concurrency=int(concurrency),
                    unique_total=0,
                    unique_done=0,
                    written=0,
                    batch_id=None,
                    error=None,
                    started_at=None,
                    finished_at=None,
                )
                worker = threading.Thread(
                    target=_run_catfood_label_engineering_job,
                    kwargs={
                        "job_id": job_id,
                        "db_cfg": db,
                        "feature_type": "biotic",
                        "source_table": biotic_source_table,
                        "target_table": biotic_table,
                        "limit": int(limit),
                        "concurrency": int(concurrency),
                    },
                    daemon=True,
                )
                worker.start()
                message = f"益生元/益生菌标签工程任务已启动：job_id={job_id}"
            else:
                raise ValueError(f"未知操作: {action}")
        except Exception as exc:
            is_error = True
            message = f"启动失败：{exc}"

    job = _get_catfood_label_job(job_id) if job_id else None
    return render_template_string(
        CATFOOD_LABEL_ENGINEERING_PAGE,
        message=message,
        is_error=is_error,
        protein_source_table=protein_source_table,
        protein_table=protein_table,
        fiber_source_table=fiber_source_table,
        fiber_carb_table=fiber_carb_table,
        biotic_source_table=biotic_source_table,
        biotic_table=biotic_table,
        limit=limit,
        concurrency=concurrency,
        db=db,
        job_id=job_id,
        job=job,
    )


@app.route("/catfood_label_engineering/status", methods=["GET"])
def catfood_label_engineering_status():
    job_id = request.args.get("job_id", "").strip()
    if not job_id:
        return jsonify({"error": "missing job_id"}), 400
    job = _get_catfood_label_job(job_id)
    if not job:
        return jsonify({"error": "job not found", "job_id": job_id}), 404

    unique_total = int(job.get("unique_total") or 0)
    unique_done = int(job.get("unique_done") or 0)
    progress_percent = 100 if job.get("status") == "completed" else 0
    if unique_total > 0:
        progress_percent = int(unique_done * 100 / unique_total)
    payload = dict(job)
    payload["progress_percent"] = progress_percent
    return jsonify(payload)


CLUSTER_PAGE = """
<!doctype html>
<html lang="zh">
  <head>
    <meta charset="utf-8">
    <title>评论聚类</title>
    <style>
      body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 32px; color: #111; }
      h1 { margin-bottom: 12px; }
      form { border: 1px solid #ccc; padding: 16px; border-radius: 8px; margin-bottom: 20px; }
      label { display: block; margin: 6px 0 2px; font-weight: 600; }
      input { width: 100%; padding: 8px; box-sizing: border-box; }
      button { margin-top: 12px; padding: 8px 16px; cursor: pointer; }
      .msg { padding: 12px; border-radius: 6px; background: #f6f8fa; margin-bottom: 16px; }
      .error { background: #ffe6e6; color: #900; }
      .success { background: #e6ffed; color: #085b27; }
      .cluster { border: 1px solid #ddd; padding: 10px; border-radius: 6px; margin-bottom: 12px; }
      .cluster h4 { margin: 0 0 6px 0; }
      .sample { margin: 4px 0; }
      pre { white-space: pre-wrap; word-wrap: break-word; }
    </style>
  </head>
  <body>
    <h1>评论聚类</h1>
    <p><a href="/">← 返回导入页</a></p>
    {% if message %}
      <div class="msg {{ 'error' if is_error else 'success' }}">{{ message }}</div>
    {% endif %}

    <form method="post">
      <label>表名</label>
      <input name="table_name" value="{{ table_name }}">
      <label>search_words（可选）</label>
      <input name="filter_search_words" value="{{ filter_search_words }}">
      <label>comment_ip（可选）</label>
      <input name="filter_comment_ip" value="{{ filter_comment_ip }}">
      <label>聚类阈值 (0-1，越高越严格)</label>
      <input name="threshold" value="{{ threshold }}">
      <label>拉取条数</label>
      <input name="limit" value="{{ limit }}">
      <label>MySQL Host</label>
      <input name="db_host" value="{{ db.host }}">
      <label>MySQL Port</label>
      <input name="db_port" value="{{ db.port }}">
      <label>MySQL User</label>
      <input name="db_user" value="{{ db.user }}">
      <label>MySQL Password</label>
      <input name="db_password" value="{{ db.password }}">
      <label>MySQL Database</label>
      <input name="db_name" value="{{ db.database }}">
      <button type="submit">开始聚类</button>
    </form>

    {% if clusters %}
      <h3>聚类结果（按簇大小排序）</h3>
      {% for cl in clusters %}
        <div class="cluster">
          <h4>簇大小：{{ cl.size }}</h4>
          {% for sid, stext in cl.samples %}
            <div class="sample">
              <strong>ID {{ sid }}:</strong>
              <pre>{{ stext }}</pre>
            </div>
          {% endfor %}
        </div>
      {% endfor %}
    {% endif %}
  </body>
</html>
"""


@app.route("/cluster", methods=["GET", "POST"])
def cluster_view():
    message = ""
    is_error = False
    table_name = request.form.get("table_name", DEFAULT_CLUSTER_TABLE)
    threshold = request.form.get("threshold", "0.35")
    limit = request.form.get("limit", "200")
    filter_search_words = request.form.get("filter_search_words", "")
    filter_comment_ip = request.form.get("filter_comment_ip", "")
    db = {
        "host": request.form.get("db_host", DEFAULT_DB["host"]),
        "port": request.form.get("db_port", DEFAULT_DB["port"]),
        "user": request.form.get("db_user", DEFAULT_DB["user"]),
        "password": request.form.get("db_password", DEFAULT_DB["password"]),
        "database": request.form.get("db_name", DEFAULT_DB["database"]),
    }
    clusters = []
    if request.method == "POST":
        try:
            th = float(threshold)
            lim = int(limit)
            rows = _fetch_extracted(
                db,
                table_name,
                lim,
                search_words=filter_search_words.strip(),
                comment_ip=filter_comment_ip.strip(),
            )
            clusters = cluster_texts(rows, threshold=th)
            message = (
                f"已从 {table_name} 拉取 {len(rows)} 条，"
                f"筛选(search_words='{filter_search_words}', comment_ip='{filter_comment_ip}'), "
                f"得到 {len(clusters)} 个簇。"
            )
        except Exception as exc:
            is_error = True
            message = f"聚类失败：{exc}"

    return render_template_string(
        CLUSTER_PAGE,
        message=message,
        is_error=is_error,
        table_name=table_name,
        threshold=threshold,
        limit=limit,
        db=db,
        clusters=clusters,
        filter_search_words=filter_search_words,
        filter_comment_ip=filter_comment_ip,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
