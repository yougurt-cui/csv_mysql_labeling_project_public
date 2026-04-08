# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
import hashlib
import json
import re
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zipfile import ZipFile
import xml.etree.ElementTree as ET

from rich.console import Console
from sqlalchemy import text
from sqlalchemy.engine import Engine

console = Console()

TABLE_RE = re.compile(r"^[A-Za-z0-9_]+$")
XML_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
EXCEL_EPOCH = datetime(1899, 12, 30)

COMMENT_HEADER_CANDIDATES = (
    "comment",
    "comment_text",
    "评论",
    "评论内容",
    "内容",
)
DATE_HEADER_CANDIDATES = (
    "date",
    "日期",
    "时间",
    "created_at",
)
ISSUE_HEADER_CANDIDATES = (
    "病症",
    "症状",
    "问题",
    "ctq",
    "问题类型",
)
BRAND_HEADER_CANDIDATES = (
    "brand",
    "品牌",
    "品牌名",
    "品牌名称",
)


@dataclass
class BrandIssueCtqIngestSummary:
    files: int
    sheets: int
    rows: int
    batch_id: str


def _safe_table(table_name: str) -> str:
    if not TABLE_RE.fullmatch(table_name):
        raise ValueError(f"Invalid table name: {table_name}")
    return table_name


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_header(value: Any) -> str:
    text_value = _safe_text(value).lower()
    return re.sub(r"\s+", "", text_value)


def _normalize_brand_key(value: Any) -> str:
    text_value = _safe_text(value).lower()
    text_value = text_value.replace("（", "(").replace("）", ")")
    text_value = re.sub(r"(?i)ctq[_ -]*case", "", text_value)
    text_value = re.sub(r"猫粮$", "", text_value)
    text_value = re.sub(r"品牌$", "", text_value)
    text_value = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", text_value)
    return text_value


def _derive_file_brand_seed(path: Path) -> str:
    stem = path.stem
    stem = re.sub(r"(?i)ctq[_ -]*case", "", stem)
    stem = re.sub(r"\d+$", "", stem)
    stem = stem.replace("_", " ").strip()
    stem = re.sub(r"猫粮$", "", stem).strip()
    return stem


def _derive_file_issue_seed(path: Path) -> str:
    stem = path.stem
    stem = re.sub(r"(?i)ctq[_ -]*case", "", stem)
    stem = re.sub(r"\d+$", "", stem)
    stem = stem.replace("_", " ").strip()
    return stem


def _column_index(ref: str) -> int:
    letters = []
    for ch in ref:
        if ch.isalpha():
            letters.append(ch.upper())
        else:
            break
    idx = 0
    for ch in letters:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return max(0, idx - 1)


def _cell_value(cell: ET.Element, shared_strings: Sequence[str]) -> str:
    cell_type = cell.attrib.get("t")
    value_node = cell.find("a:v", XML_NS)
    if cell_type == "s" and value_node is not None and value_node.text is not None:
        idx = int(value_node.text)
        return shared_strings[idx] if 0 <= idx < len(shared_strings) else ""
    if cell_type == "inlineStr":
        return "".join((node.text or "") for node in cell.findall(".//a:t", XML_NS))
    if value_node is not None and value_node.text is not None:
        return value_node.text
    return ""


def _load_shared_strings(zf: ZipFile) -> List[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    values: List[str] = []
    for item in root.findall("a:si", XML_NS):
        parts = [(node.text or "") for node in item.findall(".//a:t", XML_NS)]
        values.append("".join(parts))
    return values


def _iter_sheet_rows(zf: ZipFile, sheet_target: str, shared_strings: Sequence[str]) -> List[List[str]]:
    sheet_root = ET.fromstring(zf.read(sheet_target))
    rows: List[List[str]] = []
    for row in sheet_root.findall(".//a:sheetData/a:row", XML_NS):
        values: Dict[int, str] = {}
        max_idx = -1
        for cell in row.findall("a:c", XML_NS):
            ref = cell.attrib.get("r", "")
            idx = _column_index(ref)
            values[idx] = _cell_value(cell, shared_strings)
            if idx > max_idx:
                max_idx = idx
        if max_idx < 0:
            continue
        row_values = [_safe_text(values.get(idx, "")) for idx in range(max_idx + 1)]
        while row_values and not row_values[-1]:
            row_values.pop()
        rows.append(row_values)
    return rows


def _list_workbook_sheets(path: Path) -> List[Tuple[str, str]]:
    with ZipFile(path) as zf:
        workbook_root = ET.fromstring(zf.read("xl/workbook.xml"))
        rels_root = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        rel_map = {
            rel.attrib["Id"]: rel.attrib["Target"]
            for rel in rels_root.findall("{http://schemas.openxmlformats.org/package/2006/relationships}Relationship")
        }
        sheets: List[Tuple[str, str]] = []
        for sheet in workbook_root.find("a:sheets", XML_NS):
            name = sheet.attrib.get("name", "")
            rid = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id", "")
            target = rel_map.get(rid, "")
            if not target:
                continue
            sheets.append((name, f"xl/{target}"))
        return sheets


def _is_header_row(row_values: Sequence[str]) -> bool:
    tokens = {_normalize_header(value) for value in row_values if _safe_text(value)}
    header_tokens = {
        *COMMENT_HEADER_CANDIDATES,
        *DATE_HEADER_CANDIDATES,
        *ISSUE_HEADER_CANDIDATES,
        *BRAND_HEADER_CANDIDATES,
    }
    hits = sum(1 for token in tokens if token in header_tokens)
    if hits >= 2:
        return True
    return bool(tokens & set(COMMENT_HEADER_CANDIDATES) and tokens & (set(DATE_HEADER_CANDIDATES) | set(BRAND_HEADER_CANDIDATES)))


def _find_column(header_row: Sequence[str], candidates: Sequence[str]) -> Optional[int]:
    normalized = [_normalize_header(value) for value in header_row]
    wanted = {_normalize_header(value) for value in candidates}
    for idx, value in enumerate(normalized):
        if value in wanted:
            return idx
    return None


def _parse_event_date(raw_value: str) -> Optional[date]:
    text_value = _safe_text(raw_value)
    if not text_value:
        return None
    if re.fullmatch(r"\d+(?:\.\d+)?", text_value):
        try:
            days = float(text_value)
            if 1 <= days <= 100000:
                return (EXCEL_EPOCH + timedelta(days=days)).date()
        except Exception:
            pass
    for fmt in (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y.%m.%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
    ):
        try:
            return datetime.strptime(text_value, fmt).date()
        except ValueError:
            continue
    return None


def _chunked(seq: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for idx in range(0, len(seq), size):
        yield seq[idx : idx + size]


def _load_canonical_brands(engine: Engine, parsed_table: str) -> List[str]:
    parsed_table = _safe_table(parsed_table)
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT DISTINCT brand
                FROM `{parsed_table}`
                WHERE brand IS NOT NULL AND TRIM(brand) <> ''
                ORDER BY CHAR_LENGTH(brand), brand
                """
            )
        ).fetchall()
    return [str(row[0]).strip() for row in rows if str(row[0]).strip()]


def _align_brand(raw_brand: str, canonical_brands: Sequence[str]) -> Tuple[Optional[str], str]:
    cleaned = _safe_text(raw_brand)
    if not cleaned:
        return None, "missing"

    raw_key = _normalize_brand_key(cleaned)
    if not raw_key:
        return cleaned, "raw"

    exact_matches = [brand for brand in canonical_brands if _normalize_brand_key(brand) == raw_key]
    if exact_matches:
        return exact_matches[0], "exact"

    contains_matches = [
        brand
        for brand in canonical_brands
        if raw_key in _normalize_brand_key(brand) or _normalize_brand_key(brand) in raw_key
    ]
    if len(contains_matches) == 1:
        return contains_matches[0], "contains"

    ranked: List[Tuple[float, str]] = []
    for brand in canonical_brands:
        brand_key = _normalize_brand_key(brand)
        if not brand_key:
            continue
        ratio = SequenceMatcher(None, raw_key, brand_key).ratio()
        if ratio >= 0.60:
            ranked.append((ratio, brand))
    ranked.sort(key=lambda item: (-item[0], len(item[1]), item[1]))
    if ranked:
        top_ratio, top_brand = ranked[0]
        second_ratio = ranked[1][0] if len(ranked) > 1 else 0.0
        if top_ratio >= 0.75 and top_ratio - second_ratio >= 0.05:
            return top_brand, "similar"

    return cleaned, "raw"


def ensure_brand_issue_ctq_table(
    engine: Engine,
    table_name: str = "catfood_brand_issue_ctq_cases",
) -> None:
    table_name = _safe_table(table_name)
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
      id BIGINT PRIMARY KEY AUTO_INCREMENT,
      external_id CHAR(32) NOT NULL,
      brand VARCHAR(255) NULL,
      brand_raw VARCHAR(255) NULL,
      brand_match_type VARCHAR(32) NULL,
      issue_name VARCHAR(255) NULL,
      comment_text LONGTEXT NOT NULL,
      event_date DATE NULL,
      source_file VARCHAR(255) NOT NULL,
      sheet_name VARCHAR(128) NOT NULL,
      row_no INT NOT NULL,
      raw_json LONGTEXT NULL,
      ingest_batch_id VARCHAR(32) NOT NULL,
      ingest_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uq_external_id (external_id),
      KEY idx_brand (brand),
      KEY idx_brand_raw (brand_raw),
      KEY idx_issue_name (issue_name),
      KEY idx_event_date (event_date),
      KEY idx_source_file (source_file),
      KEY idx_sheet_name (sheet_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _build_case_records(
    path: Path,
    canonical_brands: Sequence[str],
) -> Tuple[int, List[Dict[str, Any]]]:
    file_brand_seed = _derive_file_brand_seed(path)
    file_issue_seed = _derive_file_issue_seed(path)

    rows_out: List[Dict[str, Any]] = []
    scanned_sheets = 0
    with ZipFile(path) as zf:
        shared_strings = _load_shared_strings(zf)
        for sheet_name, sheet_target in _list_workbook_sheets(path):
            sheet_rows = _iter_sheet_rows(zf, sheet_target, shared_strings)
            if not sheet_rows:
                continue

            scanned_sheets += 1
            has_header = _is_header_row(sheet_rows[0])
            if has_header:
                header_row = sheet_rows[0]
                data_rows = sheet_rows[1:]
                comment_idx = _find_column(header_row, COMMENT_HEADER_CANDIDATES)
                date_idx = _find_column(header_row, DATE_HEADER_CANDIDATES)
                issue_idx = _find_column(header_row, ISSUE_HEADER_CANDIDATES)
                brand_idx = _find_column(header_row, BRAND_HEADER_CANDIDATES)
                if comment_idx is None:
                    continue
            else:
                header_row = []
                data_rows = sheet_rows
                if not sheet_rows[0] or len(_safe_text(sheet_rows[0][0])) < 6:
                    continue
                comment_idx = 0
                date_idx = 1 if len(sheet_rows[0]) > 1 else None
                brand_idx = 2 if len(sheet_rows[0]) > 2 else None
                issue_idx = 3 if len(sheet_rows[0]) > 3 else None

            for row_offset, row_values in enumerate(data_rows, start=2 if has_header else 1):
                if comment_idx is None or comment_idx >= len(row_values):
                    continue
                comment_text = _safe_text(row_values[comment_idx])
                if not comment_text:
                    continue

                brand_raw = ""
                if brand_idx is not None and brand_idx < len(row_values):
                    brand_raw = _safe_text(row_values[brand_idx])
                if not brand_raw:
                    brand_raw = file_brand_seed

                issue_name = ""
                if issue_idx is not None and issue_idx < len(row_values):
                    issue_name = _safe_text(row_values[issue_idx])
                if not issue_name and file_issue_seed:
                    if _normalize_brand_key(file_issue_seed) != _normalize_brand_key(brand_raw):
                        issue_name = file_issue_seed

                event_date = None
                raw_date = ""
                if date_idx is not None and date_idx < len(row_values):
                    raw_date = _safe_text(row_values[date_idx])
                    event_date = _parse_event_date(raw_date)

                aligned_brand, match_type = _align_brand(brand_raw, canonical_brands)
                dedup_seed = "|".join(
                    [
                        path.name,
                        sheet_name,
                        str(row_offset),
                        aligned_brand or "",
                        brand_raw or "",
                        issue_name or "",
                        comment_text,
                        event_date.isoformat() if event_date else raw_date,
                    ]
                )
                external_id = hashlib.md5(dedup_seed.encode("utf-8")).hexdigest()

                if has_header:
                    raw_obj = {
                        _safe_text(header_row[idx]) or f"col_{idx + 1}": (_safe_text(value) or None)
                        for idx, value in enumerate(row_values)
                    }
                else:
                    raw_obj = {
                        f"col_{idx + 1}": (_safe_text(value) or None)
                        for idx, value in enumerate(row_values)
                    }

                rows_out.append(
                    {
                        "external_id": external_id,
                        "brand": aligned_brand or None,
                        "brand_raw": brand_raw or None,
                        "brand_match_type": match_type,
                        "issue_name": issue_name or None,
                        "comment_text": comment_text,
                        "event_date": event_date,
                        "source_file": path.name,
                        "sheet_name": sheet_name,
                        "row_no": row_offset,
                        "raw_json": json.dumps(raw_obj, ensure_ascii=False),
                    }
                )
    return scanned_sheets, rows_out


def ingest_brand_issue_ctq_dir(
    engine: Engine,
    input_dir: Path,
    file_pattern: str = "*.xlsx",
    table_name: str = "catfood_brand_issue_ctq_cases",
    parsed_table: str = "catfood_ingredient_ocr_parsed",
    batch_size: int = 200,
) -> BrandIssueCtqIngestSummary:
    table_name = _safe_table(table_name)
    parsed_table = _safe_table(parsed_table)
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    files = sorted(path for path in input_dir.glob(file_pattern) if path.is_file())
    if not files:
        return BrandIssueCtqIngestSummary(files=0, sheets=0, rows=0, batch_id="")

    ensure_brand_issue_ctq_table(engine, table_name=table_name)
    ingest_batch_id = uuid.uuid4().hex[:12]
    canonical_brands = _load_canonical_brands(engine, parsed_table)
    all_rows: List[Dict[str, Any]] = []
    sheet_count = 0

    for path in files:
        scanned_sheets, rows = _build_case_records(path=path, canonical_brands=canonical_brands)
        sheet_count += scanned_sheets
        all_rows.extend(rows)

    if not all_rows:
        return BrandIssueCtqIngestSummary(files=len(files), sheets=sheet_count, rows=0, batch_id=ingest_batch_id)

    sql = f"""
    INSERT INTO `{table_name}` (
      external_id, brand, brand_raw, brand_match_type, issue_name, comment_text,
      event_date, source_file, sheet_name, row_no, raw_json, ingest_batch_id
    )
    VALUES (
      :external_id, :brand, :brand_raw, :brand_match_type, :issue_name, :comment_text,
      :event_date, :source_file, :sheet_name, :row_no, :raw_json, :ingest_batch_id
    )
    ON DUPLICATE KEY UPDATE
      brand=VALUES(brand),
      brand_raw=VALUES(brand_raw),
      brand_match_type=VALUES(brand_match_type),
      issue_name=VALUES(issue_name),
      comment_text=VALUES(comment_text),
      event_date=VALUES(event_date),
      source_file=VALUES(source_file),
      sheet_name=VALUES(sheet_name),
      row_no=VALUES(row_no),
      raw_json=VALUES(raw_json),
      ingest_batch_id=VALUES(ingest_batch_id)
    """

    total_rows = 0
    for chunk in _chunked(all_rows, max(1, int(batch_size))):
        payload = [{**row, "ingest_batch_id": ingest_batch_id} for row in chunk]
        with engine.begin() as conn:
            conn.execute(text(sql), payload)
        total_rows += len(payload)

    console.print(
        f"[green]Ingested brand issue CTQ rows:[/green] {total_rows} "
        f"(files={len(files)}, sheets={sheet_count}, batch={ingest_batch_id}, table={table_name})"
    )
    return BrandIssueCtqIngestSummary(
        files=len(files),
        sheets=sheet_count,
        rows=total_rows,
        batch_id=ingest_batch_id,
    )
