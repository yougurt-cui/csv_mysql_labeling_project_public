# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import hashlib
import json
import re
import shutil
import uuid
from datetime import date, timedelta
from itertools import zip_longest
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from rich.console import Console
from sqlalchemy import text
from sqlalchemy.engine import Engine

console = Console()

DEFAULT_TEXT = "无"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARCHIVE_DIR = PROJECT_ROOT / "history" / "xiaohongshu"


def _safe_text(value: Optional[str]) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _split_lines(value: Optional[str]) -> List[str]:
    text_value = _safe_text(value)
    if not text_value:
        return []
    return [line.strip() for line in text_value.splitlines() if line.strip()]


def _parse_like_count(value: Optional[str]) -> Optional[int]:
    text_value = _safe_text(value).replace(",", "")
    if not text_value:
        return None
    if text_value.endswith(("万", "w", "W")):
        try:
            return int(float(text_value[:-1]) * 10000)
        except ValueError:
            return None
    try:
        return int(float(text_value))
    except ValueError:
        return None


def _normalize_created_at(comment_time: str, today: date) -> str:
    text_value = _safe_text(comment_time)
    if not text_value:
        return ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text_value):
        return text_value
    match = re.fullmatch(r"(\d{1,2})-(\d{1,2})", text_value)
    if match:
        month = int(match.group(1))
        day = int(match.group(2))
        return f"2026-{month:02d}-{day:02d}"
    match = re.search(r"(\d+)\s*天前", text_value)
    if match:
        days = int(match.group(1))
        return (today - timedelta(days=days)).strftime("%Y-%m-%d")
    if re.search(r"\d+\s*小时", text_value):
        return today.strftime("%Y-%m-%d")
    return ""


def _resolve_keyword(row: Dict[str, str], csv_path: Path, fallback_keyword: Optional[str]) -> str:
    keyword = _safe_text(row.get("检索词"))
    if keyword:
        return keyword
    if fallback_keyword:
        return fallback_keyword
    match = re.match(r"xiaohongshu_(.+?)_data", csv_path.stem)
    if match:
        return match.group(1)
    return "unknown"


def _make_external_id(payload: Dict[str, object]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.md5(raw).hexdigest()


def _move_to_archive(csv_path: Path, archive_dir: Path) -> Path:
    archive_dir.mkdir(parents=True, exist_ok=True)
    target = archive_dir / csv_path.name
    if not target.exists():
        return Path(shutil.move(str(csv_path), str(target)))
    stem = csv_path.stem
    suffix = csv_path.suffix
    counter = 1
    while True:
        candidate = archive_dir / f"{stem}_{counter}{suffix}"
        if not candidate.exists():
            return Path(shutil.move(str(csv_path), str(candidate)))
        counter += 1


def _build_records(
    row: Dict[str, str],
    source_name: str,
    csv_path: Path,
    row_index: int,
    ingest_batch_id: str,
    fallback_keyword: Optional[str],
) -> List[Dict[str, object]]:
    title = _safe_text(row.get("标题")) or None
    content = _safe_text(row.get("内容")) or None
    comment_text_raw = _safe_text(row.get("评论"))
    comment_time_raw = _safe_text(row.get("评论时间"))
    location = _safe_text(row.get("发布地点")) or DEFAULT_TEXT
    query_keyword = _resolve_keyword(row, csv_path, fallback_keyword)
    like_count = _parse_like_count(row.get("点赞量"))

    comments = _split_lines(comment_text_raw)
    comment_times = _split_lines(comment_time_raw)
    first_time = comment_times[0] if comment_times else ""
    today = date.today()

    records: List[Dict[str, object]] = []

    note_payload = {
        "source_name": source_name,
        "source_file": csv_path.name,
        "row_index": row_index,
        "row_type": "note",
    }
    note_external_id = _make_external_id(note_payload)
    note_created_at = _normalize_created_at(first_time, today)
    records.append(
        {
            "source_name": source_name,
            "external_id": note_external_id,
            "title": title,
            "content": content,
            "comment_text": comment_text_raw or "",
            "comment_time": first_time or "",
            "created_at": note_created_at or "",
            "like_count": like_count,
            "location": location or DEFAULT_TEXT,
            "query_keyword": query_keyword or "unknown",
            "ingest_batch_id": ingest_batch_id,
            "label_status": "PENDING",
        }
    )

    for idx, (comment, comment_time) in enumerate(
        zip_longest(comments, comment_times, fillvalue=""), start=1
    ):
        comment = _safe_text(comment)
        if not comment:
            continue
        comment_time = _safe_text(comment_time)
        comment_created_at = _normalize_created_at(comment_time, today)
        comment_payload = {
            "source_name": source_name,
            "source_file": csv_path.name,
            "row_index": row_index,
            "row_type": "comment",
            "comment_index": idx,
        }
        comment_external_id = _make_external_id(comment_payload)
        records.append(
            {
                "source_name": source_name,
                "external_id": comment_external_id,
                "title": title,
                "content": content,
                "comment_text": comment,
                "comment_time": comment_time or "",
                "created_at": comment_created_at or "",
                "like_count": like_count,
                "location": location or DEFAULT_TEXT,
                "query_keyword": query_keyword or "unknown",
                "ingest_batch_id": ingest_batch_id,
                "label_status": "PENDING",
            }
        )

    return records


def ingest_xhs_csv(
    engine: Engine,
    csv_path: Path,
    source_name: str,
    fallback_keyword: Optional[str] = None,
    encoding: str = "utf-8-sig",
    batch_size: int = 2000,
    archive_dir: Path = DEFAULT_ARCHIVE_DIR,
) -> Tuple[int, str]:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    ingest_batch_id = uuid.uuid4().hex[:12]
    console.print(f"[cyan]Ingest batch:[/cyan] {ingest_batch_id}")

    sql = """
    INSERT INTO xiaohongshu_raw_comments(
      source_name, external_id, title, content, comment_text, comment_time, created_at,
      like_count, location, query_keyword, ingest_batch_id, label_status
    )
    VALUES(
      :source_name, :external_id, :title, :content, :comment_text, :comment_time, :created_at,
      :like_count, :location, :query_keyword, :ingest_batch_id, :label_status
    )
    ON DUPLICATE KEY UPDATE
      title=VALUES(title),
      content=VALUES(content),
      comment_text=VALUES(comment_text),
      comment_time=VALUES(comment_time),
      created_at=VALUES(created_at),
      like_count=VALUES(like_count),
      location=VALUES(location),
      query_keyword=VALUES(query_keyword),
      ingest_batch_id=VALUES(ingest_batch_id),
      label_status=IF(label_status='DONE', 'DONE', 'PENDING');
    """

    total = 0
    buffer: List[Dict[str, object]] = []

    with csv_path.open("r", encoding=encoding, newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("CSV header is empty.")

        for row_index, row in enumerate(reader, start=1):
            records = _build_records(
                row=row,
                source_name=source_name,
                csv_path=csv_path,
                row_index=row_index,
                ingest_batch_id=ingest_batch_id,
                fallback_keyword=fallback_keyword,
            )
            buffer.extend(records)
            if len(buffer) >= batch_size:
                with engine.begin() as conn:
                    conn.execute(text(sql), buffer)
                total += len(buffer)
                buffer.clear()

    if buffer:
        with engine.begin() as conn:
            conn.execute(text(sql), buffer)
        total += len(buffer)

    _move_to_archive(csv_path, archive_dir)
    return total, ingest_batch_id
