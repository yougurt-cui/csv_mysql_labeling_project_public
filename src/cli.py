from __future__ import annotations

import argparse
import os
from pathlib import Path
from rich.console import Console

from .settings import load_settings
from .db import make_engine, healthcheck
from .ingest import canonicalize_csv, write_raw_comments
from .ingest_brand_issue_ctq import ingest_brand_issue_ctq_dir
from .ingest_xhs import ingest_xhs_csv
from .ingest_catfood_ingredients import ingest_catfood_ingredient_dir
from .ingest_taobao import ingest_taobao_list_dir
from .extract_catfood import run_catfood_extraction_incremental
from .extract_catfood_brand_relations import run_brand_relation_extraction
from .dictionary import upsert_dict_items, load_dict_by_type
from .label import label_pending
from .ocr_image import run_ocr_image
from .parse_catfood_guarantee import parse_catfood_guarantee_values, rebuild_product_guarantees_from_info
from .parse_catfood_ocr import clean_catfood_ingredient_compositions, parse_catfood_ingredient_ocr_json
from .parse_catfood_ingredient_types import (
    backfill_catfood_biotic_labels,
    backfill_catfood_fiber_carb_labels,
    backfill_catfood_protein_labels,
    parse_catfood_ingredient_composition_types,
    rebuild_biotic_labels_from_summary,
    rebuild_protein_labels_from_summary,
)
from .parse_taobao_title import parse_taobao_title_standardized

console = Console()
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TAOBAO_INPUT_DIR = str(PROJECT_ROOT / "data" / "taobao")
DEFAULT_BRAND_ISSUE_INPUT_DIR = str(PROJECT_ROOT / "data" / "brand_issue_ctq")
DEFAULT_GUARANTEE_HISTORY_DIR = str(PROJECT_ROOT / "history" / "catfood_ingredient_images" / "guarantee")

def _load_all_dicts(engine):
    # 只加载 prompt 需要的几个类型（你后续可扩展）
    types = ["product_category", "ctq", "purchase_intent", "sentiment"]
    return {t: load_dict_by_type(engine, t) for t in types}

def cmd_init_dicts(args):
    st = load_settings()
    engine = make_engine(st.mysql)
    healthcheck(engine)
    upsert_dict_items(engine, st.dict_seed)
    console.print("[green]init-dicts done.[/green]")

def cmd_ingest(args):
    st = load_settings()
    engine = make_engine(st.mysql)
    healthcheck(engine)

    csv_path = Path(args.csv)
    df = canonicalize_csv(csv_path, st.column_map, encoding=st.pipeline.get("ingest", {}).get("csv_encoding", "utf-8"))
    # fabricate ingest batch id in CLI (simple)
    import uuid
    ingest_batch_id = uuid.uuid4().hex[:12]

    n = write_raw_comments(engine, df, source_name=args.source, ingest_batch_id=ingest_batch_id)
    console.print(f"[green]Ingested rows:[/green] {n} (batch={ingest_batch_id})")

def cmd_ingest_xhs(args):
    st = load_settings()
    engine = make_engine(st.mysql)
    healthcheck(engine)

    csv_path = Path(args.csv)
    default_source = st.pipeline.get("source_name", "xiaohongshu")
    source_name = args.source or default_source
    encoding = args.encoding or st.pipeline.get("ingest", {}).get("csv_encoding", "utf-8-sig")
    batch_size = int(args.batch_size or st.pipeline.get("ingest", {}).get("chunksize", 2000))

    n, ingest_batch_id = ingest_xhs_csv(
        engine,
        csv_path=csv_path,
        source_name=source_name,
        fallback_keyword=args.keyword,
        encoding=encoding,
        batch_size=batch_size,
    )
    console.print(f"[green]Ingested rows:[/green] {n} (batch={ingest_batch_id})")

def cmd_ingest_catfood_ingredients(args):
    st = load_settings()
    engine = make_engine(st.mysql)
    healthcheck(engine)

    files, rows, ingest_batch_id = ingest_catfood_ingredient_dir(
        engine=engine,
        input_dir=Path(args.input_dir),
        file_pattern=args.pattern,
        table_name=args.table,
        batch_size=int(args.batch_size),
    )
    if files == 0:
        console.print("[yellow]No catfood ingredient files matched.[/yellow]")
        return
    console.print(
        f"[green]Ingested catfood ingredient rows:[/green] {rows} "
        f"(files={files}, batch={ingest_batch_id}, table={args.table})"
    )

def cmd_ingest_taobao_list(args):
    st = load_settings()
    engine = make_engine(st.mysql)
    healthcheck(engine)

    result = ingest_taobao_list_dir(
        engine=engine,
        input_dir=Path(args.input_dir),
        file_pattern=args.pattern,
        table_name=args.table,
        batch_size=int(args.batch_size),
    )
    if result.files == 0:
        console.print("[yellow]No taobao files matched.[/yellow]")
        return
    console.print(
        f"[green]Ingested taobao rows:[/green] {result.rows} "
        f"(files={result.files}, batch={result.ingest_batch_id}, table={args.table})"
    )
    console.print(
        f"[green]Cleaned taobao rows:[/green] kept {result.cleanup.rows_after}/{result.cleanup.rows_before} "
        f"(dropped={result.cleanup.total_dropped}, pay_count<{result.cleanup.min_pay_count}: {result.cleanup.low_pay_dropped}, "
        f"food_taste dup={result.cleanup.duplicate_dropped})"
    )
    if result.cleanup.backup_table:
        console.print(f"[cyan]Cleanup backup table:[/cyan] {result.cleanup.backup_table}")

def cmd_ingest_brand_issue_ctq(args):
    st = load_settings()
    engine = make_engine(st.mysql)
    healthcheck(engine)

    result = ingest_brand_issue_ctq_dir(
        engine=engine,
        input_dir=Path(args.input_dir),
        file_pattern=args.pattern,
        table_name=args.table,
        parsed_table=args.parsed_table,
        batch_size=int(args.batch_size),
    )
    if result.files == 0:
        console.print("[yellow]No brand issue CTQ files matched.[/yellow]")
        return
    console.print(
        f"[green]Ingested brand issue CTQ rows:[/green] {result.rows} "
        f"(files={result.files}, sheets={result.sheets}, batch={result.batch_id}, table={args.table})"
    )

def cmd_extract_catfood(args):
    st = load_settings()
    engine = make_engine(st.mysql)
    healthcheck(engine)

    res = run_catfood_extraction_incremental(
        engine=engine,
        target_table=args.target_table,
        state_table=args.state_table,
    )
    console.print(
        f"[green]Catfood extraction rows:[/green] {res.inserted_rows} "
        f"(batch={res.batch_id}, scanned_douyin={res.scanned_douyin_rows}, scanned_xhs={res.scanned_xhs_rows})"
    )

def cmd_extract_catfood_brand_relations(args):
    st = load_settings()
    engine = make_engine(st.mysql)
    healthcheck(engine)

    res = run_brand_relation_extraction(
        engine=engine,
        target_table=args.target_table,
        state_table=args.state_table,
    )
    console.print(
        f"[green]Catfood brand relation rows:[/green] {res.upserted_rows} "
        f"(batch={res.batch_id}, scanned={res.scanned_rows}, matched={res.matched_rows})"
    )

def cmd_label(args):
    st = load_settings()
    engine = make_engine(st.mysql)
    healthcheck(engine)

    dicts = _load_all_dicts(engine)
    label_pending(
        engine,
        openai_cfg=st.openai,
        dicts=dicts,
        limit=int(args.limit),
        batch_size=int(st.pipeline.get("labeling", {}).get("batch_size", 10)),
        temperature=int(st.pipeline.get("labeling", {}).get("temperature", 0)),
    )

def cmd_ocr_image(args):
    st = load_settings()
    engine = make_engine(st.mysql)
    healthcheck(engine)

    out_json_path = Path(args.out_json) if args.out_json else None
    result = run_ocr_image(
        engine=engine,
        image_path=Path(args.image),
        out_json_path=out_json_path,
        table_name=args.table,
        ocr_cfg=st.ocr,
        openai_cfg=st.openai,
    )
    console.print(f"[green]OCR done.[/green] image={result['image_path']}")
    console.print(f"[green]JSON saved:[/green] {result['json_path']}")
    console.print(
        f"[green]DB upsert:[/green] table={result['table_name']}, "
        f"sha256={result['file_sha256']}, latency_ms={result['model_latency_ms']}"
    )

def cmd_parse_catfood_ocr(args):
    st = load_settings()
    engine = make_engine(st.mysql)
    healthcheck(engine)

    res = parse_catfood_ingredient_ocr_json(
        engine=engine,
        source_table=args.source_table,
        target_table=args.target_table,
        limit=int(args.limit),
        incremental_only=not bool(args.reparse_existing),
    )
    console.print(
        f"[green]Parse OCR done.[/green] scanned={res.scanned}, upserted={res.upserted}, "
        f"source={res.source_table}, target={res.target_table}, batch={res.batch_id}"
    )


def cmd_parse_catfood_guarantee(args):
    st = load_settings()
    engine = make_engine(st.mysql)
    healthcheck(engine)

    processed_dir = Path(args.processed_dir) if getattr(args, "processed_dir", "") else None
    res = parse_catfood_guarantee_values(
        engine=engine,
        openai_cfg=st.openai,
        ocr_cfg=st.ocr,
        source_table=args.source_table,
        parsed_table=args.parsed_table,
        info_table=args.info_table,
        guarantee_table=args.guarantee_table,
        limit=int(args.limit),
        incremental_only=not bool(args.reparse_existing),
        processed_dir=processed_dir,
    )
    console.print(
        f"[green]Parse guarantee done.[/green] scanned={res.scanned}, succeeded={res.succeeded}, "
        f"empty_guarantees={res.empty_guarantees}, guarantee_rows={res.guarantee_rows}, failed={res.failed}, "
        f"source={res.source_table}, parsed={res.parsed_table}, info={res.info_table}, guarantee={res.guarantee_table}, "
        f"batch={res.batch_id}"
    )
    if res.error_samples:
        console.print("[yellow]Sample errors:[/yellow] " + " | ".join(res.error_samples))


def cmd_rebuild_catfood_guarantee(args):
    st = load_settings()
    engine = make_engine(st.mysql)
    healthcheck(engine)

    res = rebuild_product_guarantees_from_info(
        engine=engine,
        info_table=args.info_table,
        guarantee_table=args.guarantee_table,
        limit=int(args.limit),
        source_id=int(args.source_id) if args.source_id else None,
    )
    console.print(
        f"[green]Rebuild guarantee done.[/green] scanned={res.scanned}, rebuilt={res.rebuilt}, "
        f"guarantee_rows={res.guarantee_rows}, failed={res.failed}, "
        f"info={res.info_table}, guarantee={res.guarantee_table}, batch={res.batch_id}"
    )
    if res.error_samples:
        console.print("[yellow]Sample errors:[/yellow] " + " | ".join(res.error_samples))


def cmd_clean_catfood_ingredient_composition(args):
    st = load_settings()
    engine = make_engine(st.mysql)
    healthcheck(engine)

    res = clean_catfood_ingredient_compositions(
        engine=engine,
        table_name=args.table,
        limit=int(args.limit) if args.limit else None,
        create_backup=not bool(args.no_backup),
    )
    backup_info = f", backup={res.backup_table}" if res.backup_table else ""
    console.print(
        f"[green]Clean parsed OCR fields done.[/green] scanned={res.scanned}, updated={res.updated}, "
        f"brand_updated={res.brand_updated}, product_name_updated={res.product_name_updated}, "
        f"table={res.table_name}, batch={res.batch_id}{backup_info}"
    )

def cmd_parse_catfood_ingredient_types(args):
    st = load_settings()
    engine = make_engine(st.mysql)
    healthcheck(engine)

    res = parse_catfood_ingredient_composition_types(
        engine=engine,
        openai_cfg=st.openai,
        source_table=args.source_table,
        target_table=args.summary_table,
        protein_table=args.protein_table,
        fiber_carb_table=args.fiber_carb_table,
        biotic_table=args.biotic_table,
        limit=int(args.limit),
        incremental_only=not bool(args.reparse_existing),
        concurrency=int(args.concurrency),
        write_batch_size=int(args.write_batch_size),
    )
    console.print(
        f"[green]Parse ingredient types done.[/green] scanned={res.scanned}, upserted={res.upserted}, "
        f"protein_rows={res.protein_rows}, fiber_carb_rows={res.fiber_carb_rows}, biotic_rows={res.biotic_rows}, "
        f"source={res.source_table}, summary={res.target_table}, batch={res.batch_id}"
    )


def cmd_rebuild_protein_labels(args):
    st = load_settings()
    engine = make_engine(st.mysql)
    healthcheck(engine)

    res = rebuild_protein_labels_from_summary(
        engine=engine,
        summary_table=args.summary_table,
        protein_table=args.protein_table,
        parsed_source_table=args.source_table,
    )
    console.print(
        f"[green]Rebuild protein labels done.[/green] scanned={res.scanned}, upserted={res.upserted}, "
        f"summary={res.source_table}, protein_table={res.target_table}, batch={res.batch_id}"
    )


def cmd_rebuild_biotic_labels(args):
    st = load_settings()
    engine = make_engine(st.mysql)
    healthcheck(engine)

    res = rebuild_biotic_labels_from_summary(
        engine=engine,
        summary_table=args.summary_table,
        biotic_table=args.biotic_table,
    )
    console.print(
        f"[green]Rebuild biotic labels done.[/green] scanned={res.scanned}, upserted={res.upserted}, "
        f"summary={res.source_table}, biotic_table={res.target_table}, batch={res.batch_id}"
    )


def cmd_catfood_label_engineering(args):
    st = load_settings()
    engine = make_engine(st.mysql)
    healthcheck(engine)

    last_unique_done = -1

    def _progress(payload):
        nonlocal last_unique_done
        status = str(payload.get("status") or "")
        unique_total = int(payload.get("unique_total") or 0)
        unique_done = int(payload.get("unique_done") or 0)
        written = int(payload.get("written") or 0)
        if status == "running" and unique_total > 0 and unique_done != last_unique_done:
            last_unique_done = unique_done
            console.print(
                f"[cyan]Catfood label engineering:[/cyan] "
                f"{unique_done}/{unique_total} unique compositions, written={written}"
            )

    res = backfill_catfood_protein_labels(
        engine=engine,
        openai_cfg=st.openai,
        source_table=args.source_table,
        protein_table=args.protein_table,
        limit=int(args.limit),
        concurrency=int(args.concurrency),
        progress_callback=_progress,
    )
    console.print(
        f"[green]Catfood label engineering done.[/green] scanned={res.scanned}, upserted={res.upserted}, "
        f"source={res.source_table}, protein_table={res.target_table}, batch={res.batch_id}"
    )


def cmd_catfood_fiber_carb_engineering(args):
    st = load_settings()
    engine = make_engine(st.mysql)
    healthcheck(engine)

    last_unique_done = -1

    def _progress(payload):
        nonlocal last_unique_done
        status = str(payload.get("status") or "")
        unique_total = int(payload.get("unique_total") or 0)
        unique_done = int(payload.get("unique_done") or 0)
        written = int(payload.get("written") or 0)
        if status == "running" and unique_total > 0 and unique_done != last_unique_done:
            last_unique_done = unique_done
            console.print(
                f"[cyan]Catfood fiber/carb engineering:[/cyan] "
                f"{unique_done}/{unique_total} unique compositions, written={written}"
            )

    res = backfill_catfood_fiber_carb_labels(
        engine=engine,
        openai_cfg=st.openai,
        source_table=args.source_table,
        fiber_carb_table=args.fiber_carb_table,
        limit=int(args.limit),
        concurrency=int(args.concurrency),
        rebuild_existing=bool(args.rebuild_existing),
        progress_callback=_progress,
    )
    console.print(
        f"[green]Catfood fiber/carb engineering done.[/green] scanned={res.scanned}, upserted={res.upserted}, "
        f"source={res.source_table}, fiber_carb_table={res.target_table}, batch={res.batch_id}"
    )


def cmd_catfood_biotic_engineering(args):
    st = load_settings()
    engine = make_engine(st.mysql)
    healthcheck(engine)

    last_unique_done = -1

    def _progress(payload):
        nonlocal last_unique_done
        status = str(payload.get("status") or "")
        unique_total = int(payload.get("unique_total") or 0)
        unique_done = int(payload.get("unique_done") or 0)
        written = int(payload.get("written") or 0)
        if status == "running" and unique_total > 0 and unique_done != last_unique_done:
            last_unique_done = unique_done
            console.print(
                f"[cyan]Catfood biotic engineering:[/cyan] "
                f"{unique_done}/{unique_total} unique compositions, written={written}"
            )

    res = backfill_catfood_biotic_labels(
        engine=engine,
        openai_cfg=st.openai,
        source_table=args.source_table,
        biotic_table=args.biotic_table,
        limit=int(args.limit),
        concurrency=int(args.concurrency),
        rebuild_existing=bool(args.rebuild_existing),
        progress_callback=_progress,
    )
    console.print(
        f"[green]Catfood biotic engineering done.[/green] scanned={res.scanned}, upserted={res.upserted}, "
        f"source={res.source_table}, biotic_table={res.target_table}, batch={res.batch_id}"
    )


def cmd_parse_taobao_title(args):
    st = load_settings()
    engine = make_engine(st.mysql)
    healthcheck(engine)

    res = parse_taobao_title_standardized(
        engine=engine,
        source_table=args.source_table,
        target_table=args.target_table,
        limit=int(args.limit),
    )
    console.print(
        f"[green]Parse taobao title done.[/green] scanned={res.scanned}, upserted={res.upserted}, "
        f"source={res.source_table}, target={res.target_table}, batch={res.batch_id}"
    )

def cmd_run_all(args):
    # ingest then label
    cmd_ingest(args)
    cmd_label(args)

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="csv-mysql-labeling")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init-dicts", help="initialize dictionaries into MySQL (idempotent)")
    p_init.set_defaults(func=cmd_init_dicts)

    p_ing = sub.add_parser("ingest", help="ingest CSV into raw_comments")
    p_ing.add_argument("--csv", required=True, help="path to CSV")
    p_ing.add_argument("--source", required=True, help="source_name tag")
    p_ing.set_defaults(func=cmd_ingest)

    p_xhs = sub.add_parser("ingest-xhs", help="ingest xiaohongshu CSV into xiaohongshu_raw_comments")
    p_xhs.add_argument("--csv", required=True, help="path to CSV")
    p_xhs.add_argument("--source", help="source_name tag (default from config pipeline.source_name)")
    p_xhs.add_argument("--keyword", help="fallback keyword when CSV has no 检索词 column")
    p_xhs.add_argument("--encoding", help="CSV encoding (default from config pipeline.ingest.csv_encoding)")
    p_xhs.add_argument("--batch-size", help="insert batch size (default from config pipeline.ingest.chunksize)")
    p_xhs.set_defaults(func=cmd_ingest_xhs)

    p_ci = sub.add_parser("ingest-catfood-ingredients", help="ingest catfood ingredient csv files")
    p_ci.add_argument("--input-dir", default="data", help="directory containing catfood ingredient csv files")
    p_ci.add_argument("--pattern", default="*.csv", help="glob pattern in input dir")
    p_ci.add_argument("--table", default="catfood_ingredient_raw", help="target table")
    p_ci.add_argument("--batch-size", default="500", help="insert batch size")
    p_ci.set_defaults(func=cmd_ingest_catfood_ingredients)

    p_tb = sub.add_parser("ingest-taobao-list", help="ingest taobao detail_items_raw_*.json into taobao_catfood_list_items")
    p_tb.add_argument(
        "--input-dir",
        default=os.getenv("TAOBAO_INPUT_DIR", DEFAULT_TAOBAO_INPUT_DIR),
        help="directory containing detail_items_raw_*.json",
    )
    p_tb.add_argument(
        "--pattern",
        default="detail_items_raw_*.json",
        help="glob pattern in input dir (also supports list_items_raw_*.json)",
    )
    p_tb.add_argument("--table", default="taobao_catfood_list_items", help="target table")
    p_tb.add_argument("--batch-size", default="500", help="insert batch size")
    p_tb.set_defaults(func=cmd_ingest_taobao_list)

    p_brand_issue = sub.add_parser(
        "ingest-brand-issue-ctq",
        help="ingest brand_issue_ctq xlsx files and align brands to catfood_ingredient_ocr_parsed.brand",
    )
    p_brand_issue.add_argument(
        "--input-dir",
        default=os.getenv("BRAND_ISSUE_CTQ_DIR", DEFAULT_BRAND_ISSUE_INPUT_DIR),
        help="directory containing brand issue ctq xlsx files",
    )
    p_brand_issue.add_argument("--pattern", default="*.xlsx", help="glob pattern in input dir")
    p_brand_issue.add_argument("--table", default="catfood_brand_issue_ctq_cases", help="target table")
    p_brand_issue.add_argument("--parsed-table", default="catfood_ingredient_ocr_parsed", help="brand alignment source table")
    p_brand_issue.add_argument("--batch-size", default="200", help="insert batch size")
    p_brand_issue.set_defaults(func=cmd_ingest_brand_issue_ctq)

    p_cat = sub.add_parser("extract-catfood", help="incrementally extract catfood brand/health candidates")
    p_cat.add_argument("--target-table", default="catfood_brand_health_candidates", help="target candidate table")
    p_cat.add_argument("--state-table", default="catfood_brand_health_extract_state", help="incremental state table")
    p_cat.set_defaults(func=cmd_extract_catfood)

    p_brand_rel = sub.add_parser(
        "extract-catfood-brand-relations",
        help="incrementally extract catfood brand coverage and brand-transition relations from comments",
    )
    p_brand_rel.add_argument("--target-table", default="catfood_brand_relation_comments", help="target relation table")
    p_brand_rel.add_argument("--state-table", default="catfood_brand_relation_extract_state", help="incremental state table")
    p_brand_rel.set_defaults(func=cmd_extract_catfood_brand_relations)

    p_lab = sub.add_parser("label", help="label PENDING rows and write back")
    p_lab.add_argument("--limit", default="200", help="max rows per run")
    p_lab.set_defaults(func=cmd_label)

    p_ocr = sub.add_parser("ocr-image", help="OCR local image, save JSON and upsert to MySQL")
    p_ocr.add_argument("--image", required=True, help="path to local image file")
    p_ocr.add_argument("--out-json", help="output JSON path (default: data/ocr_json/<image>_<ts>.json)")
    p_ocr.add_argument("--table", default="ocr_image_results", help="target table name")
    p_ocr.set_defaults(func=cmd_ocr_image)

    p_parse_ocr = sub.add_parser("parse-catfood-ocr", help="parse catfood OCR json into structured fields")
    p_parse_ocr.add_argument("--source-table", default="catfood_ingredient_ocr_results", help="source table with ocr_json")
    p_parse_ocr.add_argument("--target-table", default="catfood_ingredient_ocr_parsed", help="target table for parsed fields")
    p_parse_ocr.add_argument("--limit", default="500", help="rows to parse per run")
    p_parse_ocr.add_argument(
        "--reparse-existing",
        action="store_true",
        help="reparse latest rows even if source_id already exists in target (default: strict incremental only)",
    )
    p_parse_ocr.set_defaults(func=cmd_parse_catfood_ocr)

    p_parse_guarantee = sub.add_parser(
        "parse-catfood-guarantee",
        help="extract catfood nutrition guarantee rows from OCR images with Qwen; parsed OCR rows are optional",
    )
    p_parse_guarantee.add_argument("--source-table", default="catfood_ingredient_ocr_results", help="source OCR image table")
    p_parse_guarantee.add_argument("--parsed-table", default="catfood_ingredient_ocr_parsed", help="optional parsed OCR table for product name/image fallback")
    p_parse_guarantee.add_argument("--info-table", default="product_info", help="target product info table")
    p_parse_guarantee.add_argument("--guarantee-table", default="product_guarantee", help="target guarantee table")
    p_parse_guarantee.add_argument("--limit", default="200", help="rows to process per run")
    p_parse_guarantee.add_argument(
        "--processed-dir",
        default=os.getenv("CATFOOD_GUARANTEE_HISTORY_DIR", DEFAULT_GUARANTEE_HISTORY_DIR),
        help="move successfully processed guarantee images into this directory",
    )
    p_parse_guarantee.add_argument(
        "--reparse-existing",
        action="store_true",
        help="reparse latest rows even if source_id already exists in product_info (default: strict incremental only)",
    )
    p_parse_guarantee.set_defaults(func=cmd_parse_catfood_guarantee)

    p_rebuild_guarantee = sub.add_parser(
        "rebuild-catfood-guarantee",
        help="rebuild product_guarantee from existing product_info.raw_extracted_json",
    )
    p_rebuild_guarantee.add_argument("--info-table", default="product_info", help="source product info table")
    p_rebuild_guarantee.add_argument("--guarantee-table", default="product_guarantee", help="target guarantee table")
    p_rebuild_guarantee.add_argument("--limit", default="0", help="max product rows to rebuild, 0 means all")
    p_rebuild_guarantee.add_argument("--source-id", help="only rebuild one source_id")
    p_rebuild_guarantee.set_defaults(func=cmd_rebuild_catfood_guarantee)

    p_clean_ing = sub.add_parser(
        "clean-catfood-ingredient-composition",
        help="normalize ingredient_composition and safely backfill brand/product_name from image_name",
    )
    p_clean_ing.add_argument("--table", default="catfood_ingredient_ocr_parsed", help="target parsed OCR table")
    p_clean_ing.add_argument("--limit", default="0", help="limit rows to clean, 0 means all matched rows")
    p_clean_ing.add_argument("--no-backup", action="store_true", help="skip creating a full-table backup before update")
    p_clean_ing.set_defaults(func=cmd_clean_catfood_ingredient_composition)

    p_rebuild_protein = sub.add_parser(
        "rebuild-protein-labels",
        help="rebuild catfood_feature_protein_labels from existing summary rows",
    )
    p_rebuild_protein.add_argument("--summary-table", default="catfood_feature_summary", help="source summary feature table")
    p_rebuild_protein.add_argument("--protein-table", default="catfood_feature_protein_labels", help="target protein label table")
    p_rebuild_protein.add_argument("--source-table", default="catfood_ingredient_ocr_parsed", help="reserved compatibility arg, currently unused")
    p_rebuild_protein.set_defaults(func=cmd_rebuild_protein_labels)

    p_rebuild_biotic = sub.add_parser(
        "rebuild-biotic-labels",
        help="rebuild catfood_feature_biotic_labels from existing summary rows",
    )
    p_rebuild_biotic.add_argument("--summary-table", default="catfood_feature_summary", help="source summary feature table")
    p_rebuild_biotic.add_argument("--biotic-table", default="catfood_feature_biotic_labels", help="target biotic label table")
    p_rebuild_biotic.set_defaults(func=cmd_rebuild_biotic_labels)

    p_label_engineering = sub.add_parser(
        "catfood-label-engineering",
        help="incrementally backfill catfood_feature_protein_labels only",
    )
    p_label_engineering.add_argument("--source-table", default="catfood_ingredient_ocr_parsed", help="source parsed OCR table")
    p_label_engineering.add_argument("--protein-table", default="catfood_feature_protein_labels", help="target protein label table")
    p_label_engineering.add_argument("--limit", default="0", help="max missing product rows to backfill, 0 means all")
    p_label_engineering.add_argument("--concurrency", default="4", help="parallel LLM requests per run")
    p_label_engineering.set_defaults(func=cmd_catfood_label_engineering)

    p_fiber_carb_engineering = sub.add_parser(
        "catfood-fiber-carb-engineering",
        help="incrementally backfill catfood_feature_fiber_carb_labels only",
    )
    p_fiber_carb_engineering.add_argument("--source-table", default="catfood_ingredient_ocr_parsed", help="source parsed OCR table")
    p_fiber_carb_engineering.add_argument("--fiber-carb-table", dest="fiber_carb_table", default="catfood_feature_fiber_carb_labels", help="target fiber/carb label table")
    p_fiber_carb_engineering.add_argument("--limit", default="0", help="max missing product rows to backfill, 0 means all")
    p_fiber_carb_engineering.add_argument("--concurrency", default="4", help="parallel LLM requests per run")
    p_fiber_carb_engineering.add_argument("--rebuild-existing", action="store_true", help="recompute and upsert existing product rows")
    p_fiber_carb_engineering.set_defaults(func=cmd_catfood_fiber_carb_engineering)

    p_biotic_engineering = sub.add_parser(
        "catfood-biotic-engineering",
        help="incrementally backfill catfood_feature_biotic_labels only",
    )
    p_biotic_engineering.add_argument("--source-table", default="catfood_ingredient_ocr_parsed", help="source parsed OCR table")
    p_biotic_engineering.add_argument("--biotic-table", default="catfood_feature_biotic_labels", help="target biotic label table")
    p_biotic_engineering.add_argument("--limit", default="0", help="max missing product rows to backfill, 0 means all")
    p_biotic_engineering.add_argument("--concurrency", default="4", help="parallel LLM requests per run")
    p_biotic_engineering.add_argument("--rebuild-existing", action="store_true", help="recompute and upsert existing product rows")
    p_biotic_engineering.set_defaults(func=cmd_catfood_biotic_engineering)

    p_parse_ing_type = sub.add_parser(
        "parse-catfood-ingredient-types",
        help="LLM-label ingredient_composition into 3 feature detail tables plus 1 summary table",
    )
    p_parse_ing_type.add_argument("--source-table", default="catfood_ingredient_ocr_parsed", help="source parsed OCR table")
    p_parse_ing_type.add_argument(
        "--summary-table",
        "--target-table",
        dest="summary_table",
        default="catfood_feature_summary",
        help="target summary feature table",
    )
    p_parse_ing_type.add_argument("--protein-table", default="catfood_feature_protein_labels", help="target protein detail table")
    p_parse_ing_type.add_argument(
        "--fiber-carb-table",
        dest="fiber_carb_table",
        default="catfood_feature_fiber_carb_labels",
        help="target fiber/carb label table",
    )
    p_parse_ing_type.add_argument("--biotic-table", default="catfood_feature_biotic_labels", help="target biotic label table")
    p_parse_ing_type.add_argument("--limit", default="500", help="rows to parse per run")
    p_parse_ing_type.add_argument("--concurrency", default="4", help="parallel LLM requests per run")
    p_parse_ing_type.add_argument("--write-batch-size", default="5", help="flush every N summary rows")
    p_parse_ing_type.add_argument(
        "--reparse-existing",
        action="store_true",
        help="reparse latest rows even if source_id already exists in target (default: strict incremental only)",
    )
    p_parse_ing_type.set_defaults(func=cmd_parse_catfood_ingredient_types)

    p_parse_tb = sub.add_parser("parse-taobao-title", help="standardize taobao title fields and unit price per kg")
    p_parse_tb.add_argument("--source-table", default="taobao_catfood_list_items", help="source table with taobao title")
    p_parse_tb.add_argument("--target-table", default="taobao_catfood_title_parsed", help="target table for parsed fields")
    p_parse_tb.add_argument("--limit", default="1000", help="rows to parse per run")
    p_parse_tb.set_defaults(func=cmd_parse_taobao_title)

    p_all = sub.add_parser("run-all", help="ingest then label")
    p_all.add_argument("--csv", required=True, help="path to CSV")
    p_all.add_argument("--source", required=True, help="source_name tag")
    p_all.add_argument("--limit", default="200", help="max rows per run")
    p_all.set_defaults(func=cmd_run_all)

    return p

def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
