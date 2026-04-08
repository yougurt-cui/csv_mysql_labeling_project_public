from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple

from openai import OpenAI
from rich.console import Console
from sqlalchemy import text
from sqlalchemy.engine import Engine
from tenacity import retry, stop_after_attempt, wait_exponential

from .prompts import build_labeling_messages, PROMPT_VERSION
from .utils import now_ms, safe_json_dumps, safe_json_loads

console = Console()

def _make_client(openai_cfg: Dict[str, Any]) -> OpenAI:
    api_key = (openai_cfg or {}).get("api_key", "")
    if not api_key:
        raise ValueError("OpenAI api_key is empty. Fill it in config/config.yaml")
    base_url = (openai_cfg or {}).get("base_url", "") or None
    if base_url:
        return OpenAI(api_key=api_key, base_url=base_url)
    return OpenAI(api_key=api_key)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=20))
def _call_model(client: OpenAI, model: str, messages: List[Dict[str, str]], temperature: int = 0) -> Dict[str, Any]:
    """优先用 Responses API；不可用时退回到 ChatCompletions。"""
    start = now_ms()
    try:
        resp = client.responses.create(
            model=model,
            input=messages,
            temperature=temperature,
            text={"format": {"type": "json_object"}}
        )
        # responses API: resp.output_text is the merged text
        out_text = getattr(resp, "output_text", None)
        if not out_text:
            # fallback: scan outputs
            out_text = ""
            for item in getattr(resp, "output", []) or []:
                for c in getattr(item, "content", []) or []:
                    if getattr(c, "type", "") == "output_text":
                        out_text += getattr(c, "text", "")
        latency = now_ms() - start
        return {"text": out_text, "latency_ms": latency}
    except Exception:
        # ChatCompletions fallback
        comp = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            response_format={"type": "json_object"},
        )
        out_text = comp.choices[0].message.content or "{}"
        latency = now_ms() - start
        return {"text": out_text, "latency_ms": latency}

def _fetch_pending(engine: Engine, limit: int) -> List[Dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
              SELECT id, comment_text
              FROM raw_comments
              WHERE label_status='PENDING'
              ORDER BY id
              LIMIT :lim
            """),
            {"lim": limit},
        ).mappings().all()
    return [dict(r) for r in rows]

def _mark_error(engine: Engine, raw_id: int) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""UPDATE raw_comments SET label_status='ERROR', label_ts=NOW() WHERE id=:id"""),
            {"id": raw_id},
        )

def _mark_done(engine: Engine, raw_id: int) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""UPDATE raw_comments SET label_status='DONE', label_ts=NOW() WHERE id=:id"""),
            {"id": raw_id},
        )

def label_pending(
    engine: Engine,
    openai_cfg: Dict[str, Any],
    dicts: Dict[str, Any],
    limit: int = 200,
    batch_size: int = 10,
    temperature: int = 0,
) -> int:
    client = _make_client(openai_cfg)
    model = (openai_cfg or {}).get("model", "gpt-4.1-mini")
    pending = _fetch_pending(engine, limit=limit)
    if not pending:
        console.print("[green]No PENDING rows.[/green]")
        return 0

    console.print(f"[cyan]Pending rows:[/cyan] {len(pending)} (limit={limit})")
    done = 0

    for i, row in enumerate(pending, start=1):
        raw_id = int(row["id"])
        text_in = str(row["comment_text"] or "").strip()
        if not text_in:
            _mark_error(engine, raw_id)
            continue

        messages = build_labeling_messages(text_in, dicts=dicts)
        try:
            r = _call_model(client, model=model, messages=messages, temperature=temperature)
            out = safe_json_loads(r["text"])
        except Exception as e:
            console.print(f"[red]Label error raw_id={raw_id}: {e}[/red]")
            _mark_error(engine, raw_id)
            continue

        # normalize expected keys
        product_category_code = out.get("product_category_code")
        sentiment_code = out.get("sentiment_code")
        purchase_intent_code = out.get("purchase_intent_code")
        entities = out.get("entities", [])
        ctq_labels = out.get("ctq_labels", [])
        notes = out.get("notes", "")

        model_json = out  # full json

        insert_sql = """
        INSERT INTO comment_labels(
          raw_comment_id,
          product_category_code, sentiment_code, purchase_intent_code,
          entities_json, ctq_json, model_json,
          model_name, model_latency_ms, prompt_version
        )
        VALUES(
          :raw_comment_id,
          :product_category_code, :sentiment_code, :purchase_intent_code,
          :entities_json, :ctq_json, :model_json,
          :model_name, :model_latency_ms, :prompt_version
        )
        ON DUPLICATE KEY UPDATE
          product_category_code=VALUES(product_category_code),
          sentiment_code=VALUES(sentiment_code),
          purchase_intent_code=VALUES(purchase_intent_code),
          entities_json=VALUES(entities_json),
          ctq_json=VALUES(ctq_json),
          model_json=VALUES(model_json),
          model_name=VALUES(model_name),
          model_latency_ms=VALUES(model_latency_ms),
          prompt_version=VALUES(prompt_version);
        """

        with engine.begin() as conn:
            conn.execute(text(insert_sql), {
                "raw_comment_id": raw_id,
                "product_category_code": product_category_code,
                "sentiment_code": sentiment_code,
                "purchase_intent_code": purchase_intent_code,
                "entities_json": safe_json_dumps(entities),
                "ctq_json": safe_json_dumps(ctq_labels),
                "model_json": safe_json_dumps(model_json),
                "model_name": model,
                "model_latency_ms": int(r.get("latency_ms") or 0),
                "prompt_version": PROMPT_VERSION,
            })

        _mark_done(engine, raw_id)
        done += 1
        if i % 20 == 0:
            console.print(f"[cyan]Progress[/cyan] {i}/{len(pending)} done={done}")

    console.print(f"[green]Labeling finished. done={done}[/green]")
    return done
