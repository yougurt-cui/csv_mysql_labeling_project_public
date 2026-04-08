from __future__ import annotations

import textwrap
from typing import Any, Dict, List, Tuple
from sqlalchemy import text
from sqlalchemy.engine import Engine
from rich.console import Console

console = Console()

def upsert_dict_items(engine: Engine, dict_seed: Dict[str, Any]) -> None:
    """从 dictionaries_seed.yaml 写入 dict_items（幂等，重复执行不会重复插入）。"""
    rows: List[Tuple[str, str, str, str]] = []

    # dict_seed is like: {product_category: [{code,name}, ...], ctq: [...], ...}
    for dict_type, items in (dict_seed or {}).items():
        if not items:
            continue
        for it in items:
            code = str(it.get("code", "")).strip()
            name = str(it.get("name", "")).strip()
            extra = it.get("extra_json")
            extra_json = None if extra is None else str(extra)
            if not code or not name:
                continue
            rows.append((dict_type, code, name, extra_json))

    if not rows:
        console.print("[yellow]No dictionary items to upsert (seed file empty).[/yellow]")
        return

    sql = textwrap.dedent("""
    INSERT INTO dict_items(dict_type, code, name, extra_json)
    VALUES (:dict_type, :code, :name, :extra_json)
    ON DUPLICATE KEY UPDATE
      name = VALUES(name),
      extra_json = VALUES(extra_json),
      is_active = 1;
    """).strip()

    with engine.begin() as conn:
        conn.execute(text(sql), [
            {"dict_type": r[0], "code": r[1], "name": r[2], "extra_json": r[3]}
            for r in rows
        ])

    console.print(f"[green]Upserted dict_items: {len(rows)} rows[/green]")

def load_dict_by_type(engine: Engine, dict_type: str) -> List[Dict[str, str]]:
    with engine.connect() as conn:
        res = conn.execute(
            text("""SELECT code, name FROM dict_items WHERE dict_type=:t AND is_active=1 ORDER BY code"""),
            {"t": dict_type},
        ).mappings().all()
    return [{"code": r["code"], "name": r["name"]} for r in res]
