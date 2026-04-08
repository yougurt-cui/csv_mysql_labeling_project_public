from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

def now_ms() -> int:
    return int(time.time() * 1000)

def safe_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

def safe_json_loads(s: str) -> Any:
    return json.loads(s)

def truncate(s: str, n: int = 300) -> str:
    s = s or ""
    return s if len(s) <= n else (s[:n] + "...")
