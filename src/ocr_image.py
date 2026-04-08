from __future__ import annotations

import base64
import hashlib
import json
import mimetypes
import os
import re
import shutil
import subprocess
import tempfile
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from openai import OpenAI, RateLimitError
from sqlalchemy import text
from sqlalchemy.engine import Engine
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .utils import now_ms, safe_json_dumps, safe_json_loads

OCR_PROMPT_VERSION = "v1"
DEFAULT_QWEN_OCR_MODEL = "qwen-vl-ocr-latest"
DEFAULT_QWEN_ENDPOINT = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"


def _normalize_provider(ocr_cfg: Dict[str, Any]) -> str:
    provider = str((ocr_cfg or {}).get("provider", "ocr_space")).strip().lower()
    provider = provider.replace(".", "_")
    if provider in {"ocrspace", "ocr_space"}:
        return "ocr_space"
    if provider in {"qwen", "qwen_ocr", "dashscope"}:
        return "qwen"
    if provider in {"openai"}:
        return "openai"
    raise ValueError(f"unsupported ocr provider: {provider}")


def _make_openai_client(openai_cfg: Dict[str, Any]) -> OpenAI:
    api_key = (openai_cfg or {}).get("api_key", "")
    if not api_key:
        raise ValueError("OpenAI api_key is empty. Fill it in config/config.yaml")
    base_url = (openai_cfg or {}).get("base_url", "") or None
    if base_url:
        return OpenAI(api_key=api_key, base_url=base_url)
    return OpenAI(api_key=api_key)


def _normalize_qwen_endpoint(raw: str) -> str:
    endpoint = str(raw or "").strip().rstrip("/")
    if not endpoint:
        return DEFAULT_QWEN_ENDPOINT
    if endpoint.endswith("/chat/completions"):
        return endpoint
    return f"{endpoint}/chat/completions"


def _resolve_qwen_ocr_cfg(
    ocr_cfg: Optional[Dict[str, Any]] = None,
    openai_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    ocr_cfg = dict(ocr_cfg or {})
    openai_cfg = dict(openai_cfg or {})

    api_key = (
        str(ocr_cfg.get("qwen_api_key") or "").strip()
        or str(os.getenv("QWEN_API_KEY") or "").strip()
        or str(os.getenv("DASHSCOPE_API_KEY") or "").strip()
    )
    endpoint = _normalize_qwen_endpoint(
        str(ocr_cfg.get("qwen_endpoint") or "").strip()
        or str(os.getenv("QWEN_ENDPOINT") or "").strip()
        or str(os.getenv("QWEN_BASE_URL") or "").strip()
    )
    model = (
        str(ocr_cfg.get("qwen_ocr_model") or "").strip()
        or str(ocr_cfg.get("qwen_model") or "").strip()
        or str(os.getenv("QWEN_MODEL") or "").strip()
        or DEFAULT_QWEN_OCR_MODEL
    )
    timeout_seconds = float(
        ocr_cfg.get("qwen_timeout_seconds")
        or ocr_cfg.get("ocr_timeout_seconds")
        or os.getenv("QWEN_TIMEOUT_SECONDS")
        or 90
    )

    if not api_key:
        cfg_api_key = str(openai_cfg.get("api_key") or "").strip()
        cfg_base_url = str(openai_cfg.get("base_url") or "").strip()
        cfg_model = str(openai_cfg.get("model") or "").strip()
        if cfg_api_key and (("dashscope" in cfg_base_url.lower()) or cfg_model.lower().startswith("qwen")):
            api_key = cfg_api_key
            endpoint = _normalize_qwen_endpoint(cfg_base_url or endpoint)
            model = cfg_model or model

    if not api_key:
        raise ValueError(
            "缺少千问 OCR 配置。请设置 ocr.qwen_api_key，或设置 QWEN_API_KEY / DASHSCOPE_API_KEY。"
        )

    return {
        "api_key": api_key,
        "endpoint": endpoint,
        "model": model or DEFAULT_QWEN_OCR_MODEL,
        "timeout_seconds": max(10.0, timeout_seconds),
    }


def _strip_code_fence(text_value: str) -> str:
    text_value = (text_value or "").strip()
    if text_value.startswith("```"):
        text_value = re.sub(r"^```[a-zA-Z0-9_-]*\n?", "", text_value)
        text_value = re.sub(r"\n?```$", "", text_value)
    return text_value.strip()


def _extract_content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: List[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
                continue
            if not isinstance(item, dict):
                continue
            if isinstance(item.get("text"), str):
                parts.append(str(item.get("text") or ""))
                continue
            if item.get("type") == "text" and isinstance(item.get("text"), str):
                parts.append(str(item.get("text") or ""))
        return "\n".join(x for x in parts if x).strip()
    return str(content or "").strip()


def _to_data_url(image_path: Path) -> tuple[str, str]:
    raw = image_path.read_bytes()
    mime, _ = mimetypes.guess_type(str(image_path))
    mime = mime or "image/jpeg"
    b64 = base64.b64encode(raw).decode("ascii")
    data_url = f"data:{mime};base64,{b64}"
    sha256 = hashlib.sha256(raw).hexdigest()
    return data_url, sha256


def _file_sha256(image_path: Path) -> str:
    return hashlib.sha256(image_path.read_bytes()).hexdigest()


def _safe_table_name(name: str) -> str:
    if not name:
        raise ValueError("table name is empty")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if any(ch not in allowed for ch in name):
        raise ValueError(f"unsafe table name: {name}")
    return name


def _ensure_table(engine: Engine, table_name: str) -> None:
    table_name = _safe_table_name(table_name)
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
      id BIGINT PRIMARY KEY AUTO_INCREMENT,
      image_path VARCHAR(1024) NOT NULL,
      image_name VARCHAR(255) NOT NULL,
      file_sha256 CHAR(64) NOT NULL,
      ocr_text LONGTEXT NULL,
      ocr_json LONGTEXT NOT NULL,
      model_name VARCHAR(64) NOT NULL,
      model_latency_ms INT NULL,
      prompt_version VARCHAR(32) NOT NULL DEFAULT 'v1',
      created_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uq_file_sha256 (file_sha256),
      KEY idx_image_name (image_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _is_insufficient_quota(err: RateLimitError) -> bool:
    body = getattr(err, "body", None)
    if isinstance(body, dict):
        code = str(body.get("code") or "").lower()
        err_obj = body.get("error")
        if isinstance(err_obj, dict):
            code = code or str(err_obj.get("code") or "").lower()
        if "insufficient_quota" in code:
            return True
    return "insufficient_quota" in str(err).lower()


@retry(
    stop=stop_after_attempt(6),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=retry_if_exception_type((RateLimitError,)),
    reraise=True,
)
def _call_openai_ocr(client: OpenAI, model: str, data_url: str) -> Dict[str, Any]:
    system_prompt = "你是 OCR 识别助手。你必须只返回 JSON，且必须是合法 JSON。"
    user_prompt = (
        "请识别图片中的所有可见文字，按阅读顺序输出。"
        "输出格式必须为："
        '{"full_text":"string","lines":["string"],"language_hint":"string|null","notes":"string"}'
    )
    start = now_ms()
    try:
        resp = client.responses.create(
            model=model,
            input=[
                {
                    "role": "system",
                    "content": [{"type": "input_text", "text": system_prompt}],
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": user_prompt},
                        {"type": "input_image", "image_url": data_url},
                    ],
                },
            ],
            text={"format": {"type": "json_object"}},
        )
        out_text = getattr(resp, "output_text", None)
        if not out_text:
            out_text = ""
            for item in getattr(resp, "output", []) or []:
                for content in getattr(item, "content", []) or []:
                    if getattr(content, "type", "") == "output_text":
                        out_text += getattr(content, "text", "")
        return {
            "text": out_text or "{}",
            "latency_ms": now_ms() - start,
            "model_name": model,
        }
    except Exception:
        comp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": user_prompt},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                },
            ],
            response_format={"type": "json_object"},
        )
        out_text = comp.choices[0].message.content or "{}"
        return {
            "text": out_text,
            "latency_ms": now_ms() - start,
            "model_name": model,
        }


def _call_qwen_ocr(
    image_path: Path,
    qwen_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    data_url, _ = _to_data_url(image_path)
    payload = {
        "model": qwen_cfg["model"],
        "temperature": 0,
        "messages": [
            {
                "role": "system",
                "content": "你是 OCR 引擎，只返回图片里的文字，不要解释。",
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "请提取图片中的全部可见文字，按阅读顺序原样输出。"},
                    {"type": "image_url", "image_url": {"url": data_url}},
                ],
            },
        ],
    }
    req = Request(
        qwen_cfg["endpoint"],
        data=safe_json_dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {qwen_cfg['api_key']}",
            "User-Agent": "csv-mysql-labeling/qwen-ocr",
        },
        method="POST",
    )
    start = now_ms()
    try:
        with urlopen(req, timeout=float(qwen_cfg["timeout_seconds"])) as resp:
            raw_text = resp.read().decode("utf-8", errors="ignore")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Qwen OCR HTTP {exc.code}: {detail or exc.reason}") from exc
    except URLError as exc:
        raise RuntimeError(f"Qwen OCR network error: {exc}") from exc

    obj = safe_json_loads(raw_text)
    if not isinstance(obj, dict):
        raise ValueError("Qwen OCR response is not a JSON object")
    if obj.get("error"):
        err = obj.get("error")
        if isinstance(err, dict):
            raise RuntimeError(str(err.get("message") or err.get("code") or err))
        raise RuntimeError(str(err))

    choices = obj.get("choices") or []
    if not choices:
        raise RuntimeError("Qwen OCR response missing choices")
    msg = (choices[0] or {}).get("message") or {}
    full_text = _strip_code_fence(_extract_content_text(msg.get("content")))
    lines = [ln.strip() for ln in full_text.splitlines() if ln.strip()]
    model_json = {
        "full_text": full_text,
        "lines": lines,
        "language_hint": None,
        "notes": "provider=qwen",
        "provider": "qwen",
    }
    return {
        "text": safe_json_dumps(model_json),
        "latency_ms": now_ms() - start,
        "model_name": str(obj.get("model") or qwen_cfg["model"]),
    }


def _build_multipart_body(
    fields: Dict[str, str],
    file_field: str,
    filename: str,
    file_bytes: bytes,
    file_content_type: str,
) -> Tuple[bytes, str]:
    boundary = f"----ocrspace-{uuid.uuid4().hex}"
    chunks = []
    for key, value in fields.items():
        chunks.append(f"--{boundary}\r\n".encode("utf-8"))
        chunks.append(
            f'Content-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'.encode("utf-8")
        )
    chunks.append(f"--{boundary}\r\n".encode("utf-8"))
    chunks.append(
        (
            f'Content-Disposition: form-data; name="{file_field}"; '
            f'filename="{filename}"\r\n'
            f"Content-Type: {file_content_type}\r\n\r\n"
        ).encode("utf-8")
    )
    chunks.append(file_bytes)
    chunks.append(b"\r\n")
    chunks.append(f"--{boundary}--\r\n".encode("utf-8"))
    return b"".join(chunks), boundary


def _compress_for_ocr_space(
    image_path: Path,
    max_bytes: int,
    resize_candidates: List[int],
    quality_candidates: List[int],
) -> Tuple[Path, str]:
    sips_bin = shutil.which("sips")
    if not sips_bin:
        raise RuntimeError(
            "OCR.Space 文件超过大小限制，且当前环境缺少 sips 无法自动压缩。"
        )

    tmp_dir = Path(tempfile.mkdtemp(prefix="ocrspace-img-"))
    out_path = tmp_dir / f"{image_path.stem}_ocrspace.jpg"
    src_size = image_path.stat().st_size
    last_error = ""

    try:
        for max_side in resize_candidates:
            for quality in quality_candidates:
                cmd = [
                    sips_bin,
                    "--resampleHeightWidthMax",
                    str(max_side),
                    "-s",
                    "format",
                    "public.jpeg",
                    "-s",
                    "formatOptions",
                    str(quality),
                    str(image_path),
                    "--out",
                    str(out_path),
                ]
                try:
                    subprocess.run(cmd, check=True, capture_output=True, text=True)
                except subprocess.CalledProcessError as exc:
                    last_error = (exc.stderr or exc.stdout or str(exc)).strip()
                    continue

                if not out_path.exists():
                    continue
                out_size = out_path.stat().st_size
                if out_size <= max_bytes:
                    note = (
                        f"auto_compressed=1,src={src_size}B,out={out_size}B,"
                        f"max_side={max_side},quality={quality}"
                    )
                    return out_path, note
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise

    shutil.rmtree(tmp_dir, ignore_errors=True)
    extra = f"，压缩失败详情: {last_error}" if last_error else ""
    raise RuntimeError(
        f"OCR.Space 文件超过大小限制（>{max_bytes} bytes），自动压缩后仍超限或失败{extra}"
    )


def _prepare_ocr_space_image(
    image_path: Path,
    ocr_cfg: Dict[str, Any],
    aggressive: bool = False,
    force_compress: bool = False,
) -> Tuple[Path, Optional[str], Optional[Path]]:
    max_kb = int((ocr_cfg or {}).get("ocr_space_max_file_kb", 1024) or 1024)
    if aggressive:
        retry_kb = int((ocr_cfg or {}).get("ocr_space_retry_max_file_kb", 768) or 768)
        max_kb = min(max_kb, retry_kb)
    max_bytes = max_kb * 1024
    auto_compress = bool((ocr_cfg or {}).get("ocr_space_auto_compress", True))

    src_size = image_path.stat().st_size
    if (src_size <= max_bytes) and not force_compress:
        return image_path, None, None

    if not auto_compress:
        raise RuntimeError(
            f"OCR.Space 文件超过限制：{src_size} bytes > {max_bytes} bytes。"
            "可在 config.yaml 设置 ocr.ocr_space_auto_compress: true 自动压缩。"
        )

    if aggressive:
        resize_candidates = [1800, 1500, 1200, 1000, 800, 640]
        quality_candidates = [68, 58, 48, 38, 30]
    else:
        resize_candidates = [2600, 2200, 1800, 1500, 1200, 1000, 800]
        quality_candidates = [82, 72, 62, 52, 42, 35]

    out_path, note = _compress_for_ocr_space(
        image_path=image_path,
        max_bytes=max_bytes,
        resize_candidates=resize_candidates,
        quality_candidates=quality_candidates,
    )
    return out_path, note, out_path.parent


def _is_ocr_space_retryable_error(msg: str) -> bool:
    text_value = (msg or "").lower()
    if "system resource exhaustion" in text_value:
        return True
    if "ocr binary failed" in text_value:
        return True
    if "error e500" in text_value:
        return True
    if "network error" in text_value:
        return True
    if "http 5" in text_value:
        return True
    if "timed out" in text_value or "timeout" in text_value:
        return True
    return False


def _call_ocr_space(image_path: Path, ocr_cfg: Dict[str, Any]) -> Dict[str, Any]:
    api_key = str((ocr_cfg or {}).get("ocr_space_api_key", "helloworld")).strip() or "helloworld"
    endpoint = str((ocr_cfg or {}).get("ocr_space_endpoint", "https://api.ocr.space/parse/image")).strip()
    language = str((ocr_cfg or {}).get("ocr_space_language", "chs")).strip() or "chs"
    engine = str((ocr_cfg or {}).get("ocr_space_engine", "2")).strip() or "2"
    timeout_s = int((ocr_cfg or {}).get("ocr_timeout_seconds", 90) or 90)
    max_attempts = int((ocr_cfg or {}).get("ocr_space_processing_retry", 3) or 3)
    max_attempts = max(1, max_attempts)
    start = now_ms()
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        aggressive = attempt > 1
        request_image_path, preprocess_note, temp_dir = _prepare_ocr_space_image(
            image_path=image_path,
            ocr_cfg=ocr_cfg,
            aggressive=aggressive,
            force_compress=aggressive,
        )
        try:
            file_bytes = request_image_path.read_bytes()
            mime = "image/jpeg" if request_image_path.suffix.lower() in {".jpg", ".jpeg"} else None
            if not mime:
                guessed, _ = mimetypes.guess_type(str(request_image_path))
                mime = guessed or "application/octet-stream"
            fields = {
                "apikey": api_key,
                "language": language,
                "isOverlayRequired": "false",
                "scale": "true",
                "OCREngine": engine,
            }
            body, boundary = _build_multipart_body(
                fields=fields,
                file_field="file",
                filename=image_path.name,
                file_bytes=file_bytes,
                file_content_type=mime,
            )
            req = Request(
                endpoint,
                data=body,
                method="POST",
                headers={
                    "Content-Type": f"multipart/form-data; boundary={boundary}",
                    "User-Agent": "csv-mysql-labeling/ocr-space",
                },
            )

            try:
                with urlopen(req, timeout=timeout_s) as resp:
                    raw_text = resp.read().decode("utf-8", errors="ignore")
            except HTTPError as e:
                detail = e.read().decode("utf-8", errors="ignore")
                raise RuntimeError(f"OCR.Space HTTP {e.code}: {detail or e.reason}") from e
            except URLError as e:
                raise RuntimeError(f"OCR.Space network error: {e}") from e

            obj = safe_json_loads(raw_text)
            if not isinstance(obj, dict):
                raise ValueError("OCR.Space response is not a JSON object")

            if obj.get("IsErroredOnProcessing"):
                err = obj.get("ErrorMessage")
                if isinstance(err, list):
                    err_msg = "; ".join(str(x) for x in err if x)
                else:
                    err_msg = str(err or obj.get("ErrorDetails") or "unknown error")
                raise RuntimeError(f"OCR.Space processing failed: {err_msg}")

            parsed = obj.get("ParsedResults") or []
            parts = []
            for item in parsed:
                if isinstance(item, dict):
                    txt = str(item.get("ParsedText") or "").strip()
                    if txt:
                        parts.append(txt)
            full_text = "\n".join(parts).strip()
            lines = [ln.strip() for ln in full_text.splitlines() if ln.strip()]
            notes = "provider=ocr_space"
            if preprocess_note:
                notes = f"{notes};{preprocess_note}"
            if attempt > 1:
                notes = f"{notes};retry_attempt={attempt}"
            model_json = {
                "full_text": full_text,
                "lines": lines,
                "language_hint": language or None,
                "notes": notes,
                "provider": "ocr_space",
            }
            return {
                "text": safe_json_dumps(model_json),
                "latency_ms": now_ms() - start,
                "model_name": "ocr.space-free",
            }
        except Exception as exc:
            last_exc = exc
            msg = str(exc)
            if attempt >= max_attempts or (not _is_ocr_space_retryable_error(msg)):
                raise
            sleep_s = min(8, 2 ** (attempt - 1))
            time.sleep(sleep_s)
        finally:
            if temp_dir:
                shutil.rmtree(temp_dir, ignore_errors=True)

    if last_exc:
        raise last_exc
    raise RuntimeError("OCR.Space processing failed: unknown error")


def _default_output_path(image_path: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    out_dir = Path(__file__).resolve().parents[1] / "data" / "ocr_json"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{image_path.stem}_{ts}.json"


def _call_ocr(
    image_path: Path,
    ocr_cfg: Dict[str, Any],
    openai_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    provider = _normalize_provider(ocr_cfg)
    if provider == "ocr_space":
        return _call_ocr_space(image_path=image_path, ocr_cfg=ocr_cfg)
    if provider == "qwen":
        qwen_cfg = _resolve_qwen_ocr_cfg(ocr_cfg=ocr_cfg, openai_cfg=openai_cfg)
        return _call_qwen_ocr(image_path=image_path, qwen_cfg=qwen_cfg)

    model = (openai_cfg or {}).get("model", "gpt-4.1-mini")
    client = _make_openai_client(openai_cfg)
    data_url, _ = _to_data_url(image_path)
    try:
        return _call_openai_ocr(client=client, model=model, data_url=data_url)
    except RateLimitError as exc:
        if _is_insufficient_quota(exc):
            raise RuntimeError(
                "OpenAI 配额不足（insufficient_quota）。请充值/检查账号可用额度后重试。"
            ) from exc
        raise RuntimeError(
            "OpenAI 接口触发限流（RateLimit）。请稍后重试，或降低并发/调用频率。"
        ) from exc


def run_ocr_image(
    engine: Engine,
    image_path: Path,
    out_json_path: Optional[Path] = None,
    table_name: str = "ocr_image_results",
    ocr_cfg: Optional[Dict[str, Any]] = None,
    openai_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not image_path.exists():
        raise FileNotFoundError(f"image file not found: {image_path}")
    if not image_path.is_file():
        raise ValueError(f"image path is not a file: {image_path}")

    ocr_cfg = ocr_cfg or {}
    openai_cfg = openai_cfg or {}

    table_name = _safe_table_name(table_name)
    _ensure_table(engine, table_name)
    file_sha256 = _file_sha256(image_path)

    result = _call_ocr(image_path=image_path, ocr_cfg=ocr_cfg, openai_cfg=openai_cfg)
    model_json = safe_json_loads(result["text"])
    if not isinstance(model_json, dict):
        raise ValueError("OCR model output is not a JSON object")

    full_text = str(model_json.get("full_text") or "").strip()
    if "lines" not in model_json:
        model_json["lines"] = []
    if "language_hint" not in model_json:
        model_json["language_hint"] = None
    if "notes" not in model_json:
        model_json["notes"] = ""

    out_path = out_json_path or _default_output_path(image_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(safe_json_dumps(model_json), encoding="utf-8")

    model_name = str(result.get("model_name") or "ocr")
    upsert_sql = f"""
    INSERT INTO {table_name}(
      image_path, image_name, file_sha256,
      ocr_text, ocr_json, model_name, model_latency_ms, prompt_version
    )
    VALUES(
      :image_path, :image_name, :file_sha256,
      :ocr_text, :ocr_json, :model_name, :model_latency_ms, :prompt_version
    )
    ON DUPLICATE KEY UPDATE
      image_path=VALUES(image_path),
      image_name=VALUES(image_name),
      ocr_text=VALUES(ocr_text),
      ocr_json=VALUES(ocr_json),
      model_name=VALUES(model_name),
      model_latency_ms=VALUES(model_latency_ms),
      prompt_version=VALUES(prompt_version)
    """
    with engine.begin() as conn:
        conn.execute(
            text(upsert_sql),
            {
                "image_path": str(image_path.resolve()),
                "image_name": image_path.name,
                "file_sha256": file_sha256,
                "ocr_text": full_text,
                "ocr_json": safe_json_dumps(model_json),
                "model_name": model_name,
                "model_latency_ms": int(result.get("latency_ms") or 0),
                "prompt_version": OCR_PROMPT_VERSION,
            },
        )

    return {
        "image_path": str(image_path.resolve()),
        "json_path": str(out_path.resolve()),
        "file_sha256": file_sha256,
        "table_name": table_name,
        "model_name": model_name,
        "model_latency_ms": int(result.get("latency_ms") or 0),
        "ocr_text": full_text,
    }


def update_ocr_image_record_path(
    engine: Engine,
    table_name: str,
    file_sha256: str,
    image_path: Path,
) -> None:
    table_name = _safe_table_name(table_name)
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                UPDATE {table_name}
                SET image_path = :image_path,
                    image_name = :image_name
                WHERE file_sha256 = :file_sha256
                """
            ),
            {
                "image_path": str(image_path.resolve()),
                "image_name": image_path.name,
                "file_sha256": str(file_sha256 or "").strip(),
            },
        )
