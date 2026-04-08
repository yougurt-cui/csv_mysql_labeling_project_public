from __future__ import annotations

import base64
import json
import mimetypes
import os
import re
import shutil
import uuid
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .parse_catfood_ocr import ensure_parsed_table
from .utils import now_ms, safe_json_dumps, safe_json_loads

TABLE_RE = re.compile(r"^[A-Za-z0-9_]+$")
DEFAULT_BASIS = "干物质"
DEFAULT_MODEL = "qwen-vl-ocr-latest"
DEFAULT_ENDPOINT = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INGREDIENT_HISTORY_DIR = PROJECT_ROOT / "history" / "catfood_ingredient_images"
DEFAULT_PROCESSED_HISTORY_DIR = DEFAULT_INGREDIENT_HISTORY_DIR / "guarantee"
DECIMAL_2 = Decimal("0.01")
NULLISH_MARKERS = {"null", "none", "nil", "n/a", "na", "无", "空", "未写", "未提供", "未注明", "未标注"}
GUARANTEE_SECTION_KEYWORDS = (
    "保证值",
    "保证成分",
    "营养保证值",
    "营养成分分析",
    "营养成分及含量",
    "營養成分及含量",
    "营养分析",
    "分析保证值",
    "产品成分分析",
    "guaranteed analysis",
    "guaranteedanalysis",
)
CORE_GUARANTEE_ALIASES: Dict[str, Tuple[str, ...]] = {
    "粗蛋白": ("粗蛋白", "粗蛋白质", "crude protein", "crudeprotein"),
    "粗脂肪": ("粗脂肪", "crude fat", "crudefat"),
    "水分": ("水分", "moisture"),
    "粗灰分": ("粗灰分", "crude ash", "crudeash"),
    "粗纤维": ("粗纤维", "粗纖維", "crude fibre", "crudefibre", "crude fiber", "crudefiber"),
    "牛磺酸": ("牛磺酸", "牛敬酸", "taurine"),
    "总磷": ("总磷", "磷", "total phosphorus", "totalphosphorus", "phosphorus"),
    "钙": ("钙", "calcium"),
    "水溶性氯化物(以Cl-计)": ("水溶性氯化物", "氯化物", "chloride", "cl-"),
}
SCHEMA_PLACEHOLDER_MARKERS = (
    "返回空字符串",
    "如果没有写",
    "如果图片里没有明确名称",
    "保证值口径，例如",
    "产品名称。如果图片里没有明确名称",
    "适用阶段，例如",
    "代谢能原文，例如",
    "保证值指标名，例如",
    "原始值字符串，保留符号和单位，例如",
)
SUPERSCRIPT_TRANS = str.maketrans("⁰¹²³⁴⁵⁶⁷⁸⁹⁺⁻", "0123456789+-")


@dataclass
class GuaranteeParseSummary:
    scanned: int
    succeeded: int
    empty_guarantees: int
    guarantee_rows: int
    failed: int
    source_table: str
    parsed_table: str
    info_table: str
    guarantee_table: str
    batch_id: str
    error_samples: List[str]


@dataclass
class GuaranteeRebuildSummary:
    scanned: int
    rebuilt: int
    guarantee_rows: int
    failed: int
    info_table: str
    guarantee_table: str
    batch_id: str
    error_samples: List[str]


def _safe_table(name: str) -> str:
    if not TABLE_RE.fullmatch(name):
        raise ValueError(f"invalid table name: {name}")
    return name


def _normalize_text(text: Optional[str]) -> str:
    if not text:
        return ""
    out = str(text).strip()
    out = out.replace("（", "(").replace("）", ")")
    out = out.replace("：", ":")
    out = out.replace("，", ",")
    out = out.replace("％", "%")
    out = out.replace("ＫJ", "KJ").replace("kJ", "KJ")
    out = out.replace("CL-", "Cl-").replace("CL⁻", "Cl-").replace("CI-", "Cl-").replace("CI⁻", "Cl-")
    out = out.replace("水溶性氯化物(以CL-计)", "水溶性氯化物(以Cl-计)")
    out = out.replace("水溶性氯化物(以CL⁻计)", "水溶性氯化物(以Cl-计)")
    out = out.replace("水溶性氯化物(以Cl⁻计)", "水溶性氯化物(以Cl-计)")
    return out


def _strip_code_fence(text_value: str) -> str:
    text_value = (text_value or "").strip()
    if text_value.startswith("```"):
        text_value = re.sub(r"^```[a-zA-Z0-9_-]*\n?", "", text_value)
        text_value = re.sub(r"\n?```$", "", text_value)
    return text_value.strip()


def _is_schema_placeholder(text_value: Optional[str]) -> bool:
    normalized = _normalize_text(text_value)
    if not normalized:
        return False
    return any(_normalize_text(marker) in normalized for marker in SCHEMA_PLACEHOLDER_MARKERS)


def _is_nullish_text(text_value: Optional[str]) -> bool:
    normalized = _normalize_text(text_value).lower()
    return normalized in NULLISH_MARKERS


def _clean_extracted_text(value: Optional[str], fallback: str = "") -> str:
    normalized = _normalize_text(value)
    if not normalized or _is_schema_placeholder(normalized) or _is_nullish_text(normalized):
        return _normalize_text(fallback)
    return normalized


def _normalize_search_text(text: Optional[str]) -> str:
    normalized = _normalize_numeric_text(text or "").lower()
    normalized = normalized.replace("\r", "\n")
    normalized = normalized.replace("0mega", "omega")
    normalized = normalized.replace("omeg9", "omega")
    normalized = re.sub(r"\s+", "", normalized)
    return normalized


def _ocr_raw_lines(ocr_text: Optional[str]) -> List[str]:
    lines: List[str] = []
    for raw_line in re.split(r"[\r\n]+", _normalize_text(ocr_text)):
        line = raw_line.strip()
        if line:
            lines.append(line)
    return lines


def _ocr_search_lines(ocr_text: Optional[str]) -> List[str]:
    lines: List[str] = []
    for raw_line in re.split(r"[\r\n]+", _normalize_text(ocr_text)):
        line = _normalize_search_text(raw_line)
        if line:
            lines.append(line)
    return lines


def _ocr_search_windows(ocr_text: Optional[str]) -> List[str]:
    lines = _ocr_search_lines(ocr_text)
    windows = list(lines)
    for span in (2, 3):
        for idx in range(len(lines) - span + 1):
            windows.append("".join(lines[idx : idx + span]))
    return windows


def _has_guarantee_section_hint(ocr_text: Optional[str]) -> bool:
    search_text = _normalize_search_text(ocr_text)
    if not search_text:
        return False
    if any(_normalize_search_text(keyword) in search_text for keyword in GUARANTEE_SECTION_KEYWORDS):
        return True
    hits = 0
    for aliases in CORE_GUARANTEE_ALIASES.values():
        if any(_normalize_search_text(alias) in search_text for alias in aliases):
            hits += 1
    return hits >= 2


def _trim_metric_name(metric_name: str) -> str:
    cleaned = _normalize_text(metric_name)
    for token in (
        "(至少)",
        "（至少）",
        "(至多)",
        "（至多）",
        "(min)",
        "（min）",
        "(max)",
        "（max）",
        ",至少",
        "，至少",
        ",至多",
        "，至多",
    ):
        cleaned = cleaned.replace(token, "")
    return cleaned.strip(" ,，;；")


NON_GUARANTEE_METRIC_MARKERS = (
    "保质期",
    "净含量",
    "市售规格",
    "适口性",
    "卡路里",
    "热值",
    "能量",
)
NON_GUARANTEE_METRIC_EXACT = {
    "干物质",
    "营养素",
}
METRIC_OPERATOR_HINTS = {
    ">=": ("(min)", "（min）", "(至少)", "（至少）", "(最少)", "（最少）", "min", "至少", "最少"),
    "<=": ("(max)", "（max）", "(至多)", "（至多）", "(最多)", "（最多）", "max", "至多", "最多"),
}
EXTRA_METRIC_ALIASES: Dict[str, Tuple[str, ...]] = {
    "粗蛋白": ("蛋白质", "粗蛋白(min)", "粗蛋白质", "crude protein(min)", "crudeprotein(min)"),
    "粗脂肪": ("原油和脂肪", "粗脂肪(min)", "crude fat(min)", "crudefat(min)"),
    "粗灰分": ("粗灰", "灰分", "粗灰分(max)", "灰分(参考)"),
    "Omega-3脂肪酸": ("奥米加3脂肪酸", "omega3", "omega 3", "omega-3", "omega-3脂肪酸(min)", "ω-3 脂肪酸", "omega-3脂肪酸(至少)"),
    "Omega-6脂肪酸": ("奥米加6脂肪酸", "omega6", "omega 6", "omega-6", "omega-6脂肪酸(min)", "ω-6 脂肪酸", "omega-6脂肪酸(至少)"),
    "水溶性氯化物(以Cl-计)": ("水溶性氯化物(min)", "水溶性氯化物(以 cl 计)", "水溶性氯化物(以cl计)", "水溶性氯化物(以cl-计)"),
    "镁": ("magnesium", "magnesium 镁"),
    "钠": ("sodium", "sodium(min) 钠", "sodium(min)", "sodium 钠"),
    "钾": ("potassium", "potassium(min) 钾", "potassium(min)", "potassium 钾"),
    "锌": ("zinc", "zinc(min) 锌", "zinc(min)", "zinc 锌"),
    "铜": ("copper", "copper 铜"),
    "铁": ("iron", "iron 铁"),
    "锰": ("manganese", "manganese 锰"),
    "硒": ("selenium", "selenium 硒"),
    "碘": ("iodine", "iodine 碘"),
}
UNIT_CANONICAL_MAP = {
    "%": "%",
    "mg/kg": "mg/kg",
    "毫克/千克": "mg/kg",
    "g/kg": "g/kg",
    "克/千克": "g/kg",
    "iu/kg": "IU/kg",
    "iu/千克": "IU/kg",
    "mg": "mg",
    "g": "g",
    "μg": "μg",
    "ug": "μg",
    "mcg": "μg",
    "iu": "IU",
    "cfu/g": "CFU/g",
    "cfu/kg": "CFU/kg",
    "cfu/lb": "CFU/lb",
    "kjme/100g": "KJME/100g",
    "kj/100g": "KJ/100g",
    "kj/kg": "KJ/kg",
    "kcal/kg": "kcal/kg",
    "ppm": "ppm",
    "百万分之一": "ppm",
}
BASIS_FALLBACK_MARKERS = (
    "空字符串",
    "null",
    "guaranteedanalysis",
    "保证值分析",
    "分析保证值",
    "营养保证值",
    "营养成分",
    "营养分析",
    "产品成分分析保证值",
    "产品配方表",
    "净含量",
    "营养素",
)
GUARANTEE_SECTION_END_KEYWORDS = (
    "原料组成",
    "添加剂组成",
    "产品使用说明",
    "喂食",
    "喂食方法",
    "喂食力法",
    "营养改变生命",
    "本产品符合",
    "AAFCO",
    "净含量",
    "贮存",
    "储存",
    "注意事项",
    "生产日期",
    "最佳赏味",
    "电话",
    "地址",
    "官网",
    "小红书",
)
OCR_VALUE_PATTERN = (
    r"(?:>=|<=|≥|≤|>|<)?"
    r"\d+(?:\.\d+)?"
    r"(?:(?:x10\^?[+-]?\d+)|(?:e[+-]?\d+))?"
    r"(?:%|mg/kg|g/kg|iu/kg|1u/kg|iu|mg|g|μg|ug|mcg|cfu/g|cfu/kg|cfu/lb|ppm|kjme/100g|kj/100g|kj/kg|kcal/kg)?"
)
METRIC_NOISE_HINTS = (
    "支持",
    "维持",
    "幫助",
    "帮助",
    "有助",
    "添加",
    "健康",
    "功能",
    "皮肤",
    "牙齿",
    "牙因",
    "免疫",
    "配方",
    "营养改变生命",
    "有加",
)
PERCENT_ONLY_FALLBACK_METRICS = {
    "粗蛋白",
    "粗脂肪",
    "水分",
    "粗灰分",
    "粗纤维",
    "牛磺酸",
    "总磷",
    "钙",
    "水溶性氯化物(以Cl-计)",
    "Omega-3脂肪酸",
    "Omega-6脂肪酸",
}


def _metric_aliases(metric_name: str) -> List[str]:
    seeds: List[str] = []
    for value in (metric_name, _trim_metric_name(metric_name)):
        if value and value not in seeds:
            seeds.append(value)
    trimmed = _trim_metric_name(metric_name)
    trimmed_search = _normalize_search_text(trimmed)
    for key, aliases in CORE_GUARANTEE_ALIASES.items():
        key_search = _normalize_search_text(key)
        if key in trimmed or (trimmed_search and key_search in trimmed_search):
            for alias in aliases:
                if alias not in seeds:
                    seeds.append(alias)
    normalized_aliases: List[str] = []
    for seed in seeds:
        alias = _normalize_search_text(seed)
        if alias and alias not in normalized_aliases:
            normalized_aliases.append(alias)
    return normalized_aliases


def _numeric_variants(value: Decimal) -> List[str]:
    text_value = format(value, "f")
    variants = {text_value}
    if "." in text_value:
        current = text_value
        while current.endswith("0"):
            current = current[:-1]
            variants.add(current)
        if current.endswith("."):
            variants.add(current[:-1])
    return [x for x in sorted(variants, key=len, reverse=True) if x]


def _value_variants(raw_value: str, metric_value: Decimal, metric_unit: str) -> List[str]:
    variants: List[str] = []
    raw_norm = _normalize_search_text(raw_value)
    raw_core = re.sub(r"^(>=|<=|>|<|≥|≤)+", "", raw_norm)
    if raw_core:
        variants.append(raw_core)
    unit_norm = _normalize_search_text(metric_unit)
    for number in _numeric_variants(metric_value):
        if number not in variants:
            variants.append(number)
        if unit_norm and f"{number}{unit_norm}" not in variants:
            variants.append(f"{number}{unit_norm}")
    return variants


def _normalize_unit_key(unit: Optional[str]) -> str:
    cleaned = _normalize_text(unit)
    cleaned = cleaned.replace(" ", "")
    cleaned = cleaned.replace("％", "%")
    cleaned = cleaned.replace("μ", "μ")
    return cleaned.lower()


def _normalize_metric_unit(metric_unit: Optional[str]) -> Optional[str]:
    cleaned = _normalize_text(metric_unit)
    cleaned = cleaned.replace(" ", "")
    cleaned = cleaned.replace("％", "%")
    if not cleaned:
        return None
    if cleaned.startswith("%"):
        return "%"

    normalized_key = _normalize_unit_key(cleaned)
    for raw_unit in sorted(UNIT_CANONICAL_MAP.keys(), key=len, reverse=True):
        if normalized_key.startswith(raw_unit):
            return UNIT_CANONICAL_MAP[raw_unit]

    if re.fullmatch(r"[A-Za-zμ]+", cleaned):
        return cleaned.upper() if cleaned.lower() == "iu" else cleaned
    return None


def _normalize_basis_value(raw_basis: Optional[str]) -> str:
    cleaned = _clean_extracted_text(raw_basis, fallback="")
    normalized = _normalize_search_text(cleaned)
    if not cleaned or not normalized:
        return DEFAULT_BASIS
    if "干物质" in normalized:
        return DEFAULT_BASIS
    if any(marker in normalized for marker in BASIS_FALLBACK_MARKERS):
        return DEFAULT_BASIS
    return cleaned


def _infer_operator_hint(*texts: Optional[str]) -> str:
    combined = " ".join(_normalize_text(text) for text in texts if _normalize_text(text))
    lowered = combined.lower()
    for operator_symbol, markers in METRIC_OPERATOR_HINTS.items():
        if any(marker in lowered for marker in markers):
            return operator_symbol
    return ""


def _strip_metric_hint_tokens(metric_name: str) -> str:
    cleaned = _normalize_text(metric_name)
    for markers in METRIC_OPERATOR_HINTS.values():
        for marker in markers:
            cleaned = cleaned.replace(marker, "")
            cleaned = cleaned.replace(marker.upper(), "")
    cleaned = cleaned.replace("*", "")
    return cleaned.strip(" ,，;；")


def _is_non_guarantee_metric(metric_name: Optional[str]) -> bool:
    search = _normalize_search_text(metric_name)
    if not search:
        return True
    if search in {_normalize_search_text(value) for value in NON_GUARANTEE_METRIC_EXACT}:
        return True
    return any(_normalize_search_text(marker) in search for marker in NON_GUARANTEE_METRIC_MARKERS)


def _normalize_metric_name(metric_name: Optional[str]) -> Optional[str]:
    cleaned = _strip_metric_hint_tokens(_trim_metric_name(_normalize_text(metric_name)))
    if not cleaned or _is_non_guarantee_metric(cleaned):
        return None
    search = _normalize_search_text(cleaned)
    if ("omega" in search and "6/3" in search) or ("dha" in search and "epa" in search and "/" in search):
        return cleaned
    if "水溶性" in cleaned and "cl" in search:
        return "水溶性氯化物(以Cl-计)"
    alias_groups = list(CORE_GUARANTEE_ALIASES.items()) + list(EXTRA_METRIC_ALIASES.items())
    for canonical, aliases in alias_groups:
        all_aliases = (canonical, *aliases)
        if any(_normalize_search_text(alias) in search for alias in all_aliases):
            return canonical
    cleaned = cleaned.replace("维生素 ", "维生素")
    return cleaned or None


def _split_guarantee_segments(raw_value: str) -> List[str]:
    if not raw_value:
        return []
    parts = re.split(r"(?:[，；;\n]+|,(?!\d))", _normalize_text(raw_value))
    return [part.strip() for part in parts if part and part.strip()]


def _split_segment_metric_and_value(segment: str, fallback_metric_name: str) -> Tuple[str, str]:
    segment = _normalize_text(segment)
    match = re.search(r"(>=|<=|≥|≤|>|<)?\s*\d", segment)
    if not match:
        return fallback_metric_name, segment
    prefix = segment[: match.start()].strip(" :：,，;；")
    raw_value = segment[match.start() :].strip()
    return (prefix or fallback_metric_name), raw_value


def _extract_composite_metric_values(metric_name: str, raw_value: str) -> List[Tuple[str, str, Decimal, str, str]]:
    search = _normalize_search_text(metric_name)
    metric_pair: Optional[Tuple[str, str]] = None
    if "dha" in search and "epa" in search and "/" in search:
        metric_pair = ("DHA", "EPA")
    elif "omega" in search and "6/3" in search:
        metric_pair = ("Omega-6脂肪酸", "Omega-3脂肪酸")
    if metric_pair is None:
        return []

    raw = _normalize_numeric_text(raw_value)
    match = re.match(
        r"^(>=|<=|≥|≤|>|<)?"
        r"(\d+(?:\.\d+)?)"
        r"([^\d/][^/]*)/"
        r"(\d+(?:\.\d+)?)"
        r"([^\d].*)?$",
        raw,
    )
    if not match:
        return []

    operator_symbol = _normalize_operator(match.group(1))
    first_unit = _normalize_metric_unit(match.group(3))
    second_unit = _normalize_metric_unit(match.group(5) or match.group(3))
    if not first_unit or not second_unit:
        return []

    return [
        (metric_pair[0], operator_symbol, _quantize_2(Decimal(match.group(2))), first_unit, raw_value),
        (metric_pair[1], operator_symbol, _quantize_2(Decimal(match.group(4))), second_unit, raw_value),
    ]


def _extract_range_metric_values(metric_name: str, raw_value: str) -> List[Tuple[str, str, Decimal, str, str]]:
    raw = _normalize_numeric_text(raw_value)
    match = re.match(
        r"^(\d+(?:\.\d+)?)"
        r"([^\d].*?)"
        r"(?:-|~|—|至)"
        r"(\d+(?:\.\d+)?)"
        r"([^\d].*)$",
        raw,
    )
    if not match:
        return []

    first_unit = _normalize_metric_unit(match.group(2))
    second_unit = _normalize_metric_unit(match.group(4))
    if not first_unit or not second_unit or first_unit != second_unit:
        return []

    first_value = _quantize_2(Decimal(match.group(1)))
    second_value = _quantize_2(Decimal(match.group(3)))
    lower_value, upper_value = sorted((first_value, second_value))
    return [
        (metric_name, ">=", lower_value, first_unit, raw_value),
        (metric_name, "<=", upper_value, first_unit, raw_value),
    ]


def _parse_guarantee_measurements(
    metric_name: str,
    raw_value: str,
) -> List[Tuple[str, str, Decimal, str, str]]:
    normalized_metric_name = _normalize_metric_name(metric_name)
    if not normalized_metric_name:
        return []

    segments = _split_guarantee_segments(raw_value) or [_normalize_text(raw_value)]
    current_metric_name = normalized_metric_name
    measurements: List[Tuple[str, str, Decimal, str, str]] = []

    for segment in segments:
        segment_metric_name, segment_raw_value = _split_segment_metric_and_value(segment, current_metric_name)
        normalized_segment_metric = _normalize_metric_name(segment_metric_name) or current_metric_name
        current_metric_name = normalized_segment_metric

        composite_rows = _extract_composite_metric_values(normalized_segment_metric, segment_raw_value)
        if composite_rows:
            measurements.extend(composite_rows)
            continue

        range_rows = _extract_range_metric_values(normalized_segment_metric, segment_raw_value)
        if range_rows:
            measurements.extend(range_rows)
            continue

        operator_symbol, metric_value, metric_unit = _parse_operator_and_value(segment_raw_value)
        if metric_value is None:
            continue
        normalized_unit = _normalize_metric_unit(metric_unit)
        if not normalized_unit:
            continue
        normalized_operator = _normalize_operator(operator_symbol) or _infer_operator_hint(segment_metric_name, segment_raw_value)
        measurements.append(
            (
                normalized_segment_metric,
                normalized_operator,
                _quantize_2(metric_value),
                normalized_unit,
                segment_raw_value,
            )
        )
    return measurements


def _window_contains_alias(window: str, alias: str) -> bool:
    if not alias:
        return False
    if alias in {"钙", "磷"}:
        return bool(re.search(rf"(?<![A-Za-z0-9\u4e00-\u9fff]){re.escape(alias)}(?![A-Za-z0-9\u4e00-\u9fff])", window))
    return alias in window


def _item_has_ocr_evidence(
    metric_name: str,
    raw_value: str,
    metric_value: Decimal,
    metric_unit: str,
    ocr_text: Optional[str],
) -> bool:
    lines = _ocr_search_lines(ocr_text)
    if not lines:
        return False
    windows = list(lines)
    for span in (2, 3):
        for idx in range(len(lines) - span + 1):
            windows.append("".join(lines[idx : idx + span]))
    aliases = _metric_aliases(metric_name)
    if not aliases:
        return False
    value_variants = _value_variants(raw_value, metric_value, metric_unit)
    if not value_variants:
        return False
    for window in windows:
        if not any(_window_contains_alias(window, alias) for alias in aliases):
            continue
        if any(value_variant and value_variant in window for value_variant in value_variants):
            return True
    alias_lines = [
        idx
        for idx, line in enumerate(lines)
        if any(_window_contains_alias(line, alias) for alias in aliases)
    ]
    if not alias_lines:
        return False
    value_lines = [
        idx
        for idx, line in enumerate(lines)
        if any(value_variant and value_variant in line for value_variant in value_variants)
    ]
    if not value_lines:
        return False
    return any(0 <= value_idx - alias_idx <= 5 for alias_idx in alias_lines for value_idx in value_lines)


def _is_guarantee_section_end(line: str) -> bool:
    search = _normalize_search_text(line)
    if not search:
        return False
    return any(_normalize_search_text(keyword) in search for keyword in GUARANTEE_SECTION_END_KEYWORDS)


def _extract_guarantee_section_lines(ocr_text: Optional[str]) -> List[str]:
    lines = _ocr_raw_lines(ocr_text)
    if not lines:
        return []

    start_idx: Optional[int] = None
    for idx, line in enumerate(lines):
        search = _normalize_search_text(line)
        if any(_normalize_search_text(keyword) in search for keyword in GUARANTEE_SECTION_KEYWORDS):
            start_idx = idx
            break
    if start_idx is None:
        return []

    section_lines: List[str] = []
    max_end = min(len(lines), start_idx + 40)
    for idx in range(start_idx, max_end):
        line = lines[idx].strip()
        if idx > start_idx and _is_guarantee_section_end(line):
            break
        section_lines.append(line)
    return section_lines


def _extract_metab_energy_from_section(section_lines: Sequence[str]) -> str:
    search_text = "".join(_normalize_search_text(line) for line in section_lines)
    if not search_text:
        return ""
    match = re.search(
        rf"(?:代谢能|卡路里含量)[^0-9]{{0,12}}(?P<value>{OCR_VALUE_PATTERN})",
        search_text,
    )
    if not match:
        return ""
    value = str(match.group("value") or "").strip()
    _, parsed_value, parsed_unit = _parse_operator_and_value(value)
    if parsed_value is None or not _normalize_metric_unit(parsed_unit):
        return ""
    return value


def _extract_value_tokens_from_line(line: str) -> List[str]:
    tokens: List[str] = []
    search = _normalize_search_text(line)
    for match in re.finditer(OCR_VALUE_PATTERN, search):
        value = str(match.group(0) or "").strip()
        if value and re.search(r"(?:%|/kg|/g|/lb|ppm|kj)", value):
            tokens.append(value)
    return tokens


def _is_value_only_line(line: str) -> bool:
    search = _normalize_search_text(line)
    if not search:
        return False
    stripped = re.sub(OCR_VALUE_PATTERN, "", search)
    stripped = stripped.replace("(", "").replace(")", "")
    stripped = stripped.replace(":", "").replace(",", "").replace(";", "")
    stripped = stripped.replace("min", "").replace("max", "")
    stripped = stripped.replace("至少", "").replace("至多", "")
    stripped = stripped.replace(">= ", "").replace("<= ", "")
    stripped = stripped.replace(">=", "").replace("<=", "")
    stripped = stripped.replace(">", "").replace("<", "")
    stripped = stripped.replace("≥", "").replace("≤", "")
    return not stripped


def _alias_metric_name_from_line(line: str) -> Optional[str]:
    search = _normalize_search_text(line)
    if not search:
        return None
    if any(_normalize_search_text(hint) in search for hint in METRIC_NOISE_HINTS):
        return None
    alias_groups = list(CORE_GUARANTEE_ALIASES.items()) + list(EXTRA_METRIC_ALIASES.items())
    best_match: Optional[Tuple[int, str]] = None
    for canonical_name, aliases in alias_groups:
        for alias in (canonical_name, *aliases):
            alias_search = _normalize_search_text(alias)
            if not alias_search or alias_search not in search:
                continue
            candidate = (len(alias_search), canonical_name)
            if best_match is None or candidate[0] > best_match[0]:
                best_match = candidate
    return best_match[1] if best_match else None


def _normalize_fallback_raw_value(raw_value: str) -> str:
    operator_symbol, metric_value, metric_unit = _parse_operator_and_value(raw_value)
    normalized_unit = _normalize_metric_unit(metric_unit)
    if metric_value is None or normalized_unit != "%":
        return raw_value
    if "." in raw_value:
        return raw_value
    adjusted_value = metric_value
    while adjusted_value > Decimal("100") and adjusted_value == adjusted_value.to_integral_value():
        adjusted_value = adjusted_value / Decimal("10")
    if adjusted_value == metric_value:
        return raw_value
    operator_prefix = _normalize_operator(operator_symbol) or ""
    normalized_number = format(adjusted_value.normalize(), "f")
    return f"{operator_prefix}{normalized_number}%"


def _fallback_value_unit_compatible(metric_name: str, raw_value: str) -> bool:
    if metric_name not in PERCENT_ONLY_FALLBACK_METRICS:
        return True
    _, _, metric_unit = _parse_operator_and_value(raw_value)
    return _normalize_metric_unit(metric_unit) == "%"


def _extract_inline_guarantees_from_line(line: str) -> List[Tuple[str, str]]:
    search = _normalize_search_text(line)
    if not search:
        return []

    alias_groups = list(CORE_GUARANTEE_ALIASES.items()) + list(EXTRA_METRIC_ALIASES.items())
    pairs: List[Tuple[str, str]] = []
    seen_pairs: set[Tuple[str, str]] = set()

    for canonical_name, aliases in alias_groups:
        alias_searches: List[str] = []
        for alias in (canonical_name, *aliases):
            alias_search = _normalize_search_text(alias)
            if alias_search and alias_search not in alias_searches:
                alias_searches.append(alias_search)
        alias_searches.sort(key=len, reverse=True)
        for alias_search in alias_searches:
            pattern = rf"{re.escape(alias_search)}(?P<context>.{{0,12}}?)(?P<value>{OCR_VALUE_PATTERN})"
            for match in re.finditer(pattern, search):
                raw_value = str(match.group("value") or "").strip()
                if not raw_value:
                    continue
                if not re.search(r"(?:%|/kg|/g|/lb|ppm|kj)", raw_value):
                    continue
                operator_hint = _infer_operator_hint(match.group("context"), match.group(0))
                if operator_hint and not re.match(r"^(>=|<=|≥|≤|>|<)", raw_value):
                    raw_value = f"{operator_hint}{raw_value}"
                raw_value = _normalize_fallback_raw_value(raw_value)
                pair = (canonical_name, raw_value)
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
                pairs.append(pair)
    return pairs


def _extract_guarantee_from_ocr_text(ocr_text: str) -> Dict[str, Any]:
    section_lines = _extract_guarantee_section_lines(ocr_text)
    if not section_lines:
        return {
            "product_name": "",
            "basis": "",
            "life_stage": "",
            "metab_energy": "",
            "guarantees": [],
        }

    guarantees: List[Dict[str, str]] = []
    seen_guarantees: set[Tuple[str, str]] = set()
    metric_queue: List[Dict[str, str]] = []
    pending_values: List[str] = []

    for line in section_lines:
        inline_pairs = _extract_inline_guarantees_from_line(line)
        if inline_pairs:
            metric_queue.clear()
            pending_values.clear()
            for metric_name, raw_value in inline_pairs:
                dedupe_key = (metric_name, raw_value)
                if dedupe_key in seen_guarantees:
                    continue
                seen_guarantees.add(dedupe_key)
                guarantees.append(
                    {
                        "metric_name": metric_name,
                        "metric_raw_value": raw_value,
                    }
                )
            continue

        search = _normalize_search_text(line)
        if not search:
            continue

        if metric_queue and (search.startswith("(以") or search.startswith("以")):
            metric_queue[-1]["hint_text"] = metric_queue[-1]["hint_text"] + " " + line
            continue

        value_tokens = _extract_value_tokens_from_line(line)
        metric_name = _alias_metric_name_from_line(line)
        unknown_metric_name = _normalize_metric_name(line)
        has_header_keyword = any(_normalize_search_text(keyword) in search for keyword in GUARANTEE_SECTION_KEYWORDS)
        is_english_only_metric = bool(re.search(r"[A-Za-z]", line) and not re.search(r"[\u4e00-\u9fff]", line))

        if pending_values and unknown_metric_name and not metric_name and not value_tokens and len(search) <= 32:
            pending_values.clear()

        if (
            metric_name
            and not value_tokens
            and not has_header_keyword
            and len(search) <= 32
        ):
            if is_english_only_metric:
                continue
            if pending_values:
                raw_value = _normalize_fallback_raw_value(pending_values.pop(0))
                if not _fallback_value_unit_compatible(metric_name, raw_value):
                    metric_queue.append({"metric_name": metric_name, "hint_text": line})
                    continue
                operator_hint = _infer_operator_hint(line)
                if operator_hint and not re.match(r"^(>=|<=|≥|≤|>|<)", raw_value):
                    raw_value = f"{operator_hint}{raw_value}"
                dedupe_key = (metric_name, raw_value)
                if dedupe_key not in seen_guarantees:
                    seen_guarantees.add(dedupe_key)
                    guarantees.append(
                        {
                            "metric_name": metric_name,
                            "metric_raw_value": raw_value,
                        }
                    )
                continue
            metric_queue.append({"metric_name": metric_name, "hint_text": line})
            continue

        if value_tokens and metric_queue and _is_value_only_line(line):
            for raw_value in value_tokens:
                if not metric_queue:
                    break
                raw_value = _normalize_fallback_raw_value(raw_value)
                metric_ref = metric_queue.pop(0)
                if not _fallback_value_unit_compatible(metric_ref["metric_name"], raw_value):
                    continue
                operator_hint = _infer_operator_hint(metric_ref.get("hint_text"), line)
                if operator_hint and not re.match(r"^(>=|<=|≥|≤|>|<)", raw_value):
                    raw_value = f"{operator_hint}{raw_value}"
                dedupe_key = (metric_ref["metric_name"], raw_value)
                if dedupe_key in seen_guarantees:
                    continue
                seen_guarantees.add(dedupe_key)
                guarantees.append(
                    {
                        "metric_name": metric_ref["metric_name"],
                        "metric_raw_value": raw_value,
                    }
                )
            continue

        if value_tokens and _is_value_only_line(line):
            pending_values.extend(_normalize_fallback_raw_value(value) for value in value_tokens)

    return {
        "product_name": "",
        "basis": "",
        "life_stage": "",
        "metab_energy": _extract_metab_energy_from_section(section_lines),
        "guarantees": guarantees,
    }


def _normalize_numeric_text(raw_value: str) -> str:
    raw = _normalize_text(raw_value)
    raw = raw.replace(" ", "")
    raw = raw.replace("×", "x").replace("X", "x").replace("*", "x")
    raw = re.sub(
        r"x10([⁰¹²³⁴⁵⁶⁷⁸⁹⁺⁻]+)",
        lambda m: f"x10^{m.group(1).translate(SUPERSCRIPT_TRANS)}",
        raw,
    )
    raw = raw.replace(",", "")
    return raw


def _quantize_2(value: Decimal) -> Decimal:
    return value.quantize(DECIMAL_2, rounding=ROUND_HALF_UP)


def _parse_operator_and_value(raw_value: str) -> Tuple[Optional[str], Optional[Decimal], Optional[str]]:
    if raw_value is None:
        return None, None, None

    raw = _normalize_numeric_text(str(raw_value))
    m = re.match(
        r"^(>=|<=|≥|≤|>|<)?"
        r"(\d+(?:\.\d+)?)"
        r"(?:(?:x10\^?([+-]?\d+))|(?:[eE]([+-]?\d+)))?"
        r"([^\d].*)?$",
        raw,
    )
    if not m:
        return None, None, None

    operator_symbol = m.group(1)
    numeric_value = Decimal(m.group(2))
    exponent_str = m.group(3) or m.group(4)
    unit = (m.group(5) or "").strip()
    if exponent_str:
        numeric_value = numeric_value * (Decimal(10) ** int(exponent_str))
    return operator_symbol, _quantize_2(numeric_value), unit


def _normalize_operator(op: Optional[str]) -> str:
    if op in ("≥", ">="):
        return ">="
    if op in ("≤", "<="):
        return "<="
    if op == ">":
        return ">"
    if op == "<":
        return "<"
    return ""


def _normalize_endpoint(raw: str) -> str:
    endpoint = str(raw or "").strip().rstrip("/")
    if not endpoint:
        return DEFAULT_ENDPOINT
    if endpoint.endswith("/chat/completions"):
        return endpoint
    return f"{endpoint}/chat/completions"


def _resolve_qwen_cfg(
    openai_cfg: Optional[Dict[str, Any]] = None,
    ocr_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    openai_cfg = dict(openai_cfg or {})
    ocr_cfg = dict(ocr_cfg or {})

    api_key = (
        str(ocr_cfg.get("qwen_api_key") or "").strip()
        or str(os.getenv("QWEN_API_KEY") or "").strip()
        or str(os.getenv("DASHSCOPE_API_KEY") or "").strip()
    )
    endpoint = _normalize_endpoint(
        str(ocr_cfg.get("qwen_endpoint") or "").strip()
        or str(os.getenv("QWEN_ENDPOINT") or "").strip()
        or str(os.getenv("QWEN_BASE_URL") or "").strip()
    )
    model = (
        str(ocr_cfg.get("qwen_guarantee_model") or "").strip()
        or str(os.getenv("QWEN_MODEL") or "").strip()
        or DEFAULT_MODEL
    )
    timeout_seconds = float(
        ocr_cfg.get("qwen_timeout_seconds")
        or os.getenv("QWEN_TIMEOUT_SECONDS")
        or 90
    )

    if not api_key:
        cfg_api_key = str(openai_cfg.get("api_key") or "").strip()
        cfg_base_url = str(openai_cfg.get("base_url") or "").strip()
        cfg_model = str(openai_cfg.get("model") or "").strip()
        if cfg_api_key and (("dashscope" in cfg_base_url.lower()) or cfg_model.lower().startswith("qwen")):
            api_key = cfg_api_key
            endpoint = _normalize_endpoint(cfg_base_url or endpoint)
            model = cfg_model or model

    if not api_key:
        raise ValueError(
            "缺少千问配置。请设置 QWEN_API_KEY / DASHSCOPE_API_KEY，"
            "或在 config/config.yaml 中把 openai.base_url/model 指向 DashScope/Qwen 兼容接口。"
        )
    return {
        "api_key": api_key,
        "endpoint": endpoint,
        "model": model or DEFAULT_MODEL,
        "timeout_seconds": max(10.0, timeout_seconds),
    }


def _to_data_url(image_path: Path) -> str:
    raw = image_path.read_bytes()
    mime, _ = mimetypes.guess_type(str(image_path))
    mime = mime or "image/jpeg"
    b64 = base64.b64encode(raw).decode("ascii")
    return f"data:{mime};base64,{b64}"


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


def _guarantee_result_schema() -> Dict[str, Any]:
    return {
        "product_name": "产品名称。如果图片里没有明确名称，可返回空字符串。",
        "basis": "保证值口径，例如 干物质、原物质。如果没有写，返回空字符串。",
        "life_stage": "适用阶段，例如 全年龄段猫、离乳后幼猫及妊娠期、哺乳期猫咪。如无则返回空字符串。",
        "metab_energy": "代谢能原文，例如 1570KJ/100g。如无则返回空字符串。",
        "guarantees": [
            {
                "metric_name": "保证值指标名，例如 粗蛋白、粗脂肪、粗纤维、粗灰分、牛磺酸、水分、总磷、钙、水溶性氯化物(以Cl-计)",
                "metric_raw_value": "原始值字符串，保留符号和单位，例如 ≥41%、≤8%",
            }
        ],
    }


def _parse_qwen_guarantee_response(raw_text: str, qwen_cfg: Dict[str, Any]) -> Dict[str, Any]:
    obj = safe_json_loads(raw_text)
    if not isinstance(obj, dict):
        raise ValueError("Qwen response is not a JSON object")
    if obj.get("error"):
        err = obj.get("error")
        if isinstance(err, dict):
            raise RuntimeError(str(err.get("message") or err.get("code") or err))
        raise RuntimeError(str(err))

    choices = obj.get("choices") or []
    if not choices:
        raise RuntimeError("Qwen response missing choices")
    msg = (choices[0] or {}).get("message") or {}
    content_text = _strip_code_fence(_extract_content_text(msg.get("content")))
    extracted = safe_json_loads(content_text)
    if not isinstance(extracted, dict):
        raise ValueError("Qwen structured OCR output is not a JSON object")
    return {
        "model_name": str(obj.get("model") or qwen_cfg["model"]),
        "extracted": extracted,
    }


def _call_qwen_guarantee_ocr(image_path: Path, qwen_cfg: Dict[str, Any]) -> Dict[str, Any]:
    payload = {
        "model": qwen_cfg["model"],
        "temperature": 0,
        "messages": [
            {
                "role": "system",
                "content": "你是猫粮营养保证值OCR结构化抽取助手。你必须只返回合法JSON，不要输出解释。",
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "请识别图片中的猫粮营养保证值/营养成分分析表，并只返回一个 JSON 对象。"
                            "如果图片中没有某字段，返回空字符串。"
                        ),
                    },
                    {
                        "type": "image_url",
                        "image_url": {"url": _to_data_url(image_path)},
                    },
                ],
            },
        ],
        "ocr_options": {
            "task": "key_information_extraction",
            "task_config": {
                "result_schema": _guarantee_result_schema()
            },
        },
    }
    req = Request(
        qwen_cfg["endpoint"],
        data=safe_json_dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {qwen_cfg['api_key']}",
            "User-Agent": "csv-mysql-labeling/qwen-guarantee",
        },
        method="POST",
    )
    start = now_ms()
    try:
        with urlopen(req, timeout=float(qwen_cfg["timeout_seconds"])) as resp:
            raw_text = resp.read().decode("utf-8", errors="ignore")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Qwen HTTP {exc.code}: {detail or exc.reason}") from exc
    except URLError as exc:
        raise RuntimeError(f"Qwen network error: {exc}") from exc

    parsed = _parse_qwen_guarantee_response(raw_text, qwen_cfg)
    return {
        "latency_ms": now_ms() - start,
        "model_name": str(parsed["model_name"]),
        "extracted": parsed["extracted"],
    }


def _call_qwen_guarantee_from_ocr_text(ocr_text: str, qwen_cfg: Dict[str, Any]) -> Dict[str, Any]:
    user_prompt = (
        "下面是从猫粮包装图片OCR得到的文本，可能有错别字和排版噪声。"
        "请从中提取营养保证值/营养成分分析，并只返回一个 JSON 对象。"
        f"\nOCR文本如下：\n{ocr_text}"
        f"\n返回结构：{safe_json_dumps(_guarantee_result_schema())}"
    )
    payload = {
        "model": qwen_cfg["model"],
        "temperature": 0,
        "messages": [
            {
                "role": "system",
                "content": (
                    "你是猫粮营养保证值结构化抽取助手。"
                    "你必须只依据提供的OCR文本抽取，不要臆测，不要补全未出现的指标。"
                    "如果文本里没有明确保证值/营养成分分析，则 guarantees 返回空数组。"
                    "你必须只返回合法JSON，不要输出解释。"
                ),
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": user_prompt,
                    }
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
            "User-Agent": "csv-mysql-labeling/qwen-guarantee-text",
        },
        method="POST",
    )
    start = now_ms()
    try:
        with urlopen(req, timeout=float(qwen_cfg["timeout_seconds"])) as resp:
            raw_text = resp.read().decode("utf-8", errors="ignore")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Qwen text HTTP {exc.code}: {detail or exc.reason}") from exc
    except URLError as exc:
        raise RuntimeError(f"Qwen text network error: {exc}") from exc

    parsed = _parse_qwen_guarantee_response(raw_text, qwen_cfg)
    return {
        "latency_ms": now_ms() - start,
        "model_name": str(parsed["model_name"]),
        "extracted": parsed["extracted"],
    }


def _is_under_root(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except Exception:
        return False


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


def _move_processed_image(image_path: Path, source_root: Optional[Path], archive_root: Path) -> Path:
    archive_root = archive_root.resolve()
    current_path = image_path.resolve()
    if _is_under_root(current_path, archive_root):
        return current_path

    archive_root.mkdir(parents=True, exist_ok=True)
    try:
        if source_root:
            rel = current_path.relative_to(source_root.resolve())
            target = archive_root / rel
        else:
            target = archive_root / current_path.name
    except Exception:
        target = archive_root / current_path.name

    target.parent.mkdir(parents=True, exist_ok=True)
    target = _unique_target_path(target)
    shutil.move(str(current_path), str(target))
    return target.resolve()


def _candidate_history_paths(history_dir: Path, image_name: str) -> List[Path]:
    if not image_name or not history_dir.exists():
        return []
    direct = history_dir / image_name
    candidates: List[Path] = []
    if direct.exists() and direct.is_file():
        candidates.append(direct.resolve())
    for cand in history_dir.rglob(image_name):
        if cand.is_file():
            cand = cand.resolve()
            if cand not in candidates:
                candidates.append(cand)
    return candidates


def _first_non_empty(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _row_ocr_text(row: Dict[str, Any]) -> str:
    return _first_non_empty(
        row.get("ocr_text"),
        row.get("parsed_ocr_text"),
        row.get("source_ocr_text"),
    )


def _resolve_image_path(row: Dict[str, Any]) -> Path:
    image_path_candidates = [
        _first_non_empty(row.get("parsed_image_path")),
        _first_non_empty(row.get("source_image_path")),
        _first_non_empty(row.get("image_path")),
    ]
    seen_paths: set[str] = set()
    for image_path_raw in image_path_candidates:
        if not image_path_raw or image_path_raw in seen_paths:
            continue
        seen_paths.add(image_path_raw)
        image_path = Path(image_path_raw)
        if image_path.exists() and image_path.is_file():
            row["image_name"] = _first_non_empty(
                row.get("parsed_image_name"),
                row.get("source_image_name"),
                row.get("image_name"),
                image_path.name,
            ) or None
            row["file_sha256"] = _first_non_empty(
                row.get("parsed_file_sha256"),
                row.get("source_file_sha256"),
                row.get("file_sha256"),
            ) or None
            return image_path

    image_name = _first_non_empty(
        row.get("parsed_image_name"),
        row.get("source_image_name"),
        row.get("image_name"),
    )
    file_sha256 = _first_non_empty(
        row.get("parsed_file_sha256"),
        row.get("source_file_sha256"),
        row.get("file_sha256"),
    ).lower()
    row["image_name"] = image_name or None
    row["file_sha256"] = file_sha256 or None

    candidates: List[Path] = []
    for history_dir in (DEFAULT_INGREDIENT_HISTORY_DIR, DEFAULT_PROCESSED_HISTORY_DIR):
        for cand in _candidate_history_paths(history_dir, image_name):
            if cand not in candidates:
                candidates.append(cand)
    if len(candidates) == 1:
        return candidates[0]
    if file_sha256:
        for cand in candidates:
            try:
                import hashlib

                cand_sha = hashlib.sha256(cand.read_bytes()).hexdigest().lower()
                if cand_sha == file_sha256:
                    return cand
            except Exception:
                continue
    if candidates:
        return candidates[0]

    raise FileNotFoundError(
        f"image file not found for source_id={row.get('source_id')}, "
        f"parsed_image_path={_first_non_empty(row.get('parsed_image_path')) or '<empty>'}, "
        f"source_image_path={_first_non_empty(row.get('source_image_path')) or '<empty>'}, "
        f"image_name={row.get('image_name') or '<empty>'}"
    )


def _convert_extracted_data(
    extracted: Dict[str, Any],
    row: Dict[str, Any],
    image_path: Path,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    parsed_product_name = _normalize_text(row.get("parsed_product_name"))
    fallback_product_name = parsed_product_name or _normalize_text(image_path.stem)
    product_name = _clean_extracted_text(extracted.get("product_name"), fallback=fallback_product_name)
    basis = _normalize_basis_value(extracted.get("basis"))
    life_stage = _clean_extracted_text(extracted.get("life_stage"), fallback="")
    metab_energy_raw = _clean_extracted_text(extracted.get("metab_energy"), fallback="")

    metab_energy_value = None
    metab_energy_unit = None
    if metab_energy_raw:
        _, val, unit = _parse_operator_and_value(metab_energy_raw)
        if val is not None:
            metab_energy_value = _quantize_2(val)
            metab_energy_unit = unit or "KJ/100g"

    product_info = {
        "source_id": int(row["source_id"]),
        "parsed_row_id": int(row["parsed_row_id"]) if row.get("parsed_row_id") is not None else None,
        "image_name": str(row.get("image_name") or "") or None,
        "file_sha256": str(row.get("file_sha256") or "") or None,
        "product_name": product_name,
        "life_stage": life_stage or None,
        "guarantee_basis": basis or None,
        "metab_energy_value": metab_energy_value,
        "metab_energy_unit": metab_energy_unit,
        "image_url": str(image_path.resolve()),
        "raw_extracted_json": safe_json_dumps(extracted),
    }

    guarantee_items: List[Dict[str, Any]] = []
    seen_guarantees: set[Tuple[str, str, Decimal, str]] = set()
    ocr_text = _row_ocr_text(row)
    for item in extracted.get("guarantees") or []:
        metric_name = _clean_extracted_text((item or {}).get("metric_name"), fallback="")
        raw_value = _clean_extracted_text((item or {}).get("metric_raw_value"), fallback="")
        if not metric_name or not raw_value:
            continue
        for normalized_name, operator_symbol, metric_value, metric_unit, normalized_raw_value in _parse_guarantee_measurements(
            metric_name=metric_name,
            raw_value=raw_value,
        ):
            normalized_metric_value = _quantize_2(metric_value)
            if ocr_text and not _item_has_ocr_evidence(
                metric_name=normalized_name,
                raw_value=normalized_raw_value,
                metric_value=normalized_metric_value,
                metric_unit=metric_unit,
                ocr_text=ocr_text,
            ):
                continue
            dedupe_key = (normalized_name, operator_symbol, normalized_metric_value, metric_unit)
            if dedupe_key in seen_guarantees:
                continue
            seen_guarantees.add(dedupe_key)
            guarantee_items.append(
                {
                    "source_id": int(row["source_id"]),
                    "parsed_row_id": int(row["parsed_row_id"]) if row.get("parsed_row_id") is not None else None,
                    "image_name": str(row.get("image_name") or "") or None,
                    "file_sha256": str(row.get("file_sha256") or "") or None,
                    "metric_name": normalized_name,
                    "operator_symbol": operator_symbol,
                    "metric_value": normalized_metric_value,
                    "metric_unit": metric_unit,
                    "basis": basis or None,
                    "raw_text": f"{normalized_name}{normalized_raw_value}",
                }
            )
    return product_info, guarantee_items


def _table_columns(conn: Any, table_name: str) -> set[str]:
    rows = conn.execute(
        text(
            """
            SELECT COLUMN_NAME
            FROM information_schema.columns
            WHERE table_schema = DATABASE() AND table_name = :table_name
            """
        ),
        {"table_name": table_name},
    ).fetchall()
    return {str(r[0]) for r in rows}


def _table_indexes(conn: Any, table_name: str) -> set[str]:
    rows = conn.execute(text(f"SHOW INDEX FROM `{table_name}`")).fetchall()
    return {str(r[2]) for r in rows}


def _emit_progress(
    progress_callback: Optional[Callable[[Dict[str, Any]], None]],
    **payload: Any,
) -> None:
    if not progress_callback:
        return
    try:
        progress_callback(payload)
    except Exception:
        return


def ensure_product_guarantee_tables(
    engine: Engine,
    info_table: str = "product_info",
    guarantee_table: str = "product_guarantee",
) -> None:
    info_table = _safe_table(info_table)
    guarantee_table = _safe_table(guarantee_table)

    create_info_sql = f"""
    CREATE TABLE IF NOT EXISTS `{info_table}` (
      id BIGINT PRIMARY KEY AUTO_INCREMENT,
      source_id BIGINT NULL,
      parsed_row_id BIGINT NULL,
      image_name VARCHAR(255) NULL,
      file_sha256 CHAR(64) NULL,
      product_name VARCHAR(255) NOT NULL,
      life_stage VARCHAR(255) NULL,
      guarantee_basis VARCHAR(50) NULL,
      metab_energy_value DECIMAL(18,2) NULL,
      metab_energy_unit VARCHAR(50) NULL,
      image_url VARCHAR(1000) NULL,
      raw_extracted_json LONGTEXT NULL,
      extract_batch_id VARCHAR(32) NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uq_source_id (source_id),
      KEY idx_parsed_row_id (parsed_row_id),
      KEY idx_image_name (image_name),
      KEY idx_file_sha256 (file_sha256)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    create_guarantee_sql = f"""
    CREATE TABLE IF NOT EXISTS `{guarantee_table}` (
      id BIGINT PRIMARY KEY AUTO_INCREMENT,
      product_id BIGINT NOT NULL,
      source_id BIGINT NULL,
      parsed_row_id BIGINT NULL,
      image_name VARCHAR(255) NULL,
      file_sha256 CHAR(64) NULL,
      metric_name VARCHAR(100) NOT NULL,
      operator_symbol VARCHAR(10) NOT NULL,
      metric_value DECIMAL(18,2) NOT NULL,
      metric_unit VARCHAR(50) NOT NULL,
      basis VARCHAR(50) NULL,
      raw_text VARCHAR(255) NULL,
      extract_batch_id VARCHAR(32) NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      KEY idx_product_id (product_id),
      KEY idx_source_id (source_id),
      KEY idx_parsed_row_id (parsed_row_id),
      KEY idx_image_name (image_name),
      KEY idx_file_sha256 (file_sha256)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """

    info_columns = {
        "source_id": "ADD COLUMN source_id BIGINT NULL AFTER id",
        "parsed_row_id": "ADD COLUMN parsed_row_id BIGINT NULL AFTER source_id",
        "image_name": "ADD COLUMN image_name VARCHAR(255) NULL AFTER parsed_row_id",
        "file_sha256": "ADD COLUMN file_sha256 CHAR(64) NULL AFTER image_name",
        "raw_extracted_json": "ADD COLUMN raw_extracted_json LONGTEXT NULL AFTER image_url",
        "extract_batch_id": "ADD COLUMN extract_batch_id VARCHAR(32) NOT NULL DEFAULT '' AFTER raw_extracted_json",
        "updated_at": "ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER created_at",
    }
    guarantee_columns = {
        "source_id": "ADD COLUMN source_id BIGINT NULL AFTER product_id",
        "parsed_row_id": "ADD COLUMN parsed_row_id BIGINT NULL AFTER source_id",
        "image_name": "ADD COLUMN image_name VARCHAR(255) NULL AFTER parsed_row_id",
        "file_sha256": "ADD COLUMN file_sha256 CHAR(64) NULL AFTER image_name",
        "extract_batch_id": "ADD COLUMN extract_batch_id VARCHAR(32) NOT NULL DEFAULT '' AFTER raw_text",
        "updated_at": "ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER created_at",
    }

    with engine.begin() as conn:
        conn.execute(text(create_info_sql))
        conn.execute(text(create_guarantee_sql))

        existing_info_columns = _table_columns(conn, info_table)
        for column_name, ddl in info_columns.items():
            if column_name not in existing_info_columns:
                conn.execute(text(f"ALTER TABLE `{info_table}` {ddl}"))
        conn.execute(text(f"ALTER TABLE `{info_table}` MODIFY metab_energy_value DECIMAL(18,2) NULL"))
        info_indexes = _table_indexes(conn, info_table)
        if "uq_source_id" not in info_indexes:
            conn.execute(text(f"ALTER TABLE `{info_table}` ADD UNIQUE KEY uq_source_id (source_id)"))
        if "idx_parsed_row_id" not in info_indexes:
            conn.execute(text(f"ALTER TABLE `{info_table}` ADD KEY idx_parsed_row_id (parsed_row_id)"))
        if "idx_image_name" not in info_indexes:
            conn.execute(text(f"ALTER TABLE `{info_table}` ADD KEY idx_image_name (image_name)"))
        if "idx_file_sha256" not in info_indexes:
            conn.execute(text(f"ALTER TABLE `{info_table}` ADD KEY idx_file_sha256 (file_sha256)"))

        existing_guarantee_columns = _table_columns(conn, guarantee_table)
        for column_name, ddl in guarantee_columns.items():
            if column_name not in existing_guarantee_columns:
                conn.execute(text(f"ALTER TABLE `{guarantee_table}` {ddl}"))
        conn.execute(text(f"ALTER TABLE `{guarantee_table}` MODIFY metric_value DECIMAL(18,2) NOT NULL"))
        guarantee_indexes = _table_indexes(conn, guarantee_table)
        if "idx_product_id" not in guarantee_indexes:
            conn.execute(text(f"ALTER TABLE `{guarantee_table}` ADD KEY idx_product_id (product_id)"))
        if "idx_source_id" not in guarantee_indexes:
            conn.execute(text(f"ALTER TABLE `{guarantee_table}` ADD KEY idx_source_id (source_id)"))
        if "idx_parsed_row_id" not in guarantee_indexes:
            conn.execute(text(f"ALTER TABLE `{guarantee_table}` ADD KEY idx_parsed_row_id (parsed_row_id)"))
        if "idx_image_name" not in guarantee_indexes:
            conn.execute(text(f"ALTER TABLE `{guarantee_table}` ADD KEY idx_image_name (image_name)"))
        if "idx_file_sha256" not in guarantee_indexes:
            conn.execute(text(f"ALTER TABLE `{guarantee_table}` ADD KEY idx_file_sha256 (file_sha256)"))


def _upsert_product_and_guarantees(
    conn: Any,
    info_table: str,
    guarantee_table: str,
    product_info: Dict[str, Any],
    guarantee_items: Sequence[Dict[str, Any]],
    batch_id: str,
) -> Tuple[int, int]:
    upsert_info_sql = f"""
    INSERT INTO `{info_table}`(
      source_id, parsed_row_id, image_name, file_sha256, product_name, life_stage,
      guarantee_basis, metab_energy_value, metab_energy_unit, image_url,
      raw_extracted_json, extract_batch_id
    )
    VALUES(
      :source_id, :parsed_row_id, :image_name, :file_sha256, :product_name, :life_stage,
      :guarantee_basis, :metab_energy_value, :metab_energy_unit, :image_url,
      :raw_extracted_json, :extract_batch_id
    )
    ON DUPLICATE KEY UPDATE
      id=LAST_INSERT_ID(id),
      parsed_row_id=VALUES(parsed_row_id),
      image_name=VALUES(image_name),
      file_sha256=VALUES(file_sha256),
      product_name=VALUES(product_name),
      life_stage=VALUES(life_stage),
      guarantee_basis=VALUES(guarantee_basis),
      metab_energy_value=VALUES(metab_energy_value),
      metab_energy_unit=VALUES(metab_energy_unit),
      image_url=VALUES(image_url),
      raw_extracted_json=VALUES(raw_extracted_json),
      extract_batch_id=VALUES(extract_batch_id)
    """
    delete_guarantee_sql = f"DELETE FROM `{guarantee_table}` WHERE source_id = :source_id"
    insert_guarantee_sql = f"""
    INSERT INTO `{guarantee_table}`(
      product_id, source_id, parsed_row_id, image_name, file_sha256, metric_name,
      operator_symbol, metric_value, metric_unit, basis, raw_text, extract_batch_id
    )
    VALUES(
      :product_id, :source_id, :parsed_row_id, :image_name, :file_sha256, :metric_name,
      :operator_symbol, :metric_value, :metric_unit, :basis, :raw_text, :extract_batch_id
    )
    """

    result = conn.execute(text(upsert_info_sql), {**product_info, "extract_batch_id": batch_id})
    product_id = int(result.lastrowid)
    conn.execute(text(delete_guarantee_sql), {"source_id": int(product_info["source_id"])})
    if not guarantee_items:
        return product_id, 0
    payload = [{**item, "product_id": product_id, "extract_batch_id": batch_id} for item in guarantee_items]
    conn.execute(text(insert_guarantee_sql), payload)
    return product_id, len(payload)


def _update_source_image_path(
    conn: Any,
    source_table: str,
    source_id: int,
    image_path: Path,
) -> None:
    conn.execute(
        text(
            f"""
            UPDATE `{source_table}`
            SET image_path = :image_path,
                image_name = :image_name
            WHERE id = :source_id
            """
        ),
        {
            "image_path": str(image_path.resolve()),
            "image_name": image_path.name,
            "source_id": int(source_id),
        },
    )


def _update_parsed_image_path(
    conn: Any,
    parsed_table: str,
    parsed_row_id: Optional[int],
    image_path: Path,
) -> None:
    if parsed_row_id is None:
        return
    conn.execute(
        text(
            f"""
            UPDATE `{parsed_table}`
            SET image_path = :image_path,
                image_name = :image_name
            WHERE id = :parsed_row_id
            """
        ),
        {
            "image_path": str(image_path.resolve()),
            "image_name": image_path.name,
            "parsed_row_id": int(parsed_row_id),
        },
    )


def _sync_parsed_image_fields(
    conn: Any,
    source_table: str,
    parsed_table: str,
) -> None:
    conn.execute(
        text(
            f"""
            UPDATE `{parsed_table}` p
            JOIN `{source_table}` s
              ON s.id = p.source_id
            SET
              p.image_path = COALESCE(p.image_path, s.image_path),
              p.image_name = COALESCE(p.image_name, s.image_name),
              p.file_sha256 = COALESCE(p.file_sha256, s.file_sha256)
            WHERE p.image_path IS NULL
               OR p.image_name IS NULL
               OR p.file_sha256 IS NULL
            """
        )
    )


def parse_catfood_guarantee_values(
    engine: Engine,
    openai_cfg: Optional[Dict[str, Any]] = None,
    ocr_cfg: Optional[Dict[str, Any]] = None,
    source_table: str = "catfood_ingredient_ocr_results",
    parsed_table: str = "catfood_ingredient_ocr_parsed",
    info_table: str = "product_info",
    guarantee_table: str = "product_guarantee",
    limit: int = 200,
    incremental_only: bool = True,
    processed_dir: Optional[Path] = DEFAULT_PROCESSED_HISTORY_DIR,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> GuaranteeParseSummary:
    source_table = _safe_table(source_table)
    parsed_table = _safe_table(parsed_table)
    info_table = _safe_table(info_table)
    guarantee_table = _safe_table(guarantee_table)
    ensure_parsed_table(engine, parsed_table)
    ensure_product_guarantee_tables(engine, info_table=info_table, guarantee_table=guarantee_table)
    qwen_cfg = _resolve_qwen_cfg(openai_cfg=openai_cfg, ocr_cfg=ocr_cfg)
    with engine.begin() as conn:
        _sync_parsed_image_fields(
            conn=conn,
            source_table=source_table,
            parsed_table=parsed_table,
        )

    lim = max(1, int(limit))
    if incremental_only:
        fetch_sql = f"""
        SELECT
          s.id AS source_id,
          p.image_path AS parsed_image_path,
          s.image_path AS source_image_path,
          p.ocr_text AS parsed_ocr_text,
          s.ocr_text AS source_ocr_text,
          p.image_name AS parsed_image_name,
          s.image_name AS source_image_name,
          p.file_sha256 AS parsed_file_sha256,
          s.file_sha256 AS source_file_sha256,
          COALESCE(p.image_name, s.image_name) AS image_name,
          COALESCE(p.file_sha256, s.file_sha256) AS file_sha256,
          p.id AS parsed_row_id,
          p.product_name AS parsed_product_name
        FROM `{source_table}` s
        LEFT JOIN `{parsed_table}` p
          ON p.source_id = s.id
        LEFT JOIN `{info_table}` i
          ON i.source_id = s.id
        WHERE i.source_id IS NULL
        ORDER BY s.id ASC
        LIMIT :lim
        """
    else:
        fetch_sql = f"""
        SELECT
          s.id AS source_id,
          p.image_path AS parsed_image_path,
          s.image_path AS source_image_path,
          p.ocr_text AS parsed_ocr_text,
          s.ocr_text AS source_ocr_text,
          p.image_name AS parsed_image_name,
          s.image_name AS source_image_name,
          p.file_sha256 AS parsed_file_sha256,
          s.file_sha256 AS source_file_sha256,
          COALESCE(p.image_name, s.image_name) AS image_name,
          COALESCE(p.file_sha256, s.file_sha256) AS file_sha256,
          p.id AS parsed_row_id,
          p.product_name AS parsed_product_name
        FROM `{source_table}` s
        LEFT JOIN `{parsed_table}` p
          ON p.source_id = s.id
        ORDER BY s.id DESC
        LIMIT :lim
        """

    with engine.begin() as conn:
        rows = conn.execute(text(fetch_sql), {"lim": lim}).mappings().all()

    batch_id = uuid.uuid4().hex[:12]
    if not rows:
        _emit_progress(
            progress_callback,
            batch_id=batch_id,
            total_rows=0,
            processed_rows=0,
            succeeded=0,
            failed=0,
            empty_guarantees=0,
            guarantee_rows=0,
            current_source_id=None,
            current_image_name=None,
            message="没有待处理的保证值图片。",
            error=None,
        )
        return GuaranteeParseSummary(
            scanned=0,
            succeeded=0,
            empty_guarantees=0,
            guarantee_rows=0,
            failed=0,
            source_table=source_table,
            parsed_table=parsed_table,
            info_table=info_table,
            guarantee_table=guarantee_table,
            batch_id=batch_id,
            error_samples=[],
        )

    succeeded = 0
    empty_guarantees = 0
    guarantee_rows = 0
    failed = 0
    error_samples: List[str] = []
    processed_root = Path(processed_dir).resolve() if processed_dir else None
    total_rows = len(rows)

    _emit_progress(
        progress_callback,
        batch_id=batch_id,
        total_rows=total_rows,
        processed_rows=0,
        succeeded=0,
        failed=0,
        empty_guarantees=0,
        guarantee_rows=0,
        current_source_id=None,
        current_image_name=None,
        message=f"准备处理 {total_rows} 条保证值图片。",
        error=None,
    )

    for row in rows:
        source_id = int(row["source_id"])
        row_data = dict(row)
        image_name = _first_non_empty(
            row_data.get("parsed_image_name"),
            row_data.get("source_image_name"),
            row_data.get("image_name"),
        ) or None
        _emit_progress(
            progress_callback,
            batch_id=batch_id,
            total_rows=total_rows,
            processed_rows=succeeded + failed,
            succeeded=succeeded,
            failed=failed,
            empty_guarantees=empty_guarantees,
            guarantee_rows=guarantee_rows,
            current_source_id=source_id,
            current_image_name=image_name,
            message=f"正在处理 source_id={source_id}，图片={image_name or '-'}。",
            error=None,
        )
        try:
            used_text_fallback = False
            try:
                image_path = _resolve_image_path(row_data)
            except FileNotFoundError:
                ocr_text = _row_ocr_text(row_data)
                if not ocr_text:
                    raise
                image_hint = _first_non_empty(
                    row_data.get("source_image_path"),
                    row_data.get("parsed_image_path"),
                    row_data.get("image_name"),
                    f"/tmp/product_guarantee_{source_id}.png",
                )
                image_path = Path(image_hint)
                qwen_res = {
                    "latency_ms": 0,
                    "model_name": "ocr_text_local_rule",
                    "extracted": _extract_guarantee_from_ocr_text(ocr_text=ocr_text),
                }
                final_image_path = image_path
                used_text_fallback = True
            else:
                qwen_res = _call_qwen_guarantee_ocr(image_path=image_path, qwen_cfg=qwen_cfg)
                final_image_path = image_path
                if processed_root:
                    final_image_path = _move_processed_image(
                        image_path=image_path,
                        source_root=None,
                        archive_root=processed_root,
                    )
            product_info, items = _convert_extracted_data(
                extracted=dict(qwen_res["extracted"]),
                row=row_data,
                image_path=final_image_path,
            )
            with engine.begin() as conn:
                _, inserted_rows = _upsert_product_and_guarantees(
                    conn=conn,
                    info_table=info_table,
                    guarantee_table=guarantee_table,
                    product_info=product_info,
                    guarantee_items=items,
                    batch_id=batch_id,
                )
                if not used_text_fallback:
                    _update_source_image_path(
                        conn=conn,
                        source_table=source_table,
                        source_id=int(row["source_id"]),
                        image_path=final_image_path,
                    )
                    _update_parsed_image_path(
                        conn=conn,
                        parsed_table=parsed_table,
                        parsed_row_id=row.get("parsed_row_id"),
                        image_path=final_image_path,
                    )
            succeeded += 1
            guarantee_rows += inserted_rows
            if not items:
                empty_guarantees += 1
            _emit_progress(
                progress_callback,
                batch_id=batch_id,
                total_rows=total_rows,
                processed_rows=succeeded + failed,
                succeeded=succeeded,
                failed=failed,
                empty_guarantees=empty_guarantees,
                guarantee_rows=guarantee_rows,
                current_source_id=source_id,
                current_image_name=final_image_path.name,
                message=(
                    f"已处理 {succeeded + failed}/{total_rows} 条；"
                    f"成功 {succeeded}，失败 {failed}，写入 guarantee {guarantee_rows} 条。"
                ),
                error=None,
            )
        except Exception as exc:
            failed += 1
            error_text = f"source_id={source_id}: {exc}"
            if len(error_samples) < 5:
                error_samples.append(error_text)
            _emit_progress(
                progress_callback,
                batch_id=batch_id,
                total_rows=total_rows,
                processed_rows=succeeded + failed,
                succeeded=succeeded,
                failed=failed,
                empty_guarantees=empty_guarantees,
                guarantee_rows=guarantee_rows,
                current_source_id=source_id,
                current_image_name=image_name,
                message=(
                    f"已处理 {succeeded + failed}/{total_rows} 条；"
                    f"成功 {succeeded}，失败 {failed}。"
                ),
                error=error_text,
            )

    _emit_progress(
        progress_callback,
        batch_id=batch_id,
        total_rows=total_rows,
        processed_rows=total_rows,
        succeeded=succeeded,
        failed=failed,
        empty_guarantees=empty_guarantees,
        guarantee_rows=guarantee_rows,
        current_source_id=None,
        current_image_name=None,
        message=(
            f"保证值解析完成：扫描 {total_rows} 条，成功 {succeeded} 条，"
            f"失败 {failed} 条，写入 guarantee {guarantee_rows} 条。"
        ),
        error=" | ".join(error_samples) if error_samples else None,
    )

    return GuaranteeParseSummary(
        scanned=len(rows),
        succeeded=succeeded,
        empty_guarantees=empty_guarantees,
        guarantee_rows=guarantee_rows,
        failed=failed,
        source_table=source_table,
        parsed_table=parsed_table,
        info_table=info_table,
        guarantee_table=guarantee_table,
        batch_id=batch_id,
        error_samples=error_samples,
    )


def rebuild_product_guarantees_from_info(
    engine: Engine,
    info_table: str = "product_info",
    guarantee_table: str = "product_guarantee",
    source_table: str = "catfood_ingredient_ocr_results",
    parsed_table: str = "catfood_ingredient_ocr_parsed",
    limit: int = 0,
    source_id: Optional[int] = None,
) -> GuaranteeRebuildSummary:
    info_table = _safe_table(info_table)
    guarantee_table = _safe_table(guarantee_table)
    source_table = _safe_table(source_table)
    parsed_table = _safe_table(parsed_table)
    ensure_product_guarantee_tables(engine, info_table=info_table, guarantee_table=guarantee_table)

    filters = ["i.raw_extracted_json IS NOT NULL", "TRIM(i.raw_extracted_json) <> ''"]
    params: Dict[str, Any] = {}
    if source_id is not None:
        filters.append("i.source_id = :source_id")
        params["source_id"] = int(source_id)

    limit_clause = ""
    limit_value = int(limit or 0)
    if limit_value > 0:
        limit_clause = "LIMIT :lim"
        params["lim"] = limit_value

    fetch_sql = f"""
    SELECT
      i.source_id,
      i.parsed_row_id,
      i.image_name,
      i.file_sha256,
      i.product_name,
      i.image_url,
      i.raw_extracted_json,
      p.ocr_text AS parsed_ocr_text,
      s.ocr_text AS source_ocr_text
    FROM `{info_table}` i
    LEFT JOIN `{parsed_table}` p
      ON p.source_id = i.source_id
    LEFT JOIN `{source_table}` s
      ON s.id = i.source_id
    WHERE {" AND ".join(filters)}
    ORDER BY i.source_id ASC
    {limit_clause}
    """

    with engine.begin() as conn:
        rows = conn.execute(text(fetch_sql), params).mappings().all()

    batch_id = uuid.uuid4().hex[:12]
    rebuilt = 0
    guarantee_rows = 0
    failed = 0
    error_samples: List[str] = []

    for row in rows:
        current_source_id = int(row["source_id"])
        try:
            extracted = safe_json_loads(str(row.get("raw_extracted_json") or ""))
            if not isinstance(extracted, dict):
                raise ValueError("raw_extracted_json is not a JSON object")

            image_hint = str(row.get("image_url") or row.get("image_name") or f"/tmp/product_guarantee_{current_source_id}")
            product_info, items = _convert_extracted_data(
                extracted=extracted,
                row={
                    "source_id": current_source_id,
                    "parsed_row_id": row.get("parsed_row_id"),
                    "image_name": row.get("image_name"),
                    "file_sha256": row.get("file_sha256"),
                    "parsed_product_name": row.get("product_name"),
                    "parsed_ocr_text": row.get("parsed_ocr_text"),
                    "source_ocr_text": row.get("source_ocr_text"),
                },
                image_path=Path(image_hint),
            )
            with engine.begin() as conn:
                _, inserted_rows = _upsert_product_and_guarantees(
                    conn=conn,
                    info_table=info_table,
                    guarantee_table=guarantee_table,
                    product_info=product_info,
                    guarantee_items=items,
                    batch_id=batch_id,
                )
            rebuilt += 1
            guarantee_rows += inserted_rows
        except Exception as exc:
            failed += 1
            if len(error_samples) < 5:
                error_samples.append(f"source_id={current_source_id}: {exc}")

    return GuaranteeRebuildSummary(
        scanned=len(rows),
        rebuilt=rebuilt,
        guarantee_rows=guarantee_rows,
        failed=failed,
        info_table=info_table,
        guarantee_table=guarantee_table,
        batch_id=batch_id,
        error_samples=error_samples,
    )
