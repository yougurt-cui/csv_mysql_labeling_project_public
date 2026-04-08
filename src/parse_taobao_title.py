from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

TABLE_RE = re.compile(r"^[A-Za-z0-9_]+$")

FORMULA_RULES: Sequence[Tuple[str, Sequence[str]]] = [
    ("鸡肉", ["鸡肉", "鸡配方", "鲜鸡"]),
    ("三文鱼", ["三文鱼", "鲑鱼"]),
    ("鱼肉", ["鱼肉", "鳕鱼", "金枪鱼", "吞拿鱼", "鲭鱼", "沙丁鱼", "鲱鱼"]),
    ("红肉", ["红肉", "牛肉", "羊肉", "鹿肉", "猪肉"]),
    ("鸭肉", ["鸭肉"]),
    ("兔肉", ["兔肉"]),
    ("火鸡", ["火鸡"]),
    ("禽肉", ["禽肉"]),
]

SUITABLE_CAT_RULES: Sequence[Tuple[str, Sequence[str]]] = [
    ("短毛猫", ["短毛猫", "英短", "美短", "蓝猫", "加菲"]),
    ("长毛猫", ["长毛猫", "布偶", "波斯", "缅因"]),
    ("脾胃虚弱", ["脾胃虚弱", "玻璃胃", "肠胃敏感", "肠胃脆弱", "肠胃弱", "调理肠胃", "易软便", "软便", "易腹泻"]),
]

ISSUE_TYPE_RULES: Sequence[Tuple[str, Sequence[str]]] = [
    ("美猫", ["美毛", "亮毛", "爆毛", "毛发", "毛色", "护肤"]),
    ("增肥", ["增肥", "发腮", "长肉", "增重"]),
    ("呵护肠胃", ["呵护肠胃", "调理肠胃", "肠胃", "消化", "玻璃胃", "软便", "腹泻", "益生菌"]),
]

AGE_ALL_PATTERNS = [
    "全阶段",
    "全期",
    "全龄",
    "全年龄",
    "全猫",
    "成幼通用",
    "成猫幼猫",
    "幼猫成猫",
]
AGE_KITTEN_PATTERNS = ["幼猫", "幼年期", "奶猫", "奶糕", "离乳", "kitten"]
AGE_ADULT_PATTERNS = ["成猫", "adult"]
AGE_SENIOR_PATTERNS = ["老年猫", "高龄猫", "senior"]

WEIGHT_TOKEN_RE = re.compile(
    r"(?P<val>\d+(?:\.\d+)?)\s*(?P<unit>kg|千克|公斤|㎏|g|克|斤)\s*(?P<mul>(?:\s*[x×*]\s*\d+(?:\.\d+)?){0,3})",
    flags=re.IGNORECASE,
)


@dataclass
class ParseSummary:
    scanned: int
    upserted: int
    source_table: str
    target_table: str
    batch_id: str


def _safe_table(name: str) -> str:
    if not TABLE_RE.fullmatch(name):
        raise ValueError(f"invalid table name: {name}")
    return name


def ensure_taobao_title_table(engine: Engine, table_name: str = "taobao_catfood_title_parsed") -> None:
    table_name = _safe_table(table_name)
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
      id BIGINT PRIMARY KEY AUTO_INCREMENT,
      source_id BIGINT NOT NULL,
      external_id CHAR(32) NULL,
      keywords VARCHAR(255) NULL,
      `付款人数` INT NULL,
      title TEXT NULL,
      price DECIMAL(10,2) NULL,
      `问题类型` VARCHAR(255) NULL,
      formula_type VARCHAR(255) NULL,
      suitable_cat VARCHAR(255) NULL,
      cat_age_stage VARCHAR(64) NULL,
      net_weight_kg DECIMAL(10,3) NULL,
      unit_price_per_kg DECIMAL(10,2) NULL,
      unit_price_text VARCHAR(64) NULL,
      parse_batch_id VARCHAR(32) NOT NULL,
      parse_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uq_source_id (source_id),
      KEY idx_external_id (external_id),
      KEY idx_keywords (keywords),
      KEY idx_pay_count (`付款人数`),
      KEY idx_issue_type (`问题类型`),
      KEY idx_formula_type (formula_type),
      KEY idx_suitable_cat (suitable_cat),
      KEY idx_cat_age_stage (cat_age_stage),
      KEY idx_unit_price_per_kg (unit_price_per_kg)
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
        if "keywords" not in col_set:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD COLUMN keywords VARCHAR(255) NULL AFTER external_id"))
        if "付款人数" not in col_set:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD COLUMN `付款人数` INT NULL AFTER keywords"))
        if "问题类型" not in col_set:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD COLUMN `问题类型` VARCHAR(255) NULL AFTER price"))

        idx_rows = conn.execute(text(f"SHOW INDEX FROM `{table_name}`")).fetchall()
        idx_names = {r[2] for r in idx_rows}
        if "idx_keywords" not in idx_names:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD INDEX idx_keywords (keywords)"))
        if "idx_pay_count" not in idx_names:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD INDEX idx_pay_count (`付款人数`)"))
        if "idx_issue_type" not in idx_names:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD INDEX idx_issue_type (`问题类型`)"))


def _normalize_title(title: Any) -> str:
    text_value = str(title or "")
    text_value = text_value.replace("￥", "¥")
    text_value = text_value.replace("＋", "+")
    text_value = re.sub(r"\s+", " ", text_value).strip()
    return text_value


def _extract_multi_label(text_value: str, rules: Sequence[Tuple[str, Sequence[str]]]) -> Optional[str]:
    lower_text = text_value.lower()
    found: List[str] = []
    for label, patterns in rules:
        if any(p.lower() in lower_text for p in patterns):
            found.append(label)
    if not found:
        return None
    return "、".join(found)


def _extract_cat_age_stage(text_value: str) -> Optional[str]:
    lower_text = text_value.lower()
    has_all = any(p.lower() in lower_text for p in AGE_ALL_PATTERNS)
    has_kitten = any(p.lower() in lower_text for p in AGE_KITTEN_PATTERNS)
    has_adult = any(p.lower() in lower_text for p in AGE_ADULT_PATTERNS)
    has_senior = any(p.lower() in lower_text for p in AGE_SENIOR_PATTERNS)

    if has_all or (has_kitten and has_adult):
        return "全阶段"

    stages: List[str] = []
    if has_kitten:
        stages.append("幼猫")
    if has_adult:
        stages.append("成猫")
    if has_senior:
        stages.append("老年猫")
    return "、".join(stages) if stages else None


def _extract_pay_count(title: str) -> Optional[int]:
    m = re.search(r"(\d+(?:\.\d+)?)(万)?\+?人付款", title or "")
    if not m:
        return None
    n = Decimal(m.group(1))
    if m.group(2):
        n *= Decimal("10000")
    return int(n)


def _to_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    text_value = str(value).replace(",", "").strip()
    m = re.search(r"\d+(?:\.\d+)?", text_value)
    if not m:
        return None
    try:
        return Decimal(m.group(0))
    except (InvalidOperation, ValueError):
        return None


def _extract_net_weight_kg(title: str) -> Optional[Decimal]:
    lower_title = title.lower()
    tokens: List[Decimal] = []

    for m in WEIGHT_TOKEN_RE.finditer(lower_title):
        start = m.start()
        left_snippet = lower_title[max(0, start - 12) : start]
        prev_char = ""
        for ch in reversed(left_snippet):
            if not ch.isspace():
                prev_char = ch
                break
        # 跳过营养描述里的重量，如 "50%/100g" 或 "每100g"
        if prev_char in {"/", "每"}:
            continue
        if re.search(r"%\s*/\s*$", left_snippet):
            continue

        val = _to_decimal(m.group("val"))
        if val is None:
            continue
        unit = (m.group("unit") or "").lower()
        mul = m.group("mul") or ""
        multipliers = [Decimal(x) for x in re.findall(r"\d+(?:\.\d+)?", mul)] or [Decimal("1")]

        factor = Decimal("1")
        if unit in {"g", "克"}:
            factor = Decimal("0.001")
        elif unit == "斤":
            factor = Decimal("0.5")

        total_multiplier = Decimal("1")
        for n in multipliers:
            total_multiplier *= n

        kg = val * factor * total_multiplier
        if kg > 0:
            tokens.append(kg)

    if not tokens:
        return None

    if re.search(r"\d\s*(?:kg|千克|公斤|㎏|g|克|斤)\s*[+＋]\s*\d", lower_title):
        chosen = sum(tokens, Decimal("0"))
        return chosen.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)

    gte_one = [x for x in tokens if x >= Decimal("1")]
    if gte_one:
        chosen = min(gte_one)
    else:
        chosen = max(tokens)
    return chosen.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)


def _calc_unit_price_per_kg(price: Any, weight_kg: Optional[Decimal]) -> Tuple[Optional[Decimal], Optional[str]]:
    if weight_kg is None or weight_kg <= 0:
        return None, None
    price_dec = _to_decimal(price)
    if price_dec is None or price_dec <= 0:
        return None, None
    unit_price = (price_dec / weight_kg).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return unit_price, f"每1kg{unit_price}元"


def parse_taobao_title_fields(title: Any, price: Any) -> Dict[str, Optional[Any]]:
    title_norm = _normalize_title(title)
    issue_type = _extract_multi_label(title_norm, ISSUE_TYPE_RULES)
    formula_type = _extract_multi_label(title_norm, FORMULA_RULES)
    suitable_cat = _extract_multi_label(title_norm, SUITABLE_CAT_RULES)
    cat_age_stage = _extract_cat_age_stage(title_norm)
    net_weight_kg = _extract_net_weight_kg(title_norm)
    unit_price_per_kg, unit_price_text = _calc_unit_price_per_kg(price, net_weight_kg)

    return {
        "title": title_norm or None,
        "issue_type": issue_type,
        "formula_type": formula_type,
        "suitable_cat": suitable_cat,
        "cat_age_stage": cat_age_stage,
        "net_weight_kg": net_weight_kg,
        "unit_price_per_kg": unit_price_per_kg,
        "unit_price_text": unit_price_text,
    }


def parse_taobao_title_standardized(
    engine: Engine,
    source_table: str = "taobao_catfood_list_items",
    target_table: str = "taobao_catfood_title_parsed",
    limit: int = 1000,
) -> ParseSummary:
    source_table = _safe_table(source_table)
    target_table = _safe_table(target_table)
    ensure_taobao_title_table(engine, target_table)

    lim = max(1, int(limit))
    fetch_sql = f"""
    SELECT s.id, s.external_id, s.keyword, s.pay_count, s.title, s.price
    FROM `{source_table}` s
    ORDER BY s.id DESC
    LIMIT :lim
    """
    with engine.begin() as conn:
        rows = conn.execute(text(fetch_sql), {"lim": lim}).mappings().all()

    batch_id = uuid.uuid4().hex[:12]
    if not rows:
        return ParseSummary(scanned=0, upserted=0, source_table=source_table, target_table=target_table, batch_id=batch_id)

    payload: List[Dict[str, Any]] = []
    for row in rows:
        parsed = parse_taobao_title_fields(row.get("title"), row.get("price"))
        payload.append(
            {
                "source_id": int(row["id"]),
                "external_id": str(row.get("external_id") or "") or None,
                "keywords": str(row.get("keyword") or "") or None,
                "pay_count": int(row.get("pay_count")) if row.get("pay_count") is not None else _extract_pay_count(str(row.get("title") or "")),
                "title": parsed.get("title"),
                "price": _to_decimal(row.get("price")),
                "issue_type": parsed.get("issue_type"),
                "formula_type": parsed.get("formula_type"),
                "suitable_cat": parsed.get("suitable_cat"),
                "cat_age_stage": parsed.get("cat_age_stage"),
                "net_weight_kg": parsed.get("net_weight_kg"),
                "unit_price_per_kg": parsed.get("unit_price_per_kg"),
                "unit_price_text": parsed.get("unit_price_text"),
                "parse_batch_id": batch_id,
            }
        )

    upsert_sql = f"""
    INSERT INTO `{target_table}` (
      source_id, external_id, keywords, `付款人数`, title, price, `问题类型`, formula_type, suitable_cat, cat_age_stage,
      net_weight_kg, unit_price_per_kg, unit_price_text, parse_batch_id, parse_ts
    )
    VALUES (
      :source_id, :external_id, :keywords, :pay_count, :title, :price, :issue_type, :formula_type, :suitable_cat, :cat_age_stage,
      :net_weight_kg, :unit_price_per_kg, :unit_price_text, :parse_batch_id, NOW()
    )
    ON DUPLICATE KEY UPDATE
      external_id=VALUES(external_id),
      keywords=VALUES(keywords),
      `付款人数`=VALUES(`付款人数`),
      title=VALUES(title),
      price=VALUES(price),
      `问题类型`=VALUES(`问题类型`),
      formula_type=VALUES(formula_type),
      suitable_cat=VALUES(suitable_cat),
      cat_age_stage=VALUES(cat_age_stage),
      net_weight_kg=VALUES(net_weight_kg),
      unit_price_per_kg=VALUES(unit_price_per_kg),
      unit_price_text=VALUES(unit_price_text),
      parse_batch_id=VALUES(parse_batch_id),
      parse_ts=NOW()
    """
    with engine.begin() as conn:
        conn.execute(text(upsert_sql), payload)

    return ParseSummary(
        scanned=len(rows),
        upserted=len(payload),
        source_table=source_table,
        target_table=target_table,
        batch_id=batch_id,
    )
