from __future__ import annotations

import concurrent.futures
import json
import os
import re
import time
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence

from openai import OpenAI
from rich.console import Console
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from tenacity import retry, stop_after_attempt, wait_exponential

from .parse_catfood_ocr import clean_ingredient_composition_text
from .utils import now_ms, safe_json_dumps

TABLE_RE = re.compile(r"^[A-Za-z0-9_]+$")
SPLIT_RE = re.compile(r"[、,，;；。\n\r\t]+")
SPACE_RE = re.compile(r"\s+")
PERCENT_RE = re.compile(r"[（(]?\d+(?:\.\d+)?%[）)]?")
PERCENT_CAPTURE_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%")

PROMPT_VERSION = "catfood_ingredient_types_v2_llm"
DEFAULT_LLM_CONCURRENCY = 4
DEFAULT_WRITE_BATCH_SIZE = 5
DEFAULT_SUMMARY_TABLE = "catfood_feature_summary"
DEFAULT_PROTEIN_TABLE = "catfood_feature_protein_labels"
DEFAULT_FIBER_CARB_TABLE = "catfood_feature_fiber_carb_labels"
DEFAULT_BIOTIC_TABLE = "catfood_feature_biotic_labels"
PIPE_VALUE_SEPARATOR = " | "
DEFAULT_INGREDIENT_ORDER = 10**9
SCRIPT_NORMALIZE_TABLE = str.maketrans(
    {
        "雞": "鸡",
        "鴨": "鸭",
        "鴿": "鸽",
        "鵪": "鹌",
        "鶉": "鹑",
        "魚": "鱼",
        "豬": "猪",
        "鮭": "鲑",
        "鱈": "鳕",
        "鮪": "鲔",
        "鮮": "鲜",
        "凍": "冻",
        "濃": "浓",
        "縮": "缩",
        "纖": "纤",
        "維": "维",
        "膽": "胆",
        "類": "类",
        "風": "风",
        "黃": "黄",
        "礦": "矿",
        "質": "质",
        "蘿": "萝",
        "蔔": "卜",
        "檸": "柠",
        "處": "处",
        "壓": "压",
        "帶": "带",
        "貓": "猫",
        "糧": "粮",
    }
)

console = Console()
_THREAD_LOCAL = threading.local()

CATEGORY_FIELDS: Sequence[str] = [
    "animal_protein",
    "plant_protein",
    "grain_carbohydrates",
    "legume_carbohydrates",
    "tuber_carbohydrates",
    "other_carbohydrates",
    "fat_source",
    "fiber_source",
    "prebiotics",
    "probiotics",
    "nutritional_additives",
    "functional_additives",
    "risky_ingredients",
    "unclear_ingredients",
    "main_ingredients",
]

ARRAY_FIELDS_FOR_PROMPT: Sequence[str] = [
    *CATEGORY_FIELDS,
]

RAW_KEY_ALIASES: Dict[str, str] = {
    "grain_carbs": "grain_carbohydrates",
    "grain_carb": "grain_carbohydrates",
    "legume_carbs": "legume_carbohydrates",
    "legume_carb": "legume_carbohydrates",
    "tuber_carbs": "tuber_carbohydrates",
    "tuber_carb": "tuber_carbohydrates",
    "carbohydrates": "other_carbohydrates",
    "carbs": "other_carbohydrates",
    "fiber": "fiber_source",
    "supplements": "nutritional_additives",
    "vitamins_minerals": "nutritional_additives",
    "additives": "functional_additives",
}

PROBIOTICS_KEYWORDS: Sequence[str] = [
    "益生菌",
    "芽孢杆菌",
    "乳杆菌",
    "双歧杆菌",
    "屎肠球菌",
    "酵母培养物",
    "发酵产物",
    "probiotic",
    "bacillus",
    "lactobacillus",
    "bifidobacterium",
    "enterococcus",
]

PREBIOTICS_KEYWORDS: Sequence[str] = [
    "益生元",
    "菊粉",
    "菊苣",
    "菊苣根",
    "果寡糖",
    "低聚果糖",
    "甘露寡糖",
    "寡糖",
    "mos",
    "fos",
    "gos",
    "inulin",
    "fructooligosaccharide",
    "mannan",
    "车前子",
    "洋车前子",
]

FAT_SOURCE_KEYWORDS: Sequence[str] = [
    "鱼油",
    "鸡油",
    "鸡脂肪",
    "鸭油",
    "猪油",
    "牛油",
    "羊油",
    "鲱鱼油",
    "鳕鱼油",
    "三文鱼油",
    "葵花籽油",
    "葵花油",
    "亚麻籽油",
    "菜籽油",
    "椰子油",
    "动物脂肪",
    "禽脂",
    "油脂",
    "脂肪",
]

FAT_SOURCE_EXCLUDE_KEYWORDS: Sequence[str] = [
    "维生素",
    "維生素",
    "抗氧化",
    "生育酚",
    "胆碱",
    "膽鹼",
    "矿物质",
    "礦物質",
    "牛磺酸",
    "肉碱",
    "氯化",
    "硫酸",
    "磷酸",
    "粗脂肪",
]

PLANT_PROTEIN_KEYWORDS: Sequence[str] = [
    "豌豆蛋白",
    "豌豆浓缩蛋白",
    "大豆浓缩蛋白",
    "大豆蛋白",
    "马铃薯蛋白",
    "土豆蛋白",
    "小麦蛋白",
    "植物蛋白",
    "谷朊粉",
    "pea protein",
    "soy protein",
    "potato protein",
    "wheat gluten",
]

ANIMAL_PROTEIN_KEYWORDS: Sequence[str] = [
    "鸡肉",
    "鸭肉",
    "火鸡肉",
    "鸽",
    "鸽子",
    "乳鸽",
    "鹌鹑",
    "鱼",
    "三文鱼",
    "鳕鱼",
    "鲱鱼",
    "牛肉",
    "羊肉",
    "猪肉",
    "鹿肉",
    "兔肉",
    "鸡肝",
    "鸭肝",
    "肝",
    "心",
    "肾",
    "胗",
    "内脏",
    "肉粉",
    "鱼粉",
    "蛋粉",
    "鸡蛋",
    "全蛋",
    "冻干",
]

ANIMAL_PROTEIN_EXCLUDE_KEYWORDS: Sequence[str] = [
    "鱼油",
    "鸡油",
    "鲱鱼油",
    "三文鱼油",
    "鳕鱼油",
    "肉碱",
]

GRAIN_CARB_KEYWORDS: Sequence[str] = [
    "玉米",
    "小麦",
    "大麦",
    "燕麦",
    "米",
    "糙米",
    "高粱",
    "小米",
]

LEGUME_CARB_KEYWORDS: Sequence[str] = [
    "豌豆",
    "鹰嘴豆",
    "扁豆",
    "菜豆",
    "兵豆",
    "蚕豆",
    "豆类",
]

TUBER_CARB_KEYWORDS: Sequence[str] = [
    "木薯",
    "马铃薯",
    "土豆",
    "甘薯",
    "红薯",
    "薯类",
]

OTHER_CARB_KEYWORDS: Sequence[str] = [
    "南瓜",
    "胡萝卜",
    "苹果",
    "梨",
    "蓝莓",
    "蔓越莓",
    "香蕉",
    "淀粉",
]

FIBER_KEYWORDS: Sequence[str] = [
    "甜菜粕",
    "纤维素",
    "粗纤维",
    "燕麦纤维",
    "竹纤维",
    "苜蓿",
    "车前子",
    "洋车前子",
    "果胶",
]

FIELD_SPLIT_HINTS: Dict[str, Sequence[str]] = {
    "fiber_source": [
        "豌豆纤维",
        "扁豆纤维",
        "甜菜粕(渣)",
        "甜菜粕（渣）",
        "甜菜粕（渣",
        "甜菜粕",
        "芒草干草粉",
        "苜蓿草颗粒",
        "苜宿草颗粒",
        "苜蓿",
        "车前子",
        "洋车前子",
        "果胶",
    ],
    "legume_carbohydrates": [
        "豌豆淀粉",
        "豌豆纤维",
        "黄豌豆",
        "绿豌豆",
        "鹰嘴豆",
        "红扁豆",
        "绿扁豆",
        "白扁豆",
        "扁豆纤维",
        "扁豆",
        "红兵豆",
        "绿兵豆",
        "兵豆",
        "海军豆",
        "斑豆",
        "芸豆",
        "菜豆",
    ],
    "other_carbohydrates": [
        "苹果果肉",
        "五爪苹果",
        "苹果",
        "巴特梨",
        "梨",
        "冬南瓜",
        "南瓜",
        "胡萝卜丁",
        "干制胡萝卜丁",
        "胡萝卜",
        "西葫芦",
        "栉瓜",
        "防风草",
        "羽衣甘蓝",
        "甘蓝菜",
        "甘蓝",
        "菠菜",
        "甜菜叶",
        "甜菜",
        "芜菁叶",
        "海带干",
        "棕海带",
        "海带",
        "蔓越莓",
        "曼越每",
        "蓝莓",
        "蓝每",
        "萨斯卡顿莓",
        "萨斯卡通莓",
        "萝卜菜",
        "萝卜莱",
        "萝卜",
    ],
}

FIELD_DROP_TOKENS: Dict[str, Sequence[str]] = {
    "legume_carbohydrates": ["豆"],
}

NUTRITIONAL_ADDITIVE_KEYWORDS: Sequence[str] = [
    "牛磺酸",
    "胆碱",
    "膽鹼",
    "维生素",
    "維生素",
    "矿物质",
    "礦物質",
    "蛋氨酸",
    "赖氨酸",
    "左旋肉碱",
    "肉碱",
    "氯化胆碱",
    "氯化钾",
    "氯化钠",
    "锌",
    "铁",
    "铜",
    "锰",
    "硒",
    "碘",
    "磷酸钙",
]

FUNCTIONAL_ADDITIVE_KEYWORDS: Sequence[str] = [
    "丝兰",
    "迷迭香",
    "抗氧化",
    "生育酚",
    "葡萄糖胺",
    "软骨素",
    "酵母提取物",
    "植物提取物",
    "叶黄素",
]

RISKY_INGREDIENT_KEYWORDS: Sequence[str] = [
    "动物脂肪",
    "肉类及其制品",
    "副产品",
    "诱食剂",
    "香精",
    "色素",
    "卡拉胶",
    "bht",
    "bha",
    "乙氧基喹啉",
    "丙二醇",
]

UNCLEAR_INGREDIENT_KEYWORDS: Sequence[str] = [
    "动物脂肪",
    "肉类及其制品",
    "禽脂",
    "油脂",
    "脂肪",
    "肉粉",
    "禽肉粉",
    "副产品",
    "水解蛋白",
]

TOKEN_LABEL_PREFIXES: Sequence[str] = [
    "原料组成",
    "配料组成",
    "配料表",
    "添加物",
    "ingredients",
    "ingredient",
]

ANIMAL_POULTRY_KEYWORDS: Sequence[str] = [
    "鸡",
    "鸭",
    "火鸡",
    "鸽",
    "乳鸽",
    "禽",
    "鹌鹑",
    "鹅",
]

ANIMAL_FISH_KEYWORDS: Sequence[str] = [
    "鱼",
    "三文",
    "鲑",
    "鳕",
    "鲱",
    "鲭",
    "吞拿",
    "金枪",
]

ANIMAL_RED_MEAT_KEYWORDS: Sequence[str] = [
    "牛",
    "羊",
    "猪",
    "鹿",
    "兔",
]

ANIMAL_EGG_KEYWORDS: Sequence[str] = [
    "蛋",
]

ANIMAL_SOURCE_ALIASES: Dict[str, Sequence[str]] = {
    "鸡": ["鸡", "鸡肉", "鸡肉粉", "鸡肝", "鸡心", "鸡脖", "鸡软骨"],
    "鸭": ["鸭", "鸭肉", "鸭肉粉", "鸭肝", "鸭心"],
    "鸽": ["鸽", "鸽子", "乳鸽", "鸽肉", "鸽子肉", "乳鸽肉", "鸽肉粉", "乳鸽肉粉", "鸽肝", "鸽心", "鸽蛋", "鸽子蛋"],
    "鱼": ["鱼", "鱼肉", "鱼粉", "三文鱼", "鲱鱼", "鳕鱼", "鲭鱼", "沙丁鱼", "比目鱼", "岩鱼", "白鱼"],
    "牛": ["牛", "牛肉", "牛肉粉", "牛肝", "牛心", "牛肾", "牛内脏"],
    "火鸡": ["火鸡", "火鸡肉", "火鸡肉粉", "火鸡肝", "火鸡心", "火鸡胗"],
    "猪": ["猪", "猪肉", "猪肉粉", "猪肝"],
    "兔": ["兔", "兔肉"],
    "鹿": ["鹿", "鹿肉"],
    "鹌鹑": ["鹌鹑", "鹌鹑肉", "鹌鹑肉粉", "鹌鹑肝", "鹌鹑心", "鹌鹑蛋"],
    "袋鼠": ["袋鼠"],
    "羊": ["羊", "羔羊", "羊肉", "羔羊肉"],
    "山羊": ["山羊"],
    "野猪": ["野猪"],
}

NOVEL_PROTEIN_SOURCES: Sequence[str] = [
    "兔",
    "鹿",
    "鸽",
    "鹌鹑",
    "袋鼠",
    "羊",
    "山羊",
    "野猪",
]

PROTEIN_DETAIL_SPLIT_RE = re.compile(r"[·•・‧∙●■◆|/]|(?<!\d)[\.．](?!\d)")

PROTEIN_DETAIL_PREFIXES: Sequence[str] = [
    "新鲜",
    "鲜",
    "完整",
    "冷冻干燥",
    "冷冻干",
    "冷冻",
    "冻干",
    "冻",
    "脱水",
    "风干",
    "去骨",
    "无骨",
    "整只",
    "整条",
    "太平洋",
    "阿拉斯加",
]

ANIMAL_PROTEIN_DETAIL_PATTERNS: Dict[str, Sequence[tuple[str, str]]] = {
    "鸡": [
        ("鸡软骨", "鸡软骨"),
        ("鸡脖", "鸡脖"),
        ("鸡胗", "鸡胗"),
        ("鸡心", "鸡心"),
        ("鸡肝", "鸡肝"),
        ("鸡蛋", "鸡蛋"),
        ("蛋粉", "蛋粉"),
        ("鸡肉粉", "鸡肉粉"),
        ("鸡肉", "鸡肉"),
        ("鸡油", "鸡油"),
    ],
    "火鸡": [
        ("火鸡胗", "火鸡胗"),
        ("火鸡心", "火鸡心"),
        ("火鸡肝", "火鸡肝"),
        ("火鸡蛋", "火鸡蛋"),
        ("火鸡肉粉", "火鸡肉粉"),
        ("火鸡肉", "火鸡肉"),
    ],
    "鸭": [
        ("鸭心", "鸭心"),
        ("鸭肝", "鸭肝"),
        ("鸭肉粉", "鸭肉粉"),
        ("鸭肉", "鸭肉"),
    ],
    "鸽": [
        ("鸽心", "鸽心"),
        ("鸽肝", "鸽肝"),
        ("乳鸽肉粉", "乳鸽肉粉"),
        ("鸽肉粉", "鸽肉粉"),
        ("乳鸽肉", "乳鸽肉"),
        ("鸽子肉", "鸽肉"),
        ("鸽肉", "鸽肉"),
        ("鸽子蛋", "鸽子蛋"),
        ("鸽蛋", "鸽蛋"),
        ("乳鸽", "乳鸽"),
        ("鸽子", "鸽"),
        ("鸽", "鸽"),
    ],
    "鱼": [
        ("鳕鱼肝", "鳕鱼肝"),
        ("鱼肝", "鱼肝"),
        ("蓝鳕", "蓝鳕鱼"),
        ("阿拉斯加鳕", "阿拉斯加鳕鱼"),
        ("三文鱼粉", "三文鱼粉"),
        ("鲱鱼粉", "鲱鱼粉"),
        ("鳕鱼粉", "鳕鱼粉"),
        ("鲭鱼粉", "鲭鱼粉"),
        ("沙丁鱼粉", "沙丁鱼粉"),
        ("白鱼粉", "白鱼粉"),
        ("白鮭鱼粉", "白鲑鱼粉"),
        ("白鲑鱼粉", "白鲑鱼粉"),
        ("三文", "三文鱼"),
        ("鲱", "鲱鱼"),
        ("鳕", "鳕鱼"),
        ("鲭", "鲭鱼"),
        ("沙丁", "沙丁鱼"),
        ("比目", "比目鱼"),
        ("岩鱼", "岩鱼"),
        ("白鮭", "白鲑鱼"),
        ("白鲑", "白鲑鱼"),
        ("白鱼", "白鱼"),
        ("鲽", "鲽鱼"),
        ("鱼粉", "鱼粉"),
        ("鱼肉", "鱼肉"),
    ],
    "牛": [
        ("牛肾", "牛肾"),
        ("牛心", "牛心"),
        ("牛肝", "牛肝"),
        ("牛肉粉", "牛肉粉"),
        ("牛肉", "牛肉"),
    ],
    "羊": [
        ("羔羊肉粉", "羔羊肉粉"),
        ("羔羊肉", "羔羊肉"),
        ("羊肝", "羊肝"),
        ("羊心", "羊心"),
        ("羊肉粉", "羊肉粉"),
        ("羊肉", "羊肉"),
    ],
    "山羊": [
        ("山羊肉粉", "山羊肉粉"),
        ("山羊肉", "山羊肉"),
    ],
    "猪": [
        ("猪肝", "猪肝"),
        ("猪肉粉", "猪肉粉"),
        ("猪肉", "猪肉"),
    ],
    "野猪": [
        ("野猪肉", "野猪肉"),
    ],
    "兔": [
        ("兔肉", "兔肉"),
    ],
    "鹿": [
        ("鹿肉", "鹿肉"),
    ],
    "鹌鹑": [
        ("鹌鹑肉", "鹌鹑肉"),
        ("鹌鹑", "鹌鹑"),
    ],
    "袋鼠": [
        ("袋鼠肉", "袋鼠肉"),
        ("袋鼠", "袋鼠"),
    ],
}

PLANT_PROTEIN_DETAIL_PATTERNS: Sequence[tuple[str, str]] = [
    ("豌豆蛋白", "豌豆蛋白"),
    ("大豆蛋白", "大豆蛋白"),
    ("豆蛋白", "豆蛋白"),
    ("马铃薯蛋白", "马铃薯蛋白"),
    ("土豆蛋白", "土豆蛋白"),
    ("小麦蛋白", "小麦蛋白"),
    ("玉米蛋白", "玉米蛋白"),
    ("米蛋白", "米蛋白"),
]

FISH_SPECIES_HINTS: Sequence[str] = [
    "三文",
    "鲱",
    "蓝鳕",
    "阿拉斯加鳕",
    "鳕",
    "鲭",
    "沙丁",
    "比目",
    "岩鱼",
    "白鮭",
    "白鲑",
    "白鱼",
    "鲽",
]

DEFAULT_ANIMAL_DETAIL_LABELS: Dict[str, str] = {
    "鸡": "鸡肉",
    "火鸡": "火鸡肉",
    "鸭": "鸭肉",
    "鸽": "鸽肉",
    "鱼": "鱼肉",
    "牛": "牛肉",
    "羊": "羊肉",
    "山羊": "山羊肉",
    "猪": "猪肉",
    "野猪": "野猪肉",
    "兔": "兔肉",
    "鹿": "鹿肉",
    "鹌鹑": "鹌鹑",
    "袋鼠": "袋鼠",
}

FLAVOR_ADDITIVE_KEYWORDS: Sequence[str] = [
    "天然风味",
    "天然风",
    "风味剂",
    "风味添加剂",
    "风味喷涂",
    "调味",
    "诱食",
    "适口性",
]

OIL_SOURCE_PATTERNS: Sequence[tuple[str, str]] = [
    ("明太鱼油", "明太鱼油"),
    ("鳕鱼油", "鳕鱼油"),
    ("鱈鱼油", "鳕鱼油"),
    ("鲱鱼油", "鲱鱼油"),
    ("三文鱼油", "三文鱼油"),
    ("鲭鱼油", "鲭鱼油"),
    ("沙丁鱼油", "沙丁鱼油"),
    ("鱼油", "鱼油"),
    ("鸡脂肪", "鸡油"),
    ("鸡油", "鸡油"),
    ("火鸡油", "火鸡油"),
    ("鸭油", "鸭油"),
    ("猪油", "猪油"),
    ("牛油", "牛油"),
    ("羊油", "羊油"),
    ("冷压葵花籽油", "葵花籽油"),
    ("葵花籽油", "葵花籽油"),
    ("大豆油", "大豆油"),
    ("亚麻籽油", "亚麻籽油"),
    ("菜籽油", "菜籽油"),
    ("橄榄油", "橄榄油"),
    ("椰子油", "椰子油"),
]


@dataclass
class ParseSummary:
    scanned: int
    upserted: int
    source_table: str
    target_table: str
    batch_id: str
    protein_rows: int = 0
    fiber_carb_rows: int = 0
    biotic_rows: int = 0


@dataclass
class CompositionClassificationResult:
    normalized: Dict[str, Any]
    model_json_text: str
    model_name: str
    model_latency_ms: int


def _safe_table(name: str) -> str:
    if not TABLE_RE.fullmatch(name):
        raise ValueError(f"invalid table name: {name}")
    return name


def _contains_any(text_value: str, keywords: Sequence[str]) -> bool:
    return any(k in text_value for k in keywords)


def _normalize_script(text_value: Any) -> str:
    return str(text_value or "").translate(SCRIPT_NORMALIZE_TABLE)


def _normalize_token(token: str) -> str:
    out = _normalize_script(token)
    out = out.replace("：", ":")
    out = PERCENT_RE.sub("", out)
    out = SPACE_RE.sub("", out)
    if ":" in out:
        left, right = out.split(":", 1)
        left_l = left.lower()
        if any(prefix in left_l for prefix in TOKEN_LABEL_PREFIXES):
            out = right
    out = out.strip("。、,，;；:：[]")
    return out


def _normalize_token_keep_percent(token: str) -> str:
    out = _normalize_script(token)
    out = out.replace("：", ":")
    out = SPACE_RE.sub("", out)
    if ":" in out:
        left, right = out.split(":", 1)
        left_l = left.lower()
        if any(prefix in left_l for prefix in TOKEN_LABEL_PREFIXES):
            out = right
    out = out.strip("。、,，;；:：[]")
    return out


def _normalize_output_token(token: Any) -> str:
    out = _normalize_script(token).strip()
    out = out.replace("：", ":")
    out = re.sub(r"\s+", " ", out).strip("。、,，;；:：[]{}\"'")
    if re.search(r"[\u4e00-\u9fff]", out):
        out = out.replace(" ", "")
    return out


def _unique_tokens(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        token = _normalize_output_token(value)
        if not token:
            continue
        key = token.lower()
        if key in {"null", "none", "n/a", "无", "空", "未知"}:
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(token)
    return out


def _join_tokens(values: Sequence[str]) -> Optional[str]:
    vals = _unique_tokens(values)
    return "、".join(vals) if vals else None


def _join_pipe_tokens(values: Sequence[str]) -> Optional[str]:
    vals = _unique_tokens(values)
    return PIPE_VALUE_SEPARATOR.join(vals) if vals else None


def _explode_feature_value_tokens(values: Sequence[str]) -> List[str]:
    items: List[str] = []
    for value in values:
        text_value = str(value or "").strip()
        if not text_value:
            continue
        cleaned = clean_ingredient_composition_text(text_value) or text_value
        pieces = _split_ingredient_tokens(cleaned)
        if pieces:
            items.extend(pieces)
            continue
        token = _normalize_output_token(text_value)
        if token:
            items.append(token)
    return _unique_tokens(items)


def _split_feature_token_by_hints(token: str, field_name: Optional[str]) -> List[str]:
    if not field_name:
        return []
    token_norm = _normalize_token(token)
    if not token_norm:
        return []

    candidates: List[tuple[int, int, int, str]] = []
    for idx, hint in enumerate(FIELD_SPLIT_HINTS.get(field_name) or []):
        hint_norm = _normalize_token(hint)
        if len(hint_norm) < 2:
            continue
        start = token_norm.find(hint_norm)
        if start < 0:
            continue
        candidates.append((start, -len(hint_norm), idx, hint))

    if not candidates:
        return []

    chosen_parts = _select_non_overlapping_match_parts(token_norm, candidates)
    if not chosen_parts:
        return []

    covered = sum(len(_normalize_token(part)) for part in chosen_parts)
    if len(chosen_parts) >= 2 or covered >= max(min(len(token_norm), 6) - 1, 2) or (
        len(chosen_parts) == 1 and _normalize_token(chosen_parts[0]) != token_norm
    ):
        return _unique_tokens(chosen_parts)
    return []


def _select_non_overlapping_match_parts(
    token_norm: str,
    candidates: Sequence[tuple[int, int, int, str]],
) -> List[str]:
    selected: List[tuple[int, int, int, str]] = []
    for start, neg_len, idx, part in sorted(candidates):
        end = start + abs(neg_len)
        if any(not (end <= prev_start or start >= prev_end) for prev_start, prev_end, _, _ in selected):
            continue
        selected.append((start, end, idx, part))
    selected.sort()
    return [part for _, _, _, part in selected]


def _refine_feature_tokens_by_composition(
    ingredient_composition: Optional[str],
    values: Sequence[str],
    field_name: Optional[str] = None,
) -> List[str]:
    raw_tokens = _explode_feature_value_tokens(values)
    if not raw_tokens:
        return []

    composition_tokens = _split_ingredient_tokens(_normalize_ingredient_composition_for_storage(ingredient_composition))
    if not composition_tokens:
        return raw_tokens

    refined: List[str] = []
    for token in raw_tokens:
        token_norm = _normalize_token(token)
        if not token_norm:
            continue

        hint_parts = _split_feature_token_by_hints(token, field_name)
        if hint_parts:
            refined.extend(hint_parts)
            continue

        exact_matches = [part for part in composition_tokens if _normalize_token(part) == token_norm]
        if exact_matches:
            refined.extend(exact_matches[:1])
            continue

        preferred_candidates: List[tuple[int, int, int, str]] = []
        fallback_candidates: List[tuple[int, int, int, str]] = []
        for idx, part in enumerate(composition_tokens):
            part_norm = _normalize_token(part)
            if len(part_norm) < 2:
                continue
            start = token_norm.find(part_norm)
            if start < 0:
                continue
            candidate = (start, -len(part_norm), idx, part)
            fallback_candidates.append(candidate)
            if field_name and field_name in _legacy_classify_token(part):
                preferred_candidates.append(candidate)

        chosen_parts = _select_non_overlapping_match_parts(token_norm, preferred_candidates or fallback_candidates)
        if not chosen_parts:
            refined.append(token)
            continue

        covered = sum(len(_normalize_token(part)) for part in chosen_parts)
        has_multiple_parts = len(chosen_parts) >= 2
        preferred_field_match = bool(
            field_name and any(field_name in _legacy_classify_token(part) for part in chosen_parts)
        )
        if has_multiple_parts or covered >= max(len(token_norm) - 1, 2) or preferred_field_match:
            refined.extend(chosen_parts)
        else:
            refined.append(token)

    cleaned = _unique_tokens(refined)
    drop_tokens = {_normalize_token(value) for value in (FIELD_DROP_TOKENS.get(field_name) or [])}
    return [token for token in cleaned if _normalize_token(token) not in drop_tokens]


def _split_ingredient_tokens(ingredient_composition: Optional[str]) -> List[str]:
    text_value = str(ingredient_composition or "").strip()
    if not text_value:
        return []
    text_value = re.sub(r"(\d+(?:\.\d+)?%[）)])(?=[A-Za-z\u4e00-\u9fff])", r"\1、", text_value)
    parts = SPLIT_RE.split(text_value)
    items: List[str] = []
    seen = set()
    for part in parts:
        token = _normalize_token(part)
        if not token:
            continue
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        items.append(token)
    return items


def _composition_cache_key(ingredient_composition: Optional[str]) -> str:
    out = str(ingredient_composition or "").strip()
    out = out.replace("：", ":")
    out = out.replace("，", ",")
    out = out.replace("、", ",")
    out = out.replace("；", ";")
    out = SPACE_RE.sub("", out)
    return out.strip(";,，。、")


def _normalize_ingredient_composition_for_storage(value: Optional[str]) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    cleaned = clean_ingredient_composition_text(raw)
    normalized = cleaned or raw
    normalized = re.sub(r"(\d+(?:\.\d+)?%[）)])(?=[A-Za-z\u4e00-\u9fff])", r"\1、", normalized)
    return normalized


def _extract_percent(token_with_percent: str) -> Optional[float]:
    m = PERCENT_CAPTURE_RE.search(token_with_percent or "")
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def _match_token(token: str, candidate: str) -> bool:
    token_norm = _normalize_token(token).lower()
    cand_norm = _normalize_token(candidate).lower()
    if not token_norm or not cand_norm:
        return False
    if token_norm == cand_norm:
        return True
    if len(token_norm) >= 2 and token_norm in cand_norm:
        return True
    if len(cand_norm) >= 2 and cand_norm in token_norm:
        return True
    return False


def _compute_animal_protein_ratio(ingredient_composition: Optional[str], animal_tokens: Sequence[str]) -> Optional[float]:
    text_value = str(ingredient_composition or "").strip()
    animal_norms = [_normalize_token(x) for x in animal_tokens if _normalize_token(x)]
    if not text_value or not animal_norms:
        return None

    ratio_sum = 0.0
    has_ratio = False
    for raw_part in SPLIT_RE.split(text_value):
        raw_token = _normalize_token_keep_percent(raw_part)
        normalized = _normalize_token(raw_token)
        if not normalized:
            continue
        if not any(_match_token(normalized, animal) for animal in animal_norms):
            continue
        pct = _extract_percent(raw_token)
        if pct is None:
            continue
        ratio_sum += pct
        has_ratio = True

    if has_ratio:
        return round(ratio_sum, 2)
    return None


def _compute_animal_protein_tag(animal_tokens: Sequence[str]) -> Optional[str]:
    categories: List[str] = []
    normalized = [str(token or "").lower() for token in animal_tokens]
    if any(any(k in token for k in ANIMAL_POULTRY_KEYWORDS) for token in normalized):
        categories.append("禽类")
    if any(any(k in token for k in ANIMAL_FISH_KEYWORDS) for token in normalized):
        categories.append("鱼类")
    if any(any(k in token for k in ANIMAL_RED_MEAT_KEYWORDS) for token in normalized):
        categories.append("红肉")
    if any(any(k in token for k in ANIMAL_EGG_KEYWORDS) for token in normalized):
        categories.append("蛋类")
    if not categories:
        return None
    ordered = []
    seen = set()
    for label in ("禽类", "鱼类", "红肉", "蛋类"):
        if label in categories and label not in seen:
            seen.add(label)
            ordered.append(label)
    return "+".join(ordered) if ordered else None


def _infer_animal_group(token: str) -> Optional[str]:
    token_l = str(token or "").lower()
    labels: List[str] = []
    if any(k in token_l for k in ANIMAL_POULTRY_KEYWORDS):
        labels.append("禽类")
    if any(k in token_l for k in ANIMAL_FISH_KEYWORDS):
        labels.append("鱼类")
    if any(k in token_l for k in ANIMAL_RED_MEAT_KEYWORDS):
        labels.append("红肉")
    if any(k in token_l for k in ANIMAL_EGG_KEYWORDS):
        labels.append("蛋类")
    if not labels:
        return None
    ordered: List[str] = []
    seen = set()
    for label in ("禽类", "鱼类", "红肉", "蛋类"):
        if label in labels and label not in seen:
            seen.add(label)
            ordered.append(label)
    return "+".join(ordered) if ordered else None


def _dedupe_joined_labels(value: Optional[str], separator: str = "+") -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    parts = [part.strip() for part in re.split(rf"\s*{re.escape(separator)}\s*", raw) if part.strip()]
    ordered: List[str] = []
    seen = set()
    for part in parts:
        if part in seen:
            continue
        seen.add(part)
        ordered.append(part)
    return separator.join(ordered) if ordered else None


def _normalize_product_text(value: Any) -> str:
    out = str(value or "").strip()
    out = re.sub(r"\s+", " ", out)
    return out


def _build_product_key(brand: Any, product_name: Any, source_id: Any) -> str:
    brand_text = _normalize_product_text(brand)
    product_text = _normalize_product_text(product_name)
    if brand_text or product_text:
        return f"{brand_text.lower()}||{product_text.lower()}"
    return f"source::{int(source_id)}"


def _canonicalize_animal_sources(tokens: Sequence[str]) -> List[str]:
    ordered: List[str] = []
    seen = set()
    for token in tokens:
        token_l = str(token or "").lower()
        if not token_l:
            continue
        matches: List[tuple[int, str]] = []
        for animal, aliases in ANIMAL_SOURCE_ALIASES.items():
            if animal in seen:
                continue
            best_alias_len = 0
            for alias in aliases:
                alias_l = alias.lower()
                if alias_l and alias_l in token_l:
                    best_alias_len = max(best_alias_len, len(alias_l))
            if best_alias_len > 0:
                matches.append((best_alias_len, animal))
        if not matches:
            continue
        matches.sort(key=lambda item: (-item[0], item[1]))
        chosen = matches[0][1]
        seen.add(chosen)
        ordered.append(chosen)
    return ordered


def _compute_protein_structure(animal_sources: Sequence[str]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    unique_sources = _unique_tokens(list(animal_sources))
    animal_source_count = len(unique_sources)
    if animal_source_count == 0:
        animal_source_tag = None
    elif animal_source_count == 1:
        animal_source_tag = "单一肉源"
    else:
        animal_source_tag = "多肉源"

    novel_protein = None
    if unique_sources:
        novel_protein = "是" if any(source in NOVEL_PROTEIN_SOURCES for source in unique_sources) else "否"

    tags: List[str] = []
    if animal_source_tag:
        tags.append(animal_source_tag)
    if novel_protein == "是":
        tags.append("新奇蛋白")
    protein_structure = "、".join(tags) if tags else None
    return protein_structure, novel_protein, animal_source_tag


def _normalize_percent_value(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    out = float(value)
    while out > 100 and out <= 1000:
        out = out / 10.0
    return round(out, 2)


def _extract_labeled_percent(text_value: Optional[str], labels: Sequence[str]) -> Optional[float]:
    text_value = str(text_value or "")
    if not text_value:
        return None
    normalized = text_value.replace("％", "%").replace("≥", ">=").replace("≤", "<=")
    for label in labels:
        pattern = rf"{re.escape(label)}[\s:：]*(?:>=|<=|>|<|=)?\s*(\d+(?:\.\d+)?)\s*%"
        m = re.search(pattern, normalized, flags=re.IGNORECASE)
        if not m:
            continue
        try:
            return _normalize_percent_value(float(m.group(1)))
        except Exception:
            continue
    return None


def _extract_energy_kcal_per_100g(text_value: Optional[str]) -> Optional[float]:
    text_value = str(text_value or "")
    if not text_value:
        return None
    normalized = text_value.replace("Ｋ", "K").replace("ｋ", "k").replace("／", "/")
    patterns = [
        (rf"(?:代谢能|代謝能|metabolizable energy|orie content).*?(\d+(?:\.\d+)?)\s*kcal?\s*/?\s*kg", 0.1),
        (rf"(?:代谢能|代謝能|metabolizable energy|orie content).*?(\d+(?:\.\d+)?)\s*kcal?\s*/?\s*100g", 1.0),
        (rf"(\d+(?:\.\d+)?)\s*kcal?\s*/?\s*kg", 0.1),
        (rf"(\d+(?:\.\d+)?)\s*kcal?\s*/?\s*100g", 1.0),
    ]
    for pattern, multiplier in patterns:
        m = re.search(pattern, normalized, flags=re.IGNORECASE | re.DOTALL)
        if not m:
            continue
        try:
            return round(float(m.group(1)) * multiplier, 2)
        except Exception:
            continue
    return None


def _extract_protein_content_pct(text_value: Optional[str]) -> Optional[float]:
    return _extract_labeled_percent(text_value, ["粗蛋白质", "粗蛋白", "蛋白质", "protein"])


def _extract_fat_pct(text_value: Optional[str]) -> Optional[float]:
    return _extract_labeled_percent(text_value, ["粗脂肪", "脂肪", "fat"])


def _compute_energy_fat_tags(
    energy_kcal_per_100g: Optional[float],
    fat_pct: Optional[float],
    oil_sources: Sequence[str],
) -> Optional[str]:
    tags: List[str] = []
    if energy_kcal_per_100g is not None and energy_kcal_per_100g >= 400:
        tags.append("高能量")
    if fat_pct is not None and fat_pct >= 18:
        tags.append("高脂肪")
    oil_source_count = len(_unique_tokens(oil_sources))
    oil_conditions = 0
    if fat_pct is not None and fat_pct >= 20:
        oil_conditions += 1
    if oil_source_count >= 2:
        oil_conditions += 1
    if oil_source_count >= 3:
        oil_conditions += 1
    if oil_conditions >= 2:
        tags.append("高油脂")
    return "、".join(tags) if tags else None


def _detect_detail_animal_sources(token: str) -> List[str]:
    token_l = str(token or "").lower()
    matched: List[str] = []
    for animal in ("火鸡", "山羊", "野猪", "袋鼠", "鸽", "鹌鹑", "鹿", "兔", "鸭", "鸡", "鱼", "牛", "猪", "羊"):
        aliases = ANIMAL_SOURCE_ALIASES.get(animal) or []
        if any(alias.lower() in token_l for alias in aliases):
            matched.append(animal)
    return matched


def _contains_flavor_additive_keyword(token: Any) -> bool:
    text_value = str(token or "")
    return any(keyword in text_value for keyword in FLAVOR_ADDITIVE_KEYWORDS)


def _clean_protein_detail_fragment(token: Any) -> str:
    out = str(token or "").strip()
    if not out:
        return ""
    out = out.replace("％", "%").replace("：", ":")
    out = re.sub(r"[“”‘’\"'`~!@#$%^&*_+=<>?？]", "", out)
    if "来源于" in out:
        out = out.split("来源于", 1)[-1]
    out = PERCENT_RE.sub("", out)
    out = re.sub(r"[（(].*$", "", out)
    out = re.sub(r"[《》「」『』【】\[\]{}<>]", "", out)
    out = _normalize_output_token(out)
    while out:
        changed = False
        for prefix in PROTEIN_DETAIL_PREFIXES:
            if out.startswith(prefix):
                candidate = out[len(prefix):]
                if not candidate:
                    continue
                out = candidate
                changed = True
        if not changed:
            break
    out = out.replace("鸡鸡", "鸡").replace("鸭鸭", "鸭").replace("鱼鱼", "鱼")
    out = out.strip("。、,，;；:：|/\\.-")
    return out


def _clean_oil_source_fragment(token: Any) -> str:
    out = str(token or "").strip()
    if not out:
        return ""
    out = out.replace("％", "%").replace("：", ":").replace("鱈", "鳕")
    out = re.sub(r"[“”‘’\"'`~!@#$%^&*_+=<>?？]", "", out)
    if "来源于" in out:
        out = out.split("来源于", 1)[-1]
    out = PERCENT_RE.sub("", out)
    out = re.sub(r"[（(].*$", "", out)
    out = re.sub(r"[《》「」『』【】\[\]{}<>]", "", out)
    out = _normalize_output_token(out)
    while out:
        changed = False
        for prefix in PROTEIN_DETAIL_PREFIXES:
            if out.startswith(prefix):
                candidate = out[len(prefix):]
                if not candidate:
                    continue
                out = candidate
                changed = True
        if not changed:
            break
    return out.strip("。、,，;；:：|/\\.-")


def _canonicalize_animal_protein_detail(token: str, source_label: str) -> Optional[str]:
    cleaned = _clean_protein_detail_fragment(token)
    if not cleaned:
        return None
    if _contains_flavor_additive_keyword(token):
        return None
    if "油" in cleaned or "脂" in cleaned:
        return None
    if len(_detect_detail_animal_sources(cleaned)) > 1:
        return None
    if source_label == "鱼":
        fish_hits = [hint for hint in FISH_SPECIES_HINTS if hint in cleaned]
        if len(set(fish_hits)) >= 2:
            return None
    patterns = ANIMAL_PROTEIN_DETAIL_PATTERNS.get(source_label) or []
    for needle, label in patterns:
        if needle in cleaned:
            return label
    if len(cleaned) > 12:
        return None
    detected = _detect_detail_animal_sources(cleaned)
    if detected:
        if cleaned == source_label:
            return DEFAULT_ANIMAL_DETAIL_LABELS.get(source_label, cleaned)
        return cleaned
    return None


def _canonicalize_plant_protein_detail(token: str) -> Optional[str]:
    cleaned = _clean_protein_detail_fragment(token)
    if not cleaned:
        return None
    if _contains_flavor_additive_keyword(token):
        return None
    for needle, label in PLANT_PROTEIN_DETAIL_PATTERNS:
        if needle in cleaned:
            return label
    if len(cleaned) > 12:
        return None
    if not re.search(r"蛋白|豆|谷|米|麦|玉米|土豆|马铃薯|豌豆", cleaned):
        return None
    return cleaned


def _extract_protein_source_details(item: Dict[str, Any]) -> List[tuple[str, str]]:
    raw_token = str(item.get("ingredient_name") or "").strip()
    protein_type = str(item.get("protein_type") or "")
    if not raw_token:
        return []

    raw_parts: List[str] = []
    for part in SPLIT_RE.split(raw_token):
        part = str(part or "").strip()
        if not part:
            continue
        subparts = [x.strip() for x in PROTEIN_DETAIL_SPLIT_RE.split(part) if str(x).strip()]
        raw_parts.extend(subparts or [part])

    if not raw_parts:
        raw_parts = [raw_token]

    details: List[tuple[str, str]] = []
    for part in raw_parts:
        if _contains_flavor_additive_keyword(part):
            continue
        if protein_type == "animal_protein":
            cleaned = _clean_protein_detail_fragment(part)
            if not cleaned:
                continue
            source_labels = _detect_detail_animal_sources(cleaned)
            if not source_labels and "蛋" in cleaned:
                source_labels = ["其他动物蛋白"]
            if len(source_labels) != 1:
                continue
            source_label = source_labels[0]
            detail = (
                _canonicalize_animal_protein_detail(cleaned, source_label)
                if source_label != "其他动物蛋白"
                else cleaned if len(cleaned) <= 12 else None
            )
            if not detail:
                continue
        else:
            source_label = "植物蛋白"
            detail = _canonicalize_plant_protein_detail(part)
            if not detail:
                continue
        details.append((source_label, detail))
    return details


def _canonicalize_oil_source(token: Any) -> Optional[str]:
    raw_value = str(token or "").strip()
    if not raw_value or _contains_flavor_additive_keyword(raw_value):
        return None
    cleaned = _clean_oil_source_fragment(raw_value)
    if not cleaned:
        return None
    for needle, label in OIL_SOURCE_PATTERNS:
        if needle in cleaned:
            return label
    if "油" not in cleaned:
        return None
    if len(cleaned) > 12:
        return None
    return cleaned


def _build_clean_oil_sources(fat_source_tokens: Sequence[str]) -> List[str]:
    cleaned_sources: List[str] = []
    for token in fat_source_tokens:
        for part in SPLIT_RE.split(str(token or "")):
            part = str(part or "").strip()
            if not part:
                continue
            subparts = [x.strip() for x in PROTEIN_DETAIL_SPLIT_RE.split(part) if str(x).strip()]
            for subpart in subparts or [part]:
                oil_source = _canonicalize_oil_source(subpart)
                if oil_source:
                    cleaned_sources.append(oil_source)
    return _unique_tokens(cleaned_sources)


def _build_protein_source_details(protein_feature_items: Sequence[Dict[str, Any]]) -> Optional[str]:
    grouped: Dict[str, List[str]] = {}
    for item in protein_feature_items:
        for source_label, detail_text in _extract_protein_source_details(item):
            grouped.setdefault(source_label, [])
            if detail_text not in grouped[source_label]:
                grouped[source_label].append(detail_text)
    if not grouped:
        return None
    return "；".join(f"{label}：{'、'.join(details)}" for label, details in grouped.items() if details)


def _infer_animal_protein_source_type(token: Any) -> Optional[str]:
    raw_value = str(token or "").strip()
    if not raw_value:
        return None
    raw_value = raw_value.replace("％", "%").replace("：", ":")
    if "来源于" in raw_value:
        raw_value = raw_value.split("来源于", 1)[-1]
    raw_value = PERCENT_RE.sub("", raw_value)
    raw_value = re.sub(r"[《》「」『』【】\[\]{}<>]", "", raw_value)
    raw_value = _normalize_output_token(raw_value).strip("。、,，;；:：|/\\.-")

    cleaned = _clean_protein_detail_fragment(token)
    if not cleaned and not raw_value:
        return None

    source_text = raw_value or cleaned
    source_text_l = source_text.lower()

    if "水解" in source_text or "hydroly" in source_text_l:
        return "水解蛋白"
    if any(keyword in source_text for keyword in ("全蛋粉", "蛋黄粉", "蛋粉", "全蛋", "鸡蛋", "鸭蛋", "火鸡蛋", "鹌鹑蛋", "蛋黄")):
        return "蛋类"
    if "冻干" in source_text or "冷冻干燥" in source_text or "freeze dried" in source_text_l or "freeze-dried" in source_text_l:
        return "冻干"
    if any(keyword in source_text for keyword in ("肉粉", "鱼粉", "禽肉粉", "肉骨粉", "骨粉")):
        return "肉粉"
    if any(keyword in source_text for keyword in ("脱水", "风干")):
        return "脱水肉"
    if any(keyword in source_text for keyword in ("内脏", "肝", "心", "肾", "胗", "脖", "软骨")) and "蛋" not in source_text:
        return "内脏"
    if "冻干" not in source_text and "冻" in source_text:
        return "冻肉"
    if any(keyword in source_text for keyword in ("新鲜", "鲜", "去骨", "无骨", "整只", "整条", "完整")):
        return "鲜肉"
    if _detect_detail_animal_sources(cleaned):
        return "鲜肉"
    return "动物蛋白"


def _sort_protein_source_candidates(candidates: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        candidates,
        key=lambda item: (
            0 if item.get("ratio") is not None else 1,
            -float(item.get("ratio") or 0),
            int(item.get("order") if item.get("order") is not None else DEFAULT_INGREDIENT_ORDER),
            str(item.get("species") or ""),
            str(item.get("protein_source_type") or ""),
            str(item.get("ingredient_name") or ""),
        ),
    )


def _build_protein_source_candidates(protein_feature_items: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    seen = set()
    for item in protein_feature_items:
        if str(item.get("protein_type") or "") != "animal_protein":
            continue

        ingredient_name = _normalize_output_token(item.get("ingredient_name"))
        if not ingredient_name:
            continue

        species_values = _coerce_list(item.get("source_species")) or _canonicalize_animal_sources([ingredient_name])
        if not species_values:
            continue

        protein_source_type = _clean_note(item.get("protein_source_type")) or _infer_animal_protein_source_type(ingredient_name)
        ratio = _coerce_float(item.get("ingredient_ratio_percent"))

        order_raw = item.get("ingredient_order")
        try:
            order = int(order_raw) if order_raw is not None else DEFAULT_INGREDIENT_ORDER
        except Exception:
            order = DEFAULT_INGREDIENT_ORDER

        try:
            is_primary = int(item.get("is_primary") or 0)
        except Exception:
            is_primary = 0

        for species in _unique_tokens(species_values):
            key = (
                ingredient_name.lower(),
                species,
                protein_source_type or "",
                ratio,
                order,
                is_primary,
            )
            if key in seen:
                continue
            seen.add(key)
            candidates.append(
                {
                    "ingredient_name": ingredient_name,
                    "species": species,
                    "protein_source_type": protein_source_type,
                    "ratio": ratio,
                    "order": order,
                    "is_primary": is_primary,
                }
            )
    return _sort_protein_source_candidates(candidates)


def _extract_protein_source_labels(candidates: Sequence[Dict[str, Any]]) -> tuple[List[str], List[str]]:
    ordered = _sort_protein_source_candidates(candidates)
    species_values = _unique_tokens(str(item.get("species") or "") for item in ordered)
    type_values = _unique_tokens(
        str(item.get("protein_source_type") or "")
        for item in ordered
        if str(item.get("protein_source_type") or "").strip()
    )
    return species_values, type_values


def _select_ratio_source_candidates(
    candidates: Sequence[Dict[str, Any]],
    min_ratio: float,
    max_ratio: Optional[float] = None,
    exclude_species: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    excluded = {str(value or "") for value in exclude_species or []}
    out: List[Dict[str, Any]] = []
    for item in candidates:
        species = str(item.get("species") or "")
        ratio = item.get("ratio")
        if not species or species in excluded or ratio is None:
            continue
        ratio_value = float(ratio)
        if ratio_value < min_ratio:
            continue
        if max_ratio is not None and ratio_value > max_ratio:
            continue
        out.append(item)
    return _sort_protein_source_candidates(out)


def _select_species_group_candidates(
    candidates: Sequence[Dict[str, Any]],
    rank: int,
    exclude_species: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    if rank <= 0:
        return []
    excluded = {str(value or "") for value in exclude_species or []}
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    ordered = sorted(
        candidates,
        key=lambda item: (
            int(item.get("order") if item.get("order") is not None else DEFAULT_INGREDIENT_ORDER),
            -float(item.get("ratio") or 0),
            str(item.get("ingredient_name") or ""),
        ),
    )
    for item in ordered:
        species = str(item.get("species") or "")
        if not species or species in excluded:
            continue
        grouped.setdefault(species, []).append(item)

    species_order = list(grouped.keys())
    if rank > len(species_order):
        return []
    selected = grouped.get(species_order[rank - 1], [])
    if not selected:
        return []
    first_order = min(int(item.get("order") if item.get("order") is not None else DEFAULT_INGREDIENT_ORDER) for item in selected)
    return _sort_protein_source_candidates(
        [
            item
            for item in selected
            if int(item.get("order") if item.get("order") is not None else DEFAULT_INGREDIENT_ORDER) == first_order
        ]
    )


def _build_protein_source_origin(has_animal_protein: bool, has_plant_protein: bool) -> Optional[str]:
    if has_animal_protein and has_plant_protein:
        return "动植物混合蛋白"
    if has_animal_protein:
        return "动物蛋白"
    if has_plant_protein:
        return "植物蛋白"
    return None


def _build_meat_source_structure(animal_sources: Sequence[str]) -> Optional[str]:
    source_count = len(_unique_tokens(list(animal_sources)))
    if source_count == 1:
        return "单一肉源"
    if source_count >= 2:
        return "多肉源"
    return None


def _extract_token_ratio(ingredient_composition: Optional[str], target_token: str) -> Optional[float]:
    text_value = str(ingredient_composition or "").strip()
    target_norm = _normalize_token(target_token)
    if not text_value or not target_norm:
        return None
    for raw_part in SPLIT_RE.split(text_value):
        raw_token = _normalize_token_keep_percent(raw_part)
        normalized = _normalize_token(raw_token)
        if not normalized:
            continue
        if not _match_token(normalized, target_norm):
            continue
        pct = _extract_percent(raw_token)
        if pct is not None:
            return round(pct, 2)
    return None


def _is_primary_token(token: str, main_tokens: Sequence[str]) -> int:
    normalized = _normalize_token(token)
    if not normalized:
        return 0
    for main_token in main_tokens:
        if _match_token(normalized, str(main_token or "")):
            return 1
    return 0


def _find_ingredient_order_index(ingredient_name: str, composition_tokens: Sequence[str]) -> Optional[int]:
    target_norm = _normalize_token(ingredient_name)
    if not target_norm:
        return None

    for idx, token in enumerate(composition_tokens):
        token_norm = _normalize_token(token)
        if not token_norm:
            continue
        if token_norm == target_norm or _match_token(token_norm, target_norm) or _match_token(target_norm, token_norm):
            return idx
    return None


def _maybe_split_output_string(value: str) -> List[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    parts = SPLIT_RE.split(raw)
    if len(parts) <= 1:
        return [_normalize_output_token(raw)] if _normalize_output_token(raw) else []
    return [token for token in (_normalize_output_token(x) for x in parts) if token]


def _coerce_list(value: Any) -> List[str]:
    items: List[str] = []

    def _visit(obj: Any) -> None:
        if obj is None:
            return
        if isinstance(obj, (list, tuple, set)):
            for item in obj:
                _visit(item)
            return
        if isinstance(obj, dict):
            for key in ("items", "values", "ingredients", "list"):
                if key in obj:
                    _visit(obj.get(key))
                    return
            for item in obj.values():
                if isinstance(item, (str, list, tuple, set)):
                    _visit(item)
            return
        if isinstance(obj, (int, float)):
            items.append(str(obj))
            return
        if isinstance(obj, str):
            for token in _maybe_split_output_string(obj):
                items.append(token)

    _visit(value)
    return _unique_tokens(items)


def _coerce_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return round(float(value), 2)
    text_value = str(value).strip().replace("%", "")
    try:
        return round(float(text_value), 2)
    except Exception:
        return None


def _clean_note(value: Any) -> Optional[str]:
    text_value = str(value or "").strip()
    if not text_value:
        return None
    return re.sub(r"\s+", " ", text_value)


def _legacy_classify_token(token: str) -> List[str]:
    lower_token = token.lower()
    categories: List[str] = []

    if _contains_any(lower_token, PROBIOTICS_KEYWORDS):
        categories.append("probiotics")
    if _contains_any(lower_token, PREBIOTICS_KEYWORDS):
        categories.append("prebiotics")
    if (_contains_any(lower_token, FAT_SOURCE_KEYWORDS) or lower_token.endswith("油")) and not _contains_any(
        lower_token, FAT_SOURCE_EXCLUDE_KEYWORDS
    ):
        categories.append("fat_source")
    if _contains_any(lower_token, PLANT_PROTEIN_KEYWORDS):
        categories.append("plant_protein")
    if _contains_any(lower_token, ANIMAL_PROTEIN_KEYWORDS) and not _contains_any(lower_token, ANIMAL_PROTEIN_EXCLUDE_KEYWORDS):
        categories.append("animal_protein")
    if _contains_any(lower_token, FIBER_KEYWORDS):
        categories.append("fiber_source")
    if _contains_any(lower_token, NUTRITIONAL_ADDITIVE_KEYWORDS):
        categories.append("nutritional_additives")
    if _contains_any(lower_token, FUNCTIONAL_ADDITIVE_KEYWORDS):
        categories.append("functional_additives")
    if _contains_any(lower_token, RISKY_INGREDIENT_KEYWORDS):
        categories.append("risky_ingredients")
    if _contains_any(lower_token, UNCLEAR_INGREDIENT_KEYWORDS):
        categories.append("unclear_ingredients")

    if "plant_protein" not in categories:
        if _contains_any(lower_token, GRAIN_CARB_KEYWORDS):
            categories.append("grain_carbohydrates")
        if _contains_any(lower_token, LEGUME_CARB_KEYWORDS):
            categories.append("legume_carbohydrates")
        if _contains_any(lower_token, TUBER_CARB_KEYWORDS):
            categories.append("tuber_carbohydrates")
        if _contains_any(lower_token, OTHER_CARB_KEYWORDS):
            categories.append("other_carbohydrates")

    return categories


def _legacy_parse_ingredient_composition_types(ingredient_composition: Optional[str]) -> Dict[str, Any]:
    buckets: Dict[str, List[str]] = {key: [] for key in CATEGORY_FIELDS}
    ingredient_tokens = _split_ingredient_tokens(ingredient_composition)
    for token in ingredient_tokens:
        for category in _legacy_classify_token(token):
            if token not in buckets[category]:
                buckets[category].append(token)

    out: Dict[str, Any] = {key: _unique_tokens(values) for key, values in buckets.items()}
    out["animal_protein_ratio"] = _compute_animal_protein_ratio(ingredient_composition, out["animal_protein"])
    out["animal_protein_tag"] = _compute_animal_protein_tag(out["animal_protein"])
    out["carbohydrates"] = _unique_tokens(
        out["grain_carbohydrates"]
        + out["legume_carbohydrates"]
        + out["tuber_carbohydrates"]
        + out["other_carbohydrates"]
    )
    if not out["main_ingredients"]:
        out["main_ingredients"] = ingredient_tokens[:5]
    out["ingredient_notes"] = None
    return out


def parse_ingredient_composition_types(ingredient_composition: Optional[str]) -> Dict[str, Optional[Any]]:
    legacy = _legacy_parse_ingredient_composition_types(ingredient_composition)
    out: Dict[str, Optional[Any]] = {}
    for key in CATEGORY_FIELDS:
        out[key] = _join_tokens(legacy.get(key) or [])
    out["carbohydrates"] = _join_tokens(legacy.get("carbohydrates") or [])
    out["animal_protein_ratio"] = legacy.get("animal_protein_ratio")
    out["animal_protein_tag"] = legacy.get("animal_protein_tag")
    out["ingredient_notes"] = legacy.get("ingredient_notes")
    return out


def _normalize_base_url(base_url: str) -> Optional[str]:
    raw = str(base_url or "").strip()
    if not raw:
        return None
    raw = raw.rstrip("/")
    for suffix in ("/chat/completions", "/responses"):
        if raw.endswith(suffix):
            raw = raw[: -len(suffix)]
            break
    return raw or None


def _coerce_positive_float(value: Any, default: float) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if out > 0 else default


def _resolve_openai_cfg(openai_cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    cfg = dict(openai_cfg or {})
    cfg_api_key = str(cfg.get("api_key") or "").strip()
    cfg_base_url_raw = str(cfg.get("base_url") or "").strip()
    cfg_model = str(cfg.get("model") or "").strip()

    qwen_api_key = str(os.getenv("QWEN_API_KEY") or "").strip() or str(os.getenv("DASHSCOPE_API_KEY") or "").strip()
    qwen_base_url = _normalize_base_url(
        str(os.getenv("QWEN_BASE_URL") or "").strip()
        or str(os.getenv("QWEN_ENDPOINT") or "").strip()
        or "https://dashscope.aliyuncs.com/compatible-mode/v1"
    )
    qwen_model = str(os.getenv("QWEN_MODEL") or "").strip() or "qwen-turbo-latest"

    # If the project config did not pin a provider endpoint, prefer the local DashScope/Qwen env.
    use_qwen_env = bool(qwen_api_key and not cfg_base_url_raw)

    if use_qwen_env:
        api_key = qwen_api_key
        base_url = qwen_base_url
        model = qwen_model
    else:
        api_key = cfg_api_key or qwen_api_key
        base_url = _normalize_base_url(cfg_base_url_raw) or qwen_base_url
        model = cfg_model or qwen_model or "gpt-4.1-mini"

    timeout_seconds = _coerce_positive_float(
        cfg.get("timeout_seconds")
        or os.getenv("OPENAI_TIMEOUT_SECONDS")
        or os.getenv("QWEN_TIMEOUT_SECONDS")
        or 45,
        45.0,
    )
    return {
        "api_key": api_key or None,
        "base_url": base_url,
        "model": model,
        "timeout_seconds": timeout_seconds,
    }


def _prefer_chat_completions(base_url: Optional[str], model: str) -> bool:
    base = str(base_url or "").lower()
    model_l = str(model or "").lower()
    return ("dashscope" in base) or model_l.startswith("qwen")


def _make_client(openai_cfg: Optional[Dict[str, Any]]) -> tuple[OpenAI, str, bool]:
    resolved = _resolve_openai_cfg(openai_cfg)
    api_key = resolved.get("api_key")
    if not api_key:
        raise ValueError("openai.api_key 为空；如使用千问兼容接口，也可设置 QWEN_API_KEY / DASHSCOPE_API_KEY")
    base_url = resolved.get("base_url")
    model = str(resolved.get("model") or "gpt-4.1-mini")
    timeout_seconds = _coerce_positive_float(resolved.get("timeout_seconds"), 45.0)
    prefer_chat = _prefer_chat_completions(base_url, model)
    client_kwargs: Dict[str, Any] = {
        "api_key": api_key,
        "timeout": timeout_seconds,
        "max_retries": 0,
    }
    if base_url:
        client_kwargs["base_url"] = base_url
    client = OpenAI(**client_kwargs)
    return client, model, prefer_chat


def _get_thread_client(openai_cfg: Optional[Dict[str, Any]]) -> tuple[OpenAI, str, bool]:
    resolved = _resolve_openai_cfg(openai_cfg)
    signature = (
        resolved.get("api_key"),
        resolved.get("base_url"),
        resolved.get("model"),
    )
    cached_sig = getattr(_THREAD_LOCAL, "client_signature", None)
    cached_bundle = getattr(_THREAD_LOCAL, "client_bundle", None)
    if cached_sig == signature and cached_bundle is not None:
        return cached_bundle
    bundle = _make_client(resolved)
    _THREAD_LOCAL.client_signature = signature
    _THREAD_LOCAL.client_bundle = bundle
    return bundle


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=20))
def _call_model(
    client: OpenAI,
    model: str,
    messages: List[Dict[str, str]],
    temperature: int = 0,
    prefer_chat_completions: bool = False,
) -> Dict[str, Any]:
    start = now_ms()
    if prefer_chat_completions:
        comp = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            response_format={"type": "json_object"},
        )
        out_text = comp.choices[0].message.content or "{}"
        return {"text": out_text, "latency_ms": now_ms() - start}
    try:
        resp = client.responses.create(
            model=model,
            input=messages,
            temperature=temperature,
            text={"format": {"type": "json_object"}},
        )
        out_text = getattr(resp, "output_text", None)
        if not out_text:
            out_text = ""
            for item in getattr(resp, "output", []) or []:
                for content in getattr(item, "content", []) or []:
                    if getattr(content, "type", "") == "output_text":
                        out_text += getattr(content, "text", "")
        return {"text": out_text or "{}", "latency_ms": now_ms() - start}
    except Exception:
        comp = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            response_format={"type": "json_object"},
        )
        out_text = comp.choices[0].message.content or "{}"
        return {"text": out_text, "latency_ms": now_ms() - start}


def _load_json_object(text_value: str) -> Dict[str, Any]:
    raw = str(text_value or "").strip()
    if not raw:
        return {}
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
    try:
        obj = json.loads(raw)
    except Exception:
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            obj = json.loads(raw[start : end + 1])
        else:
            raise
    if not isinstance(obj, dict):
        raise ValueError("model output is not a JSON object")
    return obj


def _prompt_schema_example() -> Dict[str, Any]:
    return {
        "animal_protein": [],
        "plant_protein": [],
        "grain_carbohydrates": [],
        "legume_carbohydrates": [],
        "tuber_carbohydrates": [],
        "other_carbohydrates": [],
        "fat_source": [],
        "fiber_source": [],
        "prebiotics": [],
        "probiotics": [],
        "nutritional_additives": [],
        "functional_additives": [],
        "risky_ingredients": [],
        "unclear_ingredients": [],
        "main_ingredients": [],
        "animal_protein_ratio": None,
        "animal_protein_tag": None,
        "ingredient_notes": "",
    }


def _build_messages(ingredient_composition: str) -> List[Dict[str, str]]:
    tokens = _split_ingredient_tokens(ingredient_composition)
    schema_text = json.dumps(_prompt_schema_example(), ensure_ascii=False, indent=2)
    user_prompt = (
        "请把下面这段猫粮原料组成做结构化拆分，只返回 JSON 对象。\n\n"
        "要求：\n"
        "1. 只能依据原文出现的成分判断，不要臆造。\n"
        "2. 数组元素尽量保留原文成分片段，去掉无关标点即可。\n"
        "3. `plant_protein` 只放植物蛋白提取物或浓缩蛋白，如豌豆蛋白、马铃薯蛋白。\n"
        "4. `grain_carbohydrates` 放谷物碳水，`legume_carbohydrates` 放豆类碳水，`tuber_carbohydrates` 放薯类碳水，"
        "`other_carbohydrates` 放其余果蔬/淀粉类碳水。\n"
        "5. `fat_source` 放油脂来源；如果来源模糊，如“动物脂肪”“禽脂”，可同时放入 `unclear_ingredients` 或 `risky_ingredients`。\n"
        "6. `fiber_source` 放甜菜粕、纤维素、车前子等纤维来源。\n"
        "7. `prebiotics` 放菊粉/FOS/MOS/菊苣根等，`probiotics` 放菌株或发酵培养物。\n"
        "8. `nutritional_additives` 放牛磺酸、维生素、矿物质、胆碱、蛋氨酸、赖氨酸等营养添加项。\n"
        "9. `functional_additives` 放丝兰、迷迭香、叶黄素、葡萄糖胺、软骨素等功能性添加项。\n"
        "10. `risky_ingredients` 放争议或低质量成分，如副产品、诱食剂、香精、BHA/BHT、卡拉胶等。\n"
        "11. `main_ingredients` 输出按配料顺序前 3-5 个主要原料。\n"
        "12. `animal_protein_ratio` 只有在原文明确给出动物蛋白百分比时才填写数字，否则为 null。\n"
        "13. `animal_protein_tag` 只允许使用这些值的组合：禽类、鱼类、红肉、蛋类，用 `+` 连接；无法判断则 null。\n"
        "14. `ingredient_notes` 只写一句简短备注；没有明显歧义就返回空字符串。\n\n"
        f"输出 JSON schema:\n{schema_text}\n\n"
        f"原料组成原文：\n{ingredient_composition}\n\n"
        f"分词参考：\n{json.dumps(tokens, ensure_ascii=False)}"
    )
    return [
        {
            "role": "system",
            "content": "你是猫粮配料分析师，只返回合法 JSON，不要写解释、Markdown 或代码块。",
        },
        {
            "role": "user",
            "content": user_prompt,
        },
    ]


def _normalize_llm_result(
    ingredient_composition: str,
    llm_raw: Dict[str, Any],
    legacy: Dict[str, Any],
    llm_error: Optional[str] = None,
) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    for field in ARRAY_FIELDS_FOR_PROMPT:
        normalized[field] = _coerce_list(llm_raw.get(field))

    for alias, target in RAW_KEY_ALIASES.items():
        if target in normalized and normalized[target]:
            continue
        if alias in llm_raw:
            normalized[target] = _coerce_list(llm_raw.get(alias))

    if not normalized["other_carbohydrates"] and llm_raw.get("carbohydrates") is not None:
        normalized["other_carbohydrates"] = _coerce_list(llm_raw.get("carbohydrates"))

    for field in CATEGORY_FIELDS:
        normalized[field] = _unique_tokens((normalized.get(field) or []) + (legacy.get(field) or []))

    normalized["carbohydrates"] = _unique_tokens(
        normalized["grain_carbohydrates"]
        + normalized["legume_carbohydrates"]
        + normalized["tuber_carbohydrates"]
        + normalized["other_carbohydrates"]
    )

    ratio = _coerce_float(llm_raw.get("animal_protein_ratio"))
    if ratio is None:
        ratio = legacy.get("animal_protein_ratio")
    if ratio is None:
        ratio = _compute_animal_protein_ratio(ingredient_composition, normalized["animal_protein"])
    normalized["animal_protein_ratio"] = ratio

    tag = _clean_note(llm_raw.get("animal_protein_tag"))
    if tag:
        tag = tag.replace("、", "+").replace("/", "+").replace(" ", "")
    if not tag:
        tag = legacy.get("animal_protein_tag")
    if not tag:
        tag = _compute_animal_protein_tag(normalized["animal_protein"])
    normalized["animal_protein_tag"] = tag

    if not normalized["main_ingredients"]:
        normalized["main_ingredients"] = (legacy.get("main_ingredients") or [])[:5]

    notes: List[str] = []
    raw_note = _clean_note(llm_raw.get("ingredient_notes"))
    if raw_note:
        notes.append(raw_note)
    if llm_error:
        notes.append(f"LLM调用失败，当前结果已用规则补齐：{llm_error}")
    normalized["ingredient_notes"] = "；".join(_unique_tokens(notes)) if notes else None

    return normalized


def _classify_ingredient_composition(
    ingredient_composition: str,
    openai_cfg: Optional[Dict[str, Any]],
) -> CompositionClassificationResult:
    legacy = _legacy_parse_ingredient_composition_types(ingredient_composition)
    llm_error: Optional[str] = None
    llm_latency_ms = 0
    llm_raw: Dict[str, Any] = {}

    client, model, prefer_chat_completions = _get_thread_client(openai_cfg)
    try:
        messages = _build_messages(ingredient_composition)
        model_result = _call_model(
            client,
            model=model,
            messages=messages,
            temperature=0,
            prefer_chat_completions=prefer_chat_completions,
        )
        llm_latency_ms = int(model_result.get("latency_ms") or 0)
        llm_raw = _load_json_object(str(model_result.get("text") or "{}"))
    except Exception as exc:
        llm_error = str(exc)

    normalized = _normalize_llm_result(
        ingredient_composition=ingredient_composition,
        llm_raw=llm_raw,
        legacy=legacy,
        llm_error=llm_error,
    )
    model_json = {
        "llm_output": llm_raw,
        "normalized": normalized,
        "legacy_fallback": legacy,
        "llm_error": llm_error,
    }
    return CompositionClassificationResult(
        normalized=normalized,
        model_json_text=safe_json_dumps(model_json),
        model_name=model,
        model_latency_ms=llm_latency_ms,
    )


def _build_summary_upsert_sql(summary_table: str) -> str:
    return f"""
    INSERT INTO `{summary_table}`(
      source_id, parsed_row_id, image_name, brand, product_name, ingredient_composition,
      animal_protein, animal_protein_ratio, animal_protein_tag, plant_protein,
      grain_carbohydrates, legume_carbohydrates, tuber_carbohydrates, other_carbohydrates, carbohydrates,
      fat_source, fiber_source, prebiotics, probiotics,
      nutritional_additives, functional_additives, risky_ingredients, unclear_ingredients,
      main_ingredients, ingredient_notes,
      protein_feature_count, fiber_carb_feature_count, biotic_feature_count,
      protein_features_json, fiber_carb_features_json, biotic_features_json, feature_summary_json,
      model_json, model_name, model_latency_ms, prompt_version, parse_batch_id, parse_ts
    )
    VALUES(
      :source_id, :parsed_row_id, :image_name, :brand, :product_name, :ingredient_composition,
      :animal_protein, :animal_protein_ratio, :animal_protein_tag, :plant_protein,
      :grain_carbohydrates, :legume_carbohydrates, :tuber_carbohydrates, :other_carbohydrates, :carbohydrates,
      :fat_source, :fiber_source, :prebiotics, :probiotics,
      :nutritional_additives, :functional_additives, :risky_ingredients, :unclear_ingredients,
      :main_ingredients, :ingredient_notes,
      :protein_feature_count, :fiber_carb_feature_count, :biotic_feature_count,
      :protein_features_json, :fiber_carb_features_json, :biotic_features_json, :feature_summary_json,
      :model_json, :model_name, :model_latency_ms, :prompt_version, :parse_batch_id, NOW()
    )
    ON DUPLICATE KEY UPDATE
      parsed_row_id=VALUES(parsed_row_id),
      image_name=VALUES(image_name),
      brand=VALUES(brand),
      product_name=VALUES(product_name),
      ingredient_composition=VALUES(ingredient_composition),
      animal_protein=VALUES(animal_protein),
      animal_protein_ratio=VALUES(animal_protein_ratio),
      animal_protein_tag=VALUES(animal_protein_tag),
      plant_protein=VALUES(plant_protein),
      grain_carbohydrates=VALUES(grain_carbohydrates),
      legume_carbohydrates=VALUES(legume_carbohydrates),
      tuber_carbohydrates=VALUES(tuber_carbohydrates),
      other_carbohydrates=VALUES(other_carbohydrates),
      carbohydrates=VALUES(carbohydrates),
      fat_source=VALUES(fat_source),
      fiber_source=VALUES(fiber_source),
      prebiotics=VALUES(prebiotics),
      probiotics=VALUES(probiotics),
      nutritional_additives=VALUES(nutritional_additives),
      functional_additives=VALUES(functional_additives),
      risky_ingredients=VALUES(risky_ingredients),
      unclear_ingredients=VALUES(unclear_ingredients),
      main_ingredients=VALUES(main_ingredients),
      ingredient_notes=VALUES(ingredient_notes),
      protein_feature_count=VALUES(protein_feature_count),
      fiber_carb_feature_count=VALUES(fiber_carb_feature_count),
      biotic_feature_count=VALUES(biotic_feature_count),
      protein_features_json=VALUES(protein_features_json),
      fiber_carb_features_json=VALUES(fiber_carb_features_json),
      biotic_features_json=VALUES(biotic_features_json),
      feature_summary_json=VALUES(feature_summary_json),
      model_json=VALUES(model_json),
      model_name=VALUES(model_name),
      model_latency_ms=VALUES(model_latency_ms),
      prompt_version=VALUES(prompt_version),
      parse_batch_id=VALUES(parse_batch_id),
      parse_ts=NOW()
    """


def _build_protein_insert_sql(protein_table: str) -> str:
    return f"""
    INSERT INTO `{protein_table}`(
      product_key, source_id, parsed_row_id, image_name, brand, product_name, ingredient_composition,
      protein_structure, energy_fat, protein_content_pct,
      main_animal_source, primary_meat_source_species, primary_meat_source_type,
      secondary_meat_source_species, secondary_meat_source_type,
      protein_source_origin, meat_source_structure,
      animal_source_count, novel_protein,
      energy_kcal_per_100g, fat_pct, oil_source_count,
      oil_sources, animal_sources, protein_source_details, plant_protein_labels, protein_label_json,
      model_name, prompt_version, parse_batch_id, parse_ts
    )
    VALUES(
      :product_key, :source_id, :parsed_row_id, :image_name, :brand, :product_name, :ingredient_composition,
      :protein_structure, :energy_fat, :protein_content_pct,
      :main_animal_source, :primary_meat_source_species, :primary_meat_source_type,
      :secondary_meat_source_species, :secondary_meat_source_type,
      :protein_source_origin, :meat_source_structure,
      :animal_source_count, :novel_protein,
      :energy_kcal_per_100g, :fat_pct, :oil_source_count,
      :oil_sources, :animal_sources, :protein_source_details, :plant_protein_labels, :protein_label_json,
      :model_name, :prompt_version, :parse_batch_id, NOW()
    )
    ON DUPLICATE KEY UPDATE
      source_id=VALUES(source_id),
      parsed_row_id=VALUES(parsed_row_id),
      image_name=VALUES(image_name),
      brand=VALUES(brand),
      product_name=VALUES(product_name),
      ingredient_composition=VALUES(ingredient_composition),
      protein_structure=VALUES(protein_structure),
      energy_fat=VALUES(energy_fat),
      protein_content_pct=VALUES(protein_content_pct),
      main_animal_source=VALUES(main_animal_source),
      primary_meat_source_species=VALUES(primary_meat_source_species),
      primary_meat_source_type=VALUES(primary_meat_source_type),
      secondary_meat_source_species=VALUES(secondary_meat_source_species),
      secondary_meat_source_type=VALUES(secondary_meat_source_type),
      protein_source_origin=VALUES(protein_source_origin),
      meat_source_structure=VALUES(meat_source_structure),
      animal_source_count=VALUES(animal_source_count),
      novel_protein=VALUES(novel_protein),
      energy_kcal_per_100g=VALUES(energy_kcal_per_100g),
      fat_pct=VALUES(fat_pct),
      oil_source_count=VALUES(oil_source_count),
      oil_sources=VALUES(oil_sources),
      animal_sources=VALUES(animal_sources),
      protein_source_details=VALUES(protein_source_details),
      plant_protein_labels=VALUES(plant_protein_labels),
      protein_label_json=VALUES(protein_label_json),
      model_name=VALUES(model_name),
      prompt_version=VALUES(prompt_version),
      parse_batch_id=VALUES(parse_batch_id),
      parse_ts=NOW()
    """


def _build_fiber_carb_insert_sql(fiber_carb_table: str) -> str:
    return f"""
    INSERT INTO `{fiber_carb_table}`(
      product_key, source_id, parsed_row_id, image_name, brand, product_name, ingredient_composition,
      carb_type, carb_details, fiber_source_details,
      grain_carbohydrates, legume_carbohydrates, tuber_carbohydrates, other_carbohydrates,
      fiber_carb_label_json,
      model_name, prompt_version, parse_batch_id, parse_ts
    )
    VALUES(
      :product_key, :source_id, :parsed_row_id, :image_name, :brand, :product_name, :ingredient_composition,
      :carb_type, :carb_details, :fiber_source_details,
      :grain_carbohydrates, :legume_carbohydrates, :tuber_carbohydrates, :other_carbohydrates,
      :fiber_carb_label_json,
      :model_name, :prompt_version, :parse_batch_id, NOW()
    )
    ON DUPLICATE KEY UPDATE
      source_id=VALUES(source_id),
      parsed_row_id=VALUES(parsed_row_id),
      image_name=VALUES(image_name),
      brand=VALUES(brand),
      product_name=VALUES(product_name),
      ingredient_composition=VALUES(ingredient_composition),
      carb_type=VALUES(carb_type),
      carb_details=VALUES(carb_details),
      fiber_source_details=VALUES(fiber_source_details),
      grain_carbohydrates=VALUES(grain_carbohydrates),
      legume_carbohydrates=VALUES(legume_carbohydrates),
      tuber_carbohydrates=VALUES(tuber_carbohydrates),
      other_carbohydrates=VALUES(other_carbohydrates),
      fiber_carb_label_json=VALUES(fiber_carb_label_json),
      model_name=VALUES(model_name),
      prompt_version=VALUES(prompt_version),
      parse_batch_id=VALUES(parse_batch_id),
      parse_ts=NOW()
    """


def _build_biotic_insert_sql(biotic_table: str) -> str:
    return f"""
    INSERT INTO `{biotic_table}`(
      product_key,
      source_id, parsed_row_id, image_name, brand, product_name, ingredient_composition,
      biotic_structure, biotic_type, prebiotic_details, probiotic_details, biotic_label_json,
      model_name, prompt_version, parse_batch_id, parse_ts
    )
    VALUES(
      :product_key,
      :source_id, :parsed_row_id, :image_name, :brand, :product_name, :ingredient_composition,
      :biotic_structure, :biotic_type, :prebiotic_details, :probiotic_details, :biotic_label_json,
      :model_name, :prompt_version, :parse_batch_id, NOW()
    )
    ON DUPLICATE KEY UPDATE
      source_id=VALUES(source_id),
      parsed_row_id=VALUES(parsed_row_id),
      image_name=VALUES(image_name),
      brand=VALUES(brand),
      product_name=VALUES(product_name),
      ingredient_composition=VALUES(ingredient_composition),
      biotic_structure=VALUES(biotic_structure),
      biotic_type=VALUES(biotic_type),
      prebiotic_details=VALUES(prebiotic_details),
      probiotic_details=VALUES(probiotic_details),
      biotic_label_json=VALUES(biotic_label_json),
      model_name=VALUES(model_name),
      prompt_version=VALUES(prompt_version),
      parse_batch_id=VALUES(parse_batch_id),
      parse_ts=NOW()
    """


def _build_protein_detail_rows(
    row: Dict[str, Any],
    ingredient_composition: str,
    classified: CompositionClassificationResult,
    batch_id: str,
) -> List[Dict[str, Any]]:
    normalized = classified.normalized
    main_tokens = normalized.get("main_ingredients") or []
    cleaned_ingredient_composition = _normalize_ingredient_composition_for_storage(ingredient_composition)
    composition_tokens = _split_ingredient_tokens(cleaned_ingredient_composition)
    protein_tokens_by_type: Dict[str, List[str]] = {}
    for protein_type in ("animal_protein", "plant_protein"):
        refined_tokens = _refine_feature_tokens_by_composition(
            cleaned_ingredient_composition,
            normalized.get(protein_type) or [],
            protein_type,
        )
        protein_tokens_by_type[protein_type] = refined_tokens or _unique_tokens(normalized.get(protein_type) or [])
    rows: List[Dict[str, Any]] = []
    for protein_type in ("animal_protein", "plant_protein"):
        for ingredient_name in protein_tokens_by_type.get(protein_type) or []:
            ingredient_name_text = _normalize_output_token(ingredient_name)
            rows.append(
                {
                    "source_id": int(row["source_id"]),
                    "parsed_row_id": int(row["id"]),
                    "image_name": str(row.get("image_name") or "") or None,
                    "brand": str(row.get("brand") or "") or None,
                    "product_name": str(row.get("product_name") or "") or None,
                    "ingredient_composition": cleaned_ingredient_composition,
                    "ingredient_name": ingredient_name_text,
                    "protein_type": protein_type,
                    "animal_group": _infer_animal_group(ingredient_name_text) if protein_type == "animal_protein" else None,
                    "source_species": (
                        _canonicalize_animal_sources([ingredient_name_text])
                        if protein_type == "animal_protein"
                        else None
                    ),
                    "protein_source_type": (
                        _infer_animal_protein_source_type(ingredient_name_text)
                        if protein_type == "animal_protein"
                        else None
                    ),
                    "ingredient_ratio_percent": _extract_token_ratio(cleaned_ingredient_composition, ingredient_name_text),
                    "ingredient_order": _find_ingredient_order_index(ingredient_name_text, composition_tokens),
                    "is_primary": _is_primary_token(ingredient_name_text, main_tokens),
                    "model_name": classified.model_name,
                    "prompt_version": PROMPT_VERSION,
                    "parse_batch_id": batch_id,
                }
            )
    return rows


def _build_protein_label_row(
    row: Dict[str, Any],
    ingredient_composition: str,
    classified: CompositionClassificationResult,
    batch_id: str,
    protein_feature_items: List[Dict[str, Any]],
) -> Dict[str, Any]:
    normalized = classified.normalized
    cleaned_ingredient_composition = _normalize_ingredient_composition_for_storage(ingredient_composition)
    animal_tokens = _refine_feature_tokens_by_composition(
        cleaned_ingredient_composition,
        normalized.get("animal_protein") or [],
        "animal_protein",
    ) or _unique_tokens(normalized.get("animal_protein") or [])
    plant_protein_tokens = _refine_feature_tokens_by_composition(
        cleaned_ingredient_composition,
        normalized.get("plant_protein") or [],
        "plant_protein",
    ) or _unique_tokens(normalized.get("plant_protein") or [])
    oil_sources = _build_clean_oil_sources(normalized.get("fat_source") or [])
    primary_animal_tokens = [
        str(item.get("ingredient_name") or "")
        for item in protein_feature_items
        if str(item.get("protein_type") or "") == "animal_protein"
        and (
            int(item.get("is_primary") or 0) == 1
            or float(item.get("ingredient_ratio_percent") or 0) >= 3
        )
    ]
    source_candidates = _build_protein_source_candidates(protein_feature_items)
    all_animal_sources = _unique_tokens([str(item.get("species") or "") for item in source_candidates]) or _canonicalize_animal_sources(animal_tokens)

    primary_ratio_candidates = _select_ratio_source_candidates(source_candidates, min_ratio=10)
    if primary_ratio_candidates:
        primary_meat_source_species, primary_meat_source_type = _extract_protein_source_labels(primary_ratio_candidates)
    else:
        primary_group_candidates = _select_species_group_candidates(source_candidates, rank=1)
        primary_meat_source_species, primary_meat_source_type = _extract_protein_source_labels(primary_group_candidates)

    secondary_ratio_candidates = _select_ratio_source_candidates(
        source_candidates,
        min_ratio=3,
        max_ratio=8,
    )
    if secondary_ratio_candidates:
        secondary_meat_source_species, secondary_meat_source_type = _extract_protein_source_labels(secondary_ratio_candidates)
    else:
        secondary_group_candidates = _select_species_group_candidates(
            source_candidates,
            rank=1,
            exclude_species=primary_meat_source_species,
        )
        secondary_meat_source_species, secondary_meat_source_type = _extract_protein_source_labels(secondary_group_candidates)

    legacy_animal_sources = primary_meat_source_species or _canonicalize_animal_sources(primary_animal_tokens or animal_tokens)
    protein_structure, novel_protein, _ = _compute_protein_structure(all_animal_sources)
    protein_source_origin = _build_protein_source_origin(bool(animal_tokens), bool(plant_protein_tokens))
    meat_source_structure = _build_meat_source_structure(all_animal_sources)
    energy_kcal_per_100g = None
    protein_content_pct = None
    fat_pct = None
    energy_fat = None
    product_key = _build_product_key(row.get("brand"), row.get("product_name"), row.get("source_id"))
    protein_source_details = _build_protein_source_details(protein_feature_items)

    protein_label_json = {
        "protein_feature_items": protein_feature_items,
        "animal_tokens": animal_tokens,
        "plant_protein_tokens": plant_protein_tokens,
        "primary_animal_tokens": primary_animal_tokens,
        "protein_source_candidates": source_candidates,
        "animal_sources": legacy_animal_sources,
        "all_animal_sources": all_animal_sources,
        "primary_meat_source_species": primary_meat_source_species,
        "primary_meat_source_type": primary_meat_source_type,
        "secondary_meat_source_species": secondary_meat_source_species,
        "secondary_meat_source_type": secondary_meat_source_type,
        "protein_source_origin": protein_source_origin,
        "meat_source_structure": meat_source_structure,
        "oil_sources": oil_sources,
        "protein_source_details": protein_source_details,
        "protein_structure": protein_structure,
        "energy_fat": energy_fat,
        "protein_content_pct": protein_content_pct,
        "fat_pct": fat_pct,
        "energy_kcal_per_100g": energy_kcal_per_100g,
        "multi_value_separator": PIPE_VALUE_SEPARATOR,
    }
    return {
        "product_key": product_key,
        "source_id": int(row["source_id"]),
        "parsed_row_id": int(row["id"]),
        "image_name": str(row.get("image_name") or "") or None,
        "brand": _normalize_product_text(row.get("brand")) or None,
        "product_name": _normalize_product_text(row.get("product_name")) or None,
        "ingredient_composition": cleaned_ingredient_composition,
        "protein_structure": protein_structure,
        "energy_fat": energy_fat,
        "protein_content_pct": protein_content_pct,
        "main_animal_source": primary_meat_source_species[0] if primary_meat_source_species else (all_animal_sources[0] if all_animal_sources else None),
        "primary_meat_source_species": _join_pipe_tokens(primary_meat_source_species),
        "primary_meat_source_type": _join_pipe_tokens(primary_meat_source_type),
        "secondary_meat_source_species": _join_pipe_tokens(secondary_meat_source_species),
        "secondary_meat_source_type": _join_pipe_tokens(secondary_meat_source_type),
        "protein_source_origin": protein_source_origin,
        "meat_source_structure": meat_source_structure,
        "animal_source_count": len(all_animal_sources) or None,
        "novel_protein": novel_protein,
        "energy_kcal_per_100g": energy_kcal_per_100g,
        "fat_pct": fat_pct,
        "oil_source_count": len(oil_sources) or None,
        "oil_sources": _join_tokens(oil_sources),
        "animal_sources": _join_tokens(all_animal_sources),
        "protein_source_details": protein_source_details,
        "plant_protein_labels": _join_pipe_tokens(plant_protein_tokens),
        "protein_label_json": safe_json_dumps(protein_label_json),
        "model_name": classified.model_name,
        "prompt_version": PROMPT_VERSION,
        "parse_batch_id": batch_id,
    }


def _build_fiber_carb_detail_rows(
    row: Dict[str, Any],
    ingredient_composition: str,
    classified: CompositionClassificationResult,
    batch_id: str,
) -> List[Dict[str, Any]]:
    normalized = classified.normalized
    main_tokens = normalized.get("main_ingredients") or []
    rows: List[Dict[str, Any]] = []
    for feature_type in (
        "fiber_source",
        "grain_carbohydrates",
        "legume_carbohydrates",
        "tuber_carbohydrates",
        "other_carbohydrates",
    ):
        for ingredient_name in normalized.get(feature_type) or []:
            rows.append(
                {
                    "source_id": int(row["source_id"]),
                    "parsed_row_id": int(row["id"]),
                    "image_name": str(row.get("image_name") or "") or None,
                    "brand": str(row.get("brand") or "") or None,
                    "product_name": str(row.get("product_name") or "") or None,
                    "ingredient_composition": ingredient_composition or None,
                    "ingredient_name": _normalize_output_token(ingredient_name),
                    "feature_type": feature_type,
                    "is_primary": _is_primary_token(str(ingredient_name), main_tokens),
                    "model_name": classified.model_name,
                    "prompt_version": PROMPT_VERSION,
                    "parse_batch_id": batch_id,
                }
            )
    return rows


def _build_carb_type(
    grain_tokens: Sequence[str],
    legume_tokens: Sequence[str],
    tuber_tokens: Sequence[str],
    other_tokens: Sequence[str],
) -> Optional[str]:
    labels: List[str] = []
    has_grain = bool(grain_tokens)
    has_any_carb = bool(grain_tokens or legume_tokens or tuber_tokens or other_tokens)
    if grain_tokens:
        labels.append("谷物碳水")
    if legume_tokens:
        labels.append("豆类碳水")
    if tuber_tokens:
        labels.append("薯类碳水")
    if has_any_carb and not has_grain:
        labels.append("无谷")
    return "、".join(labels) if labels else None


def _build_grouped_feature_details(groups: Sequence[tuple[str, Sequence[str]]]) -> Optional[str]:
    parts: List[str] = []
    for label, values in groups:
        cleaned = _unique_tokens(list(values))
        if cleaned:
            parts.append(f"{label}：{'、'.join(cleaned)}")
    return "；".join(parts) if parts else None


def _build_fiber_carb_label_row(
    row: Dict[str, Any],
    ingredient_composition: str,
    classified: CompositionClassificationResult,
    batch_id: str,
    fiber_carb_feature_items: List[Dict[str, Any]],
) -> Dict[str, Any]:
    normalized = classified.normalized
    cleaned_ingredient_composition = _normalize_ingredient_composition_for_storage(ingredient_composition)
    grain_tokens = _refine_feature_tokens_by_composition(
        cleaned_ingredient_composition,
        normalized.get("grain_carbohydrates") or [],
        "grain_carbohydrates",
    )
    legume_tokens = _refine_feature_tokens_by_composition(
        cleaned_ingredient_composition,
        normalized.get("legume_carbohydrates") or [],
        "legume_carbohydrates",
    )
    tuber_tokens = _refine_feature_tokens_by_composition(
        cleaned_ingredient_composition,
        normalized.get("tuber_carbohydrates") or [],
        "tuber_carbohydrates",
    )
    other_tokens = _refine_feature_tokens_by_composition(
        cleaned_ingredient_composition,
        normalized.get("other_carbohydrates") or [],
        "other_carbohydrates",
    )
    fiber_tokens = _refine_feature_tokens_by_composition(
        cleaned_ingredient_composition,
        normalized.get("fiber_source") or [],
        "fiber_source",
    )
    carb_type = _build_carb_type(grain_tokens, legume_tokens, tuber_tokens, other_tokens)
    carb_details = _build_grouped_feature_details(
        (
            ("谷物碳水", grain_tokens),
            ("豆类碳水", legume_tokens),
            ("薯类碳水", tuber_tokens),
            ("其他碳水", other_tokens),
        )
    )
    fiber_source_details = "、".join(fiber_tokens) if fiber_tokens else None
    product_key = _build_product_key(row.get("brand"), row.get("product_name"), row.get("source_id"))
    fiber_carb_label_json = {
        "fiber_carb_feature_items": fiber_carb_feature_items,
        "grain_carbohydrates": grain_tokens,
        "legume_carbohydrates": legume_tokens,
        "tuber_carbohydrates": tuber_tokens,
        "other_carbohydrates": other_tokens,
        "fiber_source": fiber_tokens,
        "carb_type": carb_type,
        "carb_details": carb_details,
        "fiber_source_details": fiber_source_details,
    }
    return {
        "product_key": product_key,
        "source_id": int(row["source_id"]),
        "parsed_row_id": int(row["id"]),
        "image_name": str(row.get("image_name") or "") or None,
        "brand": _normalize_product_text(row.get("brand")) or None,
        "product_name": _normalize_product_text(row.get("product_name")) or None,
        "ingredient_composition": cleaned_ingredient_composition,
        "carb_type": carb_type,
        "carb_details": carb_details,
        "fiber_source_details": fiber_source_details,
        "grain_carbohydrates": _join_tokens(grain_tokens),
        "legume_carbohydrates": _join_tokens(legume_tokens),
        "tuber_carbohydrates": _join_tokens(tuber_tokens),
        "other_carbohydrates": _join_tokens(other_tokens),
        "fiber_carb_label_json": safe_json_dumps(fiber_carb_label_json),
        "model_name": classified.model_name,
        "prompt_version": PROMPT_VERSION,
        "parse_batch_id": batch_id,
    }


def _build_biotic_detail_rows(
    row: Dict[str, Any],
    ingredient_composition: str,
    classified: CompositionClassificationResult,
    batch_id: str,
) -> List[Dict[str, Any]]:
    normalized = classified.normalized
    rows: List[Dict[str, Any]] = []
    for biotic_type, field_name in (("prebiotic", "prebiotics"), ("probiotic", "probiotics")):
        for ingredient_name in normalized.get(field_name) or []:
            rows.append(
                {
                    "source_id": int(row["source_id"]),
                    "parsed_row_id": int(row["id"]),
                    "image_name": str(row.get("image_name") or "") or None,
                    "brand": str(row.get("brand") or "") or None,
                    "product_name": str(row.get("product_name") or "") or None,
                    "ingredient_composition": ingredient_composition or None,
                    "ingredient_name": _normalize_output_token(ingredient_name),
                    "biotic_type": biotic_type,
                    "model_name": classified.model_name,
                    "prompt_version": PROMPT_VERSION,
                    "parse_batch_id": batch_id,
                }
            )
    return rows


def _build_biotic_structure(prebiotic_tokens: Sequence[str]) -> Optional[str]:
    prebiotic_count = len(_unique_tokens(list(prebiotic_tokens)))
    if prebiotic_count == 1:
        return "单一益生元"
    if prebiotic_count >= 2:
        return "复合益生元"
    return None


def _build_biotic_type(
    prebiotic_tokens: Sequence[str],
    probiotic_tokens: Sequence[str],
) -> Optional[str]:
    labels: List[str] = []
    if probiotic_tokens:
        labels.append("肠道菌群优化")
    if prebiotic_tokens:
        labels.append("肠道调节")
    return "、".join(labels) if labels else None


def _build_biotic_label_row(
    row: Dict[str, Any],
    ingredient_composition: str,
    classified: CompositionClassificationResult,
    batch_id: str,
    biotic_feature_items: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    normalized = classified.normalized
    cleaned_ingredient_composition = _normalize_ingredient_composition_for_storage(ingredient_composition)
    prebiotic_tokens = _refine_feature_tokens_by_composition(
        cleaned_ingredient_composition,
        normalized.get("prebiotics") or [],
        "prebiotics",
    )
    probiotic_tokens = _refine_feature_tokens_by_composition(
        cleaned_ingredient_composition,
        normalized.get("probiotics") or [],
        "probiotics",
    )
    if not prebiotic_tokens and not probiotic_tokens:
        return None

    biotic_structure = _build_biotic_structure(prebiotic_tokens)
    biotic_type = _build_biotic_type(prebiotic_tokens, probiotic_tokens)
    product_key = _build_product_key(row.get("brand"), row.get("product_name"), row.get("source_id"))
    biotic_label_json = {
        "biotic_feature_items": biotic_feature_items,
        "prebiotics": prebiotic_tokens,
        "probiotics": probiotic_tokens,
        "biotic_structure": biotic_structure,
        "biotic_type": biotic_type,
        "prebiotic_details": _join_tokens(prebiotic_tokens),
        "probiotic_details": _join_tokens(probiotic_tokens),
    }
    return {
        "product_key": product_key,
        "source_id": int(row["source_id"]),
        "parsed_row_id": int(row["id"]),
        "image_name": str(row.get("image_name") or "") or None,
        "brand": _normalize_product_text(row.get("brand")) or None,
        "product_name": _normalize_product_text(row.get("product_name")) or None,
        "ingredient_composition": cleaned_ingredient_composition,
        "biotic_structure": biotic_structure,
        "biotic_type": biotic_type,
        "prebiotic_details": _join_tokens(prebiotic_tokens),
        "probiotic_details": _join_tokens(probiotic_tokens),
        "biotic_label_json": safe_json_dumps(biotic_label_json),
        "model_name": classified.model_name,
        "prompt_version": PROMPT_VERSION,
        "parse_batch_id": batch_id,
    }


def _build_summary_payload_row(
    row: Dict[str, Any],
    ingredient_composition: str,
    classified: CompositionClassificationResult,
    batch_id: str,
    protein_rows: List[Dict[str, Any]],
    fiber_carb_rows: List[Dict[str, Any]],
    biotic_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    normalized = classified.normalized
    animal_group = _dedupe_joined_labels(normalized.get("animal_protein_tag"), separator="+")
    feature_summary = {
        "protein_count": len(protein_rows),
        "fiber_carb_count": len(fiber_carb_rows),
        "biotic_count": len(biotic_rows),
        "animal_protein_tag": animal_group,
        "animal_protein_ratio": normalized.get("animal_protein_ratio"),
        "risky_ingredients": normalized.get("risky_ingredients") or [],
        "unclear_ingredients": normalized.get("unclear_ingredients") or [],
    }
    return {
        "source_id": int(row["source_id"]),
        "parsed_row_id": int(row["id"]),
        "image_name": str(row.get("image_name") or "") or None,
        "brand": str(row.get("brand") or "") or None,
        "product_name": str(row.get("product_name") or "") or None,
        "ingredient_composition": ingredient_composition or None,
        "animal_protein": _join_tokens(normalized.get("animal_protein") or []),
        "animal_protein_ratio": normalized.get("animal_protein_ratio"),
        "animal_protein_tag": animal_group,
        "plant_protein": _join_tokens(normalized.get("plant_protein") or []),
        "grain_carbohydrates": _join_tokens(normalized.get("grain_carbohydrates") or []),
        "legume_carbohydrates": _join_tokens(normalized.get("legume_carbohydrates") or []),
        "tuber_carbohydrates": _join_tokens(normalized.get("tuber_carbohydrates") or []),
        "other_carbohydrates": _join_tokens(normalized.get("other_carbohydrates") or []),
        "carbohydrates": _join_tokens(normalized.get("carbohydrates") or []),
        "fat_source": _join_tokens(normalized.get("fat_source") or []),
        "fiber_source": _join_tokens(normalized.get("fiber_source") or []),
        "prebiotics": _join_tokens(normalized.get("prebiotics") or []),
        "probiotics": _join_tokens(normalized.get("probiotics") or []),
        "nutritional_additives": _join_tokens(normalized.get("nutritional_additives") or []),
        "functional_additives": _join_tokens(normalized.get("functional_additives") or []),
        "risky_ingredients": _join_tokens(normalized.get("risky_ingredients") or []),
        "unclear_ingredients": _join_tokens(normalized.get("unclear_ingredients") or []),
        "main_ingredients": _join_tokens(normalized.get("main_ingredients") or []),
        "ingredient_notes": normalized.get("ingredient_notes"),
        "protein_feature_count": len(protein_rows),
        "fiber_carb_feature_count": len(fiber_carb_rows),
        "biotic_feature_count": len(biotic_rows),
        "protein_features_json": safe_json_dumps(protein_rows),
        "fiber_carb_features_json": safe_json_dumps(fiber_carb_rows),
        "biotic_features_json": safe_json_dumps(biotic_rows),
        "feature_summary_json": safe_json_dumps(feature_summary),
        "model_json": classified.model_json_text,
        "model_name": classified.model_name,
        "model_latency_ms": classified.model_latency_ms,
        "prompt_version": PROMPT_VERSION,
        "parse_batch_id": batch_id,
    }


def _ensure_summary_table(engine: Engine, table_name: str = DEFAULT_SUMMARY_TABLE) -> None:
    table_name = _safe_table(table_name)
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
      id BIGINT PRIMARY KEY AUTO_INCREMENT,
      source_id BIGINT NOT NULL,
      parsed_row_id BIGINT NULL,
      image_name VARCHAR(255) NULL,
      brand VARCHAR(255) NULL,
      product_name VARCHAR(512) NULL,
      ingredient_composition LONGTEXT NULL,
      animal_protein LONGTEXT NULL,
      animal_protein_ratio DECIMAL(7,2) NULL,
      animal_protein_tag VARCHAR(64) NULL,
      plant_protein LONGTEXT NULL,
      grain_carbohydrates LONGTEXT NULL,
      legume_carbohydrates LONGTEXT NULL,
      tuber_carbohydrates LONGTEXT NULL,
      other_carbohydrates LONGTEXT NULL,
      carbohydrates LONGTEXT NULL,
      fat_source LONGTEXT NULL,
      fiber_source LONGTEXT NULL,
      prebiotics LONGTEXT NULL,
      probiotics LONGTEXT NULL,
      nutritional_additives LONGTEXT NULL,
      functional_additives LONGTEXT NULL,
      risky_ingredients LONGTEXT NULL,
      unclear_ingredients LONGTEXT NULL,
      main_ingredients LONGTEXT NULL,
      ingredient_notes LONGTEXT NULL,
      protein_feature_count INT NULL,
      fiber_carb_feature_count INT NULL,
      biotic_feature_count INT NULL,
      protein_features_json LONGTEXT NULL,
      fiber_carb_features_json LONGTEXT NULL,
      biotic_features_json LONGTEXT NULL,
      feature_summary_json LONGTEXT NULL,
      model_json LONGTEXT NULL,
      model_name VARCHAR(128) NULL,
      model_latency_ms INT NULL,
      prompt_version VARCHAR(64) NOT NULL DEFAULT 'catfood_ingredient_types_v2_llm',
      parse_batch_id VARCHAR(32) NOT NULL,
      parse_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uq_source_id (source_id),
      KEY idx_image_name (image_name),
      KEY idx_brand (brand),
      KEY idx_animal_protein_tag (animal_protein_tag),
      KEY idx_product_name (product_name),
      KEY idx_parsed_row_id (parsed_row_id),
      KEY idx_model_name (model_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _protein_table_ddl(table_name: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
      id BIGINT PRIMARY KEY AUTO_INCREMENT,
      product_key VARCHAR(1024) NOT NULL,
      source_id BIGINT NOT NULL,
      parsed_row_id BIGINT NULL,
      image_name VARCHAR(255) NULL,
      brand VARCHAR(255) NULL,
      product_name VARCHAR(512) NULL,
      ingredient_composition LONGTEXT NULL,
      protein_structure VARCHAR(128) NULL,
      energy_fat VARCHAR(128) NULL,
      protein_content_pct DECIMAL(7,2) NULL,
      main_animal_source VARCHAR(64) NULL,
      primary_meat_source_species VARCHAR(255) NULL,
      primary_meat_source_type VARCHAR(255) NULL,
      secondary_meat_source_species VARCHAR(255) NULL,
      secondary_meat_source_type VARCHAR(255) NULL,
      protein_source_origin VARCHAR(32) NULL,
      meat_source_structure VARCHAR(32) NULL,
      animal_source_count INT NULL,
      novel_protein VARCHAR(8) NULL,
      energy_kcal_per_100g DECIMAL(8,2) NULL,
      fat_pct DECIMAL(7,2) NULL,
      oil_source_count INT NULL,
      oil_sources LONGTEXT NULL,
      animal_sources LONGTEXT NULL,
      protein_source_details LONGTEXT NULL,
      plant_protein_labels LONGTEXT NULL,
      protein_label_json LONGTEXT NULL,
      model_name VARCHAR(128) NULL,
      prompt_version VARCHAR(64) NOT NULL DEFAULT 'catfood_ingredient_types_v2_llm',
      parse_batch_id VARCHAR(32) NOT NULL,
      parse_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uq_product_key (product_key(255)),
      KEY idx_source_id (source_id),
      KEY idx_parsed_row_id (parsed_row_id),
      KEY idx_brand (brand),
      KEY idx_product_name (product_name),
      KEY idx_protein_structure (protein_structure),
      KEY idx_energy_fat (energy_fat),
      KEY idx_primary_meat_source_species (primary_meat_source_species),
      KEY idx_protein_source_origin (protein_source_origin),
      KEY idx_meat_source_structure (meat_source_structure)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """


def _ensure_protein_table(engine: Engine, table_name: str = DEFAULT_PROTEIN_TABLE) -> None:
    table_name = _safe_table(table_name)
    with engine.begin() as conn:
        conn.execute(text(_protein_table_ddl(table_name)))
        cols = conn.execute(text(f"SHOW COLUMNS FROM `{table_name}`")).fetchall()
        col_set = {str(row[0]) for row in cols}

        column_ddls: Sequence[tuple[str, str]] = [
            ("product_key", "ADD COLUMN product_key VARCHAR(1024) NULL AFTER id"),
            ("source_id", "ADD COLUMN source_id BIGINT NULL AFTER product_key"),
            ("parsed_row_id", "ADD COLUMN parsed_row_id BIGINT NULL AFTER source_id"),
            ("image_name", "ADD COLUMN image_name VARCHAR(255) NULL AFTER parsed_row_id"),
            ("brand", "ADD COLUMN brand VARCHAR(255) NULL AFTER image_name"),
            ("product_name", "ADD COLUMN product_name VARCHAR(512) NULL AFTER brand"),
            ("ingredient_composition", "ADD COLUMN ingredient_composition LONGTEXT NULL AFTER product_name"),
            ("protein_structure", "ADD COLUMN protein_structure VARCHAR(128) NULL AFTER ingredient_composition"),
            ("energy_fat", "ADD COLUMN energy_fat VARCHAR(128) NULL AFTER protein_structure"),
            ("protein_content_pct", "ADD COLUMN protein_content_pct DECIMAL(7,2) NULL AFTER energy_fat"),
            ("main_animal_source", "ADD COLUMN main_animal_source VARCHAR(64) NULL AFTER protein_content_pct"),
            ("primary_meat_source_species", "ADD COLUMN primary_meat_source_species VARCHAR(255) NULL AFTER main_animal_source"),
            ("primary_meat_source_type", "ADD COLUMN primary_meat_source_type VARCHAR(255) NULL AFTER primary_meat_source_species"),
            ("secondary_meat_source_species", "ADD COLUMN secondary_meat_source_species VARCHAR(255) NULL AFTER primary_meat_source_type"),
            ("secondary_meat_source_type", "ADD COLUMN secondary_meat_source_type VARCHAR(255) NULL AFTER secondary_meat_source_species"),
            ("protein_source_origin", "ADD COLUMN protein_source_origin VARCHAR(32) NULL AFTER secondary_meat_source_type"),
            ("meat_source_structure", "ADD COLUMN meat_source_structure VARCHAR(32) NULL AFTER protein_source_origin"),
            ("animal_source_count", "ADD COLUMN animal_source_count INT NULL AFTER meat_source_structure"),
            ("novel_protein", "ADD COLUMN novel_protein VARCHAR(8) NULL AFTER animal_source_count"),
            ("energy_kcal_per_100g", "ADD COLUMN energy_kcal_per_100g DECIMAL(8,2) NULL AFTER novel_protein"),
            ("fat_pct", "ADD COLUMN fat_pct DECIMAL(7,2) NULL AFTER energy_kcal_per_100g"),
            ("oil_source_count", "ADD COLUMN oil_source_count INT NULL AFTER fat_pct"),
            ("oil_sources", "ADD COLUMN oil_sources LONGTEXT NULL AFTER oil_source_count"),
            ("animal_sources", "ADD COLUMN animal_sources LONGTEXT NULL AFTER oil_sources"),
            ("protein_source_details", "ADD COLUMN protein_source_details LONGTEXT NULL AFTER animal_sources"),
            ("plant_protein_labels", "ADD COLUMN plant_protein_labels LONGTEXT NULL AFTER protein_source_details"),
            ("protein_label_json", "ADD COLUMN protein_label_json LONGTEXT NULL AFTER plant_protein_labels"),
            ("model_name", "ADD COLUMN model_name VARCHAR(128) NULL AFTER protein_label_json"),
            ("prompt_version", f"ADD COLUMN prompt_version VARCHAR(64) NOT NULL DEFAULT '{PROMPT_VERSION}' AFTER model_name"),
            ("parse_batch_id", "ADD COLUMN parse_batch_id VARCHAR(32) NOT NULL DEFAULT '' AFTER prompt_version"),
            ("parse_ts", "ADD COLUMN parse_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER parse_batch_id"),
            ("updated_ts", "ADD COLUMN updated_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER parse_ts"),
        ]
        for column_name, ddl in column_ddls:
            if column_name not in col_set:
                conn.execute(text(f"ALTER TABLE `{table_name}` {ddl}"))

        idx_rows = conn.execute(text(f"SHOW INDEX FROM `{table_name}`")).fetchall()
        idx_names = {str(row[2]) for row in idx_rows}
        if "uq_product_key" not in idx_names:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD UNIQUE KEY uq_product_key (product_key(255))"))
        if "idx_source_id" not in idx_names:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD KEY idx_source_id (source_id)"))
        if "idx_parsed_row_id" not in idx_names:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD KEY idx_parsed_row_id (parsed_row_id)"))
        if "idx_brand" not in idx_names:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD KEY idx_brand (brand)"))
        if "idx_product_name" not in idx_names:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD KEY idx_product_name (product_name)"))
        if "idx_protein_structure" not in idx_names:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD KEY idx_protein_structure (protein_structure)"))
        if "idx_energy_fat" not in idx_names:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD KEY idx_energy_fat (energy_fat)"))
        if "idx_primary_meat_source_species" not in idx_names:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD KEY idx_primary_meat_source_species (primary_meat_source_species)"))
        if "idx_protein_source_origin" not in idx_names:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD KEY idx_protein_source_origin (protein_source_origin)"))
        if "idx_meat_source_structure" not in idx_names:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD KEY idx_meat_source_structure (meat_source_structure)"))


def _ensure_fiber_carb_table(engine: Engine, table_name: str = DEFAULT_FIBER_CARB_TABLE) -> None:
    table_name = _safe_table(table_name)
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
      id BIGINT PRIMARY KEY AUTO_INCREMENT,
      product_key VARCHAR(1024) NOT NULL,
      source_id BIGINT NOT NULL,
      parsed_row_id BIGINT NULL,
      image_name VARCHAR(255) NULL,
      brand VARCHAR(255) NULL,
      product_name VARCHAR(512) NULL,
      ingredient_composition LONGTEXT NULL,
      carb_type VARCHAR(128) NULL,
      carb_details LONGTEXT NULL,
      fiber_source_details LONGTEXT NULL,
      grain_carbohydrates LONGTEXT NULL,
      legume_carbohydrates LONGTEXT NULL,
      tuber_carbohydrates LONGTEXT NULL,
      other_carbohydrates LONGTEXT NULL,
      fiber_carb_label_json LONGTEXT NULL,
      model_name VARCHAR(128) NULL,
      prompt_version VARCHAR(64) NOT NULL DEFAULT 'catfood_ingredient_types_v2_llm',
      parse_batch_id VARCHAR(32) NOT NULL,
      parse_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uq_product_key (product_key(255)),
      KEY idx_source_id (source_id),
      KEY idx_parsed_row_id (parsed_row_id),
      KEY idx_brand (brand),
      KEY idx_product_name (product_name),
      KEY idx_carb_type (carb_type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with engine.begin() as conn:
        table_exists = conn.execute(text("SHOW TABLES LIKE :table_name"), {"table_name": table_name}).fetchone()
        if not table_exists:
            conn.execute(text(ddl))
            return

        cols = conn.execute(text(f"SHOW COLUMNS FROM `{table_name}`")).fetchall()
        col_set = {str(row[0]) for row in cols}
        required_columns = {
            "product_key",
            "carb_type",
            "carb_details",
            "fiber_source_details",
            "fiber_carb_label_json",
        }
        forbidden_columns = {"ingredient_name", "feature_type", "is_primary"}
        if required_columns.issubset(col_set) and col_set.isdisjoint(forbidden_columns):
            return

        backup_table = f"{table_name}_legacy_{uuid.uuid4().hex[:8]}"
        conn.execute(text(f"CREATE TABLE `{backup_table}` LIKE `{table_name}`"))
        conn.execute(text(f"INSERT INTO `{backup_table}` SELECT * FROM `{table_name}`"))
        conn.execute(text(f"DROP TABLE `{table_name}`"))
        conn.execute(text(ddl))


def _ensure_biotic_table(engine: Engine, table_name: str = DEFAULT_BIOTIC_TABLE) -> None:
    table_name = _safe_table(table_name)
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
      id BIGINT PRIMARY KEY AUTO_INCREMENT,
      product_key VARCHAR(1024) NOT NULL,
      source_id BIGINT NOT NULL,
      parsed_row_id BIGINT NULL,
      image_name VARCHAR(255) NULL,
      brand VARCHAR(255) NULL,
      product_name VARCHAR(512) NULL,
      ingredient_composition LONGTEXT NULL,
      biotic_structure VARCHAR(32) NULL,
      biotic_type VARCHAR(64) NULL,
      prebiotic_details LONGTEXT NULL,
      probiotic_details LONGTEXT NULL,
      biotic_label_json LONGTEXT NULL,
      model_name VARCHAR(128) NULL,
      prompt_version VARCHAR(64) NOT NULL DEFAULT 'catfood_ingredient_types_v2_llm',
      parse_batch_id VARCHAR(32) NOT NULL,
      parse_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uq_product_key (product_key(255)),
      KEY idx_source_id (source_id),
      KEY idx_parsed_row_id (parsed_row_id),
      KEY idx_brand (brand),
      KEY idx_product_name (product_name),
      KEY idx_biotic_structure (biotic_structure),
      KEY idx_biotic_type (biotic_type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with engine.begin() as conn:
        table_exists = conn.execute(text("SHOW TABLES LIKE :table_name"), {"table_name": table_name}).fetchone()
        if not table_exists:
            conn.execute(text(ddl))
            return

        cols = conn.execute(text(f"SHOW COLUMNS FROM `{table_name}`")).fetchall()
        col_set = {str(row[0]) for row in cols}
        required_columns = {
            "product_key",
            "biotic_structure",
            "biotic_type",
            "prebiotic_details",
            "probiotic_details",
            "biotic_label_json",
        }
        forbidden_columns = {"ingredient_name"}
        if required_columns.issubset(col_set) and col_set.isdisjoint(forbidden_columns):
            return

        backup_table = f"{table_name}_legacy_{uuid.uuid4().hex[:8]}"
        conn.execute(text(f"CREATE TABLE `{backup_table}` LIKE `{table_name}`"))
        conn.execute(text(f"INSERT INTO `{backup_table}` SELECT * FROM `{table_name}`"))
        conn.execute(text(f"DROP TABLE `{table_name}`"))
        conn.execute(text(ddl))


def ensure_feature_tables(
    engine: Engine,
    summary_table: str = DEFAULT_SUMMARY_TABLE,
    protein_table: str = DEFAULT_PROTEIN_TABLE,
    fiber_carb_table: str = DEFAULT_FIBER_CARB_TABLE,
    biotic_table: str = DEFAULT_BIOTIC_TABLE,
) -> None:
    _ensure_summary_table(engine, summary_table)
    _ensure_protein_table(engine, protein_table)
    _ensure_fiber_carb_table(engine, fiber_carb_table)
    _ensure_biotic_table(engine, biotic_table)


def _table_has_column(engine: Engine, table_name: str, column_name: str) -> bool:
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                  AND table_name = :table_name
                  AND column_name = :column_name
                LIMIT 1
                """
            ),
            {"table_name": table_name, "column_name": column_name},
        ).fetchone()
    return bool(row)


def _delete_existing_detail_rows(conn: Any, table_name: str, source_ids: Sequence[int]) -> None:
    if not source_ids:
        return
    source_ids_sql = ",".join(str(int(x)) for x in sorted(set(source_ids)))
    conn.execute(text(f"DELETE FROM `{table_name}` WHERE source_id IN ({source_ids_sql})"))


def _flush_feature_batches(
    engine: Engine,
    summary_table: str,
    protein_table: str,
    fiber_carb_table: str,
    biotic_table: str,
    summary_stmt: Any,
    protein_stmt: Any,
    fiber_carb_stmt: Any,
    biotic_stmt: Any,
    summary_batch: List[Dict[str, Any]],
    protein_batch: List[Dict[str, Any]],
    fiber_carb_batch: List[Dict[str, Any]],
    biotic_batch: List[Dict[str, Any]],
) -> tuple[int, int, int, int]:
    if not summary_batch:
        return 0, 0, 0, 0
    source_ids = [int(row["source_id"]) for row in summary_batch]
    protein_rows = list({str(row["product_key"]): row for row in protein_batch}.values())
    fiber_carb_rows = list({str(row["product_key"]): row for row in fiber_carb_batch}.values())
    biotic_rows = list({str(row["product_key"]): row for row in biotic_batch}.values())
    with engine.begin() as conn:
        _delete_existing_detail_rows(conn, biotic_table, source_ids)
        if protein_rows:
            conn.execute(protein_stmt, protein_rows)
        if fiber_carb_rows:
            conn.execute(fiber_carb_stmt, fiber_carb_rows)
        if biotic_rows:
            conn.execute(biotic_stmt, biotic_rows)
        conn.execute(summary_stmt, summary_batch)
    counts = (len(summary_batch), len(protein_rows), len(fiber_carb_rows), len(biotic_rows))
    summary_batch.clear()
    protein_batch.clear()
    fiber_carb_batch.clear()
    biotic_batch.clear()
    return counts


def parse_catfood_ingredient_composition_types(
    engine: Engine,
    openai_cfg: Optional[Dict[str, Any]] = None,
    source_table: str = "catfood_ingredient_ocr_parsed",
    target_table: str = DEFAULT_SUMMARY_TABLE,
    protein_table: str = DEFAULT_PROTEIN_TABLE,
    fiber_carb_table: str = DEFAULT_FIBER_CARB_TABLE,
    biotic_table: str = DEFAULT_BIOTIC_TABLE,
    limit: int = 500,
    incremental_only: bool = True,
    concurrency: int = DEFAULT_LLM_CONCURRENCY,
    write_batch_size: int = DEFAULT_WRITE_BATCH_SIZE,
) -> ParseSummary:
    source_table = _safe_table(source_table)
    target_table = _safe_table(target_table)
    protein_table = _safe_table(protein_table)
    fiber_carb_table = _safe_table(fiber_carb_table)
    biotic_table = _safe_table(biotic_table)
    ensure_feature_tables(
        engine,
        summary_table=target_table,
        protein_table=protein_table,
        fiber_carb_table=fiber_carb_table,
        biotic_table=biotic_table,
    )

    resolved_openai_cfg = _resolve_openai_cfg(openai_cfg)
    _, model, _ = _make_client(resolved_openai_cfg)
    max_workers = max(1, int(concurrency))
    flush_size = max(1, int(write_batch_size))
    lim = max(1, int(limit))
    ocr_text_select = "s.ocr_text" if _table_has_column(engine, source_table, "ocr_text") else "NULL AS ocr_text"
    if incremental_only:
        fetch_sql = f"""
        SELECT s.id, s.source_id, s.image_name, s.brand, s.product_name, s.ingredient_composition, {ocr_text_select}
        FROM `{source_table}` s
        LEFT JOIN `{target_table}` t
          ON t.source_id = s.source_id
        WHERE t.source_id IS NULL
          AND s.ingredient_composition IS NOT NULL
          AND TRIM(s.ingredient_composition) <> ''
        ORDER BY s.id ASC
        LIMIT :lim
        """
    else:
        fetch_sql = f"""
        SELECT s.id, s.source_id, s.image_name, s.brand, s.product_name, s.ingredient_composition, {ocr_text_select}
        FROM `{source_table}` s
        WHERE s.ingredient_composition IS NOT NULL
          AND TRIM(s.ingredient_composition) <> ''
        ORDER BY s.id DESC
        LIMIT :lim
        """

    with engine.begin() as conn:
        rows = conn.execute(text(fetch_sql), {"lim": lim}).mappings().all()

    batch_id = uuid.uuid4().hex[:12]
    if not rows:
        return ParseSummary(scanned=0, upserted=0, source_table=source_table, target_table=target_table, batch_id=batch_id)

    grouped_rows: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        ingredient_composition = str(row.get("ingredient_composition") or "").strip()
        cache_key = _composition_cache_key(ingredient_composition)
        group = grouped_rows.get(cache_key)
        if group is None:
            grouped_rows[cache_key] = {
                "ingredient_composition": ingredient_composition,
                "rows": [dict(row)],
            }
        else:
            group["rows"].append(dict(row))

    unique_count = len(grouped_rows)
    cache_hits = len(rows) - unique_count
    summary_stmt = text(_build_summary_upsert_sql(target_table))
    protein_stmt = text(_build_protein_insert_sql(protein_table))
    fiber_carb_stmt = text(_build_fiber_carb_insert_sql(fiber_carb_table))
    biotic_stmt = text(_build_biotic_insert_sql(biotic_table))
    summary_batch: List[Dict[str, Any]] = []
    protein_batch: List[Dict[str, Any]] = []
    fiber_carb_batch: List[Dict[str, Any]] = []
    biotic_batch: List[Dict[str, Any]] = []
    resolved_unique = 0
    upserted = 0
    protein_rows_total = 0
    fiber_carb_rows_total = 0
    biotic_rows_total = 0

    console.print(
        f"[cyan]Ingredient feature parse:[/cyan] rows={len(rows)}, unique={unique_count}, "
        f"cache_hits={cache_hits}, concurrency={min(max_workers, unique_count)}, flush={flush_size}, model={model}"
    )

    def _flush_if_needed(force: bool = False) -> None:
        nonlocal upserted, protein_rows_total, fiber_carb_rows_total, biotic_rows_total
        if not summary_batch:
            return
        if len(summary_batch) >= flush_size or force:
            summary_n, protein_n, fiber_n, biotic_n = _flush_feature_batches(
                engine=engine,
                summary_table=target_table,
                protein_table=protein_table,
                fiber_carb_table=fiber_carb_table,
                biotic_table=biotic_table,
                summary_stmt=summary_stmt,
                protein_stmt=protein_stmt,
                fiber_carb_stmt=fiber_carb_stmt,
                biotic_stmt=biotic_stmt,
                summary_batch=summary_batch,
                protein_batch=protein_batch,
                fiber_carb_batch=fiber_carb_batch,
                biotic_batch=biotic_batch,
            )
            upserted += summary_n
            protein_rows_total += protein_n
            fiber_carb_rows_total += fiber_n
            biotic_rows_total += biotic_n
            console.print(
                f"[cyan]Ingredient feature progress:[/cyan] unique_done={resolved_unique}/{unique_count}, "
                f"summary={upserted}/{len(rows)}, protein={protein_rows_total}, fiber_carb={fiber_carb_rows_total}, biotic={biotic_rows_total}"
            )

    future_map: Dict[concurrent.futures.Future[CompositionClassificationResult], str] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(max_workers, unique_count)) as executor:
        for cache_key, group in grouped_rows.items():
            future = executor.submit(
                _classify_ingredient_composition,
                str(group["ingredient_composition"] or ""),
                resolved_openai_cfg,
            )
            future_map[future] = cache_key

        for future in concurrent.futures.as_completed(future_map):
            cache_key = future_map[future]
            classified = future.result()
            group = grouped_rows[cache_key]
            ingredient_composition = str(group["ingredient_composition"] or "")
            for row in group["rows"]:
                protein_feature_items = _build_protein_detail_rows(row, ingredient_composition, classified, batch_id)
                protein_label_row = _build_protein_label_row(
                    row=row,
                    ingredient_composition=ingredient_composition,
                    classified=classified,
                    batch_id=batch_id,
                    protein_feature_items=protein_feature_items,
                )
                fiber_rows = _build_fiber_carb_detail_rows(row, ingredient_composition, classified, batch_id)
                fiber_carb_label_row = _build_fiber_carb_label_row(
                    row=row,
                    ingredient_composition=ingredient_composition,
                    classified=classified,
                    batch_id=batch_id,
                    fiber_carb_feature_items=fiber_rows,
                )
                biotic_feature_items = _build_biotic_detail_rows(row, ingredient_composition, classified, batch_id)
                biotic_label_row = _build_biotic_label_row(
                    row=row,
                    ingredient_composition=ingredient_composition,
                    classified=classified,
                    batch_id=batch_id,
                    biotic_feature_items=biotic_feature_items,
                )
                summary_batch.append(
                    _build_summary_payload_row(
                        row=row,
                        ingredient_composition=ingredient_composition,
                        classified=classified,
                        batch_id=batch_id,
                        protein_rows=protein_feature_items,
                        fiber_carb_rows=fiber_rows,
                        biotic_rows=biotic_feature_items,
                    )
                )
                protein_batch.append(protein_label_row)
                fiber_carb_batch.append(fiber_carb_label_row)
                if biotic_label_row:
                    biotic_batch.append(biotic_label_row)
            resolved_unique += 1
            _flush_if_needed()

    _flush_if_needed(force=True)

    return ParseSummary(
        scanned=len(rows),
        upserted=upserted,
        source_table=source_table,
        target_table=target_table,
        batch_id=batch_id,
        protein_rows=protein_rows_total,
        fiber_carb_rows=fiber_carb_rows_total,
        biotic_rows=biotic_rows_total,
    )


def rebuild_protein_labels_from_summary(
    engine: Engine,
    summary_table: str = DEFAULT_SUMMARY_TABLE,
    protein_table: str = DEFAULT_PROTEIN_TABLE,
    parsed_source_table: str = "catfood_ingredient_ocr_parsed",
) -> ParseSummary:
    summary_table = _safe_table(summary_table)
    protein_table = _safe_table(protein_table)
    parsed_source_table = _safe_table(parsed_source_table)
    _ensure_protein_table(engine, protein_table)

    fetch_sql = f"""
    SELECT s.source_id, s.parsed_row_id, s.image_name, s.brand, s.product_name,
           s.ingredient_composition, s.animal_protein, s.plant_protein, s.fat_source, s.main_ingredients,
           s.animal_protein_tag, s.model_json, s.model_name, s.model_latency_ms
    FROM `{summary_table}` s
    ORDER BY s.id ASC
    """
    with engine.begin() as conn:
        rows = conn.execute(text(fetch_sql)).mappings().all()

    batch_id = uuid.uuid4().hex[:12]
    payload: List[Dict[str, Any]] = []
    for row in rows:
        normalized = {
            "animal_protein": [x for x in str(row.get("animal_protein") or "").split("、") if x],
            "plant_protein": [x for x in str(row.get("plant_protein") or "").split("、") if x],
            "fat_source": [x for x in str(row.get("fat_source") or "").split("、") if x],
            "main_ingredients": [x for x in str(row.get("main_ingredients") or "").split("、") if x],
            "animal_protein_tag": row.get("animal_protein_tag"),
        }
        classified = CompositionClassificationResult(
            normalized=normalized,
            model_json_text=str(row.get("model_json") or "{}"),
            model_name=str(row.get("model_name") or "") or "unknown",
            model_latency_ms=int(row.get("model_latency_ms") or 0),
        )
        source_row = {
            "id": int(row.get("parsed_row_id") or 0),
            "source_id": int(row["source_id"]),
            "image_name": row.get("image_name"),
            "brand": row.get("brand"),
            "product_name": row.get("product_name"),
        }
        ingredient_composition = str(row.get("ingredient_composition") or "")
        feature_items = _build_protein_detail_rows(source_row, ingredient_composition, classified, batch_id)
        payload.append(
            _build_protein_label_row(
                row=source_row,
                ingredient_composition=ingredient_composition,
                classified=classified,
                batch_id=batch_id,
                protein_feature_items=feature_items,
            )
        )

    insert_stmt = text(_build_protein_insert_sql(protein_table))
    if not payload:
        with engine.begin() as conn:
            conn.execute(text(f"DELETE FROM `{protein_table}`"))
    else:
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DELETE FROM `{protein_table}`"))
                conn.execute(insert_stmt, payload)
        except OperationalError as exc:
            err_text = str(exc).lower()
            if "lock wait timeout" not in err_text and "deadlock" not in err_text:
                raise
            console.print(
                f"[yellow]Protein rebuild fallback:[/yellow] delete `{protein_table}` failed due to lock contention; "
                "continue with full upsert."
            )
            with engine.begin() as conn:
                conn.execute(insert_stmt, payload)

    return ParseSummary(
        scanned=len(rows),
        upserted=len(payload),
        source_table=summary_table,
        target_table=protein_table,
        batch_id=batch_id,
    )


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


def backfill_catfood_protein_labels(
    engine: Engine,
    openai_cfg: Optional[Dict[str, Any]] = None,
    source_table: str = "catfood_ingredient_ocr_parsed",
    protein_table: str = DEFAULT_PROTEIN_TABLE,
    limit: int = 0,
    concurrency: int = DEFAULT_LLM_CONCURRENCY,
    max_write_retry: int = 5,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> ParseSummary:
    source_table = _safe_table(source_table)
    protein_table = _safe_table(protein_table)
    _ensure_protein_table(engine, protein_table)

    lim = max(0, int(limit))
    max_workers = max(1, int(concurrency))
    max_retry = max(1, int(max_write_retry))

    fetch_sql = f"""
    SELECT id, source_id, image_name, brand, product_name, ingredient_composition
    FROM `{source_table}`
    WHERE ingredient_composition IS NOT NULL
      AND TRIM(ingredient_composition) <> ''
    ORDER BY id ASC
    """
    with engine.begin() as conn:
        source_rows = conn.execute(text(fetch_sql)).mappings().all()
        existing_keys = {str(row[0]) for row in conn.execute(text(f"SELECT product_key FROM `{protein_table}`"))}

    missing_rows: List[Dict[str, Any]] = []
    seen_missing_keys = set()
    for row in source_rows:
        product_key = _build_product_key(row.get("brand"), row.get("product_name"), row.get("source_id"))
        if product_key in existing_keys or product_key in seen_missing_keys:
            continue
        seen_missing_keys.add(product_key)
        missing_rows.append(dict(row))
        if lim and len(missing_rows) >= lim:
            break

    batch_id = uuid.uuid4().hex[:12]
    if not missing_rows:
        _emit_progress(
            progress_callback,
            status="completed",
            batch_id=batch_id,
            source_table=source_table,
            protein_table=protein_table,
            source_total=len(source_rows),
            missing_total=0,
            unique_total=0,
            unique_done=0,
            written=0,
            message="没有待补充的猫粮蛋白标签数据。",
        )
        return ParseSummary(
            scanned=0,
            upserted=0,
            source_table=source_table,
            target_table=protein_table,
            batch_id=batch_id,
        )

    grouped_rows: Dict[str, Dict[str, Any]] = {}
    for row in missing_rows:
        ingredient_composition = str(row.get("ingredient_composition") or "").strip()
        cache_key = _composition_cache_key(ingredient_composition)
        group = grouped_rows.get(cache_key)
        if group is None:
            grouped_rows[cache_key] = {
                "ingredient_composition": ingredient_composition,
                "rows": [row],
            }
        else:
            group["rows"].append(row)

    unique_total = len(grouped_rows)
    _emit_progress(
        progress_callback,
        status="running",
        batch_id=batch_id,
        source_table=source_table,
        protein_table=protein_table,
        source_total=len(source_rows),
        missing_total=len(missing_rows),
        unique_total=unique_total,
        unique_done=0,
        written=0,
        message="猫粮标签工程已启动，正在调用大模型处理蛋白标签。",
    )

    resolved_openai_cfg = _resolve_openai_cfg(openai_cfg)
    insert_stmt = text(_build_protein_insert_sql(protein_table))
    written = 0
    resolved_unique = 0

    def _upsert_row(row_payload: Dict[str, Any]) -> None:
        last_error: Optional[Exception] = None
        for attempt in range(1, max_retry + 1):
            try:
                with engine.begin() as conn:
                    conn.execute(insert_stmt, row_payload)
                return
            except OperationalError as exc:
                last_error = exc
                if "1205" not in str(exc) and "Lock wait timeout exceeded" not in str(exc):
                    raise
                time.sleep(min(attempt * 2, 8))
        if last_error:
            raise last_error

    future_map: Dict[concurrent.futures.Future[CompositionClassificationResult], str] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(max_workers, unique_total)) as executor:
        for cache_key, group in grouped_rows.items():
            future = executor.submit(
                _classify_ingredient_composition,
                str(group["ingredient_composition"] or ""),
                resolved_openai_cfg,
            )
            future_map[future] = cache_key

        for future in concurrent.futures.as_completed(future_map):
            cache_key = future_map[future]
            classified = future.result()
            group = grouped_rows[cache_key]
            ingredient_composition = str(group["ingredient_composition"] or "")
            for row in group["rows"]:
                protein_feature_items = _build_protein_detail_rows(row, ingredient_composition, classified, batch_id)
                payload = _build_protein_label_row(
                    row=row,
                    ingredient_composition=ingredient_composition,
                    classified=classified,
                    batch_id=batch_id,
                    protein_feature_items=protein_feature_items,
                )
                _upsert_row(payload)
                written += 1
            resolved_unique += 1
            _emit_progress(
                progress_callback,
                status="running",
                batch_id=batch_id,
                source_table=source_table,
                protein_table=protein_table,
                source_total=len(source_rows),
                missing_total=len(missing_rows),
                unique_total=unique_total,
                unique_done=resolved_unique,
                written=written,
                message=f"猫粮标签工程进行中：已完成 {resolved_unique}/{unique_total} 个唯一配方。",
            )

    _emit_progress(
        progress_callback,
        status="completed",
        batch_id=batch_id,
        source_table=source_table,
        protein_table=protein_table,
        source_total=len(source_rows),
        missing_total=len(missing_rows),
        unique_total=unique_total,
        unique_done=resolved_unique,
        written=written,
        message=f"猫粮标签工程完成：新增/更新 {written} 条蛋白标签。",
    )
    return ParseSummary(
        scanned=len(missing_rows),
        upserted=written,
        source_table=source_table,
        target_table=protein_table,
        batch_id=batch_id,
    )


def backfill_catfood_fiber_carb_labels(
    engine: Engine,
    openai_cfg: Optional[Dict[str, Any]] = None,
    source_table: str = "catfood_ingredient_ocr_parsed",
    fiber_carb_table: str = DEFAULT_FIBER_CARB_TABLE,
    limit: int = 0,
    concurrency: int = DEFAULT_LLM_CONCURRENCY,
    max_write_retry: int = 5,
    rebuild_existing: bool = False,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> ParseSummary:
    source_table = _safe_table(source_table)
    fiber_carb_table = _safe_table(fiber_carb_table)
    _ensure_fiber_carb_table(engine, fiber_carb_table)

    lim = max(0, int(limit))
    max_workers = max(1, int(concurrency))
    max_retry = max(1, int(max_write_retry))

    fetch_sql = f"""
    SELECT id, source_id, image_name, brand, product_name, ingredient_composition
    FROM `{source_table}`
    WHERE ingredient_composition IS NOT NULL
      AND TRIM(ingredient_composition) <> ''
    ORDER BY id ASC
    """
    with engine.begin() as conn:
        source_rows = conn.execute(text(fetch_sql)).mappings().all()
        existing_keys = {str(row[0]) for row in conn.execute(text(f"SELECT product_key FROM `{fiber_carb_table}`"))}

    missing_rows: List[Dict[str, Any]] = []
    seen_missing_keys = set()
    for row in source_rows:
        product_key = _build_product_key(row.get("brand"), row.get("product_name"), row.get("source_id"))
        if product_key in seen_missing_keys:
            continue
        if not rebuild_existing and product_key in existing_keys:
            continue
        seen_missing_keys.add(product_key)
        missing_rows.append(dict(row))
        if lim and len(missing_rows) >= lim:
            break

    batch_id = uuid.uuid4().hex[:12]
    if not missing_rows:
        _emit_progress(
            progress_callback,
            status="completed",
            batch_id=batch_id,
            source_table=source_table,
            fiber_carb_table=fiber_carb_table,
            source_total=len(source_rows),
            missing_total=0,
            unique_total=0,
            unique_done=0,
            written=0,
            message="没有待补充的猫粮纤维/碳水标签数据。",
        )
        return ParseSummary(
            scanned=0,
            upserted=0,
            source_table=source_table,
            target_table=fiber_carb_table,
            batch_id=batch_id,
        )

    grouped_rows: Dict[str, Dict[str, Any]] = {}
    for row in missing_rows:
        ingredient_composition = str(row.get("ingredient_composition") or "").strip()
        cache_key = _composition_cache_key(ingredient_composition)
        group = grouped_rows.get(cache_key)
        if group is None:
            grouped_rows[cache_key] = {
                "ingredient_composition": ingredient_composition,
                "rows": [row],
            }
        else:
            group["rows"].append(row)

    unique_total = len(grouped_rows)
    _emit_progress(
        progress_callback,
        status="running",
        batch_id=batch_id,
        source_table=source_table,
        fiber_carb_table=fiber_carb_table,
        source_total=len(source_rows),
        missing_total=len(missing_rows),
        unique_total=unique_total,
        unique_done=0,
        written=0,
        message="猫粮纤维/碳水标签工程已启动，正在调用大模型处理标签。",
    )

    resolved_openai_cfg = _resolve_openai_cfg(openai_cfg)
    insert_stmt = text(_build_fiber_carb_insert_sql(fiber_carb_table))
    written = 0
    resolved_unique = 0

    def _upsert_row(row_payload: Dict[str, Any]) -> None:
        last_error: Optional[Exception] = None
        for attempt in range(1, max_retry + 1):
            try:
                with engine.begin() as conn:
                    conn.execute(insert_stmt, row_payload)
                return
            except OperationalError as exc:
                last_error = exc
                if "1205" not in str(exc) and "Lock wait timeout exceeded" not in str(exc):
                    raise
                time.sleep(min(attempt * 2, 8))
        if last_error:
            raise last_error

    future_map: Dict[concurrent.futures.Future[CompositionClassificationResult], str] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(max_workers, unique_total)) as executor:
        for cache_key, group in grouped_rows.items():
            future = executor.submit(
                _classify_ingredient_composition,
                str(group["ingredient_composition"] or ""),
                resolved_openai_cfg,
            )
            future_map[future] = cache_key

        for future in concurrent.futures.as_completed(future_map):
            cache_key = future_map[future]
            classified = future.result()
            group = grouped_rows[cache_key]
            ingredient_composition = str(group["ingredient_composition"] or "")
            for row in group["rows"]:
                fiber_carb_feature_items = _build_fiber_carb_detail_rows(row, ingredient_composition, classified, batch_id)
                payload = _build_fiber_carb_label_row(
                    row=row,
                    ingredient_composition=ingredient_composition,
                    classified=classified,
                    batch_id=batch_id,
                    fiber_carb_feature_items=fiber_carb_feature_items,
                )
                _upsert_row(payload)
                written += 1
            resolved_unique += 1
            _emit_progress(
                progress_callback,
                status="running",
                batch_id=batch_id,
                source_table=source_table,
                fiber_carb_table=fiber_carb_table,
                source_total=len(source_rows),
                missing_total=len(missing_rows),
                unique_total=unique_total,
                unique_done=resolved_unique,
                written=written,
                message=f"纤维/碳水标签处理进行中：已完成 {resolved_unique}/{unique_total} 个唯一配方。",
            )

    _emit_progress(
        progress_callback,
        status="completed",
        batch_id=batch_id,
        source_table=source_table,
        fiber_carb_table=fiber_carb_table,
        source_total=len(source_rows),
        missing_total=len(missing_rows),
        unique_total=unique_total,
        unique_done=resolved_unique,
        written=written,
        message=f"纤维/碳水标签处理完成：新增/更新 {written} 条记录。",
    )
    return ParseSummary(
        scanned=len(missing_rows),
        upserted=written,
        source_table=source_table,
        target_table=fiber_carb_table,
        batch_id=batch_id,
    )


def rebuild_biotic_labels_from_summary(
    engine: Engine,
    summary_table: str = DEFAULT_SUMMARY_TABLE,
    biotic_table: str = DEFAULT_BIOTIC_TABLE,
) -> ParseSummary:
    summary_table = _safe_table(summary_table)
    biotic_table = _safe_table(biotic_table)
    _ensure_biotic_table(engine, biotic_table)

    fetch_sql = f"""
    SELECT s.source_id, s.parsed_row_id, s.image_name, s.brand, s.product_name,
           s.ingredient_composition, s.prebiotics, s.probiotics,
           s.model_json, s.model_name, s.model_latency_ms
    FROM `{summary_table}` s
    ORDER BY s.id ASC
    """
    with engine.begin() as conn:
        rows = conn.execute(text(fetch_sql)).mappings().all()

    batch_id = uuid.uuid4().hex[:12]
    payload: List[Dict[str, Any]] = []
    for row in rows:
        normalized = {
            "prebiotics": [x for x in str(row.get("prebiotics") or "").split("、") if x],
            "probiotics": [x for x in str(row.get("probiotics") or "").split("、") if x],
        }
        classified = CompositionClassificationResult(
            normalized=normalized,
            model_json_text=str(row.get("model_json") or "{}"),
            model_name=str(row.get("model_name") or "") or "unknown",
            model_latency_ms=int(row.get("model_latency_ms") or 0),
        )
        source_row = {
            "id": int(row.get("parsed_row_id") or 0),
            "source_id": int(row["source_id"]),
            "image_name": row.get("image_name"),
            "brand": row.get("brand"),
            "product_name": row.get("product_name"),
        }
        ingredient_composition = str(row.get("ingredient_composition") or "")
        feature_items = _build_biotic_detail_rows(source_row, ingredient_composition, classified, batch_id)
        biotic_label_row = _build_biotic_label_row(
            row=source_row,
            ingredient_composition=ingredient_composition,
            classified=classified,
            batch_id=batch_id,
            biotic_feature_items=feature_items,
        )
        if biotic_label_row:
            payload.append(biotic_label_row)

    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM `{biotic_table}`"))
        if payload:
            conn.execute(text(_build_biotic_insert_sql(biotic_table)), payload)

    return ParseSummary(
        scanned=len(rows),
        upserted=len(payload),
        source_table=summary_table,
        target_table=biotic_table,
        batch_id=batch_id,
    )


def backfill_catfood_biotic_labels(
    engine: Engine,
    openai_cfg: Optional[Dict[str, Any]] = None,
    source_table: str = "catfood_ingredient_ocr_parsed",
    biotic_table: str = DEFAULT_BIOTIC_TABLE,
    limit: int = 0,
    concurrency: int = DEFAULT_LLM_CONCURRENCY,
    max_write_retry: int = 5,
    rebuild_existing: bool = False,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> ParseSummary:
    source_table = _safe_table(source_table)
    biotic_table = _safe_table(biotic_table)
    _ensure_biotic_table(engine, biotic_table)

    lim = max(0, int(limit))
    max_workers = max(1, int(concurrency))
    max_retry = max(1, int(max_write_retry))

    fetch_sql = f"""
    SELECT id, source_id, image_name, brand, product_name, ingredient_composition
    FROM `{source_table}`
    WHERE ingredient_composition IS NOT NULL
      AND TRIM(ingredient_composition) <> ''
    ORDER BY id ASC
    """
    with engine.begin() as conn:
        source_rows = conn.execute(text(fetch_sql)).mappings().all()
        existing_keys = {str(row[0]) for row in conn.execute(text(f"SELECT product_key FROM `{biotic_table}`"))}

    missing_rows: List[Dict[str, Any]] = []
    seen_missing_keys = set()
    for row in source_rows:
        product_key = _build_product_key(row.get("brand"), row.get("product_name"), row.get("source_id"))
        if product_key in seen_missing_keys:
            continue
        if not rebuild_existing and product_key in existing_keys:
            continue
        seen_missing_keys.add(product_key)
        missing_rows.append(dict(row))
        if lim and len(missing_rows) >= lim:
            break

    batch_id = uuid.uuid4().hex[:12]
    if not missing_rows:
        _emit_progress(
            progress_callback,
            status="completed",
            batch_id=batch_id,
            source_table=source_table,
            biotic_table=biotic_table,
            source_total=len(source_rows),
            missing_total=0,
            unique_total=0,
            unique_done=0,
            written=0,
            message="没有待补充的猫粮益生元/益生菌标签数据。",
        )
        return ParseSummary(
            scanned=0,
            upserted=0,
            source_table=source_table,
            target_table=biotic_table,
            batch_id=batch_id,
        )

    grouped_rows: Dict[str, Dict[str, Any]] = {}
    for row in missing_rows:
        ingredient_composition = str(row.get("ingredient_composition") or "").strip()
        cache_key = _composition_cache_key(ingredient_composition)
        group = grouped_rows.get(cache_key)
        if group is None:
            grouped_rows[cache_key] = {
                "ingredient_composition": ingredient_composition,
                "rows": [row],
            }
        else:
            group["rows"].append(row)

    unique_total = len(grouped_rows)
    _emit_progress(
        progress_callback,
        status="running",
        batch_id=batch_id,
        source_table=source_table,
        biotic_table=biotic_table,
        source_total=len(source_rows),
        missing_total=len(missing_rows),
        unique_total=unique_total,
        unique_done=0,
        written=0,
        message="猫粮益生元/益生菌标签工程已启动，正在调用大模型处理标签。",
    )

    resolved_openai_cfg = _resolve_openai_cfg(openai_cfg)
    insert_stmt = text(_build_biotic_insert_sql(biotic_table))
    written = 0
    resolved_unique = 0

    def _write_row(source_id: int, row_payload: Optional[Dict[str, Any]]) -> None:
        last_error: Optional[Exception] = None
        for attempt in range(1, max_retry + 1):
            try:
                with engine.begin() as conn:
                    if rebuild_existing:
                        _delete_existing_detail_rows(conn, biotic_table, [int(source_id)])
                    if row_payload:
                        conn.execute(insert_stmt, row_payload)
                return
            except OperationalError as exc:
                last_error = exc
                if "1205" not in str(exc) and "Lock wait timeout exceeded" not in str(exc):
                    raise
                time.sleep(min(attempt * 2, 8))
        if last_error:
            raise last_error

    future_map: Dict[concurrent.futures.Future[CompositionClassificationResult], str] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(max_workers, unique_total)) as executor:
        for cache_key, group in grouped_rows.items():
            future = executor.submit(
                _classify_ingredient_composition,
                str(group["ingredient_composition"] or ""),
                resolved_openai_cfg,
            )
            future_map[future] = cache_key

        for future in concurrent.futures.as_completed(future_map):
            cache_key = future_map[future]
            classified = future.result()
            group = grouped_rows[cache_key]
            ingredient_composition = str(group["ingredient_composition"] or "")
            for row in group["rows"]:
                biotic_feature_items = _build_biotic_detail_rows(row, ingredient_composition, classified, batch_id)
                payload = _build_biotic_label_row(
                    row=row,
                    ingredient_composition=ingredient_composition,
                    classified=classified,
                    batch_id=batch_id,
                    biotic_feature_items=biotic_feature_items,
                )
                _write_row(int(row["source_id"]), payload)
                if payload:
                    written += 1
            resolved_unique += 1
            _emit_progress(
                progress_callback,
                status="running",
                batch_id=batch_id,
                source_table=source_table,
                biotic_table=biotic_table,
                source_total=len(source_rows),
                missing_total=len(missing_rows),
                unique_total=unique_total,
                unique_done=resolved_unique,
                written=written,
                message=f"益生元/益生菌标签处理进行中：已完成 {resolved_unique}/{unique_total} 个唯一配方。",
            )

    _emit_progress(
        progress_callback,
        status="completed",
        batch_id=batch_id,
        source_table=source_table,
        biotic_table=biotic_table,
        source_total=len(source_rows),
        missing_total=len(missing_rows),
        unique_total=unique_total,
        unique_done=resolved_unique,
        written=written,
        message=f"益生元/益生菌标签处理完成：新增/更新 {written} 条记录。",
    )
    return ParseSummary(
        scanned=len(missing_rows),
        upserted=written,
        source_table=source_table,
        target_table=biotic_table,
        batch_id=batch_id,
    )
