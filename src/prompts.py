from __future__ import annotations

from typing import Any, Dict, List

PROMPT_VERSION = "v1"

def build_labeling_messages(
    text: str,
    dicts: Dict[str, List[Dict[str, str]]],
) -> list[dict]:
    """把字典枚举塞进提示词，让模型在受控枚举上输出。"""

    # 仅把关键字典塞进去，避免 token 膨胀（你后续可按需扩展）
    ctq = dicts.get("ctq", [])
    product = dicts.get("product_category", [])
    purchase = dicts.get("purchase_intent", [])
    sentiment = dicts.get("sentiment", [])

    system = """你是一个严谨的数据标注助手。你的任务：根据输入的中文评论文本，输出结构化标签(JSON)。
要求：
- 必须输出合法 JSON（不要输出 Markdown 代码块，不要输出多余文字）。
- code 字段必须从给定枚举中选择；如果无法判断，填 null。
- entities_json / ctq_json 使用列表结构，且每个元素必须是对象。
- 不要臆造评论中不存在的事实。"""

    user = {
      "text": text,
      "enums": {
        "product_category": product,
        "ctq": ctq,
        "purchase_intent": purchase,
        "sentiment": sentiment
      },
      "output_schema": {
        "product_category_code": "string|null",
        "sentiment_code": "string|null",
        "purchase_intent_code": "string|null",
        "entities": [
          {"type": "string", "entity": "string"}
        ],
        "ctq_labels": [
          {
            "ctq_code": "string",
            "polarity": "POS|NEG|NEU",
            "evidence": "string"
          }
        ],
        "notes": "string"
      }
    }

    return [
        {"role": "system", "content": system},
        {"role": "user", "content": str(user)}
    ]
