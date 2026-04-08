#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
处理数据库中的评论，提取产品相关实体信息：
- 产品实体列表（product_entities）：只允许抽取评论原文中出现的商品名/品类词，不臆造
- 购买意愿标签（purchase_intent）
"""

import argparse
import json
import os
import re
import sys
import time
import mysql.connector
from openai import OpenAI

DEFAULT_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
DEFAULT_MODEL = "qwen-omni-turbo"

# ================== CLI 参数 ==================
parser = argparse.ArgumentParser(description="Extract product entities and purchase intent.")
parser.add_argument("--host", default="127.0.0.1")
parser.add_argument("--port", type=int, default=3306)
parser.add_argument("--user", default="root")
parser.add_argument("--password", default="")
parser.add_argument("--database", default="csv_labeling")
parser.add_argument("--table", default="douyin_raw_comments_pre")
parser.add_argument("--api-key", default=os.getenv("OPENAI_API_KEY") or os.getenv("DASHSCOPE_API_KEY") or "")
parser.add_argument("--base-url", default=os.getenv("OPENAI_BASE_URL", DEFAULT_BASE_URL))
parser.add_argument("--model", default=os.getenv("OPENAI_MODEL", DEFAULT_MODEL))
parser.add_argument("--limit", type=int, default=None, help="最多处理多少条（默认全量）")
parser.add_argument("--offset", type=int, default=0, help="从第几条开始处理")
parser.add_argument("--sleep", type=float, default=0.0, help="每条之间的休眠秒数，防止速率限制")
args = parser.parse_args()

# ================== 配置 OpenAI 客户端 ==================
if not args.api_key:
    print("缺少 API key。请通过 --api-key、OPENAI_API_KEY 或 DASHSCOPE_API_KEY 提供。")
    sys.exit(1)

try:
    client = OpenAI(
        api_key=args.api_key,
        base_url=args.base_url,
    )
except Exception as e:
    print(f"OpenAI 客户端初始化失败：{e}")
    sys.exit(1)

# ================== 数据库连接配置 ==================
try:
    conn = mysql.connector.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )
    cursor = conn.cursor()

    query = f"SELECT id, comment_text FROM {args.table} ORDER BY id LIMIT %s OFFSET %s" if args.limit else f"SELECT id, comment_text FROM {args.table} ORDER BY id"
    if args.limit:
        cursor.execute(query, (args.limit, args.offset))
    else:
        cursor.execute(query)
    results = cursor.fetchall()

    if not results:
        print("数据库中未找到符合条件的记录。")
        sys.exit(0)

except mysql.connector.Error as e:
    print(f"数据库连接或查询失败：{e}")
    sys.exit(1)

# ================== 创建新表（如果不存在） ==================
try:
    create_table_query = """
    CREATE TABLE IF NOT EXISTS douyin_raw_comments_pre_product (
        id INT AUTO_INCREMENT PRIMARY KEY,
        original_id INT,
        original_comment TEXT,
        extracted_json JSON
    )
    """
    cursor.execute(create_table_query)
    conn.commit()
except mysql.connector.Error as e:
    print(f"创建新表失败：{e}")
    sys.exit(1)

# ================== Prompt 模板（只抽取实体 + purchase_intent） ==================
PROMPT_TEMPLATE = """
你是一名中文电商/短视频评论的“商品实体抽取”标注专家。

现在给你一条中文用户评论，请从中抽取“商品/产品实体（只取原文出现的词）”，并识别是否存在明确购买意愿。

【实体抽取规则（非常重要）】
1) 只抽取评论原文中“明确出现”的商品名/品类词/产品称呼（必须是原文子串），不要根据常识补全或臆造。
2) 如果评论里出现多个商品实体，请全部抽取，按原文出现顺序输出。
3) 去重：同一个实体重复出现，只保留一次（保留第一次出现的位置顺序）。
4) 只抽取“商品/产品本体”，不要抽取品牌、店铺、人名、形容词、质量问题描述、部位词（除非部位词与商品名不可分割）。
   - 例： “沥水帘的两头容易发霉” -> 抽取 ["沥水帘"]
   - 例： “这个调料架和沥水架哪个好用” -> 抽取 ["调料架","沥水架"]
   - 例： “求链接” -> 抽取 []
   - 例： “这个太好看了” -> 抽取 []

【购买意愿标注规则（非常重要）】
- 如果评论中出现“主动索要购买入口”的表达，则 purchase_intent = "明确购买意愿"
- 典型触发词（出现任一即可触发）：
  - 求链接/求个链接/发链接/有链接吗/链接发一下/私我链接
  - 怎么买/哪里买/在哪买/在哪里买/购买方式/购买渠道
  - 求同款/求推荐/求店铺/求地址/求来源/同款链接
- 否则 purchase_intent = ""（空字符串）
- 注意：仅表达“想买/准备买/要入手”但没有索要入口，也不要判定为“明确购买意愿”，仍然填 ""

【输出要求（必须遵守）】
- 严格只输出 1 个 JSON 对象
- 不要输出任何解释性文字
- 不要添加任何额外字段
- JSON 必须可被严格解析

输出 JSON schema（key 名不能改）：
{
  "product_entities": ["字符串数组，元素必须为原文出现的商品实体；没有则空数组 []"],
  "purchase_intent": "字符串，若检测到明确购买表达，填 '明确购买意愿'；否则填空字符串 \"\""
}

评论内容：{comment_text}
"""

def build_prompt(comment_text: str) -> str:
    # 不使用 format，避免模板中的 { } 干扰
    return PROMPT_TEMPLATE.replace("{comment_text}", comment_text)

def normalize_entities(entities):
    """将 product_entities 归一成 list[str]。"""
    if entities is None:
        return []
    if isinstance(entities, str):
        entities = [entities]
    if not isinstance(entities, list):
        return []
    cleaned = []
    for x in entities:
        if x is None:
            continue
        s = str(x).strip()
        if not s:
            continue
        cleaned.append(s)
    return cleaned

def filter_entities_by_text(entities, original_text: str):
    """
    强约束：只保留原文中真实出现的实体，避免模型臆造。
    同时保持顺序并去重（按首次出现）。
    """
    text = original_text or ""
    kept = []
    seen = set()
    for ent in entities:
        if ent in seen:
            continue
        if ent in text:
            kept.append(ent)
            seen.add(ent)
    # 再按出现位置排序（防止模型顺序乱）
    kept.sort(key=lambda e: text.find(e))
    return kept

def safe_json_extract(s: str):
    """解析 JSON，失败则尝试截取第一个 {...} 进行兜底。"""
    s = (s or "").strip()
    if not s:
        return None, ""
    s = s.replace("```json", "").replace("```", "").strip()
    try:
        return json.loads(s), s
    except json.JSONDecodeError:
        try:
            start = s.index("{")
            end = s.rindex("}") + 1
            js = s[start:end]
            return json.loads(js), js
        except Exception:
            return None, s

# ================== 主处理循环 ==================
print(f"[info] 待处理 {len(results)} 条，limit={args.limit or 'ALL'}, offset={args.offset}")

for (original_id, full_comment) in results:

    if full_comment is None or not str(full_comment).strip():
        print(f"[跳过] 评论 ID {original_id} 内容为空或为None")
        continue

    full_comment = str(full_comment)
    print("=" * 80)
    print(f"[处理] 评论 ID: {original_id}")
    print(f"[预览] {full_comment[:120]}{'...' if len(full_comment) > 120 else ''}")

    prompt = build_prompt(full_comment)

    # 发送请求并处理流式响应
    try:
        completion = client.chat.completions.create(
            model=args.model,
            messages=[{"role": "user", "content": prompt}],
            modalities=["text"],
            stream=True,
            stream_options={"include_usage": True},
            # 可选：更稳一些
            # temperature=0,
        )
    except Exception as e:
        print(f"[失败] 发送请求失败：{e}")
        continue

    full_content = ""
    usage_printed = False
    for chunk in completion:
        if hasattr(chunk, "choices") and chunk.choices:
            delta = chunk.choices[0].delta
            if delta and hasattr(delta, "content") and delta.content is not None:
                full_content += delta.content
        if (not usage_printed) and hasattr(chunk, "usage") and chunk.usage:
            print("[usage]", chunk.usage)
            usage_printed = True

    if args.sleep > 0:
        time.sleep(args.sleep)

    parsed, raw_json_str = safe_json_extract(full_content)
    if parsed is None:
        print(f"[失败] 返回不是有效 JSON，评论ID: {original_id}")
        print("[返回内容]", (full_content or "")[:500])
        continue

    # 字段补齐与归一
    entities = normalize_entities(parsed.get("product_entities"))
    entities = filter_entities_by_text(entities, full_comment)
    purchase_intent = parsed.get("purchase_intent", "")
    if purchase_intent not in ("", "明确购买意愿"):
        # 强约束：只允许两种值
        purchase_intent = ""

    final_obj = {
        "product_entities": entities,
        "purchase_intent": purchase_intent
    }
    final_json_str = json.dumps(final_obj, ensure_ascii=False)

    # 插入数据库
    try:
        insert_query = """
        INSERT INTO douyin_raw_comments_pre_product
        (original_id, original_comment, extracted_json)
        VALUES (%s, %s, %s)
        """
        cursor.execute(insert_query, (original_id, full_comment, final_json_str))
        conn.commit()
        print(f"[成功] 评论 ID {original_id} 已入库：{final_json_str}")
    except mysql.connector.Error as e:
        print(f"[失败] 插入数据失败：{e}")

# 关闭数据库连接
try:
    if conn.is_connected():
        cursor.close()
        conn.close()
except Exception:
    pass

print("\n全部处理完成。")
