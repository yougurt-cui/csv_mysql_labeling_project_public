# CSV → MySQL → OpenAI 打标签 → 回写 MySQL（本地工程模板）

## 1. 目录结构
- `config/`：配置与列映射、字典 seed
- `sql/`：创建库、建表、seed（可选）
- `src/`：工程代码（CLI + 数据管道）
- `data/`：本地 CSV（被 `.gitignore` 忽略，可自行放数据）

## 2. 环境准备（Conda）
```bash
conda env create -f environment.yml
conda activate csv_mysql_labeling
```

## 3. MySQL 创建库 & 建表
### 3.1 创建数据库（示例）
```bash
mysql -u root -p < sql/00_create_db.sql
```

### 3.2 建表
```bash
mysql -u root -p csv_labeling < sql/01_schema.sql
```

> 说明：默认库名 `csv_labeling`，字符集 `utf8mb4`。

## 4. 配置
把示例配置复制为正式配置：
```bash
cp config/config.example.yaml config/config.yaml
cp config/column_map.example.yaml config/column_map.yaml
```

编辑 `config/config.yaml`：
- 填 MySQL 连接信息
- 打标签功能需要填 `openai.api_key`
- 如果想走千问兼容接口，可设置：
  - `openai.base_url: https://dashscope.aliyuncs.com/compatible-mode/v1`
  - `openai.model: qwen-plus-latest`（或其他兼容模型）
- OCR 默认用免费 `ocr_space`（`ocr.provider: ocr_space` + `ocr_space_api_key`）
- 若要改回 OpenAI OCR，设置 `ocr.provider: openai`

## 5. 初始化字典（推荐用 Python 写入）
```bash
python -m src.cli init-dicts
```

（可选）如果你只想先塞一小部分字典，也可以先跑：
```bash
mysql -u root -p csv_labeling < sql/02_seed_dictionaries.sql
```

## 6. CSV 入库
```bash
python -m src.cli ingest --csv data/your.csv --source local_csv
```

## 6.1 淘宝列表入库（detail_items_raw_*.json）
```bash
python -m src.cli ingest-taobao-list \
  --input-dir data/taobao \
  --pattern 'detail_items_raw_*.json' \
  --table taobao_catfood_list_items \
  --batch-size 500
```
说明：
- 默认导入 `detail_items_raw_*.json`
- 也兼容旧的 `list_items_raw_*.json`，手动传 `--pattern 'list_items_raw_*.json'` 即可
- 会同时入库 `food_taste`、`net_content`、`sold_text`、`sold_count` 等详情字段
- 导入完成后会自动清洗 `taobao_catfood_list_items`：仅保留 `pay_count >= 100`，并按 `keyword + food_taste` 的高相似记录去重，保留 `pay_count` 最高的一条；若有删除会自动创建备份表

## 6.2 淘宝标题标准化抽取（配方类型/适合猫/年龄段/每kg单价）
```bash
python -m src.cli parse-taobao-title \
  --source-table taobao_catfood_list_items \
  --target-table taobao_catfood_title_parsed \
  --limit 1000
```
说明：
- 从 `taobao_catfood_list_items.title` 抽取：
  - `keywords`（淘宝列表抓取关键词）
  - `付款人数`（优先用源表 `pay_count` 字段）
  - `问题类型`（如美猫、增肥、呵护肠胃）
  - `formula_type`（如鸡肉、三文鱼、红肉等）
  - `suitable_cat`（如短毛猫、长毛猫、脾胃虚弱等）
  - `cat_age_stage`（幼猫、成猫、全阶段、老年猫）
- 从 `title` 解析净含量 `net_weight_kg`（支持 `kg/g/斤` 与 `x`/`*` 乘包写法）
- 使用 `price / net_weight_kg` 计算 `unit_price_per_kg`，并写 `unit_price_text`（`每1kgX元`）
- 页面里也有同功能入口（`/taobao` 下方“标准化抽取 title 字段”）

## 6.3 猫粮数据抽取（增量）
```bash
python -m src.cli extract-catfood \
  --target-table catfood_brand_health_candidates \
  --state-table catfood_brand_health_extract_state
```
说明：
- 从 `douyin_raw_comments`、`xiaohongshu_raw_comments` 按规则抽取“猫粮品牌/健康问题”候选
- 抽取条件按 `comment_text` 匹配：命中“品牌词”或“健康词”（同时命中也会保留）
- 按 `ingest_ts` 做增量水位，只处理新增数据
- 结果写入 `catfood_brand_health_candidates`，状态记录在 `catfood_brand_health_extract_state`
- 页面入口 `http://127.0.0.1:5000/catfood_extract` 中新增了“原料特征拆分”功能，可直接把 `ingredient_composition` 拆成 3 张明细表和 1 张汇总表

## 6.4 猫粮原材料导入（图片 OCR）
页面入口：`http://127.0.0.1:5000/catfood_ingredients`

说明：
- 输入图片目录地址（支持子目录递归扫描）
- 配置图片匹配规则（如 `*.jpg,*.png`）
- 系统会逐张调用 OCR，结果 JSON 直接写入 MySQL
- 默认落表 `catfood_ingredient_ocr_results`（按 `file_sha256` 幂等 upsert）
- OCR Provider 默认是免费 `ocr_space`
- 若图片超过 OCR.Space 免费接口 1MB 限制，会自动压缩后再识别
  - 可在 `config.yaml` 配置：`ocr_space_auto_compress`、`ocr_space_max_file_kb`
- 若 OCR.Space 返回 `E500/System Resource Exhaustion`，会自动重试并在重试时进一步压缩
  - 可在 `config.yaml` 配置：`ocr_space_processing_retry`（默认 3 次）
- OCR 成功后的图片会自动移动到：`history/catfood_ingredient_images`

## 6.5 解析 OCR JSON（名称/原料组成/电话/进口商）
```bash
python -m src.cli parse-catfood-ocr \
  --source-table catfood_ingredient_ocr_results \
  --target-table catfood_ingredient_ocr_parsed \
  --limit 500
```
说明：
- 从 `catfood_ingredient_ocr_results.ocr_json` 提取字段：
  - `brand`（品牌，初始默认空）
  - `product_name`（名称）
  - `ingredient_composition`（原料组成）
  - `phone`（电话）
  - `importer`（进口商）
- 同步保留图片定位字段：`image_path`、`image_name`、`file_sha256`
- 默认严格增量：仅解析 `target_table` 中不存在的 `source_id`
- 如需重跑已解析数据，可加 `--reparse-existing`
- 字段缺失会写 `NULL`
- 页面里也有同功能入口（`/catfood_ingredients` 下方“解析 OCR JSON 结构化入库”）

## 6.6 解析营养保证值（Qwen OCR）
```bash
python -m src.cli parse-catfood-guarantee \
  --source-table catfood_ingredient_ocr_results \
  --parsed-table catfood_ingredient_ocr_parsed \
  --info-table product_info \
  --guarantee-table product_guarantee \
  --limit 200
```
说明：
- 只处理 `catfood_ingredient_ocr_parsed` 中存在对应 `source_id` 的图片，优先从 `catfood_ingredient_ocr_parsed.image_path` 读取图片，必要时回退到 `catfood_ingredient_ocr_results.image_path`；不再额外扫描兜底目录
- 默认严格增量：仅处理 `product_info` 中还不存在的 `source_id`
- 识别结果会写入：
  - `product_info`
  - `product_guarantee`
- 该步骤依赖千问兼容接口，优先读取 `QWEN_API_KEY / DASHSCOPE_API_KEY`
- 识别成功后，图片会移动到 `history_猫粮成分图/猫粮保证值分析`，并同步回写 `catfood_ingredient_ocr_results` 与 `catfood_ingredient_ocr_parsed` 的图片路径
- 页面里也有同功能入口（`/catfood_ingredients` 下方“识别营养保证值并写入 product_info / product_guarantee”）

## 6.7 拆分原料特征（3 张明细表 + 1 张汇总表）
```bash
python -m src.cli parse-catfood-ingredient-types \
  --source-table catfood_ingredient_ocr_parsed \
  --summary-table catfood_feature_summary \
  --protein-table catfood_feature_protein_labels \
  --fiber-carb-table catfood_feature_fiber_carb_labels \
  --biotic-table catfood_feature_biotic_labels \
  --limit 500
```
说明：
- 从 `catfood_ingredient_ocr_parsed.ingredient_composition` 做大模型结构化打标签
- 默认复用 `config/config.yaml` 的 `openai` 配置；如果 `base_url/model` 指向千问兼容接口，也可直接走 Qwen
- 结果会拆成：
  - `catfood_feature_protein_labels`
  - `catfood_feature_fiber_carb_labels`
  - `catfood_feature_biotic_labels`
  - `catfood_feature_summary`
- 汇总表中会保留整体特征字段，并额外写入：
  - `protein_feature_count`
  - `fiber_carb_feature_count`
  - `biotic_feature_count`
  - `protein_features_json`
  - `fiber_carb_features_json`
  - `biotic_features_json`
  - `feature_summary_json`
- 默认严格增量：仅处理 `summary_table` 不存在的 `source_id`
- 默认开启并发和小批量写库：`--concurrency 4 --write-batch-size 5`
- 如需重跑已拆分数据，可加 `--reparse-existing`

## 7. 打标签并回写
```bash
python -m src.cli label --limit 200
```

你也可以一条命令串起来：
```bash
python -m src.cli run-all --csv data/your.csv --source local_csv --limit 200
```

## 8. 本地图片 OCR（输出 JSON + 保存到 MySQL）
```bash
python -m src.cli ocr-image \
  --image /absolute/path/to/your_photo.jpg \
  --out-json data/ocr_json/your_photo_ocr.json \
  --table ocr_image_results
```

说明：
- `--image`：本地图片路径（必填）
- `--out-json`：OCR 结果 JSON 文件路径（可选，不填会自动写入 `data/ocr_json/`）
- `--table`：MySQL 目标表（默认 `ocr_image_results`）
- 会复用 `config/config.yaml` 中的 `mysql`、`ocr`（和可选 `openai`）配置
- 结果同时保存：
  - 本地 JSON 文件
  - MySQL 表 `ocr_image_results`（按图片文件哈希 `file_sha256` 幂等 upsert）

## 9. VSCode 运行
- 直接在 VSCode 打开工程
- 用终端激活 conda 环境
- Run/Debug 选择 `cli` 启动项

## 页面入口
- `http://127.0.0.1:5000/catfood_ingredients`：猫粮原材料导入（和“小红书数据导入”同风格）

## 常用任务
- Product_entities 标签规则任务：`bash scripts/run_tag_product_entities.sh`（默认 force 全量覆盖，可追加 `--dry-run` / `--limit` / 自定义连接参数）

## 公开仓库说明
- `config/config.yaml` 属于本地私有配置，仓库只保留 `config/config.example.yaml`
- `data/`、`history/`、根目录导出的 `search_*.csv`、临时 SQL 和虚拟环境目录不应提交到公共仓库
- 示例命令中的输入目录均已改为仓库内相对路径；真实数据请自行放到本地目录后再执行

---

## 设计原则（你后续扩展时建议遵守）
1. **原始数据不可变**：`raw_comments` 只追加，不做覆盖更新（只更新 `label_status/label_ts`）。
2. **标签可追溯**：`comment_labels.model_json` 保留完整模型输出；关键字段冗余到独立列方便统计。
3. **字典可版本化**：`dict_items` 可以加 `extra_json` 存枚举解释、阈值、单位等。
4. **幂等**：重复跑 `ingest` / `label` 不会产生重复记录（靠唯一键与状态位控制）。
