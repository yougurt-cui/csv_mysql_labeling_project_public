-- 先执行：SOURCE sql/01_schema.sql;

-- 1) 原始数据表（CSV 入库后落这里）
CREATE TABLE IF NOT EXISTS raw_comments (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  source_name VARCHAR(64) NOT NULL,
  external_id VARCHAR(128) NULL,
  title TEXT NULL,
  comment_text LONGTEXT NOT NULL,
  author VARCHAR(128) NULL,
  created_at DATETIME NULL,
  like_count INT NULL,
  raw_json LONGTEXT NULL,

  ingest_batch_id VARCHAR(64) NOT NULL,
  ingest_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  label_status ENUM('PENDING','DONE','ERROR') NOT NULL DEFAULT 'PENDING',
  label_ts DATETIME NULL,

  UNIQUE KEY uq_source_external (source_name, external_id),
  KEY idx_label_status (label_status),
  FULLTEXT KEY ft_comment_text (comment_text)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2) 字典表（产品、CTQ、情绪、购买意愿等）
CREATE TABLE IF NOT EXISTS dict_items (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  dict_type VARCHAR(64) NOT NULL,
  code VARCHAR(64) NOT NULL,
  name VARCHAR(256) NOT NULL,
  is_active TINYINT NOT NULL DEFAULT 1,
  extra_json LONGTEXT NULL,
  UNIQUE KEY uq_dict_code (dict_type, code),
  KEY idx_dict_type (dict_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3) 标签结果表（结构化输出落这里，方便后续统计/回溯）
CREATE TABLE IF NOT EXISTS comment_labels (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  raw_comment_id BIGINT NOT NULL,

  product_category_code VARCHAR(64) NULL,
  sentiment_code VARCHAR(16) NULL,
  purchase_intent_code VARCHAR(32) NULL,

  entities_json LONGTEXT NULL,
  ctq_json LONGTEXT NULL,
  model_json LONGTEXT NOT NULL,

  model_name VARCHAR(64) NOT NULL,
  model_latency_ms INT NULL,
  prompt_version VARCHAR(32) NOT NULL DEFAULT 'v1',

  created_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  UNIQUE KEY uq_raw_comment (raw_comment_id),
  KEY idx_product_category (product_category_code),
  KEY idx_sentiment (sentiment_code),
  KEY idx_purchase_intent (purchase_intent_code),

  CONSTRAINT fk_labels_raw
    FOREIGN KEY (raw_comment_id) REFERENCES raw_comments(id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4) OCR 图片识别结果表（本地图片 -> OCR JSON -> 入库）
CREATE TABLE IF NOT EXISTS ocr_image_results (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 5) 猫粮原材料 OCR 结果表（图片 OCR 导入）
CREATE TABLE IF NOT EXISTS catfood_ingredient_ocr_results (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 6) 猫粮原材料 OCR 结构化解析结果表
CREATE TABLE IF NOT EXISTS catfood_ingredient_ocr_parsed (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  source_id BIGINT NOT NULL,
  image_path VARCHAR(1024) NULL,
  image_name VARCHAR(255) NULL,
  file_sha256 CHAR(64) NULL,
  brand VARCHAR(255) NULL,
  product_name VARCHAR(512) NULL,
  ingredient_composition LONGTEXT NULL,
  phone VARCHAR(64) NULL,
  importer VARCHAR(255) NULL,
  ocr_text LONGTEXT NULL,
  ocr_json LONGTEXT NULL,
  parse_batch_id VARCHAR(32) NOT NULL,
  parse_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_source_id (source_id),
  KEY idx_brand (brand),
  KEY idx_product_name (product_name),
  KEY idx_phone (phone),
  KEY idx_importer (importer)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 7) 猫粮营养保证值产品信息表（与 OCR parsed/source_id 关联）
CREATE TABLE IF NOT EXISTS product_info (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 8) 猫粮营养保证值明细表（与 OCR parsed/source_id 关联）
CREATE TABLE IF NOT EXISTS product_guarantee (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 9) 淘宝猫粮标题标准化结果表
CREATE TABLE IF NOT EXISTS taobao_catfood_title_parsed (
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

-- 10) 猫粮特征汇总表（按 ingredient_composition 聚合）
CREATE TABLE IF NOT EXISTS catfood_feature_summary (
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

-- 11) 猫粮蛋白质特征表（按 brand + product_name 聚合，一款产品一行）
CREATE TABLE IF NOT EXISTS catfood_feature_protein_labels (
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

-- 12) 猫粮纤维/碳水特征表（按 brand + product_name 聚合，一款产品一行）
CREATE TABLE IF NOT EXISTS catfood_feature_fiber_carb_labels (
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

-- 13) 猫粮益生元/益生菌标签表（brand + product_name 一行）
CREATE TABLE IF NOT EXISTS catfood_feature_biotic_labels (
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

-- 14) 品牌问题/CTQ 评论案例表（从 brand_issue_ctq Excel 导入）
CREATE TABLE IF NOT EXISTS catfood_brand_issue_ctq_cases (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  external_id CHAR(32) NOT NULL,
  brand VARCHAR(255) NULL,
  brand_raw VARCHAR(255) NULL,
  brand_match_type VARCHAR(32) NULL,
  issue_name VARCHAR(255) NULL,
  comment_text LONGTEXT NOT NULL,
  event_date DATE NULL,
  source_file VARCHAR(255) NOT NULL,
  sheet_name VARCHAR(128) NOT NULL,
  row_no INT NOT NULL,
  raw_json LONGTEXT NULL,
  ingest_batch_id VARCHAR(32) NOT NULL,
  ingest_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_external_id (external_id),
  KEY idx_brand (brand),
  KEY idx_brand_raw (brand_raw),
  KEY idx_issue_name (issue_name),
  KEY idx_event_date (event_date),
  KEY idx_source_file (source_file),
  KEY idx_sheet_name (sheet_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
