-- 先执行：SOURCE sql/02_seed_dictionaries.sql;
-- 你也可以用 Python 脚本从 dictionaries_seed.yaml 写入（推荐），这个 SQL 仅示例

INSERT IGNORE INTO dict_items(dict_type, code, name) VALUES
('sentiment','POS','正向'),
('sentiment','NEG','负向'),
('sentiment','NEU','中性/信息'),
('purchase_intent','ASK_LINK','求链接/求购买方式'),
('purchase_intent','ASK_PRICE','询价/比价'),
('purchase_intent','WANT_BUY','明确想买'),
('purchase_intent','NO_INTENT','无购买意愿');
