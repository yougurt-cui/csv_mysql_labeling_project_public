-- 先在 mysql 客户端执行：SOURCE sql/00_create_db.sql;
-- 如果你用 root 且本地空密码，通常无需单独创建用户；否则按需修改
CREATE DATABASE IF NOT EXISTS `csv_labeling`
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_ai_ci;
