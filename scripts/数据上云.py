#!/usr/bin/env bash
set -euo pipefail

# --- 配置 ---
LOCAL_HOST="127.0.0.1"
LOCAL_USER="local_user"

RDS_HOST="your-rds-endpoint.mysql.rds.aliyuncs.com"
RDS_USER="rds_user"

if [ "$#" -lt 3 ]; then
  echo "Usage: $0 db_name table1 table2 [table3 ...]"
  exit 1
fi

DB="$1"
shift
TABLES=("$@")

# --- 凭据文件（临时） ---
local_cnf="$(mktemp)"
rds_cnf="$(mktemp)"
cleanup() { rm -f "$local_cnf" "$rds_cnf"; }
trap cleanup EXIT

read -s -p "Local MySQL password: " LOCAL_PASS; echo
read -s -p "RDS MySQL password: " RDS_PASS; echo

cat > "$local_cnf" <<EOF
[client]
host=$LOCAL_HOST
user=$LOCAL_USER
password=$LOCAL_PASS
EOF

cat > "$rds_cnf" <<EOF
[client]
host=$RDS_HOST
user=$RDS_USER
password=$RDS_PASS
EOF

chmod 600 "$local_cnf" "$rds_cnf"

# --- 导出并导入指定表 ---
mysqldump --defaults-extra-file="$local_cnf" \
  --single-transaction --routines --triggers --events \
  --set-gtid-purged=OFF \
  "$DB" "${TABLES[@]}" \
| mysql --defaults-extra-file="$rds_cnf" \
  --default-character-set=utf8mb4 "$DB"
