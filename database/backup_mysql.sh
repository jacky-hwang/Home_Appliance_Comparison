#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
backup_dir="$project_dir/database/backups"
timestamp="$(date +%Y%m%d_%H%M%S)"
backup_file="$backup_dir/tibame_$timestamp.sql"

mkdir -p "$backup_dir"
tmp_file="$(mktemp "$backup_dir/.tibame_backup.XXXXXX")"
trap 'rm -f "$tmp_file"' EXIT

cd "$project_dir"
docker compose exec -T mysql sh -c '
  credentials="$(mktemp)"
  trap '\''rm -f "$credentials"'\'' EXIT
  printf "[client]\nuser=root\npassword=%s\n" "$MYSQL_ROOT_PASSWORD" > "$credentials"
  chmod 600 "$credentials"
  mysqldump --defaults-extra-file="$credentials" --single-transaction --routines --triggers "$MYSQL_DATABASE"
' > "$tmp_file"

mv "$tmp_file" "$backup_file"
printf 'Database backup created: %s\n' "$backup_file"
