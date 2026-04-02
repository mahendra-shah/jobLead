#!/usr/bin/env bash
set -euo pipefail

# Migrates local MongoDB collections into MongoDB Atlas, then applies Neon/Postgres
# Alembic migrations so the `jobs` table has the latest columns/constraints.
#
# Edit these variables before running.
#
# SECURITY:
# - Do not paste secrets into chat history; edit locally.

LOCAL_MONGO_URI=""
ATLAS_MONGO_URI=""

# Neon connection used by Alembic migrations.
# Can be the async URL from your .env (alembic env.py converts it to sync).
NEON_POSTGRES_URL=""

# What to migrate (Mongo collections).
DB_NAME="placement_db"
COLLECTIONS=( "job_board_sources" "job_ingest" )

# If true, drops Atlas target collections before restore.
DROP_ATLAS_BEFORE_RESTORE="true"

ARCHIVE_DIR="/tmp/jobLead_mongo_migrate_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$ARCHIVE_DIR"

require_bin() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: '$1' not found in PATH. Install MongoDB Database Tools." >&2
    exit 1
  fi
}

require_bin mongodump
require_bin mongorestore

mask_uri() {
  # Mask "user:pass@" -> "user:***@" for mongodb URIs.
  # Works for both mongodb:// and mongodb+srv:// patterns.
  local s="$1"
  # If there is no credentials, return as-is.
  if [[ "$s" != *"@"* || "$s" != *"://"* ]]; then
    echo "$s"
    return 0
  fi
  # Extract scheme://prefix and suffix starting from @
  local prefix="${s%%@*}"      # up to (but not including) @
  local suffix="${s#*@}"       # after @
  # Mask everything after ":" in the credential part if present
  if [[ "$prefix" == *":"* && "$prefix" == *"://"* ]]; then
    # Keep scheme://user:
    local scheme_and_user="${prefix%:*}"  # up to before last :
    echo "${scheme_and_user}:***@${suffix}"
    return 0
  fi
  echo "$s"
}

echo "==> Mongo migrate start"
echo "    Local Mongo : $(mask_uri "$LOCAL_MONGO_URI")"
echo "    Atlas Mongo : $(mask_uri "$ATLAS_MONGO_URI")"
echo "    DB Name     : $DB_NAME"
echo "    Collections  : ${COLLECTIONS[*]}"
echo "    Archive dir  : $ARCHIVE_DIR"
echo "    Drop atlas   : $DROP_ATLAS_BEFORE_RESTORE"

for col in "${COLLECTIONS[@]}"; do
  col_trim="$(echo "$col" | xargs)"
  if [[ -z "$col_trim" ]]; then
    continue
  fi

  archive_path="${ARCHIVE_DIR}/${DB_NAME}.${col_trim}.archive.gz"

  echo ""
  echo "==> Dump local: ${DB_NAME}.${col_trim}"
  mongodump \
    --db "$DB_NAME" \
    --collection "$col_trim" \
    --archive="$archive_path" \
    --gzip \
    "$LOCAL_MONGO_URI"

  echo "==> Restore atlas: ${DB_NAME}.${col_trim}"
  restore_args=(--archive="$archive_path" --gzip --nsInclude "${DB_NAME}.${col_trim}" "$ATLAS_MONGO_URI")
  if [[ "$DROP_ATLAS_BEFORE_RESTORE" == "true" ]]; then
    restore_args+=(--drop)
  fi
  mongorestore "${restore_args[@]}"
done

echo ""
echo "==> Mongo migrate done"

echo ""
echo "==> Applying Neon/Postgres migrations (alembic upgrade head)"
echo "    Using DATABASE_URL/LOCAL_DATABASE_URL from NEON_POSTGRES_URL"

export DATABASE_URL="$NEON_POSTGRES_URL"

# Alembic env.py uses LOCAL_DATABASE_URL directly. If we pass the asyncpg URL here,
# Alembic (sync context) may crash with "MissingGreenlet". Convert async URL -> sync.
if [[ "$NEON_POSTGRES_URL" == postgresql+asyncpg://* ]]; then
  SYNC_NEON_POSTGRES_URL="${NEON_POSTGRES_URL/postgresql+asyncpg:/postgresql:}"
else
  SYNC_NEON_POSTGRES_URL="$NEON_POSTGRES_URL"
fi

# psycopg2 expects `sslmode`, not `ssl`.
# Convert any `?ssl=` or `&ssl=` query param to `sslmode=`.
SYNC_NEON_POSTGRES_URL="$(printf '%s' "$SYNC_NEON_POSTGRES_URL" | sed -E 's/([?&])ssl=/\1sslmode=/g')"

export LOCAL_DATABASE_URL="$SYNC_NEON_POSTGRES_URL"

if [[ -x "./venv/bin/alembic" ]]; then
  ./venv/bin/alembic upgrade head
else
  # Fallback if alembic is installed as a python module
  ./venv/bin/python3 -m alembic upgrade head
fi

echo "==> Done"

