#!/usr/bin/env bash
set -euo pipefail

# Migrate MongoDB collections from local Mongo to MongoDB Atlas using mongodump/mongorestore.
# Default: migrate placement_db.job_board_sources
#
# Usage examples:
# 1) job_board_sources only (recommended):
#   LOCAL_URI="mongodb://localhost:27017/placement_db" \
#   ATLAS_URI="mongodb+srv://USER:PASS@cluster0.xxxxx.mongodb.net/placement_db?retryWrites=true&w=majority" \
#   ./scripts/migrate_mongo_to_atlas.sh --local-uri "$LOCAL_URI" --atlas-uri "$ATLAS_URI"
#
# 2) job_board_sources + job_ingest, drop atlas collections before restore:
#   ./scripts/migrate_mongo_to_atlas.sh \
#     --local-uri "$LOCAL_URI" \
#     --atlas-uri "$ATLAS_URI" \
#     --collections job_board_sources,job_ingest \
#     --drop-atlas
#
# Notes:
# - This script does NOT print your URI/password.
# - You should set --drop-atlas ONLY when you want Atlas collections replaced.

DB_NAME="placement_db"
COLLECTIONS="job_board_sources"
DROP_ATLAS="false"
ARCHIVE_DIR="/tmp/mongo_migrate_jobLead"

LOCAL_URI=""
ATLAS_URI=""

usage() {
  cat <<'EOF'
Usage:
  scripts/migrate_mongo_to_atlas.sh --local-uri <uri> --atlas-uri <uri> [options]

Required:
  --local-uri     Local Mongo connection URI (include db name)
  --atlas-uri     Atlas Mongo connection URI (include db name)

Options:
  --db <name>                     Database name (default: placement_db)
  --collections <c1,c2,...>      Collections to migrate (default: job_board_sources)
  --archive-dir <path>           Temporary archive directory (default: /tmp/mongo_migrate_jobLead)
  --drop-atlas                   Drop target collections in Atlas before restore
  -h, --help                      Show help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --local-uri) LOCAL_URI="${2:-}"; shift 2 ;;
    --atlas-uri) ATLAS_URI="${2:-}"; shift 2 ;;
    --db) DB_NAME="${2:-}"; shift 2 ;;
    --collections) COLLECTIONS="${2:-}"; shift 2 ;;
    --archive-dir) ARCHIVE_DIR="${2:-}"; shift 2 ;;
    --drop-atlas) DROP_ATLAS="true"; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "${LOCAL_URI}" || -z "${ATLAS_URI}" ]]; then
  echo "Error: --local-uri and --atlas-uri are required." >&2
  usage
  exit 2
fi

if [[ ! "${LOCAL_URI}" =~ ^mongodb(\+srv)?:// ]]; then
  echo "Error: --local-uri must start with mongodb:// or mongodb+srv:// (got: ${LOCAL_URI:0:30}...)." >&2
  exit 2
fi
if [[ ! "${ATLAS_URI}" =~ ^mongodb(\+srv)?:// ]]; then
  echo "Error: --atlas-uri must start with mongodb:// or mongodb+srv:// (got: ${ATLAS_URI:0:30}...)." >&2
  exit 2
fi

if ! command -v mongodump >/dev/null 2>&1; then
  echo "Error: mongodump not found in PATH." >&2
  exit 1
fi

if ! command -v mongorestore >/dev/null 2>&1; then
  echo "Error: mongorestore not found in PATH." >&2
  exit 1
fi

mkdir -p "${ARCHIVE_DIR}"

IFS=',' read -r -a COL_ARR <<< "${COLLECTIONS}"
echo "Migrating MongoDB collections to Atlas"
echo "  DB          : ${DB_NAME}"
echo "  Collections : ${COLLECTIONS}"
echo "  Archive dir : ${ARCHIVE_DIR}"
echo "  Drop atlas  : ${DROP_ATLAS}"

for col in "${COL_ARR[@]}"; do
  col="$(echo "$col" | xargs)" # trim
  if [[ -z "$col" ]]; then
    continue
  fi

  ARCHIVE_PATH="${ARCHIVE_DIR}/${DB_NAME}.${col}.archive.gz"
  echo ""
  echo "==> Dumping local: ${DB_NAME}.${col}"
  # Use --archive with --gzip to avoid producing many files.
  mongodump \
    --db "${DB_NAME}" \
    --collection "${col}" \
    --archive="${ARCHIVE_PATH}" \
    --gzip \
    "${LOCAL_URI}"

  echo "==> Restoring to atlas: ${DB_NAME}.${col}"
  RESTORE_ARGS=(--archive="${ARCHIVE_PATH}" --gzip --nsInclude "${DB_NAME}.${col}" "${ATLAS_URI}")
  if [[ "${DROP_ATLAS}" == "true" ]]; then
    RESTORE_ARGS+=(--drop)
  fi

  mongorestore "${RESTORE_ARGS[@]}"
done

echo ""
echo "Done."

