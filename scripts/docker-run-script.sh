#!/usr/bin/env sh
# Chạy script live-test bên trong container với env AI model ĐẦY ĐỦ.
#
# Vì sao cần: `docker exec -e AI_DEFAULT_MODEL=...` chỉ override 1 biến, còn
# AI_ALLOWED_MODELS vẫn là giá trị cũ của container → model bị fallback âm thầm.
#
# Usage: sh scripts/docker-run-script.sh verify-3-fixed-bugs.ts [--dry-run|--strict-model]
set -eu

CONTAINER="${CONTAINER:-auto-research-be-local}"
ENV_FILE="${ENV_FILE:-.env}"
SCRIPT="${1:-}"
if [ "$#" -gt 1 ]; then
  shift
fi

if [ -z "$SCRIPT" ]; then
  echo "❌ Thiếu tên script. Ví dụ: sh scripts/docker-run-script.sh verify-3-fixed-bugs.ts" >&2
  exit 1
fi

if [ ! -f "$ENV_FILE" ]; then
  echo "❌ Không tìm thấy $ENV_FILE" >&2
  exit 1
fi

# Nạp .env để có AI_DEFAULT_MODEL + AI_ALLOWED_MODELS mới nhất từ file
case "$ENV_FILE" in
  /*) ENV_FILE_PATH="$ENV_FILE" ;;
  *) ENV_FILE_PATH="./$ENV_FILE" ;;
esac

set -a
# shellcheck disable=SC1090
. "$ENV_FILE_PATH"
set +a

AI_DEFAULT_MODEL="${AI_DEFAULT_MODEL:-${DEFAULT_AI_MODEL:-}}"
AI_ALLOWED_MODELS="${AI_ALLOWED_MODELS:-}"

# E2E hạ tầng: harness đọc process.env bên trong container, không kế thừa env của host
E2E_WITH_INFRA="${E2E_WITH_INFRA:-}"
INFRA_FIXTURES_PATH="${INFRA_FIXTURES_PATH:-}"

if [ -z "$AI_DEFAULT_MODEL" ] || [ -z "$AI_ALLOWED_MODELS" ]; then
  echo "❌ Thiếu AI_DEFAULT_MODEL hoặc AI_ALLOWED_MODELS trong $ENV_FILE" >&2
  exit 2
fi

# Fail-fast: không cho chạy nếu .env tự mâu thuẫn (model sẽ bị fallback)
case ",$AI_ALLOWED_MODELS," in
  *",$AI_DEFAULT_MODEL,"*) ;;
  *)
    echo "❌ AI_DEFAULT_MODEL=\"$AI_DEFAULT_MODEL\" không thuộc AI_ALLOWED_MODELS=\"$AI_ALLOWED_MODELS\"" >&2
    echo "   Sửa $ENV_FILE trước khi chạy (nếu không script sẽ fallback sang model khác)." >&2
    exit 2
    ;;
esac

echo "▶️  Chạy scripts/$SCRIPT trong $CONTAINER"
echo "   AI_DEFAULT_MODEL=$AI_DEFAULT_MODEL"
echo "   AI_ALLOWED_MODELS=$AI_ALLOWED_MODELS"
echo "   E2E_WITH_INFRA=${E2E_WITH_INFRA:-0}${INFRA_FIXTURES_PATH:+ | INFRA_FIXTURES_PATH=$INFRA_FIXTURES_PATH}"

exec docker exec \
  -e AI_DEFAULT_MODEL="$AI_DEFAULT_MODEL" \
  -e DEFAULT_AI_MODEL="$AI_DEFAULT_MODEL" \
  -e AI_ALLOWED_MODELS="$AI_ALLOWED_MODELS" \
  -e E2E_WITH_INFRA="$E2E_WITH_INFRA" \
  -e INFRA_FIXTURES_PATH="$INFRA_FIXTURES_PATH" \
  "$CONTAINER" npx ts-node "scripts/$SCRIPT" "$@"
