#!/usr/bin/env bash
set -euo pipefail

# Usage: ./build.sh [--clean] <region1> [region2 ...] [<out_iso>]
# Examples:
#   ./build.sh europe/cyprus
#   ./build.sh europe/cyprus europe/spain
#   ./build.sh europe/cyprus europe/spain mymaps.iso
#   ./build.sh --clean
#
# NEW: Pass optional flags like --format-mode SDAL, --supp-lang DAN,ENG
#   ./build.sh europe/cyprus --format-mode SDAL --supp-lang ENG

# Change to the script’s directory (project root)
cd "$(dirname "$0")"

# Define the root of the project for paths
ROOT=$(pwd)

if [[ "${1-}" == "--clean" ]]; then
  echo "Performing cleanup..."
  # Remove Python bytecode caches
  rm -rf src/sdal_builder/__pycache__

  # Remove virtual environment
  rm -rf .venv

  # Remove build artifacts
  rm -rf build
  rm -rf work*
  rm -rf *.iso

  echo "Cleanup complete."
  exit 0
fi

# ----------------------------------------------------------------
# Аргументный парсер для разделения флагов и позиционных аргументов
# ----------------------------------------------------------------

POSITIONAL_ARGS=() # regions + optional out_iso
FLAG_ARGS=()       # --format-mode, --supp-lang, -v, etc.

for arg in "$@"; do
    # Проверяем, является ли аргумент флагом (начинается с - или --)
    if [[ "$arg" == --* ]] || [[ "$arg" == -* ]]; then
        FLAG_ARGS+=("$arg")
    else
        POSITIONAL_ARGS+=("$arg")
    fi
done

if [[ ${#POSITIONAL_ARGS[@]} -lt 1 ]]; then
  echo "Usage: $0 [--clean] <region1> [region2 ...] [<out_iso>]"
  echo "Note: Use --format-mode {OEM,SDAL} to switch file structure. Default is OEM."
  exit 1
fi

# Определяем, является ли последний позиционный аргумент именем ISO файла.
LAST_POS_ARG="${POSITIONAL_ARGS[@]: -1}"

if [[ "$LAST_POS_ARG" == *.iso ]]; then
  OUT="$LAST_POS_ARG"
  # Регионы - все позиционные аргументы, кроме последнего
  REGIONS=("${POSITIONAL_ARGS[@]:0:$((${#POSITIONAL_ARGS[@]}-1))}")
else
  REGIONS=("${POSITIONAL_ARGS[@]}")
  # Выводим имя OUT из первого региона
  SLUG="${REGIONS[0]##*/}"
  OUT="${SLUG}.iso"
fi

# Определяем рабочую директорию
WORK_DIR="${ROOT}/build"


# 1) Create & activate venv
python3 -m venv .venv
# shellcheck source=/dev/null
source .venv/bin/activate
echo "Venv activated. Python: $(which python)"

# 2) Install/update dependencies
.venv/bin/python3 -m pip install -e .

# 3) Execute the builder (передавая все флаги в конце)
echo "Building regions: ${REGIONS[*]} into $OUT (work_dir: $WORK_DIR)"

# Запускаем через wrapper sdal_build.py, который настраивает PYTHONPATH
# Используем безопасный синтаксис ${ARR[@]+"${ARR[@]}"} для macOS/Bash 3.2
python3 "$ROOT/sdal_build.py" "${REGIONS[@]}" --out "$OUT" --work "$WORK_DIR" ${FLAG_ARGS[@]+"${FLAG_ARGS[@]}"}

echo "Success! ISO image is available at $OUT"