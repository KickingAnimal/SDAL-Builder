#!/usr/bin/env bash
set -euo pipefail

# Usage: ./build.sh [--clean] <region1> [region2 ...] [<out_iso>]
# Examples:
#   ./build.sh europe/cyprus
#   ./build.sh europe/cyprus europe/spain
#   ./build.sh europe/cyprus europe/spain mymaps.iso
#   ./build.sh --clean

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

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 [--clean] <region1> [region2 ...] [<out_iso>]"
  exit 1
fi

# Determine if last arg is an ISO filename (ends with .iso) or a region slug
if [[ "${@: -1}" == *.iso ]]; then
  OUT="${@: -1}"
  REGIONS=("${@:1:$(($#-1))}")
else
  REGIONS=("$@")
  # Derive OUT from the first region slug
  SLUG="${REGIONS[0]##*/}"
  OUT="${SLUG}.iso"
fi

# 1) Create & activate venv
python3 -m venv .venv
# shellcheck source=/dev/null
source .venv/bin/activate

# 2) Install dependencies
pip install -r requirements.txt

# 3) Define the working directory name (e.g., based on the output file)
# The working directory is now REQUIRED. We use a standardized name.
WORK_DIR="${ROOT}/build"

# 4) Run the builder
echo "Running sdal_build.py for ${#REGIONS[@]} regions: ${REGIONS[*]} -> $OUT (Work dir: $WORK_DIR)"

# THIS IS THE CORRECTED LINE: passing --work argument
python3 "$ROOT/sdal_build.py" "${REGIONS[@]}" --out "$OUT" --work "$WORK_DIR"