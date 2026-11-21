#!/bin/sh
# Entry point. Runs with no arguments; exit 0 means "done, check my output".
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR
# Read-only root filesystem: everything that wants a temp or home dir gets /scratch.
export TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR" POLARS_TEMP_DIR="$SCRATCH_DIR"

if [ -f /app/pipeline_bin ] && [ -x /app/pipeline_bin ]; then
    exec /app/pipeline_bin "$DATA_DIR" "$OUTPUT_DIR"
else
    exec python3 /app/pipeline/pipeline.py
fi
