#!/bin/bash
#SBATCH --job-name=qaqc_report
#SBATCH --output=logs/qaqc_%j.out
#SBATCH --error=logs/qaqc_%j.err
#SBATCH --time=12:00:00
#SBATCH --mem=15G
#SBATCH --cpus-per-task=2
#SBATCH --partition=r9          # <-- change to your cluster's partition: On Channing, use "r9" or "r9_12h"
# #SBATCH --array=0-99          # Uncomment if supplying a list of CSV inputs to run QA/QC on multiple linkages simultaneously
 
# ── Usage ──────────────────────────────────────────────────────────────────────
# sbatch run_qaqc.sh \
#   --input    /path/to/input.csv \
#   --baseline /path/to/baseline.csv \
#   --rmd      /path/to/qaqc_report.Rmd \
#   --outdir   /path/to/output/dir
# ───────────────────────────────────────────────────────────────────────────────
 
set -euo pipefail
 
# ---------- defaults ----------------------------------------------------------
INPUT_CSV=""
BASELINE_CSV=""
RMD_SCRIPT="$(dirname "$0")/qaqc_report.Rmd"
OUTPUT_DIR="$(pwd)/reports"
 
# ---------- argument parsing --------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --input)    INPUT_CSV="$2";    shift 2 ;;
    --baseline) BASELINE_CSV="$2"; shift 2 ;;
    --rmd)      RMD_SCRIPT="$2";   shift 2 ;;
    --outdir)   OUTPUT_DIR="$2";   shift 2 ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done
 
# ---------- validation --------------------------------------------------------
[[ -z "$INPUT_CSV"    ]] && { echo "ERROR: --input is required";    exit 1; }
[[ -z "$BASELINE_CSV" ]] && { echo "ERROR: --baseline is required"; exit 1; }
[[ ! -f "$INPUT_CSV"    ]] && { echo "ERROR: input CSV not found: $INPUT_CSV";       exit 1; }
[[ ! -f "$BASELINE_CSV" ]] && { echo "ERROR: baseline CSV not found: $BASELINE_CSV"; exit 1; }
[[ ! -f "$RMD_SCRIPT"   ]] && { echo "ERROR: Rmd script not found: $RMD_SCRIPT";     exit 1; }
 
mkdir -p "$OUTPUT_DIR" logs
 
# ---------- environment -------------------------------------------------------
# Load R module — adjust the module name to match your HPC setup
# Common variants: R/4.3.0, r/4.3.1-foss-2023a, etc.
module load terra/4.5.2 2>/dev/null || {
  echo "WARNING: 'module load terra/4.5.2' failed — assuming R is already on PATH"
}
 
# Optional: load a TeX distribution for PDF rendering
# module load texlive/2023  # uncomment if needed
 
# ---------- run ---------------------------------------------------------------
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_PDF="${OUTPUT_DIR}/qaqc_report_${SLURM_JOB_ID:-local}_${TIMESTAMP}.pdf"
 
echo "=========================================="
echo "  QA/QC Report — SLURM Job ${SLURM_JOB_ID:-local}"
echo "=========================================="
echo "  Input CSV   : $INPUT_CSV"
echo "  Baseline CSV: $BASELINE_CSV"
echo "  Rmd script  : $RMD_SCRIPT"
echo "  Output PDF  : $OUTPUT_PDF"
echo "  Node        : $(hostname)"
echo "  Started     : $(date)"
echo "=========================================="
 
# Pass paths to R via environment variables (picked up inside the .Rmd)
export QAQC_INPUT_CSV="$INPUT_CSV"
export QAQC_BASELINE_CSV="$BASELINE_CSV"
 
Rscript - <<RSCRIPT
rmarkdown::render(
  input       = "${RMD_SCRIPT}",
  output_file = "${OUTPUT_PDF}",
  envir       = new.env(parent = globalenv()),
  quiet       = FALSE
)
cat("PDF written to: ${OUTPUT_PDF}\n")
RSCRIPT
 
echo "=========================================="
echo "  Finished: $(date)"
echo "  PDF: $OUTPUT_PDF"
echo "=========================================="
