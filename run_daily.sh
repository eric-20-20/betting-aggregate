#!/usr/bin/env bash
# run_daily.sh — Daily pipeline: ingest → normalize → consensus → ledger → grade → report
#
# Usage:
#   ./run_daily.sh            # run full pipeline
#   ./run_daily.sh --skip-ingest  # skip scraping, just rebuild from existing data
#   ./run_daily.sh --debug    # verbose output from all scripts

set -euo pipefail
cd "$(dirname "$0")"

DEBUG=""
SKIP_INGEST=false

for arg in "$@"; do
    case "$arg" in
        --debug) DEBUG="--debug" ;;
        --skip-ingest) SKIP_INGEST=true ;;
        *) echo "Unknown arg: $arg"; exit 1 ;;
    esac
done

DATE=$(date +%Y-%m-%d)
echo "========================================="
echo "  Daily Pipeline — $DATE"
echo "========================================="

# Track timing
SECONDS=0

# Count signals before run
BEFORE_SIGNALS=0
if [ -f data/ledger/signals_latest.jsonl ]; then
    BEFORE_SIGNALS=$(wc -l < data/ledger/signals_latest.jsonl | tr -d ' ')
fi

# ─── Step 1: Ingest picks from all sources ───────────────────────────
if [ "$SKIP_INGEST" = false ]; then
    echo ""
    echo "── Step 1: Ingesting picks ──"

    # These can fail independently without killing the whole pipeline
    echo "  [1/8] Action Network..."
    python3 action_ingest.py $DEBUG 2>&1 | tail -3 || echo "  ⚠ Action ingest failed (continuing)"

    echo "  [2/8] Covers..."
    python3 covers_ingest.py $DEBUG 2>&1 | tail -3 || echo "  ⚠ Covers ingest failed (continuing)"

    echo "  [3/8] SportsLine..."
    if [ -f data/sportsline_storage_state.json ]; then
        python3 sportsline_ingest.py --storage data/sportsline_storage_state.json $DEBUG 2>&1 | tail -3 || echo "  ⚠ SportsLine ingest failed (continuing)"
    else
        echo "  ⚠ SportsLine skipped (no storage state at data/sportsline_storage_state.json)"
    fi

    echo "  [4/8] Dimers..."
    if [ -f dimers_storage_state.json ]; then
        python3 dimers_ingest.py --storage-state dimers_storage_state.json $DEBUG 2>&1 | tail -3 || echo "  ⚠ Dimers ingest failed (continuing)"
    else
        echo "  ⚠ Dimers skipped (no storage state at dimers_storage_state.json)"
    fi

    echo "  [5/8] BetQL Spreads..."
    if [ -f data/betql_storage_state.json ]; then
        python3 scripts/probe_betql_spread_nba.py $DEBUG 2>&1 | tail -3 || echo "  ⚠ BetQL Spreads failed (continuing)"
    else
        echo "  ⚠ BetQL Spreads skipped (no storage state at data/betql_storage_state.json)"
    fi

    echo "  [6/8] BetQL Totals..."
    if [ -f data/betql_storage_state.json ]; then
        python3 scripts/probe_betql_totals_nba.py $DEBUG 2>&1 | tail -3 || echo "  ⚠ BetQL Totals failed (continuing)"
    else
        echo "  ⚠ BetQL Totals skipped (no storage state)"
    fi

    echo "  [7/8] BetQL Sharp..."
    if [ -f data/betql_storage_state.json ]; then
        python3 scripts/probe_betql_sharp_nba.py $DEBUG 2>&1 | tail -3 || echo "  ⚠ BetQL Sharp failed (continuing)"
    else
        echo "  ⚠ BetQL Sharp skipped (no storage state)"
    fi

    echo "  [8/8] BetQL Props..."
    if [ -f data/betql_storage_state.json ]; then
        python3 scripts/probe_betql_prop_picks_nba.py $DEBUG 2>&1 | tail -3 || echo "  ⚠ BetQL Props failed (continuing)"
    else
        echo "  ⚠ BetQL Props skipped (no storage state)"
    fi
else
    echo ""
    echo "── Step 1: Skipped (--skip-ingest) ──"
fi

# ─── Step 2: Normalize all sources ───────────────────────────────────
echo ""
echo "── Step 2: Normalizing sources ──"
python3 scripts/normalize_sources_nba.py $DEBUG 2>&1 | tail -5

# ─── Step 3: Run consensus ───────────────────────────────────────────
echo ""
echo "── Step 3: Running consensus ──"
python3 consensus_nba.py $DEBUG 2>&1 | tail -5

# ─── Step 4: Build signal ledger ─────────────────────────────────────
echo ""
echo "── Step 4: Building signal ledger ──"
python3 scripts/build_signal_ledger.py $DEBUG \
    --include-betql-history \
    --history-root data/history/betql_normalized \
    --history-start 2025-02-09 \
    --history-end "$DATE" \
    --include-betql-game-props-history \
    --props-history-root data/history/betql_normalized/props \
    --props-history-start 2025-02-09 \
    --props-history-end "$DATE" \
    --include-action-history \
    2>&1 | tail -10

# ─── Step 5: Grade signals ──────────────────────────────────────────
echo ""
echo "── Step 5: Grading signals ──"
python3 scripts/grade_signals_nba.py $DEBUG 2>&1 | tail -10

# ─── Step 6: Build research dataset ─────────────────────────────────
echo ""
echo "── Step 6: Building research dataset ──"
python3 scripts/build_research_dataset_nba_occurrences.py 2>&1 | tail -5

# ─── Step 7: Generate reports ────────────────────────────────────────
echo ""
echo "── Step 7: Generating reports ──"
python3 scripts/report_records.py 2>&1 | tail -5

# ─── Summary ─────────────────────────────────────────────────────────
AFTER_SIGNALS=0
if [ -f data/ledger/signals_latest.jsonl ]; then
    AFTER_SIGNALS=$(wc -l < data/ledger/signals_latest.jsonl | tr -d ' ')
fi
NEW_SIGNALS=$((AFTER_SIGNALS - BEFORE_SIGNALS))

echo ""
echo "========================================="
echo "  Pipeline Complete — ${SECONDS}s"
echo "========================================="
echo "  Signals: $BEFORE_SIGNALS → $AFTER_SIGNALS (+$NEW_SIGNALS new)"
echo "  Reports: data/reports/"
echo "========================================="
