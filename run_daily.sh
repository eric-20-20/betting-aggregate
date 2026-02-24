#!/usr/bin/env bash
# run_daily.sh — Daily pipeline: ingest → normalize → consensus → ledger → grade → report
#
# Usage:
#   ./run_daily.sh            # run full pipeline (NBA + NCAAB)
#   ./run_daily.sh --nba-only # only run NBA pipeline
#   ./run_daily.sh --ncaab-only # only run NCAAB pipeline
#   ./run_daily.sh --skip-ingest  # skip scraping, just rebuild from existing data
#   ./run_daily.sh --debug    # verbose output from all scripts

set -euo pipefail
cd "$(dirname "$0")"

DEBUG=""
SKIP_INGEST=false
RUN_NBA=true
RUN_NCAAB=true

for arg in "$@"; do
    case "$arg" in
        --debug) DEBUG="--debug" ;;
        --skip-ingest) SKIP_INGEST=true ;;
        --nba-only) RUN_NCAAB=false ;;
        --ncaab-only) RUN_NBA=false ;;
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
BEFORE_NBA=0
BEFORE_NCAAB=0
if [ -f data/ledger/signals_latest.jsonl ]; then
    BEFORE_NBA=$(wc -l < data/ledger/signals_latest.jsonl | tr -d ' ')
fi
if [ -f data/ledger/ncaab/signals_latest.jsonl ]; then
    BEFORE_NCAAB=$(wc -l < data/ledger/ncaab/signals_latest.jsonl | tr -d ' ')
fi

# ═══════════════════════════════════════════════════════════════════════
#  NBA PIPELINE
# ═══════════════════════════════════════════════════════════════════════
if [ "$RUN_NBA" = true ]; then
echo ""
echo "╔═══════════════════════════════════════╗"
echo "║           NBA PIPELINE                ║"
echo "╚═══════════════════════════════════════╝"

# ─── Step 1: Ingest NBA picks ────────────────────────────────────────
if [ "$SKIP_INGEST" = false ]; then
    echo ""
    echo "── NBA Step 1: Ingesting picks ──"

    echo "  [1/10] Action Network..."
    python3 action_ingest.py $DEBUG 2>&1 | tail -3 || echo "  ⚠ Action ingest failed (continuing)"

    echo "  [2/10] Covers..."
    python3 covers_ingest.py $DEBUG 2>&1 | tail -3 || echo "  ⚠ Covers ingest failed (continuing)"

    echo "  [3/10] SportsLine..."
    if [ -f data/sportsline_storage_state.json ]; then
        python3 sportsline_ingest.py --storage data/sportsline_storage_state.json $DEBUG 2>&1 | tail -3 || echo "  ⚠ SportsLine ingest failed (continuing)"
    else
        echo "  ⚠ SportsLine skipped (no storage state at data/sportsline_storage_state.json)"
    fi

    echo "  [4/10] Dimers..."
    if [ -f dimers_storage_state.json ]; then
        python3 dimers_ingest.py --storage-state dimers_storage_state.json $DEBUG 2>&1 | tail -3 || echo "  ⚠ Dimers ingest failed (continuing)"
    else
        echo "  ⚠ Dimers skipped (no storage state at dimers_storage_state.json)"
    fi

    echo "  [5/10] OddsTrader..."
    python3 oddstrader_ingest.py --props $DEBUG 2>&1 | tail -3 || echo "  ⚠ OddsTrader ingest failed (continuing)"

    echo "  [6/10] BetQL Spreads..."
    if [ -f data/betql_storage_state.json ]; then
        python3 scripts/probe_betql_spread_nba.py $DEBUG 2>&1 | tail -3 || echo "  ⚠ BetQL Spreads failed (continuing)"
    else
        echo "  ⚠ BetQL Spreads skipped (no storage state)"
    fi

    echo "  [7/10] BetQL Totals..."
    if [ -f data/betql_storage_state.json ]; then
        python3 scripts/probe_betql_totals_nba.py $DEBUG 2>&1 | tail -3 || echo "  ⚠ BetQL Totals failed (continuing)"
    else
        echo "  ⚠ BetQL Totals skipped (no storage state)"
    fi

    echo "  [8/10] BetQL Sharp..."
    if [ -f data/betql_storage_state.json ]; then
        python3 scripts/probe_betql_sharp_nba.py $DEBUG 2>&1 | tail -3 || echo "  ⚠ BetQL Sharp failed (continuing)"
    else
        echo "  ⚠ BetQL Sharp skipped (no storage state)"
    fi

    echo "  [9/10] BetQL Props..."
    if [ -f data/betql_storage_state.json ]; then
        python3 scripts/probe_betql_prop_picks_nba.py $DEBUG 2>&1 | tail -3 || echo "  ⚠ BetQL Props failed (continuing)"
    else
        echo "  ⚠ BetQL Props skipped (no storage state)"
    fi

    echo "  [10/10] VegasInsider..."
    if [ -f data/vegasinsider_storage_state.json ]; then
        python3 vegasinsider_ingest.py --storage data/vegasinsider_storage_state.json $DEBUG 2>&1 | tail -3 || echo "  ⚠ VegasInsider ingest failed (continuing)"
    else
        echo "  ⚠ VegasInsider skipped (no storage state at data/vegasinsider_storage_state.json)"
    fi
else
    echo ""
    echo "── NBA Step 1: Skipped (--skip-ingest) ──"
fi

# ─── NBA Steps 2-7: Process pipeline ─────────────────────────────────
echo ""
echo "── NBA Step 2: Normalizing sources ──"
python3 scripts/normalize_sources_nba.py $DEBUG 2>&1 | tail -5

echo ""
echo "── NBA Step 3: Running consensus ──"
python3 consensus_nba.py $DEBUG 2>&1 | tail -5

echo ""
echo "── NBA Step 4: Building signal ledger ──"
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
    --include-oddstrader-history \
    --oddstrader-history-start 2025-10-22 \
    --oddstrader-history-end "$DATE" \
    --include-dimers-history \
    --include-normalized-jsonl data/latest/normalized_sportsline_nba_expert_picks.jsonl \
    --include-normalized-jsonl data/latest/normalized_covers_backfill.jsonl \
    2>&1 | tail -10

echo ""
echo "── NBA Step 5: Grading signals ──"
python3 scripts/grade_signals_nba.py $DEBUG 2>&1 | tail -10

echo ""
echo "── NBA Step 6: Building research dataset ──"
python3 scripts/build_research_dataset_nba_occurrences.py 2>&1 | tail -5

echo ""
echo "── NBA Step 7: Generating reports ──"
python3 scripts/report_records.py 2>&1 | tail -5
python3 scripts/report_trends.py 2>&1 | tail -5

echo ""
echo "── NBA Step 8: Scoring today's signals ──"
python3 scripts/score_signals.py 2>&1 | tail -20

echo ""
echo "── NBA Step 9: Auditing consensus integrity ──"
python3 scripts/audit_consensus.py 2>&1

echo ""
echo "── NBA Step 10: Exporting web data ──"
python3 scripts/export_web_data.py 2>&1 | tail -5

fi  # end RUN_NBA

# ═══════════════════════════════════════════════════════════════════════
#  NCAAB PIPELINE
# ═══════════════════════════════════════════════════════════════════════
if [ "$RUN_NCAAB" = true ]; then
echo ""
echo "╔═══════════════════════════════════════╗"
echo "║          NCAAB PIPELINE               ║"
echo "╚═══════════════════════════════════════╝"

# ─── NCAAB Step 1: Ingest ────────────────────────────────────────────
if [ "$SKIP_INGEST" = false ]; then
    echo ""
    echo "── NCAAB Step 1: Ingesting picks ──"

    echo "  [1/5] Action Network..."
    python3 action_ingest.py --sport NCAAB $DEBUG 2>&1 | tail -3 || echo "  ⚠ Action NCAAB failed (continuing)"

    echo "  [2/5] Covers..."
    python3 covers_ingest.py --sport NCAAB $DEBUG 2>&1 | tail -3 || echo "  ⚠ Covers NCAAB failed (continuing)"

    echo "  [3/5] SportsLine..."
    if [ -f data/sportsline_storage_state.json ]; then
        python3 sportsline_ingest.py --sport NCAAB --storage data/sportsline_storage_state.json $DEBUG 2>&1 | tail -3 || echo "  ⚠ SportsLine NCAAB failed (continuing)"
    else
        echo "  ⚠ SportsLine NCAAB skipped (no storage state)"
    fi

    echo "  [4/5] Dimers..."
    if [ -f dimers_storage_state.json ]; then
        python3 dimers_ingest.py --sport NCAAB --storage-state dimers_storage_state.json $DEBUG 2>&1 | tail -3 || echo "  ⚠ Dimers NCAAB failed (continuing)"
    else
        echo "  ⚠ Dimers NCAAB skipped (no storage state)"
    fi

    echo "  [5/5] OddsTrader..."
    python3 oddstrader_ingest.py --sport NCAAB $DEBUG 2>&1 | tail -3 || echo "  ⚠ OddsTrader NCAAB failed (continuing)"
else
    echo ""
    echo "── NCAAB Step 1: Skipped (--skip-ingest) ──"
fi

# ─── NCAAB Steps 2-5: Process pipeline ───────────────────────────────
echo ""
echo "── NCAAB Step 2: Running consensus ──"
python3 consensus_nba.py --sport NCAAB $DEBUG 2>&1 | tail -5

echo ""
echo "── NCAAB Step 3: Building signal ledger ──"
python3 scripts/build_signal_ledger.py --sport NCAAB $DEBUG 2>&1 | tail -10

echo ""
echo "── NCAAB Step 4: Grading signals ──"
python3 scripts/grade_signals_nba.py --sport NCAAB $DEBUG 2>&1 | tail -10

echo ""
echo "── NCAAB Step 5: Generating reports ──"
python3 scripts/report_records.py --sport NCAAB 2>&1 | tail -5

fi  # end RUN_NCAAB

# ─── Summary ─────────────────────────────────────────────────────────
AFTER_NBA=0
AFTER_NCAAB=0
if [ -f data/ledger/signals_latest.jsonl ]; then
    AFTER_NBA=$(wc -l < data/ledger/signals_latest.jsonl | tr -d ' ')
fi
if [ -f data/ledger/ncaab/signals_latest.jsonl ]; then
    AFTER_NCAAB=$(wc -l < data/ledger/ncaab/signals_latest.jsonl | tr -d ' ')
fi

echo ""
echo "========================================="
echo "  Pipeline Complete — ${SECONDS}s"
echo "========================================="
if [ "$RUN_NBA" = true ]; then
    NEW_NBA=$((AFTER_NBA - BEFORE_NBA))
    echo "  NBA:   $BEFORE_NBA → $AFTER_NBA (+$NEW_NBA)"
fi
if [ "$RUN_NCAAB" = true ]; then
    NEW_NCAAB=$((AFTER_NCAAB - BEFORE_NCAAB))
    echo "  NCAAB: $BEFORE_NCAAB → $AFTER_NCAAB (+$NEW_NCAAB)"
fi
echo "  Reports: data/reports/"
echo "========================================="
