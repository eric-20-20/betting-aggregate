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

# Load environment variables (.env file for ODDS_API_KEY, etc.)
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

# Portable timeout: works on macOS (no GNU coreutils needed)
run_with_timeout() {
    local secs="$1"; shift
    "$@" &
    local pid=$!
    ( sleep "$secs" && kill "$pid" 2>/dev/null && echo "  ⚠ Killed after ${secs}s timeout" ) &
    local watchdog=$!
    wait "$pid" 2>/dev/null
    local rc=$?
    kill "$watchdog" 2>/dev/null
    wait "$watchdog" 2>/dev/null
    return $rc
}

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

# ─── Pre-flight: Auth check ────────────────────────────────────────
echo ""
echo "── Pre-flight: Checking auth tokens ──"
python3 scripts/verify_pipeline_auth.py 2>&1 || echo "  ⚠ Auth issues detected — some sources may return degraded data"

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
    run_with_timeout 120 python3 action_ingest.py $DEBUG 2>&1 | tail -5 || echo "  ⚠ Action ingest failed (continuing)"

    echo "  [2/10] Covers..."
    run_with_timeout 120 python3 covers_ingest.py $DEBUG 2>&1 | tail -5 || echo "  ⚠ Covers ingest failed (continuing)"

    echo "  [3/10] SportsLine..."
    python3 scripts/refresh_sportsline_token.py 2>&1 || echo "  ⚠ SportsLine token refresh failed (continuing)"
    if [ -f data/sportsline_storage_state.json ]; then
        run_with_timeout 180 python3 sportsline_ingest.py --storage data/sportsline_storage_state.json $DEBUG 2>&1 | tail -5 || echo "  ⚠ SportsLine ingest failed (continuing)"
    else
        echo "  ⚠ SportsLine skipped (storage state still missing after refresh)"
    fi

    echo "  [4/10] Dimers..."
    python3 scripts/refresh_dimers_token.py 2>&1 || echo "  ⚠ Dimers token refresh failed (continuing)"
    if [ -f dimers_storage_state.json ]; then
        run_with_timeout 180 python3 dimers_ingest.py --storage-state dimers_storage_state.json $DEBUG 2>&1 | tail -5 || echo "  ⚠ Dimers ingest failed (continuing)"
    else
        echo "  ⚠ Dimers skipped (storage state still missing after refresh)"
    fi

    echo "  [5/10] OddsTrader..."
    run_with_timeout 120 python3 oddstrader_ingest.py --props $DEBUG 2>&1 | tail -5 || echo "  ⚠ OddsTrader ingest failed (continuing)"

    echo "  [6/10] BetQL Spreads..."
    python3 scripts/refresh_betql_token.py 2>&1 || echo "  ⚠ BetQL token refresh failed (continuing)"
    if [ -f data/betql_storage_state.json ]; then
        run_with_timeout 120 python3 scripts/probe_betql_spread_nba.py $DEBUG 2>&1 | tail -5 || echo "  ⚠ BetQL Spreads failed (continuing)"
    else
        echo "  ⚠ BetQL Spreads skipped (storage state still missing after refresh)"
    fi

    echo "  [7/10] BetQL Totals..."
    if [ -f data/betql_storage_state.json ]; then
        run_with_timeout 120 python3 scripts/probe_betql_totals_nba.py $DEBUG 2>&1 | tail -5 || echo "  ⚠ BetQL Totals failed (continuing)"
    else
        echo "  ⚠ BetQL Totals skipped (no storage state)"
    fi

    echo "  [8/10] BetQL Sharp..."
    if [ -f data/betql_storage_state.json ]; then
        run_with_timeout 120 python3 scripts/probe_betql_sharp_nba.py $DEBUG 2>&1 | tail -5 || echo "  ⚠ BetQL Sharp failed (continuing)"
    else
        echo "  ⚠ BetQL Sharp skipped (no storage state)"
    fi

    echo "  [9/10] BetQL Props..."
    if [ -f data/betql_storage_state.json ]; then
        run_with_timeout 300 python3 scripts/probe_betql_prop_picks_nba.py $DEBUG 2>&1 | tail -5 || echo "  ⚠ BetQL Props failed (continuing)"
    else
        echo "  ⚠ BetQL Props skipped (no storage state)"
    fi

    echo "  [10/10] VegasInsider..."
    if [ -f data/vegasinsider_storage_state.json ]; then
        run_with_timeout 180 python3 vegasinsider_ingest.py --storage data/vegasinsider_storage_state.json $DEBUG 2>&1 | tail -5 || echo "  ⚠ VegasInsider ingest failed (continuing)"
    else
        echo "  ⚠ VegasInsider skipped (no storage state at data/vegasinsider_storage_state.json)"
    fi

    echo "  [11/11] JuiceReel..."
    python3 scripts/refresh_juicereel_token.py 2>&1 || echo "  ⚠ JuiceReel token refresh failed (continuing)"
    if [ -f data/juicereel_storage_state.json ]; then
        run_with_timeout 180 python3 juicereel_ingest.py --storage data/juicereel_storage_state.json $DEBUG 2>&1 | tail -5 || echo "  ⚠ JuiceReel ingest failed (continuing)"
    else
        echo "  ⚠ JuiceReel skipped (storage state still missing after refresh)"
    fi
else
    echo ""
    echo "── NBA Step 1: Skipped (--skip-ingest) ──"
fi

# ─── NBA Steps 2-7: Process pipeline ─────────────────────────────────
echo ""
echo "── NBA Step 2: Normalizing sources ──"
python3 scripts/normalize_sources_nba.py $DEBUG 2>&1 | tail -5

# ─── Data validation gate ─────────────────────────────────────────────
echo ""
echo "── Validating normalized data ──"
python3 -c "
import json, sys, os
sources = {
    'action': 'out/normalized_action_nba.json',
    'covers': 'out/normalized_covers_nba.json',
    'dimers': 'out/normalized_dimers_nba.json',
    'betql_prop': 'out/normalized_betql_prop_nba.json',
    'betql_spread': 'out/normalized_betql_spread_nba.json',
    'sportsline': 'out/normalized_sportsline_nba.json',
    'oddstrader': 'out/normalized_oddstrader_nba.json',
    'vegasinsider': 'out/normalized_vegasinsider_nba.json',
}
empty, active = [], []
for name, path in sources.items():
    if not os.path.exists(path):
        empty.append(name)
    else:
        try:
            data = json.load(open(path))
            n = len(data) if isinstance(data, list) else 0
        except Exception:
            n = 0
        if n == 0:
            empty.append(name)
        else:
            active.append(f'{name}({n})')
if active:
    print(f'  Active: {\"  \".join(active)}')
if empty:
    print(f'  Empty/missing: {\", \".join(empty)}')
if len(active) == 0:
    print('  CRITICAL: ALL sources empty — aborting pipeline')
    sys.exit(1)
" || exit 1

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
    --include-juicereel-history \
    --include-normalized-jsonl data/latest/normalized_sportsline_nba_expert_picks.jsonl \
    --include-normalized-jsonl data/latest/normalized_covers_backfill.jsonl \
    2>&1 | tail -10

echo ""
echo "── NBA Step 5: Grading signals ──"
python3 scripts/grade_signals_nba.py $DEBUG 2>&1 | tail -10

echo ""
echo "── NBA Step 6: Building research dataset ──"
python3 scripts/build_research_dataset_nba_occurrences.py 2>&1 | tail -5
python3 scripts/build_research_dataset_nba.py 2>&1 | tail -5

echo ""
echo "── NBA Step 7: Generating reports ──"
python3 scripts/report_records.py 2>&1 | tail -5
python3 scripts/report_trends.py 2>&1 | tail -5
python3 scripts/report_recent_trends.py 2>&1 | tail -5

echo ""
echo "── NBA Step 7b: Pattern health check ──"
python3 scripts/discover_patterns.py 2>&1 | tail -20 || echo "  (pattern check non-fatal)"

echo ""
echo "── NBA Step 8: Fetching current market lines ──"
if [ -n "${ODDS_API_KEY:-}" ]; then
    python3 scripts/fetch_current_lines.py 2>&1 | tail -5 || echo "  ⚠ Line fetch failed (continuing without market lines)"
else
    echo "  ⚠ ODDS_API_KEY not set (skipping market line fetch)"
fi

echo ""
echo "── NBA Step 9: Scoring today's signals ──"
python3 scripts/score_signals.py 2>&1 | tail -20

echo ""
echo "── NBA Step 10: Auditing consensus integrity ──"
python3 scripts/audit_consensus.py 2>&1 || echo "  (audit found issues — non-fatal)"

echo ""
echo "── NBA Step 11: Exporting web data ──"
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
    run_with_timeout 120 python3 action_ingest.py --sport NCAAB $DEBUG 2>&1 | tail -5 || echo "  ⚠ Action NCAAB failed (continuing)"

    echo "  [2/5] Covers..."
    run_with_timeout 120 python3 covers_ingest.py --sport NCAAB $DEBUG 2>&1 | tail -5 || echo "  ⚠ Covers NCAAB failed (continuing)"

    echo "  [3/5] SportsLine..."
    if [ -f data/sportsline_storage_state.json ]; then
        run_with_timeout 180 python3 sportsline_ingest.py --sport NCAAB --storage data/sportsline_storage_state.json $DEBUG 2>&1 | tail -5 || echo "  ⚠ SportsLine NCAAB failed (continuing)"
    else
        echo "  ⚠ SportsLine NCAAB skipped (no storage state)"
    fi

    echo "  [4/5] Dimers..."
    if [ -f dimers_storage_state.json ]; then
        run_with_timeout 180 python3 dimers_ingest.py --sport NCAAB --storage-state dimers_storage_state.json $DEBUG 2>&1 | tail -5 || echo "  ⚠ Dimers NCAAB failed (continuing)"
    else
        echo "  ⚠ Dimers NCAAB skipped (no storage state)"
    fi

    echo "  [5/5] OddsTrader..."
    run_with_timeout 120 python3 oddstrader_ingest.py --sport NCAAB $DEBUG 2>&1 | tail -5 || echo "  ⚠ OddsTrader NCAAB failed (continuing)"
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

echo ""
echo "── NCAAB Step 6: Scoring today's signals ──"
python3 scripts/score_signals.py --sport NCAAB $DEBUG 2>&1 | tail -20

echo ""
echo "── NCAAB Step 7: Exporting web data ──"
python3 scripts/export_web_data.py --sport NCAAB 2>&1 | tail -5

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

# Show auth summary in final output
echo ""
python3 scripts/verify_pipeline_auth.py 2>&1 | head -10 || true
echo "========================================="
