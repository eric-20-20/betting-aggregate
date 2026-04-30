#!/usr/bin/env bash
# run_mlb.sh — MLB v1 pipeline: ingest -> consensus -> ledger

set -euo pipefail
cd "$(dirname "$0")"

DEBUG=""
SKIP_INGEST=false
INCLUDE_ACTION_HISTORY=false

for arg in "$@"; do
    case "$arg" in
        --debug) DEBUG="--debug" ;;
        --skip-ingest) SKIP_INGEST=true ;;
        --include-action-history) INCLUDE_ACTION_HISTORY=true ;;
        *) echo "Unknown arg: $arg"; exit 1 ;;
    esac
done

DATE=$(date +%Y-%m-%d)
echo "========================================="
echo "  MLB Pipeline — $DATE"
echo "========================================="

if [ "$SKIP_INGEST" = false ]; then
    echo ""
    echo "── MLB Step 1: Ingesting picks ──"
    python3 action_ingest.py --sport MLB $DEBUG
    python3 covers_ingest.py --sport MLB $DEBUG
    python3 oddstrader_ingest.py --sport MLB $DEBUG
    python3 scripts/bettingpros_prop_bets_ingest_mlb.py --storage data/bettingpros_storage_state.json $DEBUG || echo "  ⚠ BettingPros MLB props ingest failed"
    python3 sportsline_ingest.py --sport MLB --storage data/sportsline_storage_state.json $DEBUG || echo "  ⚠ SportsLine MLB ingest failed"
    python3 dimers_ingest.py --sport MLB --bets-only --storage-state dimers_storage_state.json $DEBUG || echo "  ⚠ Dimers MLB ingest failed"
    python3 vegasinsider_ingest.py --sport MLB --storage data/vegasinsider_storage_state.json $DEBUG || echo "  ⚠ VegasInsider MLB ingest failed"
else
    echo ""
    echo "── MLB Step 1: Skipped (--skip-ingest) ──"
fi

echo ""
echo "── MLB Step 2: Running consensus ──"
python3 consensus_nba.py --sport MLB $DEBUG

echo ""
echo "── MLB Step 3: Building signal ledger ──"
LEDGER_ARGS=(--sport MLB)
if [ -n "$DEBUG" ]; then
    LEDGER_ARGS+=("$DEBUG")
fi
if [ "$INCLUDE_ACTION_HISTORY" = true ]; then
    LEDGER_ARGS+=(--include-action-history)
fi
python3 scripts/build_signal_ledger.py "${LEDGER_ARGS[@]}"

echo ""
echo "MLB pipeline complete."
