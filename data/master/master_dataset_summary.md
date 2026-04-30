# Master Dataset Summary

**Generated:** 2026-04-30 18:35:11 UTC
**Date range filter:** 2026-04-10 to 2026-04-27
**MLB props included:** False

## Input Files

- pattern_registry: `/Users/Ericevans/Betting Aggregate/data/reports/pattern_registry_active.json` [FOUND]
- nba_graded_signals: `/Users/Ericevans/Betting Aggregate/data/analysis/graded_signals_latest.jsonl` [FOUND]
- nba_grades: `/Users/Ericevans/Betting Aggregate/data/ledger/grades_latest.jsonl` [FOUND]
- mlb_signals: `/Users/Ericevans/Betting Aggregate/data/ledger/mlb/signals_latest.jsonl` [FOUND]
- mlb_grades: `/Users/Ericevans/Betting Aggregate/data/ledger/mlb/grades_latest.jsonl` [FOUND]

## Row Counts

| Sport | Clean | Rejected | Total |
|-------|-------|----------|-------|
| NBA   | 1,349 | 471 | 1,820 |
| MLB   | 31 | 711 | 742 |
| **Total** | **1,380** | **1,182** | **2,562** |

## Date Ranges

- NBA: 2026-04-10 to 2026-04-22 (11 days)
- MLB: 2026-04-27 to 2026-04-27 (1 days)

## Rows by Source Combo (NBA)

- `bettingpros_props`: 433
- `oddstrader`: 341
- `action`: 104
- `covers`: 92
- `betql`: 88
- `sportsline`: 32
- `bettingpros_props|oddstrader`: 30
- `action|vegasinsider`: 24
- `action|bettingpros_props`: 23
- `bettingpros_props|covers`: 20
- `betql|oddstrader`: 18
- `bettingpros_props|sportsline`: 18
- `covers|sportsline`: 10
- `action|sportsline`: 10
- `action|betql|vegasinsider`: 10

## Rows by Source Combo (MLB)

- `action`: 13
- `action|covers`: 8
- `covers`: 6
- `action|covers|sportsline`: 2
- `oddstrader`: 2

## Rows by Market

- NBA player_prop: 1,221
- NBA spread: 82
- NBA moneyline: 23
- NBA total: 23
- MLB moneyline: 14
- MLB total: 12
- MLB spread: 5

## Performance Summary

- MLB moneyline: 4W-10L-0P (28.6%) units=-6.1 roi=-43.4%
- MLB spread: 1W-4L-0P (20.0%) units=-2.5 roi=-50.7%
- MLB total: 6W-6L-0P (50.0%) units=-0.4 roi=-3.2%
- NBA moneyline: 12W-11L-0P (52.2%) units=-1.7 roi=-7.5%
- NBA player_prop: 617W-601L-3P (50.5%) units=-50.4 roi=-4.1%
- NBA spread: 42W-40L-0P (51.2%) units=-1.5 roi=-1.8%
- NBA total: 11W-12L-0P (47.8%) units=-1.9 roi=-8.4%

## Top Quality Issues

- `not_roi_eligible`: 612
- `not_graded`: 289
- `error_status`: 188
- `ineligible`: 72
- `missing_event_key`: 21

## Readiness

- NBA: READY (1,349 clean rows)
- MLB: NOT READY (31 clean rows)
- **Master dataset READY_FOR_SOURCE_EVALUATION:** YES

## What Is Still Not Ready

- MLB: insufficient clean graded rows or player props not yet confidently graded
- MLB player props are excluded from clean set (use --include-mlb-props to include)
