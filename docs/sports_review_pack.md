# Sports Review Pack

`scripts/export_sports_review_pack.py` builds a read-only NBA/MLB inspection pack under `data/reports/sports_review_pack/`.

It does not modify pipeline behavior, does not enable MLB production, does not run pattern analysis, and does not grade MLB.

## Output Layout

- `data/reports/sports_review_pack/NBA/`
- `data/reports/sports_review_pack/MLB/`
- `data/reports/sports_review_pack/combined/`

Per sport:

- `source_coverage.csv`: one row per discovered input file. Use this to see file presence, row counts, eligible counts, date span, market mix, and missing key coverage.
- `normalized_picks.csv`: one row per normalized input pick in the requested date window.
- `signals_latest.csv`: one row per latest/current signal, sourced from ledger when available and consensus fallback otherwise.
- `cross_source_signals.csv`: only multi-source or explicit cross-source signals.
- `player_props.csv`: only normalized player props.
- `data_quality_issues.csv`: validation findings, including wrong-sport prefixes, missing keys, noncanonical teams, duplicate IDs, stale data, and low coverage.

Combined:

- `combined/source_coverage.csv`
- `combined/signals_latest.csv`
- `combined/cross_source_signals.csv`
- `combined/player_props.csv`
- `combined/data_quality_issues.csv`

## Usage

Run both sports:

```bash
python3 scripts/export_sports_review_pack.py --sport ALL --date 2026-04-10 --to 2026-04-27
```

Run NBA only:

```bash
python3 scripts/export_sports_review_pack.py --sport NBA --date 2026-04-10 --to 2026-04-27
```

Run MLB only:

```bash
python3 scripts/export_sports_review_pack.py --sport MLB --date 2026-04-10 --to 2026-04-27
```

Optional flags:

- `--out-dir`: override output root.
- `--include-normalized`
- `--include-ledger`
- `--include-consensus`

## Quality Status

Each run prints a per-sport quality status:

- `CLEAN`: no warnings or failures.
- `WARN`: data is readable, but there are coverage, freshness, or key-quality concerns.
- `FAIL`: wrong-sport prefixes or MLB noncanonical team codes were found.

## Interpreting Data Quality Issues

Common issue types:

- `wrong sport prefix`
- `NBA prefix in MLB`
- `MLB prefix in NBA`
- `missing event_key`
- `missing matchup_key`
- `missing player_key on prop`
- `noncanonical team code`
- `duplicate signal IDs`
- `duplicate pick IDs`
- `unknown market type`
- `null selection`
- `null line where line is required`
- `suspiciously low source coverage`
- `latest data is stale`
- `date outside requested range`

## What To Fix Before Pattern Analysis

Before any pattern audit:

- eliminate wrong-sport prefixes in `event_key`, `day_key`, `player_key`, and `selection`
- fix any MLB noncanonical team codes
- restore missing required source coverage
- improve `matchup_key` coverage on rows where home/away teams are identifiable
- resolve stale latest-data gaps where the latest normalized or ledger date is older than the requested end date

If the report status is `FAIL`, pattern analysis should wait.
