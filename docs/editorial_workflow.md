# Editorial Review Workflow

Daily workflow for reviewing, overriding, and publishing picks.

## Overview

```
Pipeline auto-generates picks
    ↓
generate_review_slate.py → draft slate (JSON + CSV)
    ↓
Editor reviews slate, optionally writes override file
    ↓
apply_overrides.py → approved slate
    ↓
export_web_data.py --approved → website JSON files
    ↓
reconcile_export.py → verify match
    ↓
Deploy (git push)
```

## Automated Pipeline (run_daily.sh)

The pipeline runs these steps automatically in sequence:

1. `score_signals.py` → writes `data/plays/plays_YYYY-MM-DD.json`
2. `generate_review_slate.py` → writes `data/review/slate_YYYY-MM-DD.json` + `.csv`
3. `apply_overrides.py` → writes `data/review/approved_YYYY-MM-DD.json`
4. `export_web_data.py --approved` → writes web JSON files
5. `reconcile_export.py` → verifies approved = exported

If no override file exists, the approved slate equals the auto slate. The website publishes exactly what the pipeline computed.

## Manual Review Steps

### 1. Inspect the Draft Slate

After the pipeline runs, review the draft:

```bash
# View the JSON (pipe through jq or python for pretty-print)
cat data/review/slate_2026-04-20.json | python3 -m json.tool | less

# Or open the CSV in a spreadsheet
open data/review/slate_2026-04-20.csv
```

Each candidate row has:
- `auto_tier` — what the pipeline assigned (A/B/C/D)
- `tier_reason` — human-readable explanation of why
- `bet_label` — "LeBron James OVER 25.5 points"
- `wilson_score` — statistical edge score
- `support_count` — number of agreeing sources
- `pattern_name` — matched pattern (if any)
- `auto_included` — whether it would be published

### 2. Write Overrides (Optional)

Create `data/review/nba/YYYY-MM-DD/overrides.json`:

```json
{
  "overrides": [
    {
      "pick_id": "<signal_id from slate>",
      "published_tier": "B",
      "published_included": true,
      "override_note": "Promoting — line movement confirmed"
    }
  ]
}
```

Only include picks you want to change. All other picks fall back to auto values.

### 3. Apply Overrides and Re-export

```bash
python3 scripts/apply_overrides.py
python3 scripts/export_web_data.py --approved
python3 scripts/reconcile_export.py
```

### 4. Deploy

```bash
cd web && git add -A && git commit -m "Daily picks $(date +%Y-%m-%d)" && git push
```

## Common Override Actions

### Exclude a pick (e.g., key player ruled out)

```json
{
  "pick_id": "abc123...",
  "published_included": false,
  "override_note": "Key player ruled out post-pipeline"
}
```

### Promote a near-miss D-tier to B-tier

```json
{
  "pick_id": "def456...",
  "published_tier": "B",
  "published_included": true,
  "override_note": "Line moved 2 pts in our direction"
}
```

### Demote A-tier to B-tier

```json
{
  "pick_id": "ghi789...",
  "published_tier": "B",
  "override_note": "Only 2 sources, pattern is too new"
}
```

### Exclude multiple picks

```json
{
  "overrides": [
    {"pick_id": "abc...", "published_included": false, "override_note": "Injury"},
    {"pick_id": "def...", "published_included": false, "override_note": "Weather"}
  ]
}
```

## Override Validation

The system validates overrides strictly:

- `pick_id` must exist in the slate
- `published_tier` must be A, B, C, or D
- `published_included` must be true/false
- No duplicate pick_ids
- No unknown fields

Invalid overrides cause a non-zero exit — the pipeline stops before exporting bad data.

## File Locations

All review artifacts use a dated directory layout:

```
data/review/{sport}/{YYYY-MM-DD}/
  review_slate.json     ← generated review dataset
  review_slate.csv      ← spreadsheet-friendly version
  overrides.json        ← manual override file (you create this)
  approved_slate.json   ← resolved approved slate
```

| Path | Purpose |
|------|---------|
| `data/review/nba/2026-04-20/review_slate.json` | Draft review dataset |
| `data/review/nba/2026-04-20/review_slate.csv` | Spreadsheet-friendly version |
| `data/review/nba/2026-04-20/overrides.json` | Manual override file (you create) |
| `data/review/nba/2026-04-20/approved_slate.json` | Resolved approved slate |
| `data/review/ncaab/2026-04-20/` | Same structure for NCAAB |

## NCAAB

Same workflow with `--sport NCAAB`:

```bash
python3 scripts/generate_review_slate.py --sport NCAAB
python3 scripts/apply_overrides.py --sport NCAAB
python3 scripts/export_web_data.py --sport NCAAB --approved
python3 scripts/reconcile_export.py --sport NCAAB
```

## Quick Re-export (After Editing Overrides)

```bash
python3 scripts/apply_overrides.py && \
python3 scripts/export_web_data.py --approved && \
python3 scripts/reconcile_export.py
```

## Troubleshooting

**"Missing slate file"** — Run `generate_review_slate.py` first (or the full pipeline).

**"pick_id not found in slate"** — The override references a pick that doesn't exist in today's slate. Check the pick_id against the slate JSON.

**"Reconciliation mismatch"** — The approved slate and web export diverged. Re-run `apply_overrides.py` then `export_web_data.py --approved`.

**"No override file found"** — Normal. Published values fall back to auto. Only create an override file if you want to change something.

**Pipeline re-run after overrides** — If the pipeline re-runs (e.g., late source data), the slate regenerates and pick_ids may change. Old override files will fail validation. Re-review the new slate and update overrides.
