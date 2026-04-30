# Historical Review Workflow

How to review, correct, and approve picks for past dates.

## Purpose

The editorial review workflow supports both today's picks and historical dates. This lets you:
- Inspect past tier assignments after the fact
- Correct mistakes (e.g., a pick that should not have been A-tier)
- Backfill approved slates for dates that were published without review
- Build an audit trail of editorial decisions

## File Layout

All review artifacts live in dated directories:

```
data/review/
  nba/
    2026-04-20/
      review_slate.json       ← generated from plays file
      review_slate.csv        ← spreadsheet-friendly view
      overrides.json          ← manual corrections (optional)
      approved_slate.json     ← resolved with overrides applied
  ncaab/
    2026-04-20/
      ...
```

## Single-Date Workflow

### 1. Generate the review slate

```bash
python3 scripts/generate_review_slate.py --date 2026-03-15
```

This reads `data/plays/plays_2026-03-15.json` and produces:
- `data/review/nba/2026-03-15/review_slate.json`
- `data/review/nba/2026-03-15/review_slate.csv`

### 2. Inspect the slate

```bash
# Open in spreadsheet
open data/review/nba/2026-03-15/review_slate.csv

# Or inspect JSON
cat data/review/nba/2026-03-15/review_slate.json | python3 -m json.tool | less
```

Review the `tier_reason` field for each pick to understand the auto decision.

### 3. Write overrides (if corrections needed)

Create `data/review/nba/2026-03-15/overrides.json`:

```json
{
  "overrides": [
    {
      "pick_id": "<signal_id>",
      "published_tier": "B",
      "published_included": false,
      "override_note": "Retroactive correction: player was injured"
    }
  ]
}
```

### 4. Apply overrides and produce approved slate

```bash
python3 scripts/apply_overrides.py --date 2026-03-15
```

### 5. Reconcile against the existing export

```bash
python3 scripts/reconcile_export.py --date 2026-03-15
```

This will likely show a mismatch if overrides changed anything, since the web export file was generated from raw pipeline output. This is expected for historical corrections.

## Batch Workflow (Date Range)

### Generate slates for a range

```bash
python3 scripts/generate_review_slate.py --date 2026-03-01 --to 2026-03-31
```

Skips dates without plays files.

### Apply overrides for a range

```bash
python3 scripts/apply_overrides.py --date 2026-03-01 --to 2026-03-31
```

Skips dates without slates. Only applies overrides where override files exist.

### Reconcile a range

```bash
python3 scripts/reconcile_export.py --date 2026-03-01 --to 2026-03-31
```

### View audit summary

```bash
# CLI table
python3 scripts/review_audit_summary.py --date 2026-03-01 --to 2026-03-31

# JSON output
python3 scripts/review_audit_summary.py --date 2026-03-01 --to 2026-03-31 --json

# All available dates
python3 scripts/review_audit_summary.py
```

The summary shows for each date:
- Whether a slate, approved file, and export exist
- Override count, promotion/demotion counts
- Reconciliation status (ok, mismatch, or n/a)

## NCAAB

Add `--sport NCAAB` to any command:

```bash
python3 scripts/generate_review_slate.py --date 2026-03-15 --sport NCAAB
python3 scripts/apply_overrides.py --date 2026-03-15 --sport NCAAB
python3 scripts/review_audit_summary.py --sport NCAAB
```

## What Should and Should Not Be Changed Retroactively

### Safe to change:
- **Tier corrections**: Demoting a pick that should not have been A-tier
- **Exclusions**: Removing a pick that was published but shouldn't have been (e.g., injury info missed)
- **Promotions**: Adding a pick that was wrongly filtered
- **Notes**: Adding override_note to document the reason

### Should NOT be changed:
- **Raw pipeline outputs** (`data/plays/plays_*.json`) — these are immutable records of what the pipeline computed
- **Upstream scoring logic** — historical slates reflect the scoring as it was, not as it is now
- **Grade/result data** — WIN/LOSS outcomes are factual

### How auditability is preserved:
- `auto_tier` and `auto_included` are always preserved in the approved slate
- `published_tier` and `published_included` show the editorial decision
- `override_note` explains why
- The override file is a separate artifact from the slate
- The original plays file is never modified

## Systematic Review Process

To work through historical dates efficiently:

1. **Generate audit summary** to see which dates need attention:
   ```bash
   python3 scripts/review_audit_summary.py
   ```

2. **Generate slates** for dates that don't have them:
   ```bash
   python3 scripts/generate_review_slate.py --date 2026-01-01 --to 2026-04-22
   ```

3. **Apply overrides** (creates approved slates with auto fallback):
   ```bash
   python3 scripts/apply_overrides.py --date 2026-01-01 --to 2026-04-22
   ```

4. **Review the summary** again to identify mismatches:
   ```bash
   python3 scripts/review_audit_summary.py
   ```

5. For dates needing corrections, write override files and re-run:
   ```bash
   python3 scripts/apply_overrides.py --date 2026-03-15
   ```

## Historical Export (Re-publishing)

Re-exporting historical picks to the website is supported but opt-in:

```bash
python3 scripts/export_web_data.py --date 2026-03-15 --approved
```

This overwrites the web export file for that date. Only do this if you intend to republish corrected picks. The reconciliation check will verify the new export matches the approved slate.
