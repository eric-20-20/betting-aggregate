# Pattern Policy Review

Generated: 2026-04-28 20:58 UTC

**WARNING: No production scoring changes have been applied.**
This is a read-only review. Edit `manual_decision` and `manual_note` columns
in `pattern_policy_candidates.csv` to record your decisions.

## Policy Distribution

| Policy | Count |
|--------|------:|
| needs_more_sample | 313 |
| exclude_from_auto_a | 42 |
| keep_auto_a | 20 |
| watchlist_only | 15 |

## Audit Flag Distribution

| Flag | Count |
|------|------:|
| low_sample | 311 |
| marginal | 18 |
| mlb_immature | 14 |
| below_breakeven | 11 |
| acceptable | 10 |
| very_weak | 9 |
| strong | 5 |
| solid | 5 |
| severe_loser | 4 |
| promising_low_n | 2 |
| mlb_watchlist | 1 |

## Top Proposed Exclusions (42 total)

These patterns have wilson below acceptable thresholds and should not be in A-tier.

- **[NBA] bettingpros_props|oddstrader / player_prop** — 3W-27L (10.0%) wilson=0.0346 n=30 — _Severe: wilson=0.035, 3W-27L_
- **[NBA] action|bettingpros_experts|dimers / spread** — 8W-22L (26.7%) wilson=0.1418 n=30 — _Severe: wilson=0.142, 8W-22L_
- **[NBA] action|dimers|sportsline / moneyline** — 10W-28L (26.3%) wilson=0.1497 n=38 — _Severe: wilson=0.150, 10W-28L_
- **[NBA] betql|dimers / spread** — 8W-17L (32.0%) wilson=0.172 n=25 — _Severe: wilson=0.172, 8W-17L_
- **[NBA] action|dimers|oddstrader / spread** — 17W-32L (34.7%) wilson=0.2292 n=49 — _Very weak: wilson=0.229, 17W-32L_
- **[NBA] betql|dimers / player_prop** — 12W-20L (37.5%) wilson=0.2293 n=32 — _Very weak: wilson=0.229, 12W-20L_
- **[NBA] juicereel_nukethebooks|juicereel_sxebets / player_prop** — 10W-15L (40.0%) wilson=0.234 n=27 — _Marginal: wilson=0.234, insufficient edge_
- **[NBA] bettingpros_experts|oddstrader / total** — 12W-19L (38.7%) wilson=0.2373 n=31 — _Very weak: wilson=0.237, 12W-19L_
- **[NBA] action|juicereel_nukethebooks / player_prop** — 12W-19L (38.7%) wilson=0.2373 n=31 — _Very weak: wilson=0.237, 12W-19L_
- **[NBA] covers / spread** — 12W-14L (46.2%) wilson=0.2876 n=26 — _Marginal: wilson=0.288, insufficient edge_
- **[NBA] sportsline / total** — 13W-15L (46.4%) wilson=0.2953 n=28 — _Marginal: wilson=0.295, insufficient edge_
- **[NBA] covers|oddstrader / player_prop** — 14W-16L (46.7%) wilson=0.3023 n=30 — _Very weak: wilson=0.302, 14W-16L_
- **[NBA] action|betql / total** — 63W-102L (38.2%) wilson=0.3112 n=165 — _Very weak: wilson=0.311, 63W-102L_
- **[NBA] dimers / player_prop** — 158W-264L (37.4%) wilson=0.3296 n=422 — _Very weak: wilson=0.330, 158W-264L_
- **[NBA] bettingpros_experts|oddstrader / spread** — 17W-17L (50.0%) wilson=0.3407 n=34 — _Very weak: wilson=0.341, 17W-17L_

## Top Proposed Keeps (20 total)

These patterns have demonstrated positive edge at sufficient sample.

- **[NBA] action|dimers|sportsline / spread** — 54W-22L (71.0%) wilson=0.6004 n=76 [actionable]
- **[NBA] bettingpros_experts / player_prop** — 3004W-1924L (61.0%) wilson=0.5959 n=5066 [strong]
- **[NBA] bettingpros_experts / moneyline** — 321W-197L (62.0%) wilson=0.5771 n=523 [strong]
- **[NBA] bettingpros_experts|sportsline / player_prop** — 49W-27L (64.5%) wilson=0.5326 n=76 [actionable]
- **[NBA] bettingpros_experts / total** — 220W-163L (57.4%) wilson=0.5244 n=383 [strong]
- **[NBA] action / total** — 576W-479L (54.6%) wilson=0.5158 n=1059 [strong]
- **[NBA] sportsline / player_prop** — 1016W-884L (53.5%) wilson=0.5123 n=1900 [strong]
- **[NBA] sportsline / moneyline** — 133W-99L (57.3%) wilson=0.5089 n=232 [strong]
- **[NBA] action / spread** — 728W-641L (53.2%) wilson=0.5053 n=1375 [strong]
- **[NBA] juicereel_nukethebooks / player_prop** — 74W-52L (58.7%) wilson=0.5 n=126 [actionable]
- **[NBA] action|juicereel_sxebets / player_prop** — 33W-19L (63.5%) wilson=0.4987 n=52 [watchlist]
- **[NBA] oddstrader / player_prop** — 1013W-947L (51.7%) wilson=0.4947 n=1960 [strong]
- **[NBA] action / player_prop** — 2864W-2800L (50.6%) wilson=0.4926 n=5673 [strong]
- **[NBA] action|betql / player_prop** — 494W-452L (52.2%) wilson=0.4903 n=961 [strong]
- **[NBA] betql / player_prop** — 7949W-8130L (49.4%) wilson=0.4866 n=16088 [strong]

## MLB Status (15 patterns)

- watchlist_only: 15

**No MLB patterns are recommended for A-tier promotion at this time.**
MLB data is too young for production scoring decisions.

## What Requires Your Manual Review

1. **Exclusion list**: Confirm the proposed exclusions are correct before applying
2. **Sport-specific patterns**: Decide whether to split scoring by sport
3. **Keep list**: Verify strong patterns aren't benefiting from data bias
4. **MLB watchlist**: Decide when to re-evaluate (recommended: after 4+ weeks of grading)
5. **Edge cases**: Patterns with wilson 0.42-0.46 need judgment call

## How to Apply Decisions

1. Open `pattern_policy_candidates.csv`
2. Fill in `manual_decision` column with your chosen policy
3. Add notes in `manual_note` column
4. Save and share for implementation
5. A separate script will read your decisions and update the pattern registry

**No changes have been made to production scoring.**
