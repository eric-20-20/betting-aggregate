# Cross-Sport Pattern Audit Summary

Generated: 2026-04-28 20:52 UTC


## Data Coverage

- NBA graded rows: 45626 (11119 game, 34486 prop)
- MLB graded rows: 7319 (7314 game, 5 prop)

## Headline Win Rates (game markets only)
- NBA: 51.1%
- MLB: 50.3%

## Top 10 Strongest Patterns (by Wilson, n >= 50)
   1. [NBA] action|dimers|sportsline / spread — 54W-22L (71.0%) wilson=0.600 n=76 [actionable]
   2. [NBA] bettingpros_experts / player_prop — 3004W-1924L (61.0%) wilson=0.596 n=5066 [strong]
   3. [NBA] bettingpros_experts / moneyline — 321W-197L (62.0%) wilson=0.577 n=523 [strong]
   4. [NBA] bettingpros_experts|sportsline / player_prop — 49W-27L (64.5%) wilson=0.533 n=76 [actionable]
   5. [NBA] bettingpros_experts / total — 220W-163L (57.4%) wilson=0.524 n=383 [strong]
   6. [NBA] action / total — 576W-479L (54.6%) wilson=0.516 n=1059 [strong]
   7. [NBA] sportsline / player_prop — 1016W-884L (53.5%) wilson=0.512 n=1900 [strong]
   8. [NBA] sportsline / moneyline — 133W-99L (57.3%) wilson=0.509 n=232 [strong]
   9. [NBA] action / spread — 728W-641L (53.2%) wilson=0.505 n=1375 [strong]
  10. [MLB] sportsline / spread — 99W-73L (57.6%) wilson=0.501 n=177 [strong]

## Top 10 Weakest Patterns (by Wilson, n >= 30)
   1. [NBA] bettingpros_props|oddstrader / player_prop — 3W-27L (10.0%) wilson=0.035 n=30 [watchlist]
   2. [NBA] action|bettingpros_experts|dimers / spread — 8W-22L (26.7%) wilson=0.142 n=30 [watchlist]
   3. [NBA] action|dimers|sportsline / moneyline — 10W-28L (26.3%) wilson=0.150 n=38 [watchlist]
   4. [NBA] action|dimers|oddstrader / spread — 17W-32L (34.7%) wilson=0.229 n=49 [watchlist]
   5. [NBA] betql|dimers / player_prop — 12W-20L (37.5%) wilson=0.229 n=32 [watchlist]
   6. [NBA] bettingpros_experts|oddstrader / total — 12W-19L (38.7%) wilson=0.237 n=31 [watchlist]
   7. [NBA] action|juicereel_nukethebooks / player_prop — 12W-19L (38.7%) wilson=0.237 n=31 [watchlist]
   8. [NBA] covers|oddstrader / player_prop — 14W-16L (46.7%) wilson=0.302 n=30 [watchlist]
   9. [NBA] action|betql / total — 63W-102L (38.2%) wilson=0.311 n=165 [strong]
  10. [NBA] dimers / player_prop — 158W-264L (37.4%) wilson=0.330 n=422 [strong]

## A-Tier Risk List (wilson < 0.46, n >= 25)
  - [NBA] bettingpros_props|oddstrader / player_prop — 3W-27L (10.0%) wilson=0.035 [watchlist]
  - [NBA] action|bettingpros_experts|dimers / spread — 8W-22L (26.7%) wilson=0.142 [watchlist]
  - [NBA] action|dimers|sportsline / moneyline — 10W-28L (26.3%) wilson=0.150 [watchlist]
  - [NBA] betql|dimers / spread — 8W-17L (32.0%) wilson=0.172 [watchlist]
  - [NBA] action|dimers|oddstrader / spread — 17W-32L (34.7%) wilson=0.229 [watchlist]
  - [NBA] betql|dimers / player_prop — 12W-20L (37.5%) wilson=0.229 [watchlist]
  - [NBA] juicereel_nukethebooks|juicereel_sxebets / player_prop — 10W-15L (40.0%) wilson=0.234 [watchlist]
  - [NBA] bettingpros_experts|oddstrader / total — 12W-19L (38.7%) wilson=0.237 [watchlist]
  - [NBA] action|juicereel_nukethebooks / player_prop — 12W-19L (38.7%) wilson=0.237 [watchlist]
  - [NBA] covers / spread — 12W-14L (46.2%) wilson=0.288 [watchlist]

## Market Observations
  - [MLB] moneyline                1931W-1900L (50.4%) wilson=0.488 n=3831
  - [MLB] player_prop              4W-1L (80.0%) wilson=0.376 n=5
  - [MLB] spread                   503W-541L (48.2%) wilson=0.452 n=1066
  - [MLB] total                    1192W-1138L (51.2%) wilson=0.491 n=2417
  - [NBA] 1st_half_spread          0W-1L (0.0%) wilson=0.000 n=1
  - [NBA] moneyline                1205W-1169L (50.8%) wilson=0.487 n=2380
  - [NBA] player_prop              17727W-16573L (51.7%) wilson=0.511 n=34486
  - [NBA] spread                   2776W-2615L (51.5%) wilson=0.502 n=5443
  - [NBA] team_total               13W-7L (65.0%) wilson=0.433 n=20
  - [NBA] total                    1656W-1621L (50.5%) wilson=0.488 n=3296

## Sport-Specific Divergences (|delta_wilson| > 0.05)
  - action|covers                            NBA=51.2% (n=41) MLB=50.0% (n=10) delta=0.1283
  - action|sportsline                        NBA=52.8% (n=475) MLB=47.8% (n=178) delta=0.0781

## Recommended Next Changes (do NOT apply automatically)
  1. Review A-tier risk list — any pattern with wilson < 0.46 at n >= 50 should be investigated
  2. Review sport-specific divergences — patterns that work in one sport but not the other
  3. MLB player props (n=5) need player name matching fix before inclusion
  4. Run this audit weekly as more MLB data accumulates
  5. Do NOT change production scoring until at least 2 weeks of clean MLB grading data
