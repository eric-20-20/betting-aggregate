# Data Quality Audit

Generated: `2026-03-30`

Scope:
- Local scan covers active row-based datasets under `data/ledger/` and `data/ledger/ncaab/`, with occurrence-based checks using `signals_occurrences.jsonl`.
- Supabase scan covers `signals`, `grades`, `graded_occurrences`, and `signal_sources`.

## 1. `market_type = player_prop` with invalid `selection`

### Local
- Total player-prop rows scanned: `500,605`
- Invalid selection rows: `1,066`
- Common failure modes:
  - `::CONFLICT` suffix instead of `::OVER|UNDER`
  - Plain player names such as `Brook Lopez`
  - Team or quarter pseudo-players such as `NBA:philadelphia_76ers::rebounds::UNDER` and `NBA:2nd_quarter_total::points::OVER`
  - SportsLine-style `selection="OVER"` with numeric `player_key`

Examples:
- `93422275ac519c86f0d1e0dae86412ffa77cd6700e598f128d5f6029e3d6a0d9` | `NBA:2026:01:26` | `NBA:shaedon_sharpe::points::CONFLICT`
- `68aa9568a9240f52472d767f7ac3d78d93526759de70c0a36a8556fdee470445` | `NBA:2026:03:07` | `Brook Lopez`
- `a3ae5c8acdefd77e5e7df3853d1506bf4b576012de8243697ab43a64611cb76a` | `NBA:2025:12:07` | `NBA:p_1q::points::OVER`

### Supabase
- Invalid selection rows in `signals`: `435`

Examples:
- `93422275ac519c86f0d1e0dae86412ffa77cd6700e598f128d5f6029e3d6a0d9` | `NBA:2026:01:26` | `NBA:shaedon_sharpe::points::CONFLICT`
- `06413c764a188c1f5fc20a95a75299dcacf626c8eec1bb1694148a640c885ecd` | `NBA:2026:03:25` | `Evan Mobley`
- `8554c88fbb0c288f9a3de0ad63c53ecef703bb1d4a18aa84474f17d473da3b8c` | `NBA:2026:02:25` | `OVER`

## 2. `result IN (WIN, LOSS)` with `stat_value IS NULL`

### Local
- WIN/LOSS rows scanned in grades ledgers: `46,748`
- WIN/LOSS rows missing `stat_value`: `27,277`
- Most examples are expected team markets like `spread`, but the count also includes player props where `stat_value` should exist.

Examples:
- `e81d394cee0b14175a47e5f339c375ce1a713cedf9669970658d037d46024074` | `WIN` | `spread` | `ORL`
- `9da738bb35ee23e37ec1133fb72246ef5385d73cb68263cd31949abde1b7bac1` | `LOSS` | `spread` | `BOS`

### Supabase
- WIN/LOSS rows missing `stat_value` in `grades`: `23,051`

Examples:
- `5e0d08d62cb3735e923536a15d7fa061c65d1f15c0b320707517258461b6b5cf` | `WIN` | `spread` | `POR`
- `fb51afd8eb8c29b9e586d32bcab7a9b051e69bed2bfc8291178ce94369f7914b` | `WIN` | `player_prop` | `NBA:brice_sensabaugh::points::OVER`
- `1599cea4150f22239d8257c28fe9c789af0e2506ac4053b629e10359527d2bc0` | `WIN` | `player_prop` | `NBA:luka_doncic::pts_reb_ast::UNDER`

## 3. Same `player + stat + direction + day_key` appears more than twice from the same source

### Local
- On current `signals_latest` ledgers: `272` duplicate clusters with count `> 2`
- Highest counts:
  - `betql` | `NBA:2025:02:12` | `NBA:draymond_green` | `rebounds` | `OVER` | count `5`
  - `betql` | `NBA:2025:02:20` | `NBA:evan_mobley` | `points` | `OVER` | count `5`
  - `betql` | `NBA:2025:04:08` | `NBA:ivica_zubac` | `points` | `UNDER` | count `5`
  - `action` | `NBA:2025:11:19` | `NBA:deni_avdija` | `points` | `OVER` | count `4`
  - `juicereel_sxebets` | `NBA:2026:03:24` | `NBA:maxime_raynaud` | `points` | `OVER` | count `5`

### Supabase
- Duplicate clusters in `graded_occurrences`: `199`

Highest counts:
- `covers` | `NBA:2026:02:02` | `NBA:ty_jerome` | `assists` | `OVER` | count `8`
- `bettingpros_experts` | `NBA:2026:03:12` | `NBA:anthony_edwards` | `points` | `OVER` | count `6`
- `bettingpros_experts` | `NBA:2026:03:15` | `NBA:shai_gilgeous_alexander` | `assists` | `OVER` | count `5`
- `bettingpros_experts` | `NBA:2026:03:18` | `NBA:de_aaron_fox` | `points` | `OVER` | count `5`

## 4. `event_key` values that do not match a real game date

Interpretation used here:
- `format_invalid`: not a canonical or legacy game event key
- `day_key_mismatch`: event key parses to a game date that does not match `day_key`

### Local
- Bad event rows across active ledgers: `4,986`
- Breakdown:
  - `format_invalid`: `4,969`
  - `day_key_mismatch`: `17`
- Main pattern: date-only keys like `NBA:2026:03:17` or `NCAAB:2026:02:16`, which are not real game keys.
- Mismatch examples are mostly NCAAB rows incorrectly labeled with an `NBA:` legacy event key:
  - `06a91c8cf3915a499d7762ee2094f48e8f1b951cd94447323694923a502e9db0` | `NBA:20260223:HOU@KAN:2300` vs `NCAAB:2026:02:23`
  - `96e953fe1806b873918c603194ee1d469c8ba32a97e028dfcd271e971cb455e7` | `NBA:20260224:NCST@UVA:2100` vs `NCAAB:2026:02:24`
  - `ef990d009b63978e3c62bf8dbb129ee2ed8d4d2eb1e7a2457cd54696a5538dfe` | `NBA:20260313:HALL@STJO:2130` vs `NCAAB:2026:03:13`

### Supabase
- `signals` bad event rows: `454` `format_invalid`
- `graded_occurrences` bad event rows: `439` `format_invalid`
- I did not find a surviving `day_key_mismatch` bucket in Supabase from the current query output.

## 5. Signals per source per month and anomalous volume spikes

Heuristic:
- Spike if count is at least `50` and greater than `max(2x median, median + 3*MAD)` for that source.

### Local occurrence ledgers

Top source-month counts:
- `betql` | `NBA:2025:03` | `102,970`
- `betql` | `NBA:2026:01` | `73,457`
- `betql` | `NBA:2025:12` | `51,000`
- `betql` | `NBA:2025:11` | `48,574`
- `betql` | `NBA:2025:02` | `47,186`
- `action` | `NBA:2026:01` | `34,189`
- `action` | `NBA:2025:11` | `31,319`
- `action` | `NBA:2025:12` | `27,789`
- `bettingpros_experts` | `NBA:2026:03` | `4,595`
- `oddstrader` | `NCAAB:2025:02` | `2,089`

Flagged spikes:
- `betql` | `NBA:2025:03` | `102,970` | median `38,244` | threshold `93,210`
- `action` | `NBA:2026:01` | `34,189` | median `1,101` | threshold `2,685`
- `action` | `NBA:2025:11` | `31,319` | median `1,101` | threshold `2,685`
- `action` | `NBA:2025:12` | `27,789` | median `1,101` | threshold `2,685`
- `bettingpros_experts` | `NBA:2026:03` | `4,595` | median `152` | threshold `446`
- `sportsline` | `NBA:2026:02` | `774` | median `87` | threshold `336`
- `covers` | `NBA:2026:03` | `549` | median `131` | threshold `488`
- `juicereel_nukethebooks` | `NBA:2026:03` | `402` | median `51` | threshold `177`

### Supabase (`signal_sources` joined to `signals`)

Top source-month counts:
- `betql` | `NBA:2025:03` | `5,206`
- `bettingpros_experts` | `NBA:2026:03` | `4,173`
- `betql` | `NBA:2026:01` | `3,900`
- `betql` | `NBA:2025:11` | `2,873`
- `betql` | `NBA:2025:12` | `2,807`
- `action` | `NBA:2026:01` | `2,418`
- `action` | `NBA:2025:11` | `2,152`
- `betql` | `NBA:2025:04` | `2,002`
- `oddstrader` | `NBA:2026:03` | `1,593`
- `covers` | `NBA:2026:03` | `650`

Flagged spikes:
- `betql` | `NBA:2025:03` | `5,206` | median `2,002` | threshold `4,417`
- `bettingpros_experts` | `NBA:2026:03` | `4,173` | median `152` | threshold `428`
- `oddstrader` | `NBA:2026:03` | `1,593` | median `477` | threshold `1,065`
- `dimers` | `NBA:2026:03` | `654` | median `129` | threshold `399`
- `covers` | `NBA:2026:03` | `650` | median `193.5` | threshold `582`
- `sportsline` | `NBA:2026:02` | `377` | median `26.5` | threshold `97`
- `juicereel_nukethebooks` | `NBA:2026:03` | `395` | median `17` | threshold `62`
- `juicereel_sxebets` | `NBA:2026:03` | `303` | median `12` | threshold `45`

## Summary

Highest-priority issues:
- Invalid player-prop selections are still present locally and in Supabase, especially `CONFLICT`, plain-name, and non-player pseudo-slug cases.
- `grades.stat_value` is null for a very large number of WIN/LOSS rows; some are normal team markets, but Supabase still contains player props with missing `stat_value`.
- Duplicate same-source player-prop clusters remain non-trivial in both local ledgers and Supabase.
- Event key problems are mostly format issues rather than impossible calendar dates; the NCAAB rows with `NBA:` legacy event keys are the clearest true mismatches.
