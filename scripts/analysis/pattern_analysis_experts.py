"""
Comprehensive pattern analysis against Supabase.

Sections:
  1. Expert Baseline Records
  2. Expert x Market Type
  3. Expert Consensus
  4. Source Consensus
  5. Expert x Team
  6. Expert x Player (props only)
  7. Market Type Consensus
  8. Score/Confidence Threshold
  9. Recency Split (last 60 days)
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from itertools import combinations

sys.path.insert(0, "/Users/Ericevans/Betting Aggregate")

import pandas as pd

from src.supabase_writer import get_client

# ─────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────
TODAY = date(2026, 3, 18)
CUTOFF_60 = (TODAY - timedelta(days=60)).isoformat()  # 2026-01-17
MIN_N = 30
MIN_N_EXPERT_PLAYER = 15

# ─────────────────────────────────────────────────────────────
# Pagination helper
# ─────────────────────────────────────────────────────────────

def fetch_all(client, table: str, select: str = "*", filters: dict | None = None) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    page = 1000
    while True:
        q = client.table(table).select(select).range(offset, offset + page - 1)
        if filters:
            for col, val in filters.items():
                q = q.eq(col, val)
        resp = q.execute()
        batch = resp.data or []
        rows.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return rows


# ─────────────────────────────────────────────────────────────
# Formatting helpers
# ─────────────────────────────────────────────────────────────

def fmt_pct(v: float) -> str:
    s = f"{v:.1%}"
    if v >= 0.58:
        return f"**{s}**"
    return s


def win_rate(wins: int, losses: int) -> float | None:
    n = wins + losses
    if n == 0:
        return None
    return wins / n


def md_table(headers: list[str], rows: list[list]) -> str:
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    def fmt_row(r):
        return "| " + " | ".join(str(r[i]).ljust(col_widths[i]) for i in range(len(headers))) + " |"

    header_line = fmt_row(headers)
    sep_line = "| " + " | ".join("-" * col_widths[i] for i in range(len(headers))) + " |"
    data_lines = [fmt_row(r) for r in rows]
    return "\n".join([header_line, sep_line] + data_lines)


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main():
    client = get_client()

    print("Fetching data from Supabase...")

    # 1. clean_graded_picks
    picks = fetch_all(client, "clean_graded_picks")
    print(f"  clean_graded_picks: {len(picks):,} rows")

    # 2. signal_sources
    sources = fetch_all(client, "signal_sources")
    print(f"  signal_sources: {len(sources):,} rows")

    # 3. signals (for team info)
    signals_teams = fetch_all(client, "signals", select="signal_id,away_team,home_team")
    print(f"  signals (team cols): {len(signals_teams):,} rows")

    # 4. juicereel_history_archive (for team info)
    archive_teams = fetch_all(client, "juicereel_history_archive", select="signal_id,away_team,home_team")
    print(f"  juicereel_history_archive (team cols): {len(archive_teams):,} rows")

    print()

    # ── Build DataFrames ──────────────────────────────────────
    df = pd.DataFrame(picks)
    df_ss = pd.DataFrame(sources) if sources else pd.DataFrame(columns=["signal_id", "source_id", "expert_slug"])
    df_teams = pd.concat([
        pd.DataFrame(signals_teams),
        pd.DataFrame(archive_teams),
    ], ignore_index=True).drop_duplicates(subset=["signal_id"])

    # Normalise result column to WIN/LOSS only
    df = df[df["result"].isin(["WIN", "LOSS"])].copy()
    df["win"] = (df["result"] == "WIN").astype(int)

    # Ensure types
    df["sources_count"] = pd.to_numeric(df["sources_count"], errors="coerce").fillna(0).astype(int)
    df["score"] = pd.to_numeric(df["score"], errors="coerce")

    # Join team info onto picks
    df = df.merge(df_teams, on="signal_id", how="left")

    # Build expert-level dataframe: explode signal_sources onto picks
    # One pick may have multiple expert_slugs (one per supporting expert)
    df_ss_clean = df_ss[df_ss["expert_slug"].notna() & (df_ss["expert_slug"] != "")].copy()
    df_expert = df.merge(df_ss_clean[["signal_id", "source_id", "expert_slug"]], on="signal_id", how="inner")

    print(f"Rows after WIN/LOSS filter: {len(df):,}")
    print(f"Rows with expert attribution: {len(df_expert):,}")
    print()

    # ─────────────────────────────────────────────────────────
    # Section 1: Expert Baseline Records
    # ─────────────────────────────────────────────────────────
    print("=" * 72)
    print("# Section 1: Expert Baseline Records")
    print("=" * 72)
    print()

    grp1 = (
        df_expert.groupby("expert_slug")["win"]
        .agg(wins="sum", n="count")
        .assign(losses=lambda x: x["n"] - x["wins"])
        .assign(win_rate=lambda x: x["wins"] / x["n"])
    )
    grp1 = grp1[grp1["n"] >= MIN_N].sort_values("win_rate", ascending=False)

    rows1 = []
    for slug, r in grp1.iterrows():
        wr_s = fmt_pct(r.win_rate)
        flag = " <-- 55%+" if r.win_rate >= 0.55 else ""
        rows1.append([slug, int(r["wins"]), int(r["losses"]), int(r["n"]), wr_s + flag])

    if rows1:
        print(md_table(["expert_slug", "W", "L", "N", "win_rate"], rows1))
    else:
        print("No experts with n >= 30")
    print()

    # ─────────────────────────────────────────────────────────
    # Section 2: Expert x Market Type
    # ─────────────────────────────────────────────────────────
    print("=" * 72)
    print("# Section 2: Expert × Market Type")
    print("=" * 72)
    print()

    grp2 = (
        df_expert.groupby(["expert_slug", "market_type"])["win"]
        .agg(wins="sum", n="count")
        .assign(losses=lambda x: x["n"] - x["wins"])
        .assign(win_rate=lambda x: x["wins"] / x["n"])
        .reset_index()
    )
    grp2 = grp2[grp2["n"] >= MIN_N].sort_values("win_rate", ascending=False)

    rows2 = []
    for _, r in grp2.iterrows():
        wr_s = fmt_pct(r.win_rate)
        flag = " <-- 58%+" if r.win_rate >= 0.58 else ""
        rows2.append([r.expert_slug, r.market_type, int(r["wins"]), int(r["losses"]), int(r["n"]), wr_s + flag])

    if rows2:
        print(md_table(["expert_slug", "market_type", "W", "L", "N", "win_rate"], rows2))
    else:
        print("No combos with n >= 30")
    print()

    # ─────────────────────────────────────────────────────────
    # Section 3: Expert Consensus
    # ─────────────────────────────────────────────────────────
    print("=" * 72)
    print("# Section 3: Expert Consensus")
    print("=" * 72)
    print()

    # For each (day_key, event_key, market_type, direction, selection), collect expert_slugs
    key_cols = ["day_key", "event_key", "market_type", "direction", "selection"]

    # Only use graded picks (df has result info), expert picks (df_expert)
    # Group by the signal identity (signal_id is shared across expert slugs for same signal)
    # First, get unique signal → expert mapping
    sig_experts = (
        df_ss_clean.groupby("signal_id")["expert_slug"]
        .apply(lambda x: tuple(sorted(set(x))))
        .reset_index()
        .rename(columns={"expert_slug": "expert_combo"})
    )

    df_cons_base = df.merge(sig_experts, on="signal_id", how="inner")
    df_cons_base = df_cons_base[df_cons_base["expert_combo"].map(len) >= 2]
    df_cons_base["n_experts"] = df_cons_base["expert_combo"].map(len)
    df_cons_base["n_experts_bucket"] = df_cons_base["n_experts"].clip(upper=4).map(
        lambda x: "4+" if x >= 4 else str(x)
    )

    # Part A: By n_experts bucket
    print("## 3a. By Number of Agreeing Experts (2, 3, 4+)")
    print()
    grp3a = (
        df_cons_base.groupby("n_experts_bucket")["win"]
        .agg(wins="sum", n="count")
        .assign(losses=lambda x: x["n"] - x["wins"])
        .assign(win_rate=lambda x: x["wins"] / x["n"])
        .reset_index()
    )
    grp3a = grp3a[grp3a["n"] >= MIN_N].sort_values("win_rate", ascending=False)

    rows3a = []
    for _, r in grp3a.iterrows():
        wr_s = fmt_pct(r.win_rate)
        flag = " <-- 58%+" if r.win_rate >= 0.58 else ""
        rows3a.append([r.n_experts_bucket, int(r["wins"]), int(r["losses"]), int(r["n"]), wr_s + flag])
    if rows3a:
        print(md_table(["n_experts_agreeing", "W", "L", "N", "win_rate"], rows3a))
    else:
        print("No groups with n >= 30")
    print()

    # Part B: By specific expert combo (top 30 by N)
    print("## 3b. By Specific Expert Combination (min 30)")
    print()
    df_cons_base["expert_combo_str"] = df_cons_base["expert_combo"].map(lambda x: " + ".join(x))
    grp3b = (
        df_cons_base.groupby("expert_combo_str")["win"]
        .agg(wins="sum", n="count")
        .assign(losses=lambda x: x["n"] - x["wins"])
        .assign(win_rate=lambda x: x["wins"] / x["n"])
        .reset_index()
    )
    grp3b = grp3b[grp3b["n"] >= MIN_N].sort_values("win_rate", ascending=False)

    rows3b = []
    for _, r in grp3b.iterrows():
        wr_s = fmt_pct(r.win_rate)
        flag = " <-- 58%+" if r.win_rate >= 0.58 else ""
        rows3b.append([r.expert_combo_str, int(r["wins"]), int(r["losses"]), int(r["n"]), wr_s + flag])
    if rows3b:
        print(md_table(["expert_combo", "W", "L", "N", "win_rate"], rows3b))
    else:
        print("No expert combos with n >= 30")
    print()

    # ─────────────────────────────────────────────────────────
    # Section 4: Source Consensus
    # ─────────────────────────────────────────────────────────
    print("=" * 72)
    print("# Section 4: Source Consensus (sources_combo × market_type)")
    print("=" * 72)
    print()

    grp4 = (
        df.groupby(["sources_combo", "market_type"])["win"]
        .agg(wins="sum", n="count")
        .assign(losses=lambda x: x["n"] - x["wins"])
        .assign(win_rate=lambda x: x["wins"] / x["n"])
        .reset_index()
    )
    grp4 = grp4[grp4["n"] >= MIN_N].sort_values("win_rate", ascending=False)

    rows4 = []
    for _, r in grp4.iterrows():
        wr_s = fmt_pct(r.win_rate)
        flag = " <-- 58%+" if r.win_rate >= 0.58 else ""
        rows4.append([str(r.sources_combo)[:60], r.market_type, int(r["wins"]), int(r["losses"]), int(r["n"]), wr_s + flag])
    if rows4:
        print(md_table(["sources_combo", "market_type", "W", "L", "N", "win_rate"], rows4))
    else:
        print("No combos with n >= 30")
    print()

    # ─────────────────────────────────────────────────────────
    # Section 5: Expert x Team
    # ─────────────────────────────────────────────────────────
    print("=" * 72)
    print("# Section 5: Expert × Team")
    print("=" * 72)
    print()

    # Melt away_team and home_team into a single "team" column per row
    df_expert_teams = df_expert[df_expert["away_team"].notna() | df_expert["home_team"].notna()].copy()
    rows_away = df_expert_teams[df_expert_teams["away_team"].notna()][
        ["signal_id", "expert_slug", "away_team", "win"]
    ].rename(columns={"away_team": "team"})
    rows_home = df_expert_teams[df_expert_teams["home_team"].notna()][
        ["signal_id", "expert_slug", "home_team", "win"]
    ].rename(columns={"home_team": "team"})
    df_et = pd.concat([rows_away, rows_home], ignore_index=True).drop_duplicates(subset=["signal_id", "expert_slug", "team"])

    grp5 = (
        df_et.groupby(["expert_slug", "team"])["win"]
        .agg(wins="sum", n="count")
        .assign(losses=lambda x: x["n"] - x["wins"])
        .assign(win_rate=lambda x: x["wins"] / x["n"])
        .reset_index()
    )
    grp5 = grp5[grp5["n"] >= MIN_N].sort_values("win_rate", ascending=False)

    rows5 = []
    for _, r in grp5.iterrows():
        wr_s = fmt_pct(r.win_rate)
        flag = ""
        if r.win_rate >= 0.60:
            flag = " <-- 60%+"
        elif r.win_rate <= 0.40:
            flag = " <-- <=40%"
        rows5.append([r.expert_slug, r.team, int(r["wins"]), int(r["losses"]), int(r["n"]), wr_s + flag])
    if rows5:
        print(md_table(["expert_slug", "team", "W", "L", "N", "win_rate"], rows5))
    else:
        print("No combos with n >= 30")
    print()

    # ─────────────────────────────────────────────────────────
    # Section 6: Expert x Player (props only)
    # ─────────────────────────────────────────────────────────
    print("=" * 72)
    print("# Section 6: Expert × Player (props only, min 15)")
    print("=" * 72)
    print()

    prop_markets = {"player_prop", "prop"}
    non_game_stats = {"spread", "total", "moneyline"}

    df_props = df_expert[
        (df_expert["market_type"].str.lower().isin(prop_markets)) |
        (~df_expert["atomic_stat"].fillna("").str.lower().isin(non_game_stats))
    ].copy()
    df_props = df_props[df_props["player_key"].notna() & (df_props["player_key"] != "")]

    grp6 = (
        df_props.groupby(["expert_slug", "player_key"])["win"]
        .agg(wins="sum", n="count")
        .assign(losses=lambda x: x["n"] - x["wins"])
        .assign(win_rate=lambda x: x["wins"] / x["n"])
        .reset_index()
    )
    grp6 = grp6[grp6["n"] >= MIN_N_EXPERT_PLAYER].sort_values("win_rate", ascending=False)

    rows6 = []
    for _, r in grp6.iterrows():
        wr_s = fmt_pct(r.win_rate)
        flag = " <-- 58%+" if r.win_rate >= 0.58 else ""
        rows6.append([r.expert_slug, r.player_key, int(r["wins"]), int(r["losses"]), int(r["n"]), wr_s + flag])
    if rows6:
        print(md_table(["expert_slug", "player_key", "W", "L", "N", "win_rate"], rows6))
    else:
        print("No combos with n >= 15")
    print()

    # ─────────────────────────────────────────────────────────
    # Section 7: Market Type Consensus
    # ─────────────────────────────────────────────────────────
    print("=" * 72)
    print("# Section 7: Market Type Consensus")
    print("=" * 72)
    print()

    df["sc_bucket"] = df["sources_count"].apply(lambda x: "5+" if x >= 5 else str(x))

    # Totals
    print("## 7a. Totals (direction × sources_count)")
    print()
    df_tot = df[df["market_type"].str.lower().str.contains("total", na=False)]
    grp7a = (
        df_tot.groupby(["direction", "sc_bucket"])["win"]
        .agg(wins="sum", n="count")
        .assign(losses=lambda x: x["n"] - x["wins"])
        .assign(win_rate=lambda x: x["wins"] / x["n"])
        .reset_index()
    )
    grp7a = grp7a[grp7a["n"] >= MIN_N].sort_values(["direction", "sc_bucket"])
    rows7a = []
    for _, r in grp7a.iterrows():
        wr_s = fmt_pct(r.win_rate)
        flag = " <-- 58%+" if r.win_rate >= 0.58 else ""
        rows7a.append([str(r.direction), r.sc_bucket, int(r["wins"]), int(r["losses"]), int(r["n"]), wr_s + flag])
    if rows7a:
        print(md_table(["direction", "sources_count", "W", "L", "N", "win_rate"], rows7a))
    else:
        print("No combos with n >= 30")
    print()

    # Props
    print("## 7b. Props (direction × sources_count)")
    print()
    df_prop_all = df[
        df["market_type"].str.lower().str.contains("prop", na=False) |
        (~df["atomic_stat"].fillna("").str.lower().isin(non_game_stats))
    ]
    grp7b = (
        df_prop_all.groupby(["direction", "sc_bucket"])["win"]
        .agg(wins="sum", n="count")
        .assign(losses=lambda x: x["n"] - x["wins"])
        .assign(win_rate=lambda x: x["wins"] / x["n"])
        .reset_index()
    )
    grp7b = grp7b[grp7b["n"] >= MIN_N].sort_values(["direction", "sc_bucket"])
    rows7b = []
    for _, r in grp7b.iterrows():
        wr_s = fmt_pct(r.win_rate)
        flag = " <-- 58%+" if r.win_rate >= 0.58 else ""
        rows7b.append([str(r.direction), r.sc_bucket, int(r["wins"]), int(r["losses"]), int(r["n"]), wr_s + flag])
    if rows7b:
        print(md_table(["direction", "sources_count", "W", "L", "N", "win_rate"], rows7b))
    else:
        print("No combos with n >= 30")
    print()

    # Spreads
    print("## 7c. Spreads (sources_count)")
    print()
    df_spr = df[df["market_type"].str.lower().str.contains("spread", na=False)]
    grp7c = (
        df_spr.groupby("sc_bucket")["win"]
        .agg(wins="sum", n="count")
        .assign(losses=lambda x: x["n"] - x["wins"])
        .assign(win_rate=lambda x: x["wins"] / x["n"])
        .reset_index()
    )
    grp7c = grp7c[grp7c["n"] >= MIN_N].sort_values("sc_bucket")
    rows7c = []
    for _, r in grp7c.iterrows():
        wr_s = fmt_pct(r.win_rate)
        flag = " <-- 58%+" if r.win_rate >= 0.58 else ""
        rows7c.append([r.sc_bucket, int(r["wins"]), int(r["losses"]), int(r["n"]), wr_s + flag])
    if rows7c:
        print(md_table(["sources_count", "W", "L", "N", "win_rate"], rows7c))
    else:
        print("No combos with n >= 30")
    print()

    # ─────────────────────────────────────────────────────────
    # Section 8: Score/Confidence Threshold
    # ─────────────────────────────────────────────────────────
    print("=" * 72)
    print("# Section 8: Score/Confidence Threshold")
    print("=" * 72)
    print()

    df_scored = df[df["score"].notna()].copy()
    df_scored["score_int"] = df_scored["score"].round(0).astype(int)

    grp8 = (
        df_scored.groupby("score_int")["win"]
        .agg(wins="sum", n="count")
        .assign(losses=lambda x: x["n"] - x["wins"])
        .assign(win_rate=lambda x: x["wins"] / x["n"])
        .reset_index()
    )
    grp8 = grp8[grp8["n"] >= MIN_N].sort_values("score_int")

    rows8 = []
    threshold_found = None
    for _, r in grp8.iterrows():
        wr_s = fmt_pct(r.win_rate)
        flag = ""
        if r.win_rate >= 0.60:
            flag = " <-- >=60%"
            if threshold_found is None:
                threshold_found = int(r.score_int)
        rows8.append([int(r.score_int), int(r["wins"]), int(r["losses"]), int(r["n"]), wr_s + flag])
    if rows8:
        print(md_table(["score", "W", "L", "N", "win_rate"], rows8))
        if threshold_found is not None:
            print(f"\n  --> First score threshold with win_rate >= 60%: score = {threshold_found}")
    else:
        print("No score buckets with n >= 30")
    print()

    # Also try ranges for sparse scores
    print("## 8b. Score Ranges (grouped)")
    print()

    def score_range_label(s):
        if pd.isna(s):
            return "unknown"
        s = int(round(s))
        if s <= 0:
            return "<=0"
        if s <= 2:
            return "1-2"
        if s <= 4:
            return "3-4"
        if s <= 6:
            return "5-6"
        if s <= 8:
            return "7-8"
        if s <= 10:
            return "9-10"
        return "11+"

    df_scored["score_range"] = df_scored["score"].apply(score_range_label)
    grp8b = (
        df_scored.groupby("score_range")["win"]
        .agg(wins="sum", n="count")
        .assign(losses=lambda x: x["n"] - x["wins"])
        .assign(win_rate=lambda x: x["wins"] / x["n"])
        .reset_index()
    )
    grp8b = grp8b[grp8b["n"] >= MIN_N].sort_values("score_range")
    rows8b = []
    for _, r in grp8b.iterrows():
        wr_s = fmt_pct(r.win_rate)
        flag = " <-- >=60%" if r.win_rate >= 0.60 else ""
        rows8b.append([r.score_range, int(r["wins"]), int(r["losses"]), int(r["n"]), wr_s + flag])
    if rows8b:
        print(md_table(["score_range", "W", "L", "N", "win_rate"], rows8b))
    else:
        print("No score range buckets with n >= 30")
    print()

    # ─────────────────────────────────────────────────────────
    # Section 9: Recency Split (last 60 days)
    # ─────────────────────────────────────────────────────────
    print("=" * 72)
    print("# Section 9: Recency Split (last 60 days vs all-time)")
    print("=" * 72)
    print()
    print(f"  60-day cutoff: {CUTOFF_60}  (today = {TODAY})")
    print()

    df_recent = df[df["day_key"] >= CUTOFF_60].copy()
    df_expert_recent = df_expert[df_expert["day_key"] >= CUTOFF_60].copy()

    DIVERGE_THRESHOLD = 0.08

    def recency_section(label, all_df, recent_df, group_cols, min_n=MIN_N):
        all_grp = (
            all_df.groupby(group_cols)["win"]
            .agg(wins_all="sum", n_all="count")
            .assign(wr_all=lambda x: x["wins_all"] / x["n_all"])
        )
        rec_grp = (
            recent_df.groupby(group_cols)["win"]
            .agg(wins_rec="sum", n_rec="count")
            .assign(wr_rec=lambda x: x["wins_rec"] / x["n_rec"])
        )
        merged = all_grp.join(rec_grp, how="inner").reset_index()
        merged = merged[merged["n_all"] >= min_n]
        merged["divergence"] = (merged["wr_rec"] - merged["wr_all"]).abs()
        diverged = merged[merged["divergence"] > DIVERGE_THRESHOLD].sort_values("divergence", ascending=False)

        print(f"## 9 - {label} (all-time n>={min_n}, |recent - alltime| > {DIVERGE_THRESHOLD:.0%})")
        print()
        if diverged.empty:
            print("  No significant divergence found.")
        else:
            headers = list(group_cols) + ["n_all", "wr_all", "n_rec", "wr_rec", "divergence", "flag"]
            table_rows = []
            for _, r in diverged.iterrows():
                wr_all_s = fmt_pct(r.wr_all)
                wr_rec_s = fmt_pct(r.wr_rec)
                div_dir = "UP" if r.wr_rec > r.wr_all else "DOWN"
                flag = f"<-- {div_dir} {r.divergence:.1%}"
                row_vals = [r[c] for c in group_cols] + [int(r.n_all), wr_all_s, int(r.n_rec), wr_rec_s, f"{r.divergence:.1%}", flag]
                table_rows.append(row_vals)
            print(md_table(headers, table_rows))
        print()

    recency_section("Expert Baseline", df_expert, df_expert_recent, ["expert_slug"])
    recency_section("Expert × Market Type", df_expert, df_expert_recent, ["expert_slug", "market_type"])
    recency_section("Expert Consensus by N", df_cons_base, df_cons_base[df_cons_base["day_key"] >= CUTOFF_60], ["n_experts_bucket"])
    recency_section("Source Consensus (sources_combo × market_type)", df, df_recent, ["sources_combo", "market_type"])

    print("=" * 72)
    print("# End of Report")
    print("=" * 72)


if __name__ == "__main__":
    main()
