#!/usr/bin/env python3
"""
Deep Pattern Analysis — Full Historical Grade Audit

Surfaces every combination with 30+ picks and genuine edge above 60% win rate
or positive ROI. Adds odds-adjusted ROI, line buckets, score bucket validation,
and temporal stability — dimensions missing from the existing report_records reports.

Usage:
  python3 scripts/deep_pattern_analysis.py              # all modules
  python3 scripts/deep_pattern_analysis.py --module 1   # single module
  python3 scripts/deep_pattern_analysis.py --min-n 30   # sample size floor
  python3 scripts/deep_pattern_analysis.py --min-win 60 # show 60%+ only
"""
from __future__ import annotations

import argparse
import datetime
import json
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

REPO_ROOT    = Path(__file__).resolve().parent.parent
GRADES_FILE  = REPO_ROOT / "data/ledger/grades_latest.jsonl"
SIGNALS_FILE = REPO_ROOT / "data/ledger/signals_latest.jsonl"
OUT_FILE     = REPO_ROOT / "data/reports/deep_pattern_analysis.json"

DEFAULT_MIN_N   = 30
DEFAULT_MIN_WIN = 55.0


# ─────────────────────────────────────────────
# Math helpers
# ─────────────────────────────────────────────

def wilson_lb(wins: int, n: int, z: float = 1.96) -> float:
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    spread = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (centre - spread) / denom


def get_odds(sig: dict, grade: dict) -> Optional[float]:
    o = grade.get("odds") or sig.get("odds")
    if isinstance(o, list):
        o = o[0] if o else None
    try:
        return float(o) if o is not None else None
    except (TypeError, ValueError):
        return None


def break_even(odds: float) -> float:
    if odds < 0:
        return abs(odds) / (abs(odds) + 100)
    return 100 / (odds + 100)


def roi_per_unit(odds: float, result: str) -> float:
    if result == "WIN":
        return (100 / abs(odds)) if odds < 0 else (odds / 100)
    return -1.0


def odds_bucket_label(odds: float) -> str:
    if odds <= -500:  return "≤-500 (pure fav)"
    if odds <= -300:  return "-300 to -500"
    if odds <= -200:  return "-200 to -300"
    if odds <= -150:  return "-150 to -200"
    if odds <= -110:  return "-110 to -150"
    if odds <= -101:  return "near-even"
    return "even/plus"


def line_bucket_label(line) -> str:
    if line is None: return "unknown"
    try:
        v = float(line)
    except (TypeError, ValueError):
        return "unknown"
    if v <  5:  return "0-5"
    if v < 10:  return "5-10"
    if v < 15:  return "10-15"
    if v < 20:  return "15-20"
    if v < 25:  return "20-25"
    if v < 30:  return "25-30"
    return "30+"


def score_bucket_label(score) -> str:
    if score is None: return "unknown"
    try:
        s = float(score)
    except (TypeError, ValueError):
        return "unknown"
    if s < 30:  return "0-29"
    if s < 45:  return "30-44"
    if s < 60:  return "45-59"
    if s < 75:  return "60-74"
    if s < 90:  return "75-89"
    return "90+"


def dow_label(day_key: str) -> str:
    try:
        parts = day_key.split(":")
        d = datetime.date(int(parts[1]), int(parts[2]), int(parts[3]))
        return d.strftime("%A")
    except Exception:
        return "unknown"


def period_label(day_key: str) -> str:
    return "2026+" if day_key >= "NBA:2026" else "pre-2026"


def norm_direction(dirn: str) -> str:
    d = (dirn or "").upper()
    if "OVER" in d:  return "OVER"
    if "UNDER" in d: return "UNDER"
    if d:            return "team"
    return ""


# ─────────────────────────────────────────────
# Bucket accumulator
# ─────────────────────────────────────────────

class Bucket:
    __slots__ = ["wins", "losses", "_roi", "_be", "_odds_n"]

    def __init__(self):
        self.wins = 0; self.losses = 0
        self._roi = 0.0; self._be = 0.0; self._odds_n = 0

    def add(self, result: str, odds: Optional[float] = None):
        if result == "WIN": self.wins += 1
        else:               self.losses += 1
        if odds is not None:
            self._roi   += roi_per_unit(odds, result)
            self._be    += break_even(odds)
            self._odds_n += 1

    @property
    def n(self): return self.wins + self.losses

    @property
    def win_pct(self): return self.wins / self.n if self.n else 0.0

    @property
    def avg_roi(self): return self._roi / self.n if self.n else 0.0

    @property
    def avg_be(self): return self._be / self._odds_n if self._odds_n else None

    @property
    def edge(self):
        be = self.avg_be
        return (self.win_pct - be) if be is not None else None

    @property
    def wilson(self): return wilson_lb(self.wins, self.n)

    @property
    def odds_cov(self): return f"{self._odds_n}/{self.n}"

    def to_dict(self) -> dict:
        return {
            "n": self.n, "wins": self.wins, "losses": self.losses,
            "win_pct": round(self.win_pct * 100, 1),
            "wilson":  round(self.wilson  * 100, 1),
            "avg_roi": round(self.avg_roi, 4),
            "avg_be":  round(self.avg_be  * 100, 1) if self.avg_be  is not None else None,
            "edge":    round(self.edge    * 100, 1) if self.edge     is not None else None,
            "odds_coverage": self.odds_cov,
        }


def aggregate(records: List[dict], key_fn) -> Dict[tuple, Bucket]:
    out: Dict[tuple, Bucket] = defaultdict(Bucket)
    for r in records:
        k = key_fn(r)
        if k is None: continue
        odds = get_odds(r["sig"], r["grade"])
        out[k].add(r["grade"]["result"], odds)
    return out


def top_rows(buckets: Dict[tuple, Bucket], min_n: int, min_win: float) -> List[Tuple]:
    rows = [(k, b) for k, b in buckets.items()
            if b.n >= min_n and b.win_pct * 100 >= min_win]
    rows.sort(key=lambda x: (-x[1].win_pct, -x[1].n))
    return rows


# ─────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────

def load_joined() -> List[dict]:
    signals = {}
    with open(SIGNALS_FILE) as f:
        for line in f:
            line = line.strip()
            if not line: continue
            r = json.loads(line)
            sid = r.get("signal_id")
            if sid: signals[sid] = r

    records = []
    with open(GRADES_FILE) as f:
        for line in f:
            line = line.strip()
            if not line: continue
            g = json.loads(line)
            if g.get("result") not in ("WIN", "LOSS"): continue
            sid = g.get("signal_id")
            if not sid: continue
            sig = signals.get(sid)
            if not sig: continue
            records.append({"sig": sig, "grade": g})
    return records


# ─────────────────────────────────────────────
# Print helpers
# ─────────────────────────────────────────────

def print_header(title: str):
    print()
    print("═" * 115)
    print(f"  {title}")
    print("═" * 115)


def star_flag(b: Bucket, min_win: float = 60.0) -> str:
    if b.win_pct * 100 >= min_win and (b.edge is None or b.edge > 0):
        return "★"
    return " "


def illusion_flag(b: Bucket) -> str:
    """High win% but negative ROI — inflated by heavy-favorite odds."""
    if b.win_pct >= 0.60 and b._odds_n > 0 and b.avg_roi < -0.02:
        return "⚠"
    return " "


# ─────────────────────────────────────────────
# Module 1: Source Combo × Market × Direction
# ─────────────────────────────────────────────

def module1(records, min_n, min_win):
    print_header("MODULE 1 — Source Combo × Market × Direction  (odds-adjusted)")

    def key_fn(r):
        combo = r["sig"].get("sources_combo", "")
        mkt   = r["sig"].get("market_type", "")
        dirn  = norm_direction(r["sig"].get("direction", ""))
        return (combo, mkt, dirn)

    buckets = aggregate(records, key_fn)
    rows = top_rows(buckets, min_n, min_win)

    print()
    print(f"  {'':2}  {'COMBO':<42}  {'MARKET':<14}  {'DIR':<8}  {'WIN%':>6}  {'WILSON':>6}  {'EDGE':>7}  {'ROI':>7}  {'ODDS_COV':>10}  {'N':>5}")
    print("  " + "-" * 113)

    results = []
    for key, b in rows:
        combo, mkt, dirn = key
        sf = star_flag(b, min_win); ilf = illusion_flag(b)
        marker = sf + ilf
        edge_s = f"{b.edge*100:+.1f}%" if b.edge is not None else "n/a"
        print(f"  {marker:<2}  {combo[:40]:<42}  {mkt[:12]:<14}  {dirn:<8}  "
              f"{b.win_pct*100:>5.1f}%  {b.wilson*100:>5.1f}%  {edge_s:>7}  "
              f"{b.avg_roi:>+7.3f}  {b.odds_cov:>10}  {b.n:>5}")
        results.append({"key": list(key), **b.to_dict()})

    return {"module": 1, "title": "combo_market_direction", "rows": results}


# ─────────────────────────────────────────────
# Module 2: Individual Source × Market × Direction (exploded)
# ─────────────────────────────────────────────

def module2(records, min_n, min_win):
    print_header("MODULE 2 — Individual Source × Market × Direction  (exploded — source credited for every signal it appears in)")

    buckets: Dict[tuple, Bucket] = defaultdict(Bucket)
    for r in records:
        combo = r["sig"].get("sources_combo", "")
        sources = [s.strip() for s in combo.split("|") if s.strip()]
        mkt  = r["sig"].get("market_type", "")
        dirn = norm_direction(r["sig"].get("direction", ""))
        odds = get_odds(r["sig"], r["grade"])
        result = r["grade"]["result"]
        for src in sources:
            buckets[(src, mkt, dirn)].add(result, odds)

    rows = top_rows(buckets, min_n, min_win)

    print()
    print(f"  {'':2}  {'SOURCE':<35}  {'MARKET':<14}  {'DIR':<8}  {'WIN%':>6}  {'WILSON':>6}  {'EDGE':>7}  {'ROI':>7}  {'N':>5}")
    print("  " + "-" * 95)

    results = []
    for key, b in rows:
        src, mkt, dirn = key
        sf = star_flag(b, min_win); ilf = illusion_flag(b)
        edge_s = f"{b.edge*100:+.1f}%" if b.edge is not None else "n/a"
        print(f"  {sf+ilf:<2}  {src[:33]:<35}  {mkt[:12]:<14}  {dirn:<8}  "
              f"{b.win_pct*100:>5.1f}%  {b.wilson*100:>5.1f}%  {edge_s:>7}  {b.avg_roi:>+7.3f}  {b.n:>5}")
        results.append({"key": list(key), **b.to_dict()})

    return {"module": 2, "title": "source_market_direction_exploded", "rows": results}


# ─────────────────────────────────────────────
# Module 3: Prop Line Buckets × Direction
# ─────────────────────────────────────────────

def module3(records, min_n, min_win):
    print_header("MODULE 3 — Player Prop Line Buckets × Direction  (where on the line do edges live?)")

    prop_records = [r for r in records if r["sig"].get("market_type") == "player_prop"]

    def key_fn(r):
        dirn = norm_direction(r["sig"].get("direction", ""))
        if dirn not in ("OVER", "UNDER"): return None
        ln = r["sig"].get("line") or r["grade"].get("line")
        lb = line_bucket_label(ln)
        return (dirn, lb)

    buckets = aggregate(prop_records, key_fn)
    lb_order = ["0-5", "5-10", "10-15", "15-20", "20-25", "25-30", "30+", "unknown"]
    rows = [(k, b) for k, b in buckets.items() if b.n >= min_n]
    rows.sort(key=lambda x: (x[0][0], lb_order.index(x[0][1]) if x[0][1] in lb_order else 99))

    print()
    print(f"  {'':2}  {'DIR':<8}  {'LINE_BUCKET':<14}  {'WIN%':>6}  {'WILSON':>6}  {'EDGE':>7}  {'ROI':>7}  {'N':>5}")
    print("  " + "-" * 65)

    results = []
    for key, b in rows:
        dirn, lb = key
        sf = star_flag(b, min_win)
        edge_s = f"{b.edge*100:+.1f}%" if b.edge is not None else "n/a"
        print(f"  {sf:<2}  {dirn:<8}  {lb:<14}  {b.win_pct*100:>5.1f}%  {b.wilson*100:>5.1f}%  {edge_s:>7}  {b.avg_roi:>+7.3f}  {b.n:>5}")
        results.append({"key": list(key), **b.to_dict()})

    return {"module": 3, "title": "prop_line_bucket_direction", "rows": results}


# ─────────────────────────────────────────────
# Module 4: Source Combo × Stat Type (props)
# ─────────────────────────────────────────────

def module4(records, min_n, min_win):
    print_header("MODULE 4 — Source Combo × Stat Type  (player props only — which stats are profitable per source?)")

    prop_records = [r for r in records if r["sig"].get("market_type") == "player_prop"]

    def key_fn(r):
        combo = r["sig"].get("sources_combo", "")
        stat  = r["sig"].get("atomic_stat") or "unknown"
        dirn  = norm_direction(r["sig"].get("direction", ""))
        return (combo, stat, dirn)

    buckets = aggregate(prop_records, key_fn)
    rows = top_rows(buckets, min_n, min_win)

    print()
    print(f"  {'':2}  {'COMBO':<40}  {'STAT':<16}  {'DIR':<8}  {'WIN%':>6}  {'WILSON':>6}  {'ROI':>7}  {'N':>5}")
    print("  " + "-" * 90)

    results = []
    for key, b in rows:
        combo, stat, dirn = key
        sf = star_flag(b, min_win)
        print(f"  {sf:<2}  {combo[:38]:<40}  {stat[:14]:<16}  {dirn:<8}  "
              f"{b.win_pct*100:>5.1f}%  {b.wilson*100:>5.1f}%  {b.avg_roi:>+7.3f}  {b.n:>5}")
        results.append({"key": list(key), **b.to_dict()})

    return {"module": 4, "title": "combo_stat_direction", "rows": results}


# ─────────────────────────────────────────────
# Module 5: Score Bucket Validation
# ─────────────────────────────────────────────

def module5(records, min_n, _min_win):
    print_header("MODULE 5 — Signal Score Bucket Validation  (do higher confidence scores actually win more?)")

    def key_fn(r):
        sb  = score_bucket_label(r["sig"].get("score"))
        mkt = r["sig"].get("market_type", "")
        return (mkt, sb)

    buckets = aggregate(records, key_fn)
    sb_order = ["0-29", "30-44", "45-59", "60-74", "75-89", "90+", "unknown"]
    rows = [(k, b) for k, b in buckets.items() if b.n >= min_n]
    rows.sort(key=lambda x: (x[0][0], sb_order.index(x[0][1]) if x[0][1] in sb_order else 99))

    print()
    print(f"  {'MARKET':<16}  {'SCORE_BUCKET':<14}  {'WIN%':>6}  {'WILSON':>6}  {'ROI':>7}  {'N':>6}  {'TREND'}")
    print("  " + "-" * 70)

    results = []
    prev_mkt = None
    for key, b in rows:
        mkt, sb = key
        if mkt != prev_mkt:
            print()
            prev_mkt = mkt
        trend = "▲" if b.win_pct >= 0.56 else ("▼" if b.win_pct < 0.49 else "─")
        print(f"  {mkt:<16}  {sb:<14}  {b.win_pct*100:>5.1f}%  {b.wilson*100:>5.1f}%  {b.avg_roi:>+7.3f}  {b.n:>6}  {trend}")
        results.append({"key": list(key), **b.to_dict()})

    return {"module": 5, "title": "score_bucket_validation", "rows": results}


# ─────────────────────────────────────────────
# Module 6: Sources Count × Market × Direction
# ─────────────────────────────────────────────

def module6(records, min_n, _min_win):
    print_header("MODULE 6 — Sources Count × Market × Direction  (does consensus strength matter?)")

    def key_fn(r):
        cnt = r["sig"].get("sources_count") or 0
        try: cnt = int(cnt)
        except: cnt = 0
        cnt_label = "5+" if cnt >= 5 else str(cnt)
        mkt  = r["sig"].get("market_type", "")
        dirn = norm_direction(r["sig"].get("direction", ""))
        return (mkt, dirn, cnt_label)

    buckets = aggregate(records, key_fn)
    cnt_order = ["1", "2", "3", "4", "5+"]
    rows = [(k, b) for k, b in buckets.items() if b.n >= min_n]
    rows.sort(key=lambda x: (x[0][0], x[0][1],
                              cnt_order.index(x[0][2]) if x[0][2] in cnt_order else 99))

    print()
    print(f"  {'':2}  {'MARKET':<14}  {'DIR':<8}  {'SOURCES':>8}  {'WIN%':>6}  {'WILSON':>6}  {'ROI':>7}  {'N':>6}")
    print("  " + "-" * 70)

    results = []
    prev_mkt_dir = None
    for key, b in rows:
        mkt, dirn, cnt = key
        if (mkt, dirn) != prev_mkt_dir:
            print()
            prev_mkt_dir = (mkt, dirn)
        sf = "★" if b.win_pct >= 0.58 else " "
        print(f"  {sf:<2}  {mkt:<14}  {dirn:<8}  {cnt:>8}  "
              f"{b.win_pct*100:>5.1f}%  {b.wilson*100:>5.1f}%  {b.avg_roi:>+7.3f}  {b.n:>6}")
        results.append({"key": list(key), **b.to_dict()})

    return {"module": 6, "title": "sources_count_market_direction", "rows": results}


# ─────────────────────────────────────────────
# Module 7: Temporal Stability
# ─────────────────────────────────────────────

def module7(records, min_n, min_win):
    print_header("MODULE 7 — Temporal Stability  (pre-2026 vs 2026+ for patterns passing win% threshold)")

    def key_fn(r):
        combo = r["sig"].get("sources_combo", "")
        mkt   = r["sig"].get("market_type", "")
        dirn  = norm_direction(r["sig"].get("direction", ""))
        p     = period_label(r["sig"].get("day_key", ""))
        return (combo, mkt, dirn, p)

    buckets = aggregate(records, key_fn)

    # Build combined (no period) to find qualifying patterns
    combined: Dict[tuple, Bucket] = defaultdict(Bucket)
    for (combo, mkt, dirn, p), b in buckets.items():
        for _ in range(b.wins):   combined[(combo, mkt, dirn)].add("WIN")
        for _ in range(b.losses): combined[(combo, mkt, dirn)].add("LOSS")

    qualifying = {k for k, b in combined.items()
                  if b.n >= min_n and b.win_pct * 100 >= min_win}

    print()
    print(f"  {'COMBO':<38}  {'MKT':<12}  {'DIR':<8}  {'PRE-2026':>14}  {'2026+':>14}  {'DRIFT':>7}  NOTE")
    print("  " + "-" * 105)

    results = []
    for key in sorted(qualifying, key=lambda x: -combined[x].win_pct):
        combo, mkt, dirn = key
        b_pre  = buckets.get((combo, mkt, dirn, "pre-2026"))
        b_26   = buckets.get((combo, mkt, dirn, "2026+"))
        wp_pre = f"{b_pre.win_pct*100:.1f}% n={b_pre.n}" if b_pre and b_pre.n >= 10 else "n/a"
        wp_26  = f"{b_26.win_pct*100:.1f}% n={b_26.n}"  if b_26  and b_26.n  >= 10 else "n/a"
        drift  = None
        if b_pre and b_pre.n >= 10 and b_26 and b_26.n >= 10:
            drift = b_26.win_pct - b_pre.win_pct
        drift_s = f"{drift*100:+.1f}%" if drift is not None else "n/a"
        note = ""
        if drift is not None:
            if drift > 0.10:  note = "⬆ IMPROVING"
            elif drift < -0.10: note = "⚠ COOLING OFF"
            else: note = "✓ STABLE"
        elif wp_pre == "n/a":   note = "recent only"
        elif wp_26  == "n/a":   note = "no 2026 data"
        print(f"  {combo[:36]:<38}  {mkt[:10]:<12}  {dirn:<8}  {wp_pre:>14}  {wp_26:>14}  {drift_s:>7}  {note}")
        results.append({
            "combo": combo, "market_type": mkt, "direction": dirn,
            "pre_2026": b_pre.to_dict() if b_pre else None,
            "2026_plus": b_26.to_dict() if b_26 else None,
            "drift_pct": round(drift * 100, 1) if drift is not None else None,
        })

    return {"module": 7, "title": "temporal_stability", "rows": results}


# ─────────────────────────────────────────────
# Module 8: Day of Week × Market
# ─────────────────────────────────────────────

def module8(records, min_n, _min_win):
    print_header("MODULE 8 — Day of Week × Market × Direction  (calendar patterns)")

    def key_fn(r):
        dow  = dow_label(r["sig"].get("day_key", ""))
        mkt  = r["sig"].get("market_type", "")
        dirn = norm_direction(r["sig"].get("direction", ""))
        return (mkt, dirn, dow)

    buckets = aggregate(records, key_fn)
    dow_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday","unknown"]
    rows = [(k, b) for k, b in buckets.items() if b.n >= min_n]
    rows.sort(key=lambda x: (x[0][0], x[0][1],
                              dow_order.index(x[0][2]) if x[0][2] in dow_order else 99))

    print()
    print(f"  {'':2}  {'MARKET':<14}  {'DIR':<8}  {'DAY':<12}  {'WIN%':>6}  {'WILSON':>6}  {'ROI':>7}  {'N':>6}")
    print("  " + "-" * 70)

    results = []
    prev_mkt_dir = None
    for key, b in rows:
        mkt, dirn, dow = key
        if (mkt, dirn) != prev_mkt_dir:
            print()
            prev_mkt_dir = (mkt, dirn)
        sf = "★" if b.win_pct >= 0.58 else " "
        print(f"  {sf:<2}  {mkt:<14}  {dirn:<8}  {dow:<12}  "
              f"{b.win_pct*100:>5.1f}%  {b.wilson*100:>5.1f}%  {b.avg_roi:>+7.3f}  {b.n:>6}")
        results.append({"key": list(key), **b.to_dict()})

    return {"module": 8, "title": "day_of_week_market_direction", "rows": results}


# ─────────────────────────────────────────────
# Module 9: Odds-Adjusted ROI by Combo
# ─────────────────────────────────────────────

def module9(records, min_n, _min_win):
    print_header("MODULE 9 — Odds-Adjusted ROI by Source Combo  (exposes illusion patterns)")

    odds_records = [r for r in records if get_odds(r["sig"], r["grade"]) is not None]

    def key_fn(r):
        combo = r["sig"].get("sources_combo", "")
        ob    = odds_bucket_label(get_odds(r["sig"], r["grade"]))
        return (combo, ob)

    buckets = aggregate(odds_records, key_fn)

    # Only show combos with enough total volume
    combo_totals: Dict[str, int] = defaultdict(int)
    for (combo, ob), b in buckets.items():
        combo_totals[combo] += b.n
    top_combos = {c for c, n in combo_totals.items() if n >= min_n}

    ob_order = ["≤-500 (pure fav)", "-300 to -500", "-200 to -300", "-150 to -200",
                "-110 to -150", "near-even", "even/plus"]
    rows = [(k, b) for k, b in buckets.items()
            if k[0] in top_combos and b.n >= 15]
    rows.sort(key=lambda x: (x[0][0],
                              ob_order.index(x[0][1]) if x[0][1] in ob_order else 99))

    print()
    print(f"  {'':2}  {'COMBO':<40}  {'ODDS_BUCKET':<22}  {'WIN%':>6}  {'BE%':>6}  {'EDGE':>7}  {'ROI':>7}  {'N':>5}")
    print("  " + "-" * 103)

    results = []
    prev_combo = None
    for key, b in rows:
        combo, ob = key
        if combo != prev_combo:
            print()
            prev_combo = combo
        sf  = "★" if b.win_pct >= 0.60 and b.avg_roi > 0.05 else " "
        ilf = illusion_flag(b)
        edge_s = f"{b.edge*100:+.1f}%" if b.edge is not None else "n/a"
        be_s   = f"{b.avg_be:.1f}%" if b.avg_be is not None else "n/a"
        print(f"  {sf+ilf:<2}  {combo[:38]:<40}  {ob:<22}  "
              f"{b.win_pct*100:>5.1f}%  {be_s:>6}  {edge_s:>7}  {b.avg_roi:>+7.3f}  {b.n:>5}")
        results.append({"key": list(key), **b.to_dict()})

    return {"module": 9, "title": "odds_adjusted_roi_by_combo", "rows": results}


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Deep pattern analysis of graded picks")
    parser.add_argument("--module",  type=int,   default=0,
                        help="Run single module 1-9 (0 = all)")
    parser.add_argument("--min-n",   type=int,   default=DEFAULT_MIN_N,
                        help="Minimum sample size (default 30)")
    parser.add_argument("--min-win", type=float, default=DEFAULT_MIN_WIN,
                        help="Minimum win%% to show in most modules (default 55)")
    args = parser.parse_args()

    print("Loading data...")
    records = load_joined()
    print(f"Loaded {len(records)} usable (WIN/LOSS) graded records")

    module_fns = {
        1: module1, 2: module2, 3: module3, 4: module4, 5: module5,
        6: module6, 7: module7, 8: module8, 9: module9,
    }

    all_results = {}
    run = [args.module] if args.module else list(module_fns.keys())

    for m in run:
        fn = module_fns.get(m)
        if not fn:
            print(f"Unknown module {m}"); continue
        # Modules 5/6/8 are calibration tables — show all rows not just ≥min_win
        mw = 0.0 if m in (5, 6, 8) else args.min_win
        result = fn(records, args.min_n, mw)
        all_results[str(m)] = result

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_FILE, "w") as f:
        json.dump(all_results, f, indent=2)

    print(f"\n{'═'*60}")
    print(f"  Results → {OUT_FILE.relative_to(REPO_ROOT)}")
    print(f"  ★ = win%≥60% with positive edge/ROI")
    print(f"  ⚠ = high win% but NEGATIVE ROI (illusion — heavy odds)")
    print(f"{'═'*60}")


if __name__ == "__main__":
    main()
