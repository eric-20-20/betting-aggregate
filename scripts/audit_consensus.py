#!/usr/bin/env python3
"""
Consensus integrity audit.

Reads a run directory and cross-checks consensus signals against normalized
records. Produces four report sections:

  A. Star-rating check  — BetQL picks below the 3-star minimum
  B. Source coverage     — per-game source presence, highlights gaps
  C. Line discrepancies  — consensus picks where sources disagree on lines
  D. Player-name collisions — normalized player IDs mapping to multiple names

Usage:
    python3 scripts/audit_consensus.py                        # latest run
    python3 scripts/audit_consensus.py --run-dir data/runs/2026-02-24T19-28-33Z-f3f122
"""

import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path


RUNS_DIR = Path("data/runs")
MIN_STARS = 3


def read_jsonl(path: Path):
    rows = []
    if not path.exists():
        return rows
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def find_latest_run(runs_dir: Path) -> Path:
    ts_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}T")
    dirs = sorted(
        [d for d in runs_dir.iterdir() if d.is_dir() and ts_pattern.match(d.name)],
        key=lambda d: d.name,
        reverse=True,
    )
    if not dirs:
        print(f"[audit] No run directories found in {runs_dir}")
        sys.exit(1)
    return dirs[0]


def extract_run_date(run_dir: Path) -> str:
    """Extract YYYY-MM-DD from run directory name like 2026-02-24T20-38-53Z-..."""
    m = re.match(r"(\d{4}-\d{2}-\d{2})", run_dir.name)
    if m:
        return m.group(1)
    return datetime.utcnow().strftime("%Y-%m-%d")


def make_today_dates(run_date_str: str):
    """Return a set of full YYYY:MM:DD strings for 'today' and next day."""
    d = datetime.strptime(run_date_str, "%Y-%m-%d")
    dates = set()
    for delta in range(0, 2):
        dt = d + timedelta(days=delta)
        # event_key format: NBA:YYYY:MM:DD:AWAY@HOME — match full year
        dates.add(dt.strftime("%Y:%m:%d"))
    return dates


def is_today_event(event_key, today_dates):
    """Check if an event_key matches today's dates."""
    if not event_key:
        return False
    for d in today_dates:
        if d in event_key:
            return True
    return False


# ── Section A: Star-rating check ──────────────────────────────────────────

def audit_star_ratings(normalized, today_dates):
    """Flag BetQL records eligible for consensus with rating_stars < MIN_STARS."""
    issues = []
    betql_today = 0
    for rec in normalized:
        prov = rec.get("provenance", {})
        if prov.get("source_id") != "betql":
            continue
        if not rec.get("eligible_for_consensus"):
            continue
        event = rec.get("event", {})
        ek = event.get("event_key") or ""
        if not is_today_event(ek, today_dates):
            continue
        stars = prov.get("rating_stars")
        if stars is None:
            continue
        betql_today += 1
        if stars < MIN_STARS:
            mkt = rec.get("market", {})
            issues.append({
                "event_key": ek,
                "market_type": mkt.get("market_type", "?"),
                "player_key": mkt.get("player_key"),
                "stat_key": mkt.get("stat_key"),
                "selection": mkt.get("selection"),
                "line": mkt.get("line"),
                "stars": stars,
            })
    return issues, betql_today


# ── Section B: Source coverage ─────────────────────────────────────────────

def audit_source_coverage(normalized, today_dates):
    """Per-game matrix for today's games: which sources have picks."""
    game_sources = defaultdict(set)
    all_sources = set()
    for rec in normalized:
        if not rec.get("eligible_for_consensus"):
            continue
        event = rec.get("event", {})
        ek = event.get("event_key") or ""
        if not is_today_event(ek, today_dates):
            continue
        src = rec.get("provenance", {}).get("source_id") or "?"
        game_sources[ek].add(src)
        all_sources.add(src)
    return dict(game_sources), sorted(all_sources)


# ── Section C: Line discrepancies ──────────────────────────────────────────

def audit_line_discrepancies(signals):
    """Consensus picks where supporting sources have different lines."""
    issues = []
    for sig in signals:
        supports = sig.get("supports", [])
        if len(supports) < 2:
            continue
        lines_by_src = {}
        for sup in supports:
            src = sup.get("source_id", "?")
            line = sup.get("line")
            if line is not None:
                lines_by_src[src] = line
        if len(set(lines_by_src.values())) > 1:
            issues.append({
                "signal_id": sig.get("signal_id", "?"),
                "event_key": sig.get("event_key", "?"),
                "market_type": sig.get("market_type", "?"),
                "player_id": sig.get("player_id"),
                "selection": sig.get("selection"),
                "lines_by_source": lines_by_src,
            })
    return issues


# ── Section D: Player-name collisions ─────────────────────────────────────

def audit_player_collisions_from_signals(signals):
    """Flag player_id values where supports use different display_player_id forms.

    This catches cases where the same short ID (e.g. d_mitchell) aggregates
    supports from sources that may mean different players.
    """
    # For each player_id, collect all distinct display_player_id values
    pid_to_display = defaultdict(set)
    for sig in signals:
        pid = sig.get("player_id")
        if not pid:
            continue
        for sup in sig.get("supports", []):
            dpid = sup.get("display_player_id") or ""
            if dpid:
                pid_to_display[pid].add(dpid)

    # Flag IDs with multiple display forms
    collisions = {
        pid: forms for pid, forms in pid_to_display.items()
        if len(forms) > 1
    }
    return collisions


# ── Display ────────────────────────────────────────────────────────────────

def print_report(run_dir, normalized, signals, run_date):
    today_dates = make_today_dates(run_date)

    print(f"\n{'=' * 72}")
    print(f"  CONSENSUS AUDIT -- {run_dir.name}  (date: {run_date})")
    print(f"{'=' * 72}")

    # A: Star ratings
    star_issues, betql_today = audit_star_ratings(normalized, today_dates)
    print(f"\n  A. Star-Rating Check  (BetQL eligible today: {betql_today})")
    print(f"  {'-' * 68}")
    if star_issues:
        for iss in star_issues:
            player = iss['player_key'] or iss['selection']
            stat = iss['stat_key'] or iss['market_type']
            print(f"     !  {iss['stars']}*  {player} {stat} {iss['line']}  ({iss['event_key']})")
        print(f"     Total below {MIN_STARS}*: {len(star_issues)}")
    else:
        print(f"     OK  All BetQL consensus picks are {MIN_STARS}*+")

    # B: Source coverage
    game_sources, all_sources = audit_source_coverage(normalized, today_dates)
    print(f"\n  B. Source Coverage  ({len(game_sources)} games today, {len(all_sources)} sources)")
    print(f"  {'-' * 68}")
    if game_sources:
        for ek in sorted(game_sources.keys()):
            srcs = game_sources[ek]
            missing = set(all_sources) - srcs
            tag = ""
            if missing:
                tag = f"  missing: {', '.join(sorted(missing))}"
            short = ek.replace("NBA:2026:", "").replace("NCAAB:2026:", "")
            print(f"     {short:30s} [{len(srcs)}/{len(all_sources)}]{tag}")
    else:
        print(f"     No games found for {run_date}")

    # C: Line discrepancies
    line_issues = audit_line_discrepancies(signals)
    print(f"\n  C. Line Discrepancies  ({len(line_issues)} signals with divergent lines)")
    print(f"  {'-' * 68}")
    if line_issues:
        for iss in line_issues[:25]:
            pid = iss.get('player_id') or iss.get('selection', '?')
            lines_str = "  ".join(f"{s}={v}" for s, v in iss['lines_by_source'].items())
            print(f"     {pid:30s} {iss['market_type']:10s} {lines_str}")
        if len(line_issues) > 25:
            print(f"     ... and {len(line_issues) - 25} more")
    else:
        print(f"     OK  All consensus picks have matching lines across sources")

    # D: Player-name collisions
    collisions = audit_player_collisions_from_signals(signals)
    # Split into real collisions (different people) vs normalization variants
    real_collisions = {}
    norm_variants = {}
    for pid, forms in collisions.items():
        # Extract last-name parts from each form
        last_names = set()
        for f in forms:
            name_part = f.split(":")[-1]  # e.g. "donovan_mitchell"
            tokens = name_part.split("_", 1)
            if len(tokens) == 2 and len(tokens[0]) > 1:
                last_names.add(tokens[1])  # "mitchell"
            elif len(tokens) == 2:
                last_names.add(tokens[1])  # from short form
        # If multiple distinct full first names → real collision
        full_names = set()
        for f in forms:
            name_part = f.split(":")[-1]
            tokens = name_part.split("_", 1)
            if len(tokens) == 2 and len(tokens[0]) > 1:
                full_names.add(name_part)
        if len(full_names) > 1:
            real_collisions[pid] = forms
        else:
            norm_variants[pid] = forms

    print(f"\n  D. Player-Name Collisions  ({len(real_collisions)} real, {len(norm_variants)} normalization variants)")
    print(f"  {'-' * 68}")
    if real_collisions:
        print(f"     REAL COLLISIONS (different players, same short ID):")
        for pid, forms in sorted(real_collisions.items()):
            full = [f for f in forms if len(f.split(":")[-1].split("_", 1)[0]) > 1]
            print(f"     !  {pid}: {' vs '.join(sorted(full))}")
    if norm_variants:
        print(f"     Normalization variants ({len(norm_variants)} — same player, different ID forms):")
        for pid, forms in sorted(norm_variants.items()):
            print(f"        {pid}: {' | '.join(sorted(forms))}")
    if not collisions:
        print(f"     OK  No player ID collisions detected")

    print(f"\n{'=' * 72}\n")

    # Summary — only count real collisions as issues, not normalization variants
    total_issues = len(star_issues) + len(line_issues) + len(real_collisions)
    if total_issues == 0:
        print("  [audit] All checks passed")
    else:
        print(f"  [audit] {total_issues} issue(s) found -- review above")
    return total_issues


def main():
    ap = argparse.ArgumentParser(description="Audit consensus data integrity")
    ap.add_argument("--run-dir", type=str, default=None,
                    help="Path to specific run directory (default: latest)")
    args = ap.parse_args()

    if args.run_dir:
        run_dir = Path(args.run_dir)
    else:
        run_dir = find_latest_run(RUNS_DIR)

    run_date = extract_run_date(run_dir)
    print(f"[audit] Using run: {run_dir.name}")

    norm_path = run_dir / "normalized_records.jsonl"
    sig_path = run_dir / "signals.jsonl"

    if not norm_path.exists():
        print(f"[audit] Missing {norm_path}")
        sys.exit(1)
    if not sig_path.exists():
        print(f"[audit] Missing {sig_path}")
        sys.exit(1)

    normalized = read_jsonl(norm_path)
    signals = read_jsonl(sig_path)
    print(f"[audit] Loaded {len(normalized)} normalized records, {len(signals)} signals")

    total_issues = print_report(run_dir, normalized, signals, run_date)
    sys.exit(1 if total_issues > 0 else 0)


if __name__ == "__main__":
    main()
