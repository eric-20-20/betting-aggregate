#!/usr/bin/env python3
"""
Audit source_combo fields in graded_occurrences_latest.jsonl

Checks:
1. sources_combo accurately reflects sources_present
2. No duplicates in any combo
3. Combos are sorted alphabetically
4. Cross-check: 5 signals with sources_combo="action|betql|dimers" — verify supports
5. Duplicate signal_ids
6. Win/Loss/Push math across combo buckets vs total
7. Total graded signals summary
"""

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

DATA = Path("/Users/Ericevans/Betting Aggregate/data/analysis/graded_occurrences_latest.jsonl")

def load_records():
    records = []
    with open(DATA) as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"  [WARN] JSON parse error on line {i}: {e}")
    return records

def main():
    print("=" * 80)
    print("SOURCE COMBO AUDIT — graded_occurrences_latest.jsonl")
    print("=" * 80)

    records = load_records()
    print(f"\nTotal records loaded: {len(records):,}")

    # ─── Tally statuses ───
    status_counts = Counter(r.get("result") or r.get("status") or "UNKNOWN" for r in records)
    print(f"\nResult/Status distribution:")
    for k, v in sorted(status_counts.items()):
        print(f"  {k}: {v:,}")

    graded = [r for r in records if r.get("result") in ("WIN", "LOSS", "PUSH")]
    print(f"\nGraded records (WIN/LOSS/PUSH): {len(graded):,}")

    # ════════════════════════════════════════════════════════════════
    # CHECK 1 & 2 & 3: combo vs sources_present, duplicates, sort
    # ════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("CHECK 1-3: sources_combo vs sources_present, duplicates, sort order")
    print("=" * 80)

    issues_mismatch = []
    issues_dup = []
    issues_sort = []
    issues_missing_combo = []
    issues_missing_present = []

    for i, r in enumerate(records):
        combo = r.get("sources_combo")
        present = r.get("sources_present")
        sig_id = r.get("signal_id", f"row_{i}")

        # Missing fields
        if combo is None:
            issues_missing_combo.append(sig_id)
            continue
        if present is None:
            issues_missing_present.append(sig_id)
            continue

        combo_parts = combo.split("|") if combo else []

        # Check duplicates in combo
        if len(combo_parts) != len(set(combo_parts)):
            issues_dup.append((sig_id, combo))

        # Check sort order
        if combo_parts != sorted(combo_parts):
            issues_sort.append((sig_id, combo, "|".join(sorted(combo_parts))))

        # Check match to sources_present
        if sorted(combo_parts) != sorted(present):
            issues_mismatch.append((sig_id, combo, present))

    print(f"  Missing sources_combo field: {len(issues_missing_combo):,}")
    print(f"  Missing sources_present field: {len(issues_missing_present):,}")
    print(f"  Duplicate sources within a combo: {len(issues_dup):,}")
    if issues_dup:
        for sid, c in issues_dup[:10]:
            print(f"    signal_id={sid}  combo={c}")

    print(f"  Combo NOT sorted alphabetically: {len(issues_sort):,}")
    if issues_sort:
        for sid, c, expected in issues_sort[:10]:
            print(f"    signal_id={sid}  combo={c}  expected={expected}")

    print(f"  Mismatch between sources_combo and sources_present: {len(issues_mismatch):,}")
    if issues_mismatch:
        for sid, c, p in issues_mismatch[:10]:
            print(f"    signal_id={sid}  combo={c}  present={p}")

    # ─── Also check signal_sources_combo vs signal_sources_present ───
    print("\n  --- Also checking signal_sources_combo vs signal_sources_present ---")
    sig_issues = []
    for i, r in enumerate(records):
        sc = r.get("signal_sources_combo")
        sp = r.get("signal_sources_present")
        if sc is None or sp is None:
            continue
        sc_parts = sc.split("|") if sc else []
        if sorted(sc_parts) != sorted(sp):
            sig_issues.append((r.get("signal_id", f"row_{i}"), sc, sp))
    print(f"  signal_sources_combo vs signal_sources_present mismatch: {len(sig_issues):,}")
    if sig_issues:
        for sid, c, p in sig_issues[:5]:
            print(f"    signal_id={sid}  combo={c}  present={p}")

    # ─── Check sources_combo == signal_sources_combo (should be same?) ───
    print("\n  --- Comparing sources_combo vs signal_sources_combo ---")
    cross_mismatch = []
    for i, r in enumerate(records):
        sc = r.get("sources_combo")
        ssc = r.get("signal_sources_combo")
        if sc != ssc:
            cross_mismatch.append((r.get("signal_id", f"row_{i}"), sc, ssc))
    print(f"  sources_combo != signal_sources_combo: {len(cross_mismatch):,}")
    if cross_mismatch:
        for sid, a, b in cross_mismatch[:5]:
            print(f"    signal_id={sid}  sources_combo={a}  signal_sources_combo={b}")

    # ════════════════════════════════════════════════════════════════
    # CHECK 4: Cross-check "action|betql|dimers" signals via supports
    # ════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print('CHECK 4: Cross-check signals with sources_combo="action|betql|dimers"')
    print("=" * 80)

    target_combo = "action|betql|dimers"
    target_recs = [r for r in records if r.get("sources_combo") == target_combo]
    print(f"  Records with sources_combo='{target_combo}': {len(target_recs):,}")

    if not target_recs:
        # Try any 3-source combo
        combo_counts = Counter(r.get("sources_combo") for r in records)
        three_source = [(c, n) for c, n in combo_counts.items() if c and c.count("|") == 2]
        three_source.sort(key=lambda x: -x[1])
        if three_source:
            target_combo = three_source[0][0]
            target_recs = [r for r in records if r.get("sources_combo") == target_combo]
            print(f"  Falling back to '{target_combo}' ({len(target_recs):,} records)")

    # For the cross-check, we need to look at the supports at the SIGNAL level.
    # The occurrence's supports[] may only contain the sources relevant to that occurrence.
    # So we gather all occurrences for the same signal_id and union their supports.
    signal_id_to_occs = defaultdict(list)
    for r in records:
        sid = r.get("signal_id")
        if sid:
            signal_id_to_occs[sid].append(r)

    sample = target_recs[:5]
    for idx, r in enumerate(sample):
        sid = r.get("signal_id")
        combo = r.get("sources_combo")
        present = r.get("sources_present")

        # Gather all supports across all occurrences of this signal
        all_occs = signal_id_to_occs.get(sid, [])
        all_support_sources = set()
        all_occ_sources = set()
        for occ in all_occs:
            for sup in occ.get("supports", []):
                all_support_sources.add(sup.get("source_id"))
            all_occ_sources.add(occ.get("occ_source_id"))

        # Each occurrence IS a source contribution, so the union of occ_source_id across
        # all occurrences of a signal should equal sources_present
        print(f"\n  Sample {idx+1}: signal_id={sid[:20]}...")
        print(f"    sources_combo:   {combo}")
        print(f"    sources_present: {present}")
        print(f"    # occurrences for this signal: {len(all_occs)}")
        print(f"    occ_source_ids across all occurrences: {sorted(all_occ_sources)}")
        print(f"    support source_ids (union): {sorted(all_support_sources)}")

        expected = set(combo.split("|"))
        if all_occ_sources == expected:
            print(f"    VERIFIED: occ_source_ids match sources_combo")
        else:
            missing = expected - all_occ_sources
            extra = all_occ_sources - expected
            if missing:
                print(f"    MISMATCH: sources in combo but no occurrence: {sorted(missing)}")
            if extra:
                print(f"    MISMATCH: occ sources not in combo: {sorted(extra)}")

    # ════════════════════════════════════════════════════════════════
    # CHECK 5: Duplicate signal_ids
    # ════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("CHECK 5: Duplicate signal_ids in the dataset")
    print("=" * 80)

    signal_ids = [r.get("signal_id") for r in records if r.get("signal_id")]
    total_signals = len(signal_ids)
    unique_signals = len(set(signal_ids))
    dup_count = total_signals - unique_signals
    print(f"  Total records with signal_id: {total_signals:,}")
    print(f"  Unique signal_ids: {unique_signals:,}")
    print(f"  Duplicate signal_ids (expected -- multiple occurrences per signal): {dup_count:,}")

    # Also check occurrence_id duplicates (these SHOULD be unique)
    occ_ids = [r.get("occurrence_id") for r in records if r.get("occurrence_id")]
    total_occ = len(occ_ids)
    unique_occ = len(set(occ_ids))
    dup_occ = total_occ - unique_occ
    print(f"\n  Total records with occurrence_id: {total_occ:,}")
    print(f"  Unique occurrence_ids: {unique_occ:,}")
    print(f"  Duplicate occurrence_ids: {dup_occ:,}")
    if dup_occ > 0:
        occ_counter = Counter(occ_ids)
        dups = [(oid, cnt) for oid, cnt in occ_counter.items() if cnt > 1]
        print(f"  [FLAG] {len(dups)} occurrence_ids appear more than once!")
        for oid, cnt in dups[:5]:
            print(f"    occurrence_id={oid}  count={cnt}")

    # ════════════════════════════════════════════════════════════════
    # CHECK 6 & 7: Win/Loss/Push math across combo buckets
    # ════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("CHECK 6-7: Win/Loss/Push totals across combo buckets vs total graded")
    print("=" * 80)

    # The reports group by sources_combo. Each occurrence is one row.
    combo_buckets = defaultdict(Counter)
    total_result_counts = Counter()
    for r in graded:
        combo = r.get("sources_combo", "MISSING")
        result = r.get("result")
        combo_buckets[combo][result] += 1
        total_result_counts[result] += 1

    sum_across_buckets = sum(sum(v.values()) for v in combo_buckets.values())

    print(f"\n  Total graded occurrences: {len(graded):,}")
    print(f"  Sum across all combo buckets: {sum_across_buckets:,}")
    print(f"  Match: {'YES' if len(graded) == sum_across_buckets else 'NO -- MISMATCH!'}")

    print(f"\n  Result totals:")
    for res in ("WIN", "LOSS", "PUSH"):
        print(f"    {res}: {total_result_counts[res]:,}")

    win_rate = total_result_counts["WIN"] / (total_result_counts["WIN"] + total_result_counts["LOSS"]) * 100 if (total_result_counts["WIN"] + total_result_counts["LOSS"]) > 0 else 0
    print(f"\n  Win rate (excl. PUSH): {win_rate:.1f}%")

    # Show combo bucket breakdown
    print(f"\n  Combo bucket breakdown (top 20 by volume):")
    sorted_combos = sorted(combo_buckets.items(), key=lambda x: -sum(x[1].values()))
    for combo, counts in sorted_combos[:20]:
        w = counts.get("WIN", 0)
        l = counts.get("LOSS", 0)
        p = counts.get("PUSH", 0)
        tot = w + l + p
        wr = w / (w + l) * 100 if (w + l) > 0 else 0
        print(f"    {combo:<50s}  W={w:>4d}  L={l:>4d}  P={p:>3d}  total={tot:>5d}  WR={wr:.1f}%")

    # ════════════════════════════════════════════════════════════════
    # ADDITIONAL: Check if every source in sources_present also appears
    # as occ_source_id in at least one occurrence for that signal
    # ════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("ADDITIONAL: Verify each source in sources_present has an occurrence")
    print("=" * 80)

    # Group by signal_id, check occ_source_ids cover sources_present
    signals_checked = 0
    signals_missing_occ = 0
    examples_missing = []
    for sid, occs in signal_id_to_occs.items():
        if not occs:
            continue
        # sources_present should be the same across all occurrences of a signal
        expected_sources = set(occs[0].get("sources_present") or [])
        actual_occ_sources = set(o.get("occ_source_id") for o in occs if o.get("occ_source_id"))
        signals_checked += 1

        if not expected_sources.issubset(actual_occ_sources):
            missing = expected_sources - actual_occ_sources
            signals_missing_occ += 1
            if len(examples_missing) < 5:
                examples_missing.append((sid, sorted(expected_sources), sorted(actual_occ_sources), sorted(missing)))

    print(f"  Signals checked: {signals_checked:,}")
    print(f"  Signals where sources_present has a source with no occurrence: {signals_missing_occ:,}")
    if examples_missing:
        for sid, exp, act, miss in examples_missing:
            print(f"    signal_id={sid[:30]}...")
            print(f"      expected: {exp}")
            print(f"      actual occ sources: {act}")
            print(f"      missing: {miss}")

    # ════════════════════════════════════════════════════════════════
    # SUMMARY
    # ════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    all_ok = True
    checks = [
        ("Combo matches sources_present", len(issues_mismatch) == 0),
        ("No duplicate sources in combos", len(issues_dup) == 0),
        ("Combos sorted alphabetically", len(issues_sort) == 0),
        ("signal_sources_combo matches signal_sources_present", len(sig_issues) == 0),
        ("sources_combo == signal_sources_combo", len(cross_mismatch) == 0),
        ("No duplicate occurrence_ids", dup_occ == 0),
        ("Win/Loss/Push totals match across buckets", len(graded) == sum_across_buckets),
        ("All sources in sources_present have occurrences", signals_missing_occ == 0),
    ]
    for label, ok in checks:
        status = "PASS" if ok else "FAIL"
        if not ok:
            all_ok = False
        print(f"  [{status}] {label}")

    if all_ok:
        print("\n  All checks passed.")
    else:
        print("\n  Some checks failed -- see details above.")

if __name__ == "__main__":
    main()
