import os, json, argparse
from pathlib import Path
from supabase import create_client

def read_json(path: Path):
    return json.loads(path.read_text())

def read_jsonl(path: Path):
    out = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out

def chunked(lst, n=500):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, help="data/runs/<run_id>")
    args = ap.parse_args()

    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]  # use service role for server-side inserts
    sb = create_client(url, key)

    run_dir = Path(args.run_dir)
    meta = read_json(run_dir / "run_meta.json")
    run_id = meta.get("run_id") or run_dir.name

    # 1) upsert run
    run_row = {
        "run_id": run_id,
        "observed_at_utc": meta.get("observed_at_utc") or meta.get("observed_at") or None,
        "sport": meta.get("sport") or "NBA",
        "sources": meta.get("sources") or meta.get("by_source") or None,
        "counts": meta.get("counts") or {
            "records_loaded": meta.get("records_loaded"),
            "eligible": meta.get("eligible"),
            "hard": meta.get("hard_count"),
            "soft": meta.get("soft_count"),
            "avoid": meta.get("avoid_count"),
        },
        "git_commit": meta.get("git_commit"),
        "argv": meta.get("argv"),
        "versions": meta.get("versions"),
    }
    sb.table("runs").upsert(run_row).execute()

    # helper to compute day_key/matchup from event_key-ish fields if present
    def extract_day_matchup(obj):
        day_key = obj.get("day_key") or obj.get("event_day_key")
        matchup = obj.get("matchup")
        # evidence records may have nested event
        if not matchup and "event" in obj:
            ev = obj["event"] or {}
            a = ev.get("away_team"); h = ev.get("home_team")
            if a and h:
                matchup = f"{a}@{h}"
        # day_key from nested event_key if needed
        if not day_key:
            ek = obj.get("event_key") or (obj.get("event") or {}).get("event_key")
            # if ek looks like NBA:YYYYMMDD:AWY@HME:time -> take first 3 parts
            if ek and ":" in ek:
                parts = ek.split(":")
                if len(parts) >= 3:
                    day_key = ":".join(parts[:3])
        return day_key, matchup

    # 2) upsert evidence
    evidence_path = run_dir / "normalized_records.jsonl"
    evidence = read_jsonl(evidence_path)

    evidence_rows = []
    for r in evidence:
        prov = r.get("provenance") or {}
        ev = r.get("event") or {}
        mk = r.get("market") or {}
        day_key, matchup = extract_day_matchup(r)

        evidence_rows.append({
            "evidence_id": r.get("evidence_id"),
            "run_id": run_id,
            "source_id": prov.get("source_id"),
            "sport": prov.get("sport"),
            "observed_at_utc": prov.get("observed_at_utc"),
            "canonical_url": prov.get("canonical_url"),
            "raw_fingerprint": prov.get("raw_fingerprint"),
            "expert_id": prov.get("expert_id"),
            "expert_name": prov.get("expert_name"),
            "event_key": ev.get("event_key"),
            "day_key": day_key,
            "matchup": matchup,
            "market_type": mk.get("market_type"),
            "selection": mk.get("selection"),
            "line": mk.get("line"),
            "odds": mk.get("odds"),
            "eligible_for_consensus": r.get("eligible_for_consensus") or (r.get("eligibility") or {}).get("eligible_for_consensus"),
            "record": r,
        })

    for batch in chunked(evidence_rows, 500):
        sb.table("evidence").upsert(batch).execute()

    # 3) upsert signals (post dedupe)
    signals_path = run_dir / "signals.jsonl"
    signals = read_jsonl(signals_path)

    signal_rows = []
    for s in signals:
        day_key = s.get("day_key") or s.get("event_day_key")
        matchup = s.get("matchup")
        signal_rows.append({
            "signal_id": s.get("signal_id"),
            "run_id": run_id,
            "signal_type": s.get("signal_type"),
            "sport": s.get("sport") or "NBA",
            "day_key": day_key,
            "matchup": matchup,
            "market_type": s.get("market_type"),
            "selection": s.get("selection"),
            "player_id": s.get("player_id"),
            "atomic_stat": s.get("atomic_stat"),
            "direction": s.get("direction"),
            "line_median": s.get("line_median"),
            "line_min": s.get("line_min"),
            "line_max": s.get("line_max"),
            "lines": s.get("lines"),
            "sources": s.get("sources"),
            "expert_strength": s.get("expert_strength"),
            "source_strength": s.get("source_strength"),
            "count_total": s.get("count_total"),
            "score": s.get("score"),
            "record": s,
        })

    for batch in chunked(signal_rows, 500):
        sb.table("signals").upsert(batch).execute()

    print(f"✅ Uploaded run {run_id}: evidence={len(evidence_rows)} signals={len(signal_rows)}")

if __name__ == "__main__":
    main()