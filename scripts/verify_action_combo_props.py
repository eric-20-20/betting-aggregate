import json
from pathlib import Path
from typing import Any, Dict, List, Set


def day_key(event_key: str | None) -> str | None:
    if not event_key or not isinstance(event_key, str):
        return None
    parts = event_key.split(":")
    if len(parts) >= 3:
        return ":".join(parts[:3])
    return None


def load_json(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError:
        return []
    return data if isinstance(data, list) else []


def find_combo_normalized(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    combo_keys: Set[str] = {"pts_reb", "pts_reb_ast", "pra", "reb_ast", "pts_ast"}
    combos: List[Dict[str, Any]] = []
    for rec in records:
        if not rec.get("eligible_for_consensus"):
            continue
        market = rec.get("market") or {}
        if market.get("market_type") != "player_prop":
            continue
        stat_key = market.get("stat_key") or ""
        if stat_key in combo_keys:
            combos.append(rec)
    return combos


def find_raw_combo_mentions(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    patterns = ["pts+reb", "pts+rebs", "pts+rebounds", "pts+rebs+asts", "pra"]
    hits: List[Dict[str, Any]] = []
    for rec in raw_records:
        blob = f"{rec.get('raw_pick_text', '')} {rec.get('raw_block', '')}".lower()
        if any(p in blob for p in patterns):
            hits.append(rec)
    return hits


def main() -> None:
    norm_path = Path("out/normalized_action_nba.json")
    raw_path = Path("out/raw_action_nba.json")
    normalized = load_json(norm_path)
    raw_records = load_json(raw_path)

    combo_norm = find_combo_normalized(normalized)
    print(f"[combo normalized] records={len(combo_norm)} (source file exists={norm_path.exists()})")
    for rec in combo_norm[:20]:
        event = rec.get("event") or {}
        market = rec.get("market") or {}
        prov = rec.get("provenance") or {}
        print(
            f"{day_key(event.get('event_key'))} {market.get('selection')} {market.get('side')} "
            f"line={market.get('line')} stat_key={market.get('stat_key')} source={prov.get('source_id')}"
        )

    raw_hits = find_raw_combo_mentions(raw_records)
    print(f"[raw combo mentions] records={len(raw_hits)} (source file exists={raw_path.exists()})")
    for rec in raw_hits[:20]:
        print(f"raw_pick_text={rec.get('raw_pick_text')}")


if __name__ == "__main__":
    main()
