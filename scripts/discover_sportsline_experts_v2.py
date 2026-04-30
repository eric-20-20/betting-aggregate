#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("ERROR: playwright not installed. Run: pip install playwright && playwright install chromium")
    sys.exit(1)


DEFAULT_OUTPUT = "out/raw_sportsline_experts_index_v2.json"
EXPERTS_URL = "https://www.sportsline.com/experts/"


EXTRACT_EXPERTS_INDEX_JS = """() => {
  const cards = [];
  const links = Array.from(document.querySelectorAll('a[href*="/experts/"][href$="/"], a[href*="/experts/"]'));
  const seen = new Set();
  for (const link of links) {
    const href = link.getAttribute('href') || '';
    if (!href.includes('/experts/')) continue;
    const abs = new URL(href, window.location.origin).toString();
    const slugMatch = abs.match(/\\/experts\\/\\d+\\/([^/?#]+)\\/?/);
    if (!slugMatch) continue;
    const slug = slugMatch[1];
    if (seen.has(slug)) continue;
    seen.add(slug);

    let card = link.closest('article, section, div');
    let hops = 0;
    while (card && hops < 6) {
      const txt = (card.innerText || '').trim();
      if (txt.includes('View Profile') || txt.includes('Live Picks')) break;
      card = card.parentElement;
      hops += 1;
    }
    const text = (card?.innerText || link.innerText || '').trim();
    const lines = text.split('\\n').map(s => s.trim()).filter(Boolean);
    const maybeName = lines.find(line => line && !/^View Profile$/i.test(line) && !/^Live Picks$/i.test(line) && !/^[+\\-]?\\d/.test(line));
    cards.push({
      expert_slug: slug,
      expert_name: maybeName || slug.replace(/-/g, ' '),
      profile_url: abs,
      card_text: text.slice(0, 4000),
      live_pick_lines: lines.filter(line => /^[a-z]+\\s+-\\s+/i.test(line)).slice(0, 10),
      record_lines: lines.filter(line => /IN LAST \\d+/i.test(line)).slice(0, 20),
    });
  }
  return cards;
}""".strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Discover SportsLine experts from the experts index page")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--headful", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    out_path = ROOT / args.output
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.headful)
        page = browser.new_page()
        page.goto(EXPERTS_URL, wait_until="domcontentloaded", timeout=45000)
        page.wait_for_timeout(3000)
        cards: List[Dict[str, Any]] = page.evaluate(EXTRACT_EXPERTS_INDEX_JS) or []
        browser.close()

    cards = sorted(cards, key=lambda item: item.get("expert_slug", ""))
    payload = {
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_url": EXPERTS_URL,
        "experts": cards,
        "count": len(cards),
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Discovered {len(cards)} experts -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
