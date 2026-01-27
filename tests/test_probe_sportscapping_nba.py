import pathlib
import sys
from datetime import datetime, timezone

from bs4 import BeautifulSoup

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.probe_sportscapping_nba import parse_blocks  # noqa: E402


FIXTURE_HTML = """
<html>
  <body>
    <div class="pick-card">
      <h3>John Martin</h3>
      <div class="matchup">NBA | Magic vs Cavs</div>
      <div class="play">Play on: Magic +6½ -110 at Buckeye</div>
      <div class="released">Released on Jan 23, 2026 11:00 AM EST</div>
    </div>
  </body>
</html>
"""


def test_parse_single_block():
    soup = BeautifulSoup(FIXTURE_HTML, "html.parser")
    observed = datetime(2026, 1, 23, 12, 0, tzinfo=timezone.utc).isoformat()
    recs = parse_blocks(soup, "https://www.sportscapping.com/free-nba-picks.html", observed)
    assert len(recs) == 1
    rec = recs[0]
    assert rec["expert_name"] == "John Martin"
    assert "Magic +6½ -110" in rec["raw_pick_text"]
    assert rec["canonical_url"] == "https://www.sportscapping.com/free-nba-picks.html"
    assert rec["market_family"] == "spread"
    assert rec.get("matchup_hint") == "Magic vs Cavs"
