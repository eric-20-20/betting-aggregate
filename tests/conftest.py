import sys
from pathlib import Path

# Ensure repo root and src/ are importable during tests.
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
for path in (str(SRC), str(ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)
