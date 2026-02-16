"""Source Replay Harness: Store and replay HTML/JSON snapshots for deterministic testing.

This module provides:
- Snapshot storage for HTML pages and API responses
- Replay capability for debugging parsers
- Diff detection between snapshot runs
- Fixture generation for unit tests

Usage:
    # During ingestion, capture snapshots
    harness = ReplayHarness("action", sport="NBA")
    harness.capture("game_page", url, html_content)

    # Later, replay for testing
    harness = ReplayHarness("action", sport="NBA")
    html = harness.replay("game_page", url)

    # Generate test fixtures
    harness.export_fixtures("tests/fixtures/action/")
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import shutil
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# =============================================================================
# CONFIGURATION
# =============================================================================

DEFAULT_SNAPSHOT_DIR = Path("data/snapshots")
DEFAULT_FIXTURE_DIR = Path("tests/fixtures")


# =============================================================================
# SNAPSHOT METADATA
# =============================================================================

@dataclass
class SnapshotMeta:
    """Metadata for a captured snapshot."""
    source_id: str
    sport: str
    snapshot_type: str  # "html", "json", "api_response"
    url: str
    url_hash: str
    captured_at: str
    content_hash: str
    content_size: int
    compressed: bool = True
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "SnapshotMeta":
        return SnapshotMeta(**d)


@dataclass
class SnapshotIndex:
    """Index of all snapshots for a source."""
    source_id: str
    sport: str
    created_at: str
    updated_at: str
    snapshots: Dict[str, SnapshotMeta]  # url_hash -> metadata

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "sport": self.sport,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "snapshots": {k: v.to_dict() for k, v in self.snapshots.items()},
        }

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "SnapshotIndex":
        return SnapshotIndex(
            source_id=d["source_id"],
            sport=d["sport"],
            created_at=d["created_at"],
            updated_at=d["updated_at"],
            snapshots={k: SnapshotMeta.from_dict(v) for k, v in d.get("snapshots", {}).items()},
        )


# =============================================================================
# REPLAY HARNESS
# =============================================================================

class ReplayHarness:
    """Harness for capturing and replaying source snapshots."""

    def __init__(
        self,
        source_id: str,
        sport: str = "NBA",
        snapshot_dir: Optional[Path] = None,
        mode: str = "capture",  # "capture", "replay", or "passthrough"
    ):
        """Initialize replay harness.

        Args:
            source_id: Source identifier (e.g., "action", "covers")
            sport: Sport identifier (e.g., "NBA", "NCAAB")
            snapshot_dir: Directory for snapshots (default: data/snapshots)
            mode: Operating mode:
                - "capture": Save snapshots during fetch
                - "replay": Load from snapshots instead of fetching
                - "passthrough": No snapshot handling
        """
        self.source_id = source_id
        self.sport = sport
        self.snapshot_dir = snapshot_dir or DEFAULT_SNAPSHOT_DIR
        self.mode = mode

        self._source_dir = self.snapshot_dir / source_id / sport
        self._index_path = self._source_dir / "index.json"
        self._index: Optional[SnapshotIndex] = None

        # Create directories if capturing
        if mode == "capture":
            self._source_dir.mkdir(parents=True, exist_ok=True)

    def _load_index(self) -> SnapshotIndex:
        """Load or create snapshot index."""
        if self._index is not None:
            return self._index

        if self._index_path.exists():
            try:
                data = json.loads(self._index_path.read_text())
                self._index = SnapshotIndex.from_dict(data)
            except Exception:
                self._index = self._create_empty_index()
        else:
            self._index = self._create_empty_index()

        return self._index

    def _create_empty_index(self) -> SnapshotIndex:
        """Create empty snapshot index."""
        now = datetime.now(timezone.utc).isoformat()
        return SnapshotIndex(
            source_id=self.source_id,
            sport=self.sport,
            created_at=now,
            updated_at=now,
            snapshots={},
        )

    def _save_index(self) -> None:
        """Save snapshot index."""
        if self._index is None:
            return
        self._index.updated_at = datetime.now(timezone.utc).isoformat()
        self._index_path.write_text(json.dumps(self._index.to_dict(), indent=2))

    @staticmethod
    def _hash_url(url: str) -> str:
        """Create short hash of URL for filename."""
        # Use first 12 chars of SHA256
        return hashlib.sha256(url.encode()).hexdigest()[:12]

    @staticmethod
    def _hash_content(content: str) -> str:
        """Create hash of content."""
        return hashlib.sha256(content.encode()).hexdigest()

    def _snapshot_path(self, url_hash: str, snapshot_type: str) -> Path:
        """Get path for snapshot file."""
        ext = ".html.gz" if snapshot_type == "html" else ".json.gz"
        return self._source_dir / f"{url_hash}{ext}"

    def capture(
        self,
        snapshot_type: str,
        url: str,
        content: str,
        tags: Optional[List[str]] = None,
    ) -> SnapshotMeta:
        """Capture a snapshot.

        Args:
            snapshot_type: Type of content ("html", "json", "api_response")
            url: Source URL
            content: Content to snapshot
            tags: Optional tags for filtering

        Returns:
            Snapshot metadata
        """
        if self.mode == "passthrough":
            # Return metadata without saving
            return SnapshotMeta(
                source_id=self.source_id,
                sport=self.sport,
                snapshot_type=snapshot_type,
                url=url,
                url_hash=self._hash_url(url),
                captured_at=datetime.now(timezone.utc).isoformat(),
                content_hash=self._hash_content(content),
                content_size=len(content),
                compressed=False,
                tags=tags or [],
            )

        index = self._load_index()
        url_hash = self._hash_url(url)
        content_hash = self._hash_content(content)

        # Check if we already have this exact content
        existing = index.snapshots.get(url_hash)
        if existing and existing.content_hash == content_hash:
            # Content unchanged, just update timestamp
            existing.captured_at = datetime.now(timezone.utc).isoformat()
            self._save_index()
            return existing

        # Save compressed content
        snapshot_path = self._snapshot_path(url_hash, snapshot_type)
        with gzip.open(snapshot_path, "wt", encoding="utf-8") as f:
            f.write(content)

        # Create metadata
        meta = SnapshotMeta(
            source_id=self.source_id,
            sport=self.sport,
            snapshot_type=snapshot_type,
            url=url,
            url_hash=url_hash,
            captured_at=datetime.now(timezone.utc).isoformat(),
            content_hash=content_hash,
            content_size=len(content),
            compressed=True,
            tags=tags or [],
        )

        index.snapshots[url_hash] = meta
        self._save_index()

        return meta

    def replay(self, url: str) -> Optional[str]:
        """Replay a snapshot.

        Args:
            url: Source URL to replay

        Returns:
            Snapshot content or None if not found
        """
        if self.mode == "passthrough":
            return None

        index = self._load_index()
        url_hash = self._hash_url(url)

        meta = index.snapshots.get(url_hash)
        if meta is None:
            return None

        snapshot_path = self._snapshot_path(url_hash, meta.snapshot_type)
        if not snapshot_path.exists():
            return None

        with gzip.open(snapshot_path, "rt", encoding="utf-8") as f:
            return f.read()

    def has_snapshot(self, url: str) -> bool:
        """Check if snapshot exists for URL."""
        index = self._load_index()
        url_hash = self._hash_url(url)
        return url_hash in index.snapshots

    def get_metadata(self, url: str) -> Optional[SnapshotMeta]:
        """Get metadata for a snapshot."""
        index = self._load_index()
        url_hash = self._hash_url(url)
        return index.snapshots.get(url_hash)

    def list_snapshots(self, tags: Optional[List[str]] = None) -> List[SnapshotMeta]:
        """List all snapshots, optionally filtered by tags."""
        index = self._load_index()
        snapshots = list(index.snapshots.values())

        if tags:
            snapshots = [s for s in snapshots if any(t in s.tags for t in tags)]

        return sorted(snapshots, key=lambda s: s.captured_at, reverse=True)

    def detect_drift(self, url: str, current_content: str) -> Optional[Dict[str, Any]]:
        """Detect if content has drifted from snapshot.

        Returns dict with drift info or None if no drift.
        """
        index = self._load_index()
        url_hash = self._hash_url(url)

        meta = index.snapshots.get(url_hash)
        if meta is None:
            return {"type": "new", "url": url}

        current_hash = self._hash_content(current_content)
        if current_hash == meta.content_hash:
            return None  # No drift

        # Content changed
        return {
            "type": "changed",
            "url": url,
            "previous_hash": meta.content_hash,
            "current_hash": current_hash,
            "previous_size": meta.content_size,
            "current_size": len(current_content),
            "size_delta": len(current_content) - meta.content_size,
            "captured_at": meta.captured_at,
        }

    def export_fixtures(
        self,
        output_dir: Optional[Path] = None,
        max_fixtures: int = 10,
        tags: Optional[List[str]] = None,
    ) -> List[Path]:
        """Export snapshots as test fixtures.

        Args:
            output_dir: Directory for fixtures (default: tests/fixtures/{source_id})
            max_fixtures: Maximum number of fixtures to export
            tags: Optional tags to filter snapshots

        Returns:
            List of exported fixture paths
        """
        output_dir = output_dir or (DEFAULT_FIXTURE_DIR / self.source_id)
        output_dir.mkdir(parents=True, exist_ok=True)

        snapshots = self.list_snapshots(tags=tags)[:max_fixtures]
        exported = []

        for meta in snapshots:
            content = self.replay(meta.url)
            if content is None:
                continue

            # Create fixture file (uncompressed for readability)
            ext = ".html" if meta.snapshot_type == "html" else ".json"
            fixture_path = output_dir / f"{meta.url_hash}{ext}"
            fixture_path.write_text(content)

            # Create metadata sidecar
            meta_path = output_dir / f"{meta.url_hash}.meta.json"
            meta_path.write_text(json.dumps(meta.to_dict(), indent=2))

            exported.append(fixture_path)

        return exported

    def clear_snapshots(self, older_than_days: Optional[int] = None) -> int:
        """Clear snapshots.

        Args:
            older_than_days: Only clear snapshots older than this many days.
                           If None, clear all.

        Returns:
            Number of snapshots cleared
        """
        index = self._load_index()
        cleared = 0
        now = datetime.now(timezone.utc)

        to_remove = []
        for url_hash, meta in index.snapshots.items():
            if older_than_days is not None:
                captured = datetime.fromisoformat(meta.captured_at.replace("Z", "+00:00"))
                age_days = (now - captured).days
                if age_days <= older_than_days:
                    continue

            # Remove snapshot file
            snapshot_path = self._snapshot_path(url_hash, meta.snapshot_type)
            if snapshot_path.exists():
                snapshot_path.unlink()
            to_remove.append(url_hash)
            cleared += 1

        for url_hash in to_remove:
            del index.snapshots[url_hash]

        self._save_index()
        return cleared


# =============================================================================
# DIFF UTILITIES
# =============================================================================

def diff_html_structure(old_html: str, new_html: str) -> Dict[str, Any]:
    """Compare HTML structure (not content) for detecting layout changes.

    This helps detect when a site's DOM structure changes, which often
    breaks parsers even if the visible content is similar.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return {"error": "beautifulsoup4 not installed"}

    old_soup = BeautifulSoup(old_html, "html.parser")
    new_soup = BeautifulSoup(new_html, "html.parser")

    def extract_structure(soup) -> Dict[str, int]:
        """Extract tag counts and class names."""
        structure = {}
        for tag in soup.find_all(True):
            tag_name = tag.name
            structure[tag_name] = structure.get(tag_name, 0) + 1

            classes = tag.get("class", [])
            for cls in classes:
                key = f".{cls}"
                structure[key] = structure.get(key, 0) + 1

        return structure

    old_struct = extract_structure(old_soup)
    new_struct = extract_structure(new_soup)

    # Find differences
    all_keys = set(old_struct.keys()) | set(new_struct.keys())
    added = []
    removed = []
    changed = []

    for key in all_keys:
        old_count = old_struct.get(key, 0)
        new_count = new_struct.get(key, 0)

        if old_count == 0 and new_count > 0:
            added.append({"selector": key, "count": new_count})
        elif old_count > 0 and new_count == 0:
            removed.append({"selector": key, "count": old_count})
        elif abs(new_count - old_count) > max(1, old_count * 0.2):  # >20% change
            changed.append({
                "selector": key,
                "old_count": old_count,
                "new_count": new_count,
                "delta": new_count - old_count,
            })

    return {
        "added": added,
        "removed": removed,
        "changed": changed,
        "has_drift": len(added) + len(removed) + len(changed) > 0,
    }


# =============================================================================
# FIXTURE LOADER
# =============================================================================

class FixtureLoader:
    """Load test fixtures for parser testing."""

    def __init__(self, source_id: str, fixture_dir: Optional[Path] = None):
        self.source_id = source_id
        self.fixture_dir = fixture_dir or (DEFAULT_FIXTURE_DIR / source_id)

    def list_fixtures(self) -> List[Tuple[str, Path]]:
        """List available fixtures.

        Returns:
            List of (url_hash, fixture_path) tuples
        """
        if not self.fixture_dir.exists():
            return []

        fixtures = []
        for path in self.fixture_dir.glob("*.html"):
            url_hash = path.stem
            if not url_hash.endswith(".meta"):
                fixtures.append((url_hash, path))

        for path in self.fixture_dir.glob("*.json"):
            url_hash = path.stem
            if not url_hash.endswith(".meta"):
                fixtures.append((url_hash, path))

        return fixtures

    def load_fixture(self, url_hash: str) -> Tuple[str, Optional[SnapshotMeta]]:
        """Load a fixture by URL hash.

        Returns:
            (content, metadata) tuple
        """
        # Try HTML first, then JSON
        for ext in [".html", ".json"]:
            path = self.fixture_dir / f"{url_hash}{ext}"
            if path.exists():
                content = path.read_text()

                # Load metadata if available
                meta_path = self.fixture_dir / f"{url_hash}.meta.json"
                meta = None
                if meta_path.exists():
                    try:
                        meta = SnapshotMeta.from_dict(json.loads(meta_path.read_text()))
                    except Exception:
                        pass

                return content, meta

        raise FileNotFoundError(f"Fixture not found: {url_hash}")

    def load_all_fixtures(self) -> List[Tuple[str, str, Optional[SnapshotMeta]]]:
        """Load all fixtures.

        Returns:
            List of (url_hash, content, metadata) tuples
        """
        results = []
        for url_hash, _ in self.list_fixtures():
            try:
                content, meta = self.load_fixture(url_hash)
                results.append((url_hash, content, meta))
            except Exception:
                continue
        return results


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def capture_html_snapshot(
    source_id: str,
    url: str,
    html: str,
    sport: str = "NBA",
    tags: Optional[List[str]] = None,
) -> SnapshotMeta:
    """Convenience function to capture an HTML snapshot."""
    harness = ReplayHarness(source_id, sport=sport, mode="capture")
    return harness.capture("html", url, html, tags=tags)


def replay_html_snapshot(
    source_id: str,
    url: str,
    sport: str = "NBA",
) -> Optional[str]:
    """Convenience function to replay an HTML snapshot."""
    harness = ReplayHarness(source_id, sport=sport, mode="replay")
    return harness.replay(url)


def check_for_drift(
    source_id: str,
    url: str,
    current_html: str,
    sport: str = "NBA",
) -> Optional[Dict[str, Any]]:
    """Check if HTML content has drifted from snapshot."""
    harness = ReplayHarness(source_id, sport=sport, mode="capture")
    return harness.detect_drift(url, current_html)
