"""
Deploy migration 002: Create bulk_update_expert_results Postgres function.

Deploys via the Supabase CLI (npx supabase db query --linked).
Falls back to printing the SQL for manual paste in the SQL Editor.

Usage:
  python3 scripts/migrations/deploy_002_bulk_update.py             # deploy + smoke test
  python3 scripts/migrations/deploy_002_bulk_update.py --smoke-only # skip deploy, just smoke test
  python3 scripts/migrations/deploy_002_bulk_update.py --print-sql  # print SQL only

Prerequisites:
  npx supabase must be available (run: npm install -g supabase OR use npx).
  The Supabase project must be linked (run: npx supabase link --project-ref <ref>)
  in the repo root.

  Alternatively, paste 002_batch_update_expert_results.sql directly into:
  https://supabase.com/dashboard/project/yoqmcowordktnchfjdqm/sql/new
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SQL_PATH = Path(__file__).resolve().parent / "002_batch_update_expert_results.sql"
PROJECT_REF = "yoqmcowordktnchfjdqm"

sys.path.insert(0, str(REPO_ROOT))


def load_env():
    env_file = REPO_ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())


def deploy_via_cli() -> bool:
    """
    Run: npx supabase db query --linked --file <sql_path> --workdir <repo_root>
    Returns True on success.
    """
    print(f"[deploy] Running: npx supabase db query --linked --file {SQL_PATH.name}")
    try:
        result = subprocess.run(
            [
                "npx", "supabase", "db", "query",
                "--linked",
                "--file", str(SQL_PATH),
                "--workdir", str(REPO_ROOT),
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode == 0:
            print(f"[deploy] CLI output: {result.stdout.strip()[:200]}")
            return True
        else:
            print(f"[deploy] CLI failed (exit {result.returncode}):")
            print(f"  stdout: {result.stdout.strip()[:200]}")
            print(f"  stderr: {result.stderr.strip()[:200]}")
            return False
    except FileNotFoundError:
        print("[deploy] npx not found — cannot deploy via CLI.")
        return False
    except subprocess.TimeoutExpired:
        print("[deploy] CLI timed out.")
        return False


def print_sql_fallback():
    print("\n[fallback] Paste the following SQL into the Supabase SQL Editor:")
    print(f"  https://supabase.com/dashboard/project/{PROJECT_REF}/sql/new")
    print("\n" + "=" * 60)
    print(SQL_PATH.read_text())
    print("=" * 60)


def smoke_test(client) -> bool:
    """Call bulk_update_expert_results([]) — expect 0."""
    try:
        resp = client.rpc("bulk_update_expert_results", {"updates": []}).execute()
        result = resp.data
        print(f"  bulk_update_expert_results([]) => {result}")
        return result == 0
    except Exception as e:
        print(f"  FAILED: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Deploy 002_batch_update_expert_results.sql to Supabase."
    )
    parser.add_argument(
        "--smoke-only",
        action="store_true",
        help="Skip deployment; only smoke-test (use after manual SQL Editor deploy).",
    )
    parser.add_argument(
        "--print-sql",
        action="store_true",
        help="Print the SQL to stdout without deploying.",
    )
    args = parser.parse_args()

    load_env()

    if args.print_sql:
        print(SQL_PATH.read_text())
        return

    from src.supabase_writer import get_client
    client = get_client()

    if not args.smoke_only:
        ok = deploy_via_cli()
        if not ok:
            print_sql_fallback()
            print(
                "\nAfter deploying manually, run:\n"
                "  python3 scripts/migrations/deploy_002_bulk_update.py --smoke-only"
            )
            sys.exit(1)

    print("\n[smoke-test] Verifying function is live...")
    ok = smoke_test(client)
    if ok:
        print("[smoke-test] PASSED — bulk_update_expert_results is live.")
    else:
        print(
            "[smoke-test] FAILED.\n"
            "  Check that 002_batch_update_expert_results.sql was deployed."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
