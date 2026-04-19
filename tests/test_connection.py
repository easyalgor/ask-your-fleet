"""
Quick smoke test – verifies connectivity to Confluent Cloud Flink.

Usage:
    python -m tests.test_connection
    # or
    make check
"""

import sys
import os

# Ensure the project root is on sys.path so sibling modules resolve
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

from flink_client import execute_query
from config import FLINK_API_URL, ORG_ID, ENV_ID, COMPUTE_POOL_ID


def main():
    print("=" * 60)
    print("Flink MCP – Connection Smoke Test")
    print("=" * 60)
    print(f"  API URL:      {FLINK_API_URL}")
    print(f"  Org ID:       {ORG_ID or '⚠️  NOT SET'}")
    print(f"  Env ID:       {ENV_ID or '⚠️  NOT SET'}")
    print(f"  Compute Pool: {COMPUTE_POOL_ID or '⚠️  NOT SET'}")
    print()

    missing = [k for k, v in [("ORG_ID", ORG_ID), ("ENV_ID", ENV_ID), ("COMPUTE_POOL_ID", COMPUTE_POOL_ID)] if not v]
    if missing:
        print(f"❌  Missing required config: {', '.join(missing)}")
        print("    Fill them in .env or export as environment variables.")
        sys.exit(1)

    print("▶  Running: SHOW TABLES …")
    try:
        result = execute_query("SHOW TABLES")
        print(f"   Status:  {result['status']}")
        print(f"   Columns: {result['columns']}")
        print(f"   Rows:    {result['rows']}")
        if result["status"] == "COMPLETED":
            print("\n✅  Connection successful!")
        else:
            print(f"\n❌  Query failed: {result['detail']}")
            sys.exit(1)
    except Exception as exc:
        print(f"\n❌  Exception: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
