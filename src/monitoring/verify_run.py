import argparse
import os
from pathlib import Path
import sqlite3
import sys

def main():
    default_db = os.getenv("SQLITE_PATH", "data/realtime_analytics.db")

    parser = argparse.ArgumentParser(description="Verify incident run in SQLite processed_events table.")
    parser.add_argument("--run-id", type=str, required=True, help="Run ID to verify")
    parser.add_argument("--expected", type=int, required=True, help="Expected distinct event count")
    parser.add_argument("--db", type=str, default=default_db, help="SQLite database path")

    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"FAILED: Database file not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    try:
        # Search by order_id or event_id containing run_id
        query = """
        SELECT COUNT(DISTINCT event_id)
        FROM processed_events
        WHERE payload_json LIKE ?;
        """
        pattern = f"%{args.run_id}%"
        cursor.execute(query, (pattern,))
        actual_count = cursor.fetchone()[0]

        print(f"\n--- VERIFICATION RESULT FOR RUN: {args.run_id} ---")
        print(f"Database: {db_path}")
        print(f"Expected distinct events: {args.expected}")
        print(f"Actual distinct events:   {actual_count}")

        if actual_count == args.expected:
            print("STATUS: PASSED (Exact match! Pipeline successfully caught up without loss or logic duplicates)\n")
            sys.exit(0)
        else:
            print(f"STATUS: FAILED (Count mismatch: expected {args.expected}, got {actual_count})\n", file=sys.stderr)
            sys.exit(1)

    except Exception as err:
        print(f"Query error: {err}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
