import argparse
import sqlite3
from tabulate import tabulate


def main():
    parser = argparse.ArgumentParser(description="Inspect SQLite analytics database tables/views.")
    parser.add_argument("--db", type=str, default="data/realtime_analytics_lab3.db", help="Path to SQLite database")
    parser.add_argument("--table", type=str, default="dashboard_minute", help="Table or view name to inspect")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        cursor.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
            (args.table,),
        )
        if cursor.fetchone() is None:
            raise sqlite3.OperationalError("table or view does not exist")

        quoted_table = '"' + args.table.replace('"', '""') + '"'
        cursor.execute(f"PRAGMA table_info({quoted_table})")
        columns = [row[1] for row in cursor.fetchall()]
        order_column = next(
            (name for name in ("window_start_utc", "id", "event_time_utc", "created_at_utc") if name in columns),
            None,
        )
        order_clause = f' ORDER BY "{order_column}" ASC' if order_column else ""
        cursor.execute(f"SELECT * FROM {quoted_table}{order_clause};")
        rows = cursor.fetchall()
        if not rows:
            print(f"Table/View '{args.table}' is empty in {args.db}.")
            return

        headers = rows[0].keys()
        data = [[row[col] for col in headers] for row in rows]
        print(f"\n--- DATA INSPECTION: {args.table} in {args.db} ---")
        try:
            print(tabulate(data, headers=headers, tablefmt="psql"))
        except Exception:
            # Fallback simple print if tabulate not installed
            print(" | ".join(headers))
            print("-" * 60)
            for r in data:
                print(" | ".join(str(x) for x in r))
        print(f"Total rows: {len(rows)}\n")

    except sqlite3.OperationalError as err:
        print(f"Error querying table/view '{args.table}': {err}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
