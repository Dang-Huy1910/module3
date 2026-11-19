import argparse
import logging
from src.analytics.analytics_store import AnalyticsStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def main():
    parser = argparse.ArgumentParser(description="Initialize SQLite schema for Realtime Analytics.")
    parser.add_argument("--db", type=str, default="data/realtime_analytics_lab3.db", help="Path to SQLite database")
    args = parser.parse_args()

    store = AnalyticsStore(db_path=args.db)
    print(f"Database initialized successfully at: {args.db}")


if __name__ == "__main__":
    main()
