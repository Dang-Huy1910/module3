import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import sys
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="BK Fintech Realtime Analytics",
    page_icon="📈",
    layout="wide",
)

def get_db_path() -> str:
    # 1. Environment variable (Container preference)
    env_path = os.getenv("SQLITE_PATH")
    if env_path:
        return env_path

    # 2. CLI Argument
    if "--db" in sys.argv:
        idx = sys.argv.index("--db")
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1]

    # 3. Default fallback
    return "data/realtime_analytics.db"


DB_PATH = get_db_path()


def load_data(db_file: str) -> pd.DataFrame:
    path = Path(db_file).resolve()
    if not path.exists():
        return pd.DataFrame()

    # Note on SQLite WAL in Docker: read-only URI works when -shm and -wal are accessible
    uri = f"file:{path.as_posix()}?mode=ro"
    try:
        import sqlite3
        conn = sqlite3.connect(uri, uri=True, timeout=5.0)
        query = "SELECT * FROM dashboard_minute ORDER BY window_start_utc DESC LIMIT 60;"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as err:
        # Fallback to direct connect if mode=ro uri faces WAL lock issues on shared volumes
        try:
            import sqlite3
            conn = sqlite3.connect(str(path), timeout=5.0)
            query = "SELECT * FROM dashboard_minute ORDER BY window_start_utc DESC LIMIT 60;"
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        except Exception as inner_err:
            st.error(f"Error querying database: {inner_err}")
            return pd.DataFrame()


st.title("🛒 E-Commerce Streaming Analytics Dashboard")
st.caption(f"Realtime data source: `{DB_PATH}` · Refreshing automatically")

def _render_content():
    df = load_data(DB_PATH)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    st.write(f"⏱️ **Last Dashboard Refresh:** `{now_str}`")

    if df.empty:
        st.warning("⚠️ No data available yet. Start the analytics consumer and publish events.")
        return

    total_orders = int(df["order_count"].sum())
    total_revenue = int(df["created_order_value_vnd"].sum())
    total_views = int(df["product_view_count"].sum())
    max_active_customers = int(df["active_customers"].max())

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(label="📦 Total Orders", value=f"{total_orders:,}")
    with col2:
        st.metric(label="💰 Order Value (VND)", value=f"{total_revenue:,} đ")
    with col3:
        st.metric(label="👀 Product Views", value=f"{total_views:,}")
    with col4:
        st.metric(label="👥 Peak Active Customers", value=f"{max_active_customers:,}")

    st.markdown("---")

    st.subheader("📊 1-Minute Aggregation Timeline")
    chart_df = df.sort_values(by="window_start_utc", ascending=True).copy()
    st.line_chart(
        chart_df.set_index("window_start_utc")[["order_count", "product_view_count", "active_customers"]],
        use_container_width=True,
    )

    st.subheader("📋 Recent Window Details")
    st.dataframe(
        df[["window_start_utc", "order_count", "created_order_value_vnd", "product_view_count", "active_customers", "updated_at_utc"]],
        use_container_width=True,
        hide_index=True,
    )


if hasattr(st, "fragment"):
    @st.fragment(run_every="5s")
    def render_dashboard():
        _render_content()

    render_dashboard()
else:
    _render_content()
    import time

    time.sleep(5)
    st.rerun()
