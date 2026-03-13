import os
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Data Pipeline Monitor", layout="wide")

DB_PATH = os.getenv('DUCKDB_PATH', '/mnt/c/Users/MY LAPTOP/Logistics_Data_Pipeline/src/warehouse/data/warehouse.duckdb')

@st.cache_resource
def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)

con = get_connection()

st.title("Logistics Pipeline Health")

# High Level Metrics
col1, col2, col3 = st.columns(3)
total_events  = con.execute("SELECT COUNT(*) FROM raw_logistics").fetchone()[0]
unique_orders = con.execute("SELECT COUNT(DISTINCT order_id) FROM raw_logistics").fetchone()[0]
latest_event  = con.execute("SELECT MAX(timestamp) FROM raw_logistics").fetchone()[0]

col1.metric("Total Events Ingested", f"{total_events:,}")
col2.metric("Unique Orders", f"{unique_orders:,}")
col3.metric("Last Pulse", str(latest_event)[:19] if latest_event else "N/A")

# Ingestion Trend
st.subheader("Ingestion Velocity")
df_trend = con.execute("""
    SELECT
        date_trunc('minute', timestamp::TIMESTAMP) AS minute,
        COUNT(*) AS event_count
    FROM raw_logistics
    GROUP BY 1
    ORDER BY 1 DESC
    LIMIT 60
""").df()

if not df_trend.empty:
    fig = px.line(
        df_trend.sort_values('minute'),
        x='minute',
        y='event_count',
        title="Events Landed in MinIO (Last 60 Minutes)"
    )
    st.plotly_chart(fig, width="stretch")
else:
    st.info("No ingestion data yet.")

# Invalid Records Summary
st.subheader("Data Quality Issues")
df_dq = con.execute("""
    SELECT invalid_reason, SUM(invalid_count) AS total_invalid
    FROM dq_invalid_delivery_summary
    GROUP BY invalid_reason
    ORDER BY total_invalid DESC
""").df()

if not df_dq.empty:
    fig_dq = px.bar(
        df_dq,
        x='invalid_reason',
        y='total_invalid',
        title="Invalid Records by Reason",
        color='invalid_reason'
    )
    st.plotly_chart(fig_dq, width="stretch")
else:
    st.success("No data quality issues found.")

# Raw Data Preview
st.subheader("Recent Raw Payloads (Bronze)")
st.dataframe(
    con.execute("SELECT * FROM raw_logistics ORDER BY ingestion_timestamp DESC LIMIT 20").df()
)