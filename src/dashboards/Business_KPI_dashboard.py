import os
import streamlit as st
import duckdb
import plotly.express as px

st.set_page_config(page_title="Logistics Executive View", layout="wide")

DB_PATH = os.getenv('DUCKDB_PATH', '/mnt/c/Users/MY LAPTOP/Logistics_Data_Pipeline/src/warehouse/data/warehouse.duckdb')

@st.cache_resource
def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)

con = get_connection()

st.title("Logistics Performance Overview")

# Carrier Performance from fact + dim tables
st.subheader("Carrier Performance")
df_carrier = con.execute("""
    SELECT
        c.carrier_name,
        COUNT(f.event_sk)                                          AS total_events,
        SUM(CASE WHEN f.is_late_delivery THEN 1 ELSE 0 END)       AS late_deliveries,
        ROUND(
            SUM(CASE WHEN NOT f.is_late_delivery THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(f.event_sk), 0), 2
        )                                                          AS on_time_rate,
        ROUND(AVG(f.weight_kg), 2)                                 AS avg_weight_kg
    FROM fact_event f
    LEFT JOIN dim_carrier c ON f.carrier_sk = c.carrier_sk
    GROUP BY c.carrier_name
    ORDER BY on_time_rate DESC
""").df()

if not df_carrier.empty:
    fig_bar = px.bar(
        df_carrier,
        x='carrier_name',
        y='on_time_rate',
        color='late_deliveries',
        title="On-Time Rate vs Late Deliveries per Carrier",
        labels={
            'on_time_rate': 'On-Time Rate (%)',
            'late_deliveries': 'Late Deliveries'
        }
    )
    st.plotly_chart(fig_bar, width='stretch')
    st.dataframe(df_carrier)
else:
    st.info("No carrier data available yet.")

# Geospatial Map of Active Shipments
st.subheader("Active Shipment Locations")
df_map = con.execute("""
    SELECT
        l.latitude,
        l.longitude,
        s.status_name AS status,
        c.carrier_name
    FROM fact_event f
    LEFT JOIN dim_location  l ON f.origin_location_sk = l.location_sk
    LEFT JOIN dim_status    s ON f.status_sk          = s.status_sk
    LEFT JOIN dim_carrier   c ON f.carrier_sk         = c.carrier_sk
    WHERE s.status_name != 'Delivered'
      AND l.latitude  IS NOT NULL
      AND l.longitude IS NOT NULL
""").df()

if not df_map.empty:
    df_map['latitude'] = df_map['latitude'].astype('float64')
    df_map['longitude'] = df_map['longitude'].astype('float64')
    st.map(df_map)
else:
    st.info("No active shipments to display.")
    st.info("No active shipments to display.")

# Weight Distribution
st.subheader("Package Weight Distribution")
df_weight = con.execute("""
    SELECT weight_kg FROM fact_event WHERE weight_kg IS NOT NULL
""").df()

if not df_weight.empty:
    fig_hist = px.histogram(
        df_weight,
        x='weight_kg',
        title="Distribution of Package Weights (kg)",
        nbins=50
    )
    st.plotly_chart(fig_hist, width='stretch')

# Events by Status
st.subheader("Events by Status")
df_status = con.execute("""
    SELECT s.status_name, COUNT(*) AS event_count
    FROM fact_event f
    LEFT JOIN dim_status s ON f.status_sk = s.status_sk
    GROUP BY s.status_name
    ORDER BY event_count DESC
""").df()

if not df_status.empty:
    fig_pie = px.pie(
        df_status,
        names='status_name',
        values='event_count',
        title="Event Distribution by Status"
    )
    st.plotly_chart(fig_pie, width='stretch')