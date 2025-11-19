import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Post-Trade Operations Dashboard", layout="wide")
st.title("🏦 Hedge Fund Post-Trade Operations Dashboard")
st.markdown("*Real-time trade lifecycle monitoring and reconciliation*")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5432/kafka_db"

@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)

engine = get_engine(DATABASE_URL)

def load_data(status_filter: str | None = None, limit: int = 500) -> pd.DataFrame:
    base_query = "SELECT * FROM trades"
    params = {}
    if status_filter and status_filter != "All":
        base_query += " WHERE status = :status"
        params["status"] = status_filter
    base_query += " ORDER BY timestamp DESC LIMIT :limit"
    params["limit"] = limit

    try:
        df = pd.read_sql_query(text(base_query), con=engine.connect(), params=params)
        return df
    except Exception as e:
        st.error(f"Error loading data from database: {e}")
        return pd.DataFrame()

# Sidebar controls
st.sidebar.header("⚙️ Dashboard Controls")
status_options = [
    "All", "Pending Confirmation", "Confirmed", 
    "Settlement Pending", "Settled", "Break - Mismatch", "Break - Missing Trade"
]
selected_status = st.sidebar.selectbox("Filter by Status", status_options)
update_interval = st.sidebar.slider("Update Interval (seconds)", min_value=2, max_value=20, value=5)
limit_records = st.sidebar.number_input("Number of records to load", min_value=100, max_value=5000, value=500, step=100)

st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Dashboard Features")
st.sidebar.markdown("- Real-time trade monitoring")
st.sidebar.markdown("- Break/exception tracking")
st.sidebar.markdown("- Settlement analysis")
st.sidebar.markdown("- Counterparty exposure")
st.sidebar.markdown("- Fee analytics")

if st.sidebar.button("🔄 Refresh Now"):
    st.rerun()

placeholder = st.empty()

while True:
    df_trades = load_data(selected_status, limit=int(limit_records))

    with placeholder.container():
        if df_trades.empty:
            st.warning("⏳ Waiting for trade data... Make sure producer and consumer are running.")
            time.sleep(update_interval)
            continue

        if "timestamp" in df_trades.columns:
            df_trades["timestamp"] = pd.to_datetime(df_trades["timestamp"])

        # Calculate KPIs
        total_trades = len(df_trades)
        total_notional = df_trades["notional_value"].sum()
        total_fees = df_trades["total_fees"].sum()
        
        # Status breakdowns
        settled_count = len(df_trades[df_trades["status"] == "Settled"])
        pending_count = len(df_trades[df_trades["status"].str.contains("Pending")])
        break_count = len(df_trades[df_trades["status"].str.contains("Break")])
        
        stp_rate = (df_trades["stp_eligible"].sum() / total_trades * 100) if total_trades > 0 else 0.0
        settlement_rate = (settled_count / total_trades * 100) if total_trades > 0 else 0.0
        break_rate = (break_count / total_trades * 100) if total_trades > 0 else 0.0
        
        avg_fee_bps = (total_fees / total_notional * 10000) if total_notional > 0 else 0.0

        # ============ KPI SECTION ============
        st.markdown("### 📈 Key Performance Indicators")
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("Total Trades", f"{total_trades:,}")
            st.metric("Settled", f"{settled_count:,}")
        
        with col2:
            st.metric("Total Notional", f"${total_notional:,.2f}")
            st.metric("Avg Trade Size", f"${total_notional/total_trades:,.2f}" if total_trades > 0 else "$0")
        
        with col3:
            st.metric("Total Fees", f"${total_fees:,.2f}")
            st.metric("Avg Fee (bps)", f"{avg_fee_bps:.2f}")
        
        with col4:
            st.metric("STP Rate", f"{stp_rate:.1f}%", delta=None)
            st.metric("Settlement Rate", f"{settlement_rate:.1f}%")
        
        with col5:
            st.metric("⚠️ Breaks", f"{break_count}", delta=f"-{break_rate:.2f}%", delta_color="inverse")
            st.metric("Pending", f"{pending_count}")

        st.markdown("---")

        # ============ OPERATIONAL ALERTS ============
        if break_count > 0:
            st.error(f"🚨 **{break_count} TRADE BREAKS DETECTED** - Immediate attention required!")
            breaks_df = df_trades[df_trades["status"].str.contains("Break")][
                ["trade_id", "instrument", "counterparty", "notional_value", "status", "priority"]
            ].head(10)
            st.dataframe(breaks_df, use_container_width=True)

        # ============ CHARTS ROW 1 ============
        st.markdown("### 📊 Trade Analytics")
        
        chart_col1, chart_col2 = st.columns(2)
        
        with chart_col1:
            # Status distribution
            status_counts = df_trades["status"].value_counts().reset_index()
            status_counts.columns = ["status", "count"]
            
            fig_status = px.pie(
                status_counts, 
                values="count", 
                names="status", 
                title="Trade Status Distribution",
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            st.plotly_chart(fig_status, use_container_width=True)
        
        with chart_col2:
            # Asset class breakdown
            asset_notional = df_trades.groupby("asset_class")["notional_value"].sum().reset_index()
            asset_notional = asset_notional.sort_values("notional_value", ascending=False)
            
            fig_asset = px.bar(
                asset_notional,
                x="asset_class",
                y="notional_value",
                title="Notional Value by Asset Class",
                labels={"notional_value": "Notional ($)", "asset_class": "Asset Class"},
                color="notional_value",
                color_continuous_scale="Blues"
            )
            st.plotly_chart(fig_asset, use_container_width=True)

        # ============ CHARTS ROW 2 ============
        chart_col3, chart_col4 = st.columns(2)
        
        with chart_col3:
            # Counterparty exposure
            cp_exposure = df_trades.groupby("counterparty")["notional_value"].sum().reset_index()
            cp_exposure = cp_exposure.sort_values("notional_value", ascending=True).tail(10)
            
            fig_cp = px.bar(
                cp_exposure,
                x="notional_value",
                y="counterparty",
                orientation="h",
                title="Top 10 Counterparty Exposure",
                labels={"notional_value": "Notional ($)", "counterparty": "Counterparty"},
                color="notional_value",
                color_continuous_scale="Reds"
            )
            st.plotly_chart(fig_cp, use_container_width=True)
        
        with chart_col4:
            # Settlement venue distribution
            venue_counts = df_trades["settlement_venue"].value_counts().reset_index()
            venue_counts.columns = ["venue", "count"]
            
            fig_venue = px.bar(
                venue_counts,
                x="venue",
                y="count",
                title="Trades by Settlement Venue",
                labels={"count": "Trade Count", "venue": "Venue"},
                color="count",
                color_continuous_scale="Greens"
            )
            st.plotly_chart(fig_venue, use_container_width=True)

        # ============ CHARTS ROW 3 ============
        chart_col5, chart_col6 = st.columns(2)
        
        with chart_col5:
            # Fee breakdown
            fee_data = pd.DataFrame({
                "Fee Type": ["Brokerage", "Clearing", "Exchange"],
                "Amount": [
                    df_trades["brokerage_fee"].sum(),
                    df_trades["clearing_fee"].sum(),
                    df_trades["exchange_fee"].sum()
                ]
            })
            
            fig_fees = px.pie(
                fee_data,
                values="Amount",
                names="Fee Type",
                title="Fee Breakdown",
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            st.plotly_chart(fig_fees, use_container_width=True)
        
        with chart_col6:
            # Buy vs Sell
            side_notional = df_trades.groupby("side")["notional_value"].sum().reset_index()
            
            fig_side = px.bar(
                side_notional,
                x="side",
                y="notional_value",
                title="Buy vs Sell Volume",
                labels={"notional_value": "Notional ($)", "side": "Side"},
                color="side",
                color_discrete_map={"Buy": "#00CC96", "Sell": "#EF553B"}
            )
            st.plotly_chart(fig_side, use_container_width=True)

        # ============ TIME SERIES ============
        st.markdown("### 📈 Time Series Analysis")
        
        df_ts = df_trades.copy()
        df_ts["minute"] = df_ts["timestamp"].dt.floor("1min")
        
        ts_agg = df_ts.groupby("minute").agg({
            "trade_id": "count",
            "notional_value": "sum"
        }).reset_index()
        ts_agg.columns = ["minute", "trade_count", "notional_value"]
        
        fig_ts = make_subplots(
            rows=2, cols=1,
            subplot_titles=("Trade Volume Over Time", "Notional Value Over Time"),
            vertical_spacing=0.15
        )
        
        fig_ts.add_trace(
            go.Scatter(x=ts_agg["minute"], y=ts_agg["trade_count"], 
                      mode="lines+markers", name="Trade Count",
                      line=dict(color="#636EFA", width=2)),
            row=1, col=1
        )
        
        fig_ts.add_trace(
            go.Scatter(x=ts_agg["minute"], y=ts_agg["notional_value"], 
                      mode="lines+markers", name="Notional",
                      line=dict(color="#00CC96", width=2)),
            row=2, col=1
        )
        
        fig_ts.update_xaxes(title_text="Time", row=2, col=1)
        fig_ts.update_yaxes(title_text="Trades", row=1, col=1)
        fig_ts.update_yaxes(title_text="Notional ($)", row=2, col=1)
        fig_ts.update_layout(height=600, showlegend=False)
        
        st.plotly_chart(fig_ts, use_container_width=True)

        # ============ DETAILED TRADE TABLE ============
        st.markdown("### 📋 Recent Trades (Top 20)")
        
        display_cols = [
            "trade_id", "instrument", "side", "quantity", "price", 
            "notional_value", "counterparty", "status", "priority", 
            "total_fees", "settlement_date"
        ]
        
        recent_trades = df_trades[display_cols].head(20).copy()
        
        # Format currency columns
        recent_trades["notional_value"] = recent_trades["notional_value"].apply(lambda x: f"${x:,.2f}")
        recent_trades["total_fees"] = recent_trades["total_fees"].apply(lambda x: f"${x:,.2f}")
        recent_trades["price"] = recent_trades["price"].apply(lambda x: f"{x:.4f}")
        
        st.dataframe(recent_trades, use_container_width=True, height=400)

        # ============ FOOTER ============
        st.markdown("---")
        col_footer1, col_footer2, col_footer3 = st.columns(3)
        
        with col_footer1:
            st.caption(f"🕐 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        with col_footer2:
            st.caption(f"🔄 Auto-refresh: {update_interval}s")
        
        with col_footer3:
            st.caption(f"📊 Displaying: {len(df_trades):,} / {total_trades:,} trades")

    time.sleep(update_interval)
