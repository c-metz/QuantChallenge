"""
Task 4: PnL Dashboard (Streamlit)
Interactive web dashboard for traders to view their daily performance
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import sqlite3
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Dict

# Page configuration
st.set_page_config(
    page_title="Trading Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database path
DB_PATH = Path(__file__).resolve().parent / "trades.sqlite"

@st.cache_data(ttl=60)
def get_available_traders():
    """Get list of unique trader IDs from database"""
    if not DB_PATH.exists():
        return []
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT trader_id FROM trades ORDER BY trader_id")
    traders = [row[0] for row in cursor.fetchall()]
    conn.close()
    return traders

@st.cache_data(ttl=60)
def get_trader_date_range(trader_id: str):
    """Get min and max delivery dates for a trader"""
    if not DB_PATH.exists():
        return None, None
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT MIN(delivery_day), MAX(delivery_day) 
        FROM trades 
        WHERE trader_id = ?
    """, (trader_id,))
    result = cursor.fetchone()
    conn.close()
    
    if result[0] and result[1]:
        return date.fromisoformat(result[0]), date.fromisoformat(result[1])
    return None, None

@st.cache_data(ttl=60)
def load_trader_data(trader_id: str, delivery_day: date) -> pd.DataFrame:
    """Load trading data for a specific trader and delivery day"""
    if not DB_PATH.exists():
        return pd.DataFrame()
    
    conn = sqlite3.connect(DB_PATH)
    
    query = """
        SELECT trade_id, delivery_hour, quantity, price, side, strategy, timestamp
        FROM trades
        WHERE trader_id = ? AND delivery_day = ?
        ORDER BY delivery_hour, timestamp
    """
    
    df = pd.read_sql_query(query, conn, params=(trader_id, delivery_day.isoformat()))
    conn.close()
    
    return df

def compute_hourly_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Compute hourly trading metrics"""
    if df.empty:
        # Return empty dataframe with all 24 hours
        hours = list(range(24))
        return pd.DataFrame({
            'Hour': [f"{h:02d}-{h+1:02d}" for h in hours],
            'Number of Trades': [0] * 24,
            'BUY_DA [MW]': [0.0] * 24,
            'SELL_DA [MW]': [0.0] * 24,
            'BUY_IDA [MW]': [0.0] * 24,
            'SELL_IDA [MW]': [0.0] * 24,
            'PnL [EUR]': [0.0] * 24
        })
    
    results = []
    
    for hour in range(24):
        hour_data = df[df['delivery_hour'] == hour]
        
        num_trades = len(hour_data)
        
        # Separate DA and IDA trades based on trade_id
        da_trades = hour_data[hour_data['trade_id'].str.contains('_DA_', na=False)]
        ida_trades = hour_data[hour_data['trade_id'].str.contains('_ID_', na=False)]
        
        buy_da = da_trades[da_trades['side'] == 'buy']['quantity'].sum()
        sell_da = da_trades[da_trades['side'] == 'sell']['quantity'].sum()
        buy_ida = ida_trades[ida_trades['side'] == 'buy']['quantity'].sum()
        sell_ida = ida_trades[ida_trades['side'] == 'sell']['quantity'].sum()
        
        # PnL: sell = +q*p, buy = -q*p
        pnl = (
            (hour_data[hour_data['side'] == 'sell']['quantity'] * 
             hour_data[hour_data['side'] == 'sell']['price']).sum() -
            (hour_data[hour_data['side'] == 'buy']['quantity'] * 
             hour_data[hour_data['side'] == 'buy']['price']).sum()
        )
        
        results.append({
            'Hour': f"{hour:02d}-{hour+1:02d}",
            'delivery_hour': hour,
            'Number of Trades': num_trades,
            'BUY_DA [MW]': buy_da,
            'SELL_DA [MW]': sell_da,
            'BUY_IDA [MW]': buy_ida,
            'SELL_IDA [MW]': sell_ida,
            'PnL [EUR]': pnl
        })
    
    return pd.DataFrame(results)

def main():
    # Header
    st.title("⚡ Energy Trading Dashboard")
    st.markdown("Real-time trading performance and PnL analysis")
    
    # Sidebar
    st.sidebar.header("Filters")
    
    # Check database
    if not DB_PATH.exists():
        st.error(f"❌ Database not found at {DB_PATH}")
        st.info("Please ensure the trades database exists and contains data.")
        return
    
    # Trader selection
    traders = get_available_traders()
    
    if not traders:
        st.warning("⚠️ No traders found in database")
        st.info("Add trades using the API to see data here.")
        return
    
    selected_trader = st.sidebar.selectbox(
        "Select Trader",
        traders,
        help="Choose trader ID to view their performance"
    )
    
    # Date selection
    min_date, max_date = get_trader_date_range(selected_trader)
    
    if min_date and max_date:
        delivery_day = st.sidebar.date_input(
            "Delivery Day",
            value=max_date,
            min_value=min_date,
            max_value=max_date,
            help="Select trading day to analyze"
        )
    else:
        st.warning(f"⚠️ No data found for trader: {selected_trader}")
        return
    
    # Load data
    with st.spinner("Loading trading data..."):
        df_trades = load_trader_data(selected_trader, delivery_day)
        df_hourly = compute_hourly_metrics(df_trades)
    
    # Key metrics
    st.markdown("---")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    total_trades = df_hourly['Number of Trades'].sum()
    total_buy_da = df_hourly['BUY_DA [MW]'].sum()
    total_sell_da = df_hourly['SELL_DA [MW]'].sum()
    total_buy_ida = df_hourly['BUY_IDA [MW]'].sum()
    total_sell_ida = df_hourly['SELL_IDA [MW]'].sum()
    total_pnl = df_hourly['PnL [EUR]'].sum()
    
    with col1:
        st.metric("Total Trades", f"{int(total_trades):,}")
    
    with col2:
        st.metric("BUY DA", f"{total_buy_da:.2f} MW")
    
    with col3:
        st.metric("SELL DA", f"{total_sell_da:.2f} MW")
    
    with col4:
        st.metric("BUY IDA", f"{total_buy_ida:.2f} MW")
        st.metric("SELL IDA", f"{total_sell_ida:.2f} MW")
    
    with col5:
        pnl_color = "normal" if total_pnl >= 0 else "inverse"
        st.metric("Total PnL", f"€{total_pnl:,.2f}", delta_color=pnl_color)
    
    st.markdown("---")
    
    # Main content tabs
    tab1, tab2, tab3 = st.tabs(["📊 Hourly Report", "📈 Visualizations", "📋 Trade Details"])
    
    with tab1:
        st.subheader("Hourly Trading Report")
        
        if total_trades == 0:
            st.info(f"ℹ️ No trades found for {selected_trader} on {delivery_day}")
        else:
            # Format for display
            df_display = df_hourly.copy()
            df_display['PnL [EUR]'] = df_display['PnL [EUR]'].apply(lambda x: f"€{x:+,.2f}")
            df_display['BUY_DA [MW]'] = df_display['BUY_DA [MW]'].apply(lambda x: f"{x:.2f}")
            df_display['SELL_DA [MW]'] = df_display['SELL_DA [MW]'].apply(lambda x: f"{x:.2f}")
            df_display['BUY_IDA [MW]'] = df_display['BUY_IDA [MW]'].apply(lambda x: f"{x:.2f}")
            df_display['SELL_IDA [MW]'] = df_display['SELL_IDA [MW]'].apply(lambda x: f"{x:.2f}")
            
            # Display table
            st.dataframe(
                df_display[['Hour', 'Number of Trades', 'BUY_DA [MW]', 'SELL_DA [MW]', 'BUY_IDA [MW]', 'SELL_IDA [MW]', 'PnL [EUR]']],
                use_container_width=True,
                hide_index=True
            )
            
            # Totals row
            st.markdown("---")
            total_col1, total_col2, total_col3, total_col4, total_col5, total_col6, total_col7 = st.columns(7)
            total_col1.markdown("**TOTAL**")
            total_col2.markdown(f"**{int(total_trades)}**")
            total_col3.markdown(f"**{total_buy_da:.2f}**")
            total_col4.markdown(f"**{total_sell_da:.2f}**")
            total_col5.markdown(f"**{total_buy_ida:.2f}**")
            total_col6.markdown(f"**{total_sell_ida:.2f}**")
            total_col7.markdown(f"**€{total_pnl:+,.2f}**")
    
    with tab2:
        st.subheader("Performance Visualizations")
        
        if total_trades == 0:
            st.info("No data to visualize")
        else:
            # PnL by hour
            fig_pnl = go.Figure()
            
            colors = ['green' if x >= 0 else 'red' for x in df_hourly['PnL [EUR]']]
            
            fig_pnl.add_trace(go.Bar(
                x=df_hourly['delivery_hour'],
                y=df_hourly['PnL [EUR]'],
                marker_color=colors,
                name='PnL',
                text=df_hourly['PnL [EUR]'].apply(lambda x: f"€{x:,.0f}"),
                textposition='outside'
            ))
            
            fig_pnl.update_layout(
                title="PnL by Hour",
                xaxis_title="Hour of Day",
                yaxis_title="PnL (EUR)",
                xaxis=dict(tickmode='linear', tick0=0, dtick=1),
                hovermode='x unified',
                height=400
            )
            
            st.plotly_chart(fig_pnl, use_container_width=True)
            
            # Trading volume
            col1, col2 = st.columns(2)
            
            with col1:
                fig_vol = go.Figure()
                
                fig_vol.add_trace(go.Bar(
                    x=df_hourly['delivery_hour'],
                    y=df_hourly['BUY_DA [MW]'],
                    name='BUY DA',
                    marker_color='indianred'
                ))
                
                fig_vol.add_trace(go.Bar(
                    x=df_hourly['delivery_hour'],
                    y=df_hourly['SELL_DA [MW]'],
                    name='SELL DA',
                    marker_color='lightseagreen'
                ))
                
                fig_vol.add_trace(go.Bar(
                    x=df_hourly['delivery_hour'],
                    y=df_hourly['BUY_IDA [MW]'],
                    name='BUY IDA',
                    marker_color='lightcoral',
                    opacity=0.7
                ))
                
                fig_vol.add_trace(go.Bar(
                    x=df_hourly['delivery_hour'],
                    y=df_hourly['SELL_IDA [MW]'],
                    name='SELL IDA',
                    marker_color='mediumaquamarine',
                    opacity=0.7
                ))
                
                fig_vol.update_layout(
                    title="DA vs IDA Trading Volume by Hour",
                    xaxis_title="Hour",
                    yaxis_title="Volume (MW)",
                    barmode='group',
                    height=350
                )
                
                st.plotly_chart(fig_vol, use_container_width=True)
            
            with col2:
                # Number of trades by hour
                fig_trades = px.bar(
                    df_hourly[df_hourly['Number of Trades'] > 0],
                    x='delivery_hour',
                    y='Number of Trades',
                    title='Number of Trades by Hour',
                    labels={'delivery_hour': 'Hour', 'Number of Trades': 'Trades'},
                    color='Number of Trades',
                    color_continuous_scale='Blues'
                )
                fig_trades.update_layout(height=350)
                st.plotly_chart(fig_trades, use_container_width=True)
    
    with tab3:
        st.subheader("Trade Details")
        
        if df_trades.empty:
            st.info("No trades to display")
        else:
            # Format timestamp
            df_trades_display = df_trades.copy()
            df_trades_display['timestamp'] = pd.to_datetime(df_trades_display['timestamp']).dt.strftime('%H:%M:%S')
            
            # Color code by side
            def highlight_side(row):
                if row['side'] == 'buy':
                    return ['background-color: #ffebee'] * len(row)
                elif row['side'] == 'sell':
                    return ['background-color: #e8f5e9'] * len(row)
                return [''] * len(row)
            
            st.dataframe(
                df_trades_display.style.apply(highlight_side, axis=1),
                use_container_width=True,
                hide_index=True
            )
            
            # Download button
            csv = df_trades.to_csv(index=False)
            st.download_button(
                label="📥 Download trades as CSV",
                data=csv,
                file_name=f"trades_{selected_trader}_{delivery_day}.csv",
                mime="text/csv"
            )
    
    # Footer
    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
