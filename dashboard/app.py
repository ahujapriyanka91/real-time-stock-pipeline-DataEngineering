"""
Step 8: Stock market dashboard (Streamlit) on the Snowflake GOLD layer.
Run from the project root:  venv\\Scripts\\streamlit run dashboard/app.py
"""

import os
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

st.set_page_config(page_title="Stock Market Dashboard", page_icon="📈", layout="wide")

# ---------------------------------------------------------------------------
# Colors (validated reference palette). Light and dark steps of the same hues.
# Each stock keeps its color everywhere, whatever is filtered.
# ---------------------------------------------------------------------------
DARK = st.context.theme.type == "dark"
SERIES = (
    ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9"]
    if DARK else
    ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6"]
)
GRID = "rgba(128,128,128,0.15)"


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------
@st.cache_resource
def connection():
    key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    if not os.path.isabs(key_path):
        key_path = str(ROOT / key_path)
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        authenticator="SNOWFLAKE_JWT",
        private_key_file=key_path,
        session_parameters={"TIMEZONE": "UTC"},
    )


@st.cache_data(ttl=300)  # re-query at most every 5 minutes (each query wakes the warehouse)
def query(sql):
    cur = connection().cursor()
    try:
        cur.execute(sql)
        df = pd.DataFrame(cur.fetchall(), columns=[c[0].lower() for c in cur.description])
    finally:
        cur.close()
    # Snowflake NUMBER comes back as Decimal; charts want floats
    for col in df.columns:
        if df[col].dtype == object and len(df) and type(df[col].iloc[0]).__name__ == "Decimal":
            df[col] = df[col].astype(float)
    return df


daily = query("select * from gold.gold_daily_summary order by trading_date, symbol")
latest = query("select * from gold.gold_latest_quotes order by symbol")
prices = query("select * from gold.gold_prices_5min order by symbol, interval_start")

if daily.empty:
    st.warning("No data in the GOLD layer yet. Run the pipeline first.")
    st.stop()

SYMBOLS = sorted(latest["symbol"].unique())
COLOR = {s: SERIES[i % len(SERIES)] for i, s in enumerate(SYMBOLS)}


def style(fig, height=400):
    fig.update_layout(
        height=height,
        margin=dict(l=8, r=8, t=8, b=8),
        hoverlabel=dict(namelength=-1),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor=GRID, zeroline=False)
    return fig


# ---------------------------------------------------------------------------
# Header + filters
# ---------------------------------------------------------------------------
st.title("Stock Market Dashboard")
last_update = pd.to_datetime(latest["ingested_at"].max())
st.caption(
    f"Live quotes from Finnhub → Kafka → MinIO → Snowflake → dbt.  "
    f"Last update: {last_update:%Y-%m-%d %H:%M} UTC · data updates hourly, refresh the page for the latest"
)

dates = sorted(daily["trading_date"].unique(), reverse=True)
f1, f2, _ = st.columns([1, 1, 2])
day = f1.selectbox("Trading day", dates, format_func=lambda d: d.strftime("%a %d %b %Y"))
stock = f2.selectbox("Stock (price chart)", SYMBOLS)

today = daily[daily["trading_date"] == day].copy()

# ---------------------------------------------------------------------------
# KPI tiles: close and daily return per stock
# ---------------------------------------------------------------------------
tiles = st.columns(len(SYMBOLS))
for col, sym in zip(tiles, SYMBOLS):
    row = today[today["symbol"] == sym]
    if row.empty:
        col.metric(sym, "—")
        continue
    r = row.iloc[0]
    col.metric(sym, f"${r.close_price:,.2f}", f"{r.daily_return_pct:+.2f}%")

st.divider()

# ---------------------------------------------------------------------------
# Row 1: performance through the day, all stocks on one % scale
# ---------------------------------------------------------------------------
st.subheader("Performance through the day")
st.caption("% change vs previous close, 5-minute closes (New York time). "
           "Indexed so stocks with different prices share one axis.")

day_prices = prices[pd.to_datetime(prices["interval_start"]).dt.date == day]
perf = day_prices.merge(today[["symbol", "prev_close"]], on="symbol")
perf["pct"] = (perf["close_price"] / perf["prev_close"] - 1) * 100

fig = go.Figure()
for sym in SYMBOLS:
    s = perf[perf["symbol"] == sym]
    if s.empty:
        continue
    fig.add_trace(go.Scatter(
        x=s["interval_start"], y=s["pct"], name=sym, mode="lines+markers",
        line=dict(color=COLOR[sym], width=2), marker=dict(size=8),
        hovertemplate=f"<b>{sym}</b> %{{y:+.2f}}%<extra></extra>",
    ))
fig.add_hline(y=0, line_width=1, line_color="gray")
style(fig).update_layout(hovermode="x unified")
fig.update_yaxes(ticksuffix="%")
st.plotly_chart(fig, width="stretch", theme="streamlit")

# ---------------------------------------------------------------------------
# Row 2: price through the day for the selected stock
# ---------------------------------------------------------------------------
st.subheader(f"{stock} price through the day")
info = latest[latest["symbol"] == stock].iloc[0]
st.caption(f"{info.company_name} · {info.sector} · price every 5 minutes, New York time")

p = day_prices[day_prices["symbol"] == stock]
color = COLOR[stock]
band = "rgba({},{},{},0.18)".format(*(int(color[i:i + 2], 16) for i in (1, 3, 5)))
fig = go.Figure([
    # light band: lowest to highest price within each 5 minutes
    go.Scatter(x=p["interval_start"], y=p["high_price"], mode="lines", line=dict(width=0),
               hoverinfo="skip", showlegend=False),
    go.Scatter(x=p["interval_start"], y=p["low_price"], mode="lines", line=dict(width=0),
               fill="tonexty", fillcolor=band, hoverinfo="skip", showlegend=False),
    # the price line
    go.Scatter(x=p["interval_start"], y=p["close_price"], mode="lines+markers", name=stock,
               line=dict(color=color, width=2), marker=dict(size=8),
               customdata=p[["low_price", "high_price"]],
               hovertemplate="%{x|%H:%M}<br><b>$%{y:,.2f}</b><br>"
                             "range $%{customdata[0]:,.2f} – $%{customdata[1]:,.2f}<extra></extra>"),
])
style(fig).update_layout(showlegend=False)
fig.update_yaxes(tickprefix="$")
st.plotly_chart(fig, width="stretch", theme="streamlit")
st.caption("Line = price at the end of each 5 minutes. Shaded band = how low and high it went in those 5 minutes.")

# ---------------------------------------------------------------------------
# Table view
# ---------------------------------------------------------------------------
with st.expander("Data table — daily summary"):
    st.dataframe(
        today[["symbol", "company_name", "sector", "open_price", "high_price", "low_price",
               "close_price", "prev_close", "daily_return_pct", "intraday_range_pct", "num_quotes"]],
        hide_index=True, width="stretch",
    )
