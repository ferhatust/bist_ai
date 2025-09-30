# dashboard.py
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from sqlalchemy import text
from db_utils import engine

# -------------------- PAGE CONFIG & THEME --------------------
st.set_page_config(page_title="BIST AI Dashboard", page_icon="📈", layout="wide")

# Küçük CSS: Streamlit menü/footeri gizle, buton ve başlıkları iyileştir
st.markdown(
    """
    <style>
      /* menü ve footer */
      #MainMenu {visibility: hidden;}
      footer {visibility: hidden;}
      /* sidebar başlık aralığı */
      section[data-testid="stSidebar"] h2, section[data-testid="stSidebar"] h3 { margin-top: 0.25rem; }
      /* kart benzeri kutular */
      .metric-card {
        padding: 0.75rem 1rem; border-radius: 12px;
        background: #0e1117; border: 1px solid #2a2f3a;
      }
      /* tablo font küçültme */
      .small-table td, .small-table th { font-size: 0.9rem; }
      /* favori rozet */
      .fav-badge {
        display: inline-block; padding: 2px 8px; border-radius: 999px;
        background: #1e88e5; color: white; font-size: 0.75rem; margin-left: 6px;
      }
    </style>
    """,
    unsafe_allow_html=True
)

st.title("📊 BIST AI Dashboard")

# -------------------- STATE: FAVORILER --------------------
if "favorites" not in st.session_state:
    st.session_state["favorites"] = []

# -------------------- DB: SEMBOL LİSTESİ --------------------
with engine.begin() as conn:
    tickers_df = pd.read_sql(
        text("SELECT symbol, name, sector FROM tickers WHERE active=1 ORDER BY symbol"),
        conn
    )

symbols = tickers_df["symbol"].tolist()
name_map = dict(zip(tickers_df["symbol"], tickers_df["name"]))
sector_map = dict(zip(tickers_df["symbol"], tickers_df["sector"]))

# -------------------- SIDEBAR --------------------
st.sidebar.header("Seçimler")

# Sektör filtresi (opsiyonel)
sectors = ["Tümü"] + sorted(tickers_df["sector"].dropna().unique().tolist())
sel_sector = st.sidebar.selectbox("Sektör", sectors)

if sel_sector != "Tümü":
    filtered_symbols = tickers_df.loc[tickers_df["sector"] == sel_sector, "symbol"].tolist()
else:
    filtered_symbols = symbols

# Hisse seçimi
chosen_symbol = st.sidebar.selectbox(
    "Hisse",
    filtered_symbols,
    format_func=lambda s: f"{s} - {name_map.get(s, s)}"
)

# Favori işlemleri
c1, c2 = st.sidebar.columns([1,1])
if c1.button("⭐ Favorilere Ekle"):
    if chosen_symbol not in st.session_state["favorites"]:
        st.session_state["favorites"].append(chosen_symbol)
if c2.button("🗑️ Favoriden Çıkar"):
    if chosen_symbol in st.session_state["favorites"]:
        st.session_state["favorites"].remove(chosen_symbol)

# Favori kısayolları
if st.session_state["favorites"]:
    st.sidebar.subheader("Favoriler")
    for fav in st.session_state["favorites"]:
        if st.sidebar.button(f"➡ {fav}", key=f"fav_{fav}"):
            chosen_symbol = fav  # hızlı geçiş

# Görünüm ayarları
days = st.sidebar.slider("Gün aralığı (mum sayısı)", 60, 500, 180, step=10)
show_ma20 = st.sidebar.checkbox("MA20", value=True)
show_ma50 = st.sidebar.checkbox("MA50", value=True)
show_volume = st.sidebar.checkbox("Hacim", value=True)

# -------------------- DB: FİYAT VERİSİ --------------------
with engine.begin() as conn:
    df = pd.read_sql(
        text("""
            SELECT date, open, high, low, close, volume
            FROM prices
            WHERE symbol = :s
            ORDER BY date DESC
            LIMIT :limit
        """),
        conn,
        params={"s": chosen_symbol, "limit": days}
    )

if df.empty:
    st.error("Bu hisse için veri bulunamadı.")
    st.stop()

df = df.sort_values("date").reset_index(drop=True)

# Hareketli ortalamalar
if show_ma20:
    df["MA20"] = df["close"].rolling(20).mean()
if show_ma50:
    df["MA50"] = df["close"].rolling(50).mean()

# -------------------- GRAFİK --------------------
# Ana figür
fig = go.Figure()

# Candlestick (canlı renkler)
fig.add_trace(
    go.Candlestick(
        x=df["date"],
        open=df["open"],
        high=df["high"],
        low=df["low"],
        close=df["close"],
        increasing_line_color="#26a69a", increasing_fillcolor="#26a69a",
        decreasing_line_color="#ef5350", decreasing_fillcolor="#ef5350",
        name="Fiyat",
        yaxis="y1"
    )
)

# MA çizgileri
if show_ma20 and "MA20" in df:
    fig.add_trace(
        go.Scatter(
            x=df["date"], y=df["MA20"], mode="lines",
            line=dict(width=1.5, color="#42a5f5"),
            name="MA20", yaxis="y1"
        )
    )
if show_ma50 and "MA50" in df:
    fig.add_trace(
        go.Scatter(
            x=df["date"], y=df["MA50"], mode="lines",
            line=dict(width=1.5, color="#ab47bc"),
            name="MA50", yaxis="y1"
        )
    )

# Hacim barları (ikincil eksen)
if show_volume:
    fig.add_trace(
        go.Bar(
            x=df["date"], y=df["volume"],
            marker=dict(color="#546e7a"),
            name="Hacim", yaxis="y2", opacity=0.4
        )
    )

# Düzen / eksenler
fig.update_layout(
    title=f"{chosen_symbol} - {name_map.get(chosen_symbol, '')}"
          + (f" <span class='fav-badge'>FAVORİ</span>" if chosen_symbol in st.session_state['favorites'] else ""),
    xaxis=dict(domain=[0, 1]),
    yaxis=dict(title="Fiyat", side="left"),
    xaxis_rangeslider_visible=False,
    template="plotly_dark",
    height=650,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0)
)

if show_volume:
    fig.update_layout(
        yaxis2=dict(title="Hacim", overlaying="y", side="right", showgrid=False, rangemode="tozero")
    )

st.plotly_chart(fig, use_container_width=True)

# -------------------- ÖZET / SON 10 SATIR --------------------
colA, colB = st.columns([1, 1])

with colA:
    st.markdown("#### 🔎 Son 10 Satır")
    st.dataframe(df.tail(10), use_container_width=True, height=260)

# -------------------- SİNYALLER: EN İYİ / EN KÖTÜ 5 --------------------
with engine.begin() as conn:
    best30 = pd.read_sql(
        text("""
            SELECT symbol, cum_return_pct, vol_daily_pct, score
            FROM signals
            WHERE run_date = CURDATE() AND window_days = 30
            ORDER BY score DESC
            LIMIT 5
        """),
        conn
    )
    worst30 = pd.read_sql(
        text("""
            SELECT symbol, cum_return_pct, vol_daily_pct, score
            FROM signals
            WHERE run_date = CURDATE() AND window_days = 30
            ORDER BY score ASC
            LIMIT 5
        """),
        conn
    )

with colB:
    st.markdown("#### 🏆 En İyi 5 Hisse(30g)")
    st.table(best30.style.hide(axis="index"))
    st.markdown("#### 🧊 En Kötü 5 Hisse(30g)")
    st.table(worst30.style.hide(axis="index"))
