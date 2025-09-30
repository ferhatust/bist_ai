# fetch_prices.py
import time
from datetime import date
import pandas as pd
import yfinance as yf
from sqlalchemy import text
from db_utils import engine

# --- Yahoo sembol eşleşmeleri ---
# Genel kural: BIST hisseleri -> ".IS"
# Özel varlıklar ve bazı istisnalar aşağıda.
SPECIAL_MAP = {
    # 🔥 Altın için iki alternatif: önce XAUUSD=X, bulamazsa GC=F (Gold Futures)
    "XAUUSD": ["XAUUSD=X", "GC=F"],  
    "USDTRY": "USDTRY=X",             # Dolar/TL
    "ALTAY": ["ALTNY.IS", "ALTAY.IS"],  # Altınay Savunma
    "KUZEY": ["KBORU.IS", "KUZEY.IS"],  # Kuzey Boru
}

def candidates_for(symbol: str):
    # Özel tanım varsa onu dön
    if symbol in SPECIAL_MAP:
        v = SPECIAL_MAP[symbol]
        return v if isinstance(v, list) else [v]
    # değilse varsayılan BIST formatı
    return [f"{symbol}.IS"]

UPSERT_SQL = text("""
INSERT INTO prices (symbol, date, open, high, low, close, volume)
VALUES (:symbol, :date, :open, :high, :low, :close, :volume)
ON DUPLICATE KEY UPDATE
  open=VALUES(open), high=VALUES(high), low=VALUES(low),
  close=VALUES(close), volume=VALUES(volume)
""")

def download_yf(yf_symbol: str):
    try:
        df = yf.download(yf_symbol, period="2y", interval="1d", auto_adjust=False, progress=False)
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        print(f"  ! yfinance hata ({yf_symbol}): {e}")
        return pd.DataFrame()

def fetch_and_store():
    with engine.begin() as conn:
        # aktif sembolleri al
        rows = conn.execute(text("SELECT symbol FROM tickers WHERE active=1 ORDER BY symbol")).fetchall()
        symbols = [r.symbol for r in rows]
        print(f"Aktif sembol sayısı: {len(symbols)}")

        for sym in symbols:
            print(f"\n=== {sym} ===")
            df = pd.DataFrame()
            chosen = None
            for cand in candidates_for(sym):
                print(f"  Deneniyor: {cand}")
                df = download_yf(cand)
                if not df.empty:
                    chosen = cand
                    break
                time.sleep(1.0)  # nazik olalım
            if df.empty:
                print(f"  ! Veri bulunamadı, atlandı: {sym}")
                continue

            # index -> date
            df = df.reset_index().rename(columns={"Date": "date"})
            df["date"] = pd.to_datetime(df["date"]).dt.date  # DATE tipine çevir
            inserted = 0

            # 🔥 itertuples ile tuple unpacking
            for d, o, h, l, c, v in df[["date", "Open", "High", "Low", "Close", "Volume"]].itertuples(index=False, name=None):
                params = {
                    "symbol": sym,
                    "date": d,
                    "open":  float(o) if pd.notna(o) else None,
                    "high":  float(h) if pd.notna(h) else None,
                    "low":   float(l) if pd.notna(l) else None,
                    "close": float(c) if pd.notna(c) else None,
                    "volume": int(v) if pd.notna(v) else 0,
                }
                conn.execute(UPSERT_SQL, params)
                inserted += 1

            print(f"  ✓ {sym} <- {chosen} | {inserted} satır kaydedildi")
            time.sleep(1.0)  # rate-limit dostu

if __name__ == "__main__":
    fetch_and_store()
    print("\n✅ Tamamlandı.")
