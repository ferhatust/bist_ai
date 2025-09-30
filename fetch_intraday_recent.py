import yfinance as yf
import pandas as pd
from sqlalchemy import text
from db_utils import engine

TICKERS = ["ASELS.IS","THYAO.IS","GARAN.IS","AKBNK.IS","BIMAS.IS",
           "TUPRS.IS","SASA.IS","KCHOL.IS","EREGL.IS","SISE.IS"]

INTERVAL = "5m"   # 1m de mümkün ama daha çok rate limit yer
PERIOD   = "60d"  # yfinance intraday geçmiş sınırı ~60 gün

UPSERT_SQL = """
INSERT INTO prices_intraday (ticker_id, ts, open_p, high_p, low_p, close_p, volume)
VALUES (:ticker_id, :ts, :open_p, :high_p, :low_p, :close_p, :volume)
ON DUPLICATE KEY UPDATE
  open_p=VALUES(open_p), high_p=VALUES(high_p), low_p=VALUES(low_p),
  close_p=VALUES(close_p), volume=VALUES(volume)
"""

def main():
    with engine.begin() as conn:
        r = conn.execute(text(
            "SELECT id, symbol FROM tickers WHERE symbol IN :symbols"
        ), {"symbols": tuple(TICKERS)})
        id_by_sym = {row.symbol: row.id for row in r}

        miss = [s for s in TICKERS if s not in id_by_sym]
        if miss:
            raise SystemExit(f"Tickers tablosunda eksik: {miss}")

        for sym in TICKERS:
            print("Intraday (5m, 60d) indiriliyor:", sym)
            df = yf.download(sym, period=PERIOD, interval=INTERVAL, progress=False)
            if df.empty:
                print("Intraday veri yok:", sym); continue

            # zaman damgasını Istanbul’a çevirip timezone'u düşür
            try:
                if df.index.tz is not None:
                    df = df.tz_convert('Europe/Istanbul')
                    df.index = df.index.tz_localize(None)
            except Exception:
                # bazı durumlarda tz yok; sorun değil
                pass

            for ts, row in df.iterrows():
                params = {
                    "ticker_id": id_by_sym[sym],
                    "ts": pd.to_datetime(ts).to_pydatetime(),
                    "open_p": float(row['Open']),
                    "high_p": float(row['High']),
                    "low_p":  float(row['Low']),
                    "close_p":float(row['Close']),
                    "volume": int(row['Volume']) if not pd.isna(row['Volume']) else None
                }
                conn.execute(text(UPSERT_SQL), params)
            print("Bitti:", sym)

if __name__ == "__main__":
    main()
