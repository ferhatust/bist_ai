# save_signals.py
import argparse
from datetime import date
import pandas as pd
from sqlalchemy import text
from db_utils import engine
from analyze import load_prices, compute_metrics  # analiz fonksiyonlarını kullan

UPSERT = text("""
INSERT INTO signals (
  run_date, window_days, symbol, start, end, n_rows,
  cum_return_pct, avg_daily_pct, vol_daily_pct, momentum_5_pct, momentum_20_pct, score
) VALUES (
  :run_date, :window_days, :symbol, :start, :end, :n_rows,
  :cum, :avg, :vol, :m5, :m20, :score
)
ON DUPLICATE KEY UPDATE
  start=VALUES(start), end=VALUES(end), n_rows=VALUES(n_rows),
  cum_return_pct=VALUES(cum_return_pct), avg_daily_pct=VALUES(avg_daily_pct),
  vol_daily_pct=VALUES(vol_daily_pct), momentum_5_pct=VALUES(momentum_5_pct),
  momentum_20_pct=VALUES(momentum_20_pct), score=VALUES(score)
""")

def safe_val(x):
    """NaN -> None dönüşümü"""
    return None if pd.isna(x) else x

def save_window(window_days: int):
    df = load_prices(window_days=window_days, ytd=False)
    res = compute_metrics(df)
    if res.empty:
        print(f"{window_days}g: hesaplanacak veri yok.")
        return
    today = date.today()
    with engine.begin() as conn:
        for _, r in res.iterrows():
            conn.execute(UPSERT, {
                "run_date": today,
                "window_days": window_days,
                "symbol": r["symbol"],
                "start": r["start"],
                "end": r["end"],
                "n_rows": int(r["n_rows"]),
                "cum": safe_val(r["cum_return_pct"]),
                "avg": safe_val(r["avg_daily_pct"]),
                "vol": safe_val(r["vol_daily_pct"]),
                "m5":  safe_val(r["momentum_5_pct"]),
                "m20": safe_val(r["momentum_20_pct"]),
                "score": safe_val(r["score"]),
            })
    print(f"✓ {window_days}g sinyalleri kaydedildi.")

def save_ytd():
    df = load_prices(window_days=None, ytd=True)
    res = compute_metrics(df)
    if res.empty:
        print("YTD: hesaplanacak veri yok.")
        return
    today = date.today()
    with engine.begin() as conn:
        for _, r in res.iterrows():
            conn.execute(UPSERT, {
                "run_date": today,
                "window_days": 9999,
                "symbol": r["symbol"],
                "start": r["start"],
                "end": r["end"],
                "n_rows": int(r["n_rows"]),
                "cum": safe_val(r["cum_return_pct"]),
                "avg": safe_val(r["avg_daily_pct"]),
                "vol": safe_val(r["vol_daily_pct"]),
                "m5":  safe_val(r["momentum_5_pct"]),
                "m20": safe_val(r["momentum_20_pct"]),
                "score": safe_val(r["score"]),
            })
    print("✓ YTD sinyalleri kaydedildi.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--window", type=int, default=30, help="ör: 30")
    parser.add_argument("--ytd", action="store_true")
    args = parser.parse_args()

    save_window(args.window)
    if args.ytd:
        save_ytd()


# Bu kod, finansal verileri analiz edip sinyalleri veritabanına kaydeder.
