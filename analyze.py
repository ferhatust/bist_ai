# analyze.py
import argparse
from datetime import date, datetime
import pandas as pd
from sqlalchemy import text
from db_utils import engine

def load_prices(window_days: int | None, ytd: bool) -> pd.DataFrame:
    with engine.begin() as conn:
        if ytd:
            # Yıl başından bugüne
            sql = text("""
                SELECT symbol, date, close
                FROM prices
                WHERE date >= DATE(CONCAT(YEAR(CURDATE()), '-01-01'))
                ORDER BY symbol, date
            """)
            df = pd.read_sql_query(sql, conn)
        elif window_days:
            sql = text("""
                SELECT symbol, date, close
                FROM prices
                WHERE date >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
                ORDER BY symbol, date
            """)
            df = pd.read_sql_query(sql, conn, params={"days": window_days})
        else:
            # Varsayılan: son 60 gün
            sql = text("""
                SELECT symbol, date, close
                FROM prices
                WHERE date >= DATE_SUB(CURDATE(), INTERVAL 60 DAY)
                ORDER BY symbol, date
            """)
            df = pd.read_sql_query(sql, conn)

    # tip temizliği
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"])
    return df

def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for sym, g in df.groupby("symbol"):
        g = g.sort_values("date")
        closes = g["close"]
        if len(closes) < 5:
            continue

        ret = closes.pct_change().dropna()
        avg_daily = ret.mean() if not ret.empty else float("nan")
        vol_daily = ret.std() if not ret.empty else float("nan")
        cum_ret = (closes.iloc[-1] / closes.iloc[0] - 1) if closes.iloc[0] != 0 else float("nan")

        mom5  = closes.pct_change(5).iloc[-1]  if len(closes) >= 6  else float("nan")
        mom20 = closes.pct_change(20).iloc[-1] if len(closes) >= 21 else float("nan")

        rows.append({
            "symbol": sym,
            "n_rows": len(g),
            "start": g["date"].min(),
            "end": g["date"].max(),
            "cum_return_pct": round(cum_ret * 100, 2) if pd.notna(cum_ret) else None,
            "avg_daily_pct":  round(avg_daily * 100, 3) if pd.notna(avg_daily) else None,
            "vol_daily_pct":  round(vol_daily * 100, 3) if pd.notna(vol_daily) else None,
            "momentum_5_pct":  round(mom5 * 100, 2) if pd.notna(mom5) else None,
            "momentum_20_pct": round(mom20 * 100, 2) if pd.notna(mom20) else None,
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    # Skor: getiriyi ödüllendir, volatiliteyi cezalandır (z-score ile)
    # std sıfır olursa bölme hata vermesin diye 1'e düşürüyoruz.
    cr = out["cum_return_pct"]
    vol = out["vol_daily_pct"]
    z_cum = (cr - cr.mean()) / (cr.std(ddof=0) if cr.std(ddof=0) not in (0, None) else 1)
    z_vol = ((vol.mean() - vol) / (vol.std(ddof=0) if vol.std(ddof=0) not in (0, None) else 1))
    out["score"] = 0.7 * z_cum + 0.3 * z_vol

    # Sıralama büyükten küçüğe (yüksek skor = daha iyi)
    out = out.sort_values("score", ascending=False).reset_index(drop=True)
    return out

def main():
    parser = argparse.ArgumentParser(description="BIST/Altın/Döviz basit analiz")
    parser.add_argument("--window", type=int, default=30, help="Kaç günlük pencere (varsayılan 30)")
    parser.add_argument("--ytd", action="store_true", help="Yıl başından bugüne mod")
    parser.add_argument("--csv", type=str, default="", help="Sonucu CSV olarak kaydet (örn: output.csv)")
    args = parser.parse_args()

    df = load_prices(window_days=None if args.ytd else args.window, ytd=args.ytd)
    if df.empty:
        print("Veri yok (prices tablosunu kontrol et).")
        return

    res = compute_metrics(df)
    if res.empty:
        print("Hesaplanacak yeterli veri yok.")
        return

    cols_show = ["symbol", "start", "end", "n_rows", "cum_return_pct", "avg_daily_pct", "vol_daily_pct", "momentum_5_pct", "momentum_20_pct", "score"]
    print("\n=== ÖZET TABLO ===")
    print(res[cols_show].to_string(index=False))

    print("\n=== EN İYİ 5 (skor) ===")
    print(res[["symbol", "cum_return_pct", "vol_daily_pct", "score"]].head(5).to_string(index=False))

    print("\n=== EN KÖTÜ 5 (skor) ===")
    print(res[["symbol", "cum_return_pct", "vol_daily_pct", "score"]].tail(5).to_string(index=False))

    if args.csv:
        res.to_csv(args.csv, index=False)
        print(f"\nCSV kaydedildi: {args.csv}")

if __name__ == "__main__":
    main()
