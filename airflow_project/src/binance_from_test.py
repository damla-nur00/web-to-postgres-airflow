import os
import requests
import pandas as pd
import psycopg2
from datetime import datetime, timezone, timedelta
from decimal import Decimal

BINANCE_URL = "https://api.binance.com/api/v3/ticker/24hr"

# --- PostgreSQL bağlantı ayarları ---
PG_HOST = os.getenv("PG_HOST", "localhost")       # Airflow konteynerinde: "metadata-db" veya "host.docker.internal"
PG_DB   = os.getenv("PG_DB",   "postgres")        # Airflow’de metadata-db kullanacaksan: "airflow"
PG_USER = os.getenv("PG_USER", "postgres")        # Airflow’de: "airflow"
PG_PASS = os.getenv("PG_PASS", "1234")            # Airflow’de: "airflow"

def main():
    # Binance API'den verileri çek
    r = requests.get(BINANCE_URL, timeout=50)
    r.raise_for_status()
    data = r.json()
    df = pd.DataFrame(data)

    # Sadece USDT paritelerini filtrele ve hacme göre sırala
    usdt_df = df[df["symbol"].str.endswith("USDT")].copy()
    usdt_df["quoteVolume"] = usdt_df["quoteVolume"].astype(float)
    sirali = usdt_df.sort_values("quoteVolume", ascending=False)

    print(sirali[["symbol", "quoteVolume", "priceChangePercent", "highPrice", "lowPrice", "count"]].head(10))

    # PostgreSQL'e bağlan ve veri tablolarını oluştur
    conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()

    # PostgreSQL oturumunu UTC saat dilimine alıyoruz
    cur.execute("SET TIME ZONE 'UTC';")


    # Snapshot tablo: her sembol için tek satır saklar
    cur.execute("""
    CREATE TABLE IF NOT EXISTS binance_usdt (
        symbol TEXT PRIMARY KEY,
        quote_volume NUMERIC,
        price_change_percent NUMERIC,
        high_price NUMERIC,
        low_price NUMERIC,
        trade_count BIGINT,
        close_time   TIMESTAMPTZ NOT NULL DEFAULT now(),                              -- UTC
        close_time_tr TIMESTAMP   NOT NULL DEFAULT (now() AT TIME ZONE 'Europe/Istanbul')  -- TR duvar saati
    );
    """)

    # History tablo: zaman serisi şeklinde tüm geçmiş kayıtları saklar
    cur.execute("""
    CREATE TABLE IF NOT EXISTS binance_usdt_history (
        symbol TEXT,
        quote_volume NUMERIC,
        price_change_percent NUMERIC,
        high_price NUMERIC,
        low_price NUMERIC,
        trade_count BIGINT,
        scraped_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """)

    #  History tablosunda hızlı sorgu için indeks
    cur.execute("""
    CREATE INDEX IF NOT EXISTS ix_bu_hist_symbol_time
    ON binance_usdt_history(symbol, scraped_at DESC);
    """)
    conn.commit()

    # Snapshot için UPSERT (varsa güncelle, yoksa ekle)
    upsert_sql = """
    INSERT INTO binance_usdt
    (symbol, quote_volume, price_change_percent, high_price, low_price, trade_count, close_time,close_time_tr)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (symbol) DO UPDATE SET
      quote_volume=EXCLUDED.quote_volume,
      price_change_percent=EXCLUDED.price_change_percent,
      high_price=EXCLUDED.high_price,
      low_price=EXCLUDED.low_price,
      trade_count=EXCLUDED.trade_count,
      close_time=EXCLUDED.close_time,
      close_time_tr=EXCLUDED.close_time_tr
    """

    # History tablosuna her kaydı ekler
    append_sql = """
    INSERT INTO binance_usdt_history
    (symbol, quote_volume, price_change_percent, high_price, low_price, trade_count, scraped_at)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    """

    # UTC zamanı alıyoruz
    now_utc = datetime.now(timezone.utc)
    # Türkiye saati = UTC +3
    now_tr = now_utc.astimezone(timezone(timedelta(hours=3)))

    # Her sembol için snapshot ve history tablolarına veri yaz
    for _, row in sirali.iterrows():
        symbol = row["symbol"]
        quote_volume = Decimal(str(row["quoteVolume"]))
        price_change_percent = Decimal(str(row.get("priceChangePercent", "0") or "0"))
        high_price = Decimal(str(row.get("highPrice", "0") or "0"))
        low_price = Decimal(str(row.get("lowPrice", "0") or "0"))
        trade_count = int(row.get("count", 0) or 0)


        # Snapshot tabloya UPSERT
        cur.execute(upsert_sql, (
            symbol, quote_volume, price_change_percent,
            high_price, low_price, trade_count, now_utc, now_tr
        ))

        # History tablosuna ekleme
        cur.execute(append_sql, (
            symbol, quote_volume, price_change_percent,
            high_price, low_price, trade_count, now_utc
        ))

    # Tüm işlemleri veritabanına kaydet
    conn.commit()
    cur.close()
    conn.close()
    print("Veriler PostgreSQL'e yazıldı. (snapshot + history,UTC)")

# Bu blok, dosya doğrudan çalıştırıldığında main()'i çağırır.
# Modül olarak import edilirse kod çalışmaz — Airflow DAG'lerinde kritik.
if __name__ == "__main__":
    main()
