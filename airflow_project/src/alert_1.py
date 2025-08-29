import os
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone
import psycopg2
from psycopg2.extras import DictCursor


def get_env(name, default=None, data_type=str):
    """Ortam değişkenini okur, yoksa varsayılanı döndürür."""
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return data_type(value)
    except Exception :
        return default


# ---- Eşikler (ENV ile override edilebilir) ----
PRICE_PCT_THRESHOLD = get_env("ALERT_PCT_THRESH", 10.0, float)  # ≥ %10
VOL_Z_THRESHOLD = get_env("ALERT_VOLZ_THRESH", 2.0, float)  # z ≥ 2
COOLDOWN_HOURS = get_env("ALERT_COOLDOWN_HOURS", 6, int)  # 6 saat
DRY_RUN = bool(int(get_env("ALERT_DRY_RUN", 0, int)))  # 1 -> mail atma

# ---- Postgres bağlantısı (varsayılan: host makinedeki PostgreSQL) ----
PG_CFG = dict(
    host=get_env("PG_HOST", "host.docker.internal"),
    dbname=get_env("PG_DB", "postgres"),
    user=get_env("PG_USER", "postgres"),
    password=get_env("PG_PASS", "1234"),
)

# Mail gönderimi
def send_mail(rows):
    """ SMTP mail gönderimi """
    to = get_env("ALERT_EMAIL_TO")
    host = get_env("SMTP_HOST")
    port = get_env("SMTP_PORT", 587, int)
    user = get_env("SMTP_USER")
    pwd = get_env("SMTP_PASS")

    if DRY_RUN:
        print(f"[alert] DRY_RUN=1 → {len(rows)} kayıt için mail atılmadı.")
        return
    if not (to and host and user and pwd):
        print("[alert] Email ayarı eksik; gönderim atlandı.")
        return
    if not rows:
        print("[alert] Gönderilecek satır yok.")
        return

    # Özel durum: Bilgilendirme maili (sinyal yok)
    if len(rows) == 1 and rows[0]["symbol"] == "Bilgi":
        body = "Bilgilendirme: Şu anda kriteri sağlayan sinyal yok."
        subj = "[CRYPTO ALERT] Bilgilendirme"
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subj
        msg["From"] = user
        msg["To"] = to
        try:
            with smtplib.SMTP(host, port, timeout=20) as s:
                s.starttls()
                s.login(user, pwd)
                s.send_message(msg)
            print(f"[alert] Bilgilendirme maili gönderildi → {to}")
        except Exception as e:
            print(f"[alert][ERR] Bilgilendirme maili gönderilemedi: {e}")
        return

    # Normal sinyal maili gönderimi
    lines = []
    for r in rows:
        pct = r["pricechangepercent"]
        volz = r["vol_z"]
        scraped_at = r["scraped_at"]
        volz_str = "None" if volz is None else f"{round(volz, 2)}"

        # Mail gövdesinde hem UTC hem TR göster
        utc_time = scraped_at.astimezone(timezone.utc)
        tr_time = scraped_at.astimezone(timezone(timedelta(hours=3)))

        lines.append(
            f"{r['symbol']}: Δ%={pct:.2f}, vol_z={volz_str} "
            f"@ UTC={utc_time.strftime('%Y-%m-%d %H:%M:%S')} | "
            f"TR={tr_time.strftime('%Y-%m-%d %H:%M:%S')}"
        )

    body = "Kriteri sağlayan sinyaller:\n\n" + "\n".join(lines)
    subj = f"[CRYPTO ALERT] {len(rows)} sinyal (≥{PRICE_PCT_THRESHOLD}% & z≥{VOL_Z_THRESHOLD})"

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subj
    msg["From"] = user
    msg["To"] = to

    try:
        with smtplib.SMTP(host, port, timeout=20) as s:
            s.starttls()
            s.login(user, pwd)
            s.send_message(msg)
        print(f"[alert] Email gönderildi → {to} ({len(rows)} kayıt)")
    except Exception as e:
        print(f"[alert][ERR] E-posta gönderilemedi: {e}")



def main():
    """Alert üretimi: view'den çek, cooldown uygula, mail at."""
    row_to_send = []
    try:
        with psycopg2.connect(**PG_CFG) as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                # Oturum TZ'yi UTC yapalım (tutarlılık için)
                cur.execute("SET TIME ZONE 'UTC';")

                # Cooldown tablosu (TIMESTAMPTZ)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS public.alerts_sent (
                        id      BIGSERIAL PRIMARY KEY,
                        symbol  TEXT        NOT NULL,
                        sent_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS ix_alerts_sent_symbol_time
                    ON public.alerts_sent(symbol, sent_at DESC);
                """)
                conn.commit()

                # Adayları çek
                cur.execute("""
                    SELECT symbol, pricechangepercent, vol_z, scraped_at
                    FROM public.v_latest_24h
                    WHERE pricechangepercent IS NOT NULL
                        AND ABS(pricechangepercent) >= %s
                        AND (vol_z IS NULL OR vol_z >= %s)
                    ORDER BY ABS(pricechangepercent) DESC NULLS LAST,
                            vol_z DESC NULLS LAST 
                    LIMIT 100;
                """, (PRICE_PCT_THRESHOLD, VOL_Z_THRESHOLD))
                candidates = cur.fetchall()
                print(f"[alert] Aday sayısı: {len(candidates)}")

                now_utc = datetime.now(timezone.utc)

                for r in candidates:
                    # Son gönderim (aynı sembol için)
                    cur.execute("""
                        SELECT sent_at
                        FROM alerts_sent 
                        WHERE symbol=%s
                        ORDER BY sent_at DESC
                        LIMIT 1
                    """, (r["symbol"],))

                    prev = cur.fetchone()
                    prev_ts = prev["sent_at"] if prev else None

                    # Güvenlik: naive datetime  gelirse UTC varsay
                    if prev_ts is not None and getattr(prev_ts, "tzinfo", None) is None:
                        prev_ts = prev_ts.replace(tzinfo=timezone.utc)

                    # Cooldown kontrolü
                    if prev_ts and (now_utc - prev_ts) < timedelta(hours=COOLDOWN_HOURS):
                        continue  # cooldown içinde

                    # Cooldown kaydı (append-only)
                    cur.execute("""
                        INSERT INTO alerts_sent(symbol, sent_at)
                        VALUES (%s, %s)
                    """, (r["symbol"], now_utc))

                    row_to_send.append(r)

                conn.commit()

    except Exception as e:
        print(f"[alert][ERR] DB işlemi sırasında hata: {e}")
        return

    # E-posta gönderimi
    if row_to_send:
        send_mail(row_to_send)
    else:
        # Bilgilendirme maili
        send_mail([{ "symbol": "Bilgi" }])

# Bu blok, dosya doğrudan çalıştırıldığında main()'i çağırır.
# Modül olarak import edilirse kod çalışmaz — Airflow DAG'lerinde kritik.
if __name__ == "__main__":
    main()
