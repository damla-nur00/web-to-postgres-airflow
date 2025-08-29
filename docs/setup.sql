-- ============================================
-- SETUP / MIGRATION: Zaman serisi + Alert Sent
-- ============================================

SET TIME ZONE 'UTC';

-- 0) SNAPSHOT TABLOSU (sembol başına tek satır, son ölçüm)
CREATE TABLE IF NOT EXISTS public.binance_usdt (
  symbol               TEXT PRIMARY KEY,
  quote_volume         NUMERIC,
  price_change_percent NUMERIC,
  high_price           NUMERIC,
  low_price            NUMERIC,
  trade_count          BIGINT,
  -- UTC saklanır (TIMESTAMPTZ). Oturum UTC olduğu için now() → UTC.
  close_time   TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- TR duvar saati ayrıca saklanır (gösterim/raporlama için)
  close_time_tr TIMESTAMP  NOT NULL DEFAULT (now() AT TIME ZONE 'Europe/Istanbul')
);




-- 1) Zaman serisi tablo (history)
CREATE TABLE IF NOT EXISTS public.binance_usdt_history (
  symbol             TEXT,
  quote_volume        NUMERIC,
  price_change_percent NUMERIC,
  high_price         NUMERIC,
  low_price          NUMERIC,
  trade_count              BIGINT,
  scraped_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Tekilleştirme 
CREATE UNIQUE INDEX IF NOT EXISTS uq_hist_symbol_ts
ON public.binance_usdt_history(symbol, scraped_at);

-- 2) Z-skor view (son 30 satır üzerinden)
CREATE OR REPLACE VIEW public.v_stats_24h AS
SELECT
  h.symbol,
  h.quote_volume,
  h.price_change_percent,
  h.high_price,
  h.low_price,
  h.trade_count,
  h.scraped_at,
  (
    h.quote_volume
      - AVG(h.quote_volume) OVER (
          PARTITION BY h.symbol
          ORDER BY h.scraped_at
          ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )
  )
  / NULLIF(
      STDDEV_SAMP(h.quote_volume) OVER (
        PARTITION BY h.symbol
        ORDER BY h.scraped_at
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
      ),
      0
    ) AS vol_z
FROM public.binance_usdt_history h;

-- 3) En güncel snapshot (sembol başına 1 satır)
CREATE OR REPLACE VIEW public.v_latest_24h AS
SELECT DISTINCT ON (s.symbol)
  s.symbol,
  s.quote_volume,
  s.price_change_percent,
  s.high_price,
  s.low_price,
  s.trade_count,
  
  -- UTC tarih formatlı
  TO_CHAR(s.scraped_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS scraped_at_utc,

  -- TR tarih formatlı
  TO_CHAR(s.scraped_at AT TIME ZONE 'Europe/Istanbul', 'YYYY-MM-DD HH24:MI:SS') AS scraped_at_tr,

  s.vol_z
FROM public.v_stats_24h s
ORDER BY s.symbol, s.scraped_at DESC;

-- 4) SNAPSHOT için gösterim view’u (UTC + TR)
CREATE OR REPLACE VIEW public.v_binance_usdt AS
SELECT
  symbol,
  quote_volume,
  price_change_percent,
  high_price,
  low_price,
  trade_count,
  TO_CHAR(close_time   AT TIME ZONE 'UTC',             'YYYY-MM-DD HH24:MI:SS') AS close_time_utc,
  TO_CHAR(close_time_tr,                               'YYYY-MM-DD HH24:MI:SS') AS close_time_tr
FROM public.binance_usdt
ORDER BY close_time DESC;

-- 5) ALERTS_SENT: append-only (her mailde yeni satır)
-- (a) tablo yoksa oluştur
CREATE TABLE IF NOT EXISTS public.alerts_sent (
  id       BIGSERIAL   PRIMARY KEY,
  symbol   TEXT        NOT NULL,
  sent_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- (b) eski kurulumdan kalan "symbol PK" durumunu migrasyonla düzelt
--     (varsa PK'yı kaldır, id kolonu ve PK ekle)
ALTER TABLE public.alerts_sent
  DROP CONSTRAINT IF EXISTS alerts_sent_pkey;

ALTER TABLE public.alerts_sent
  ADD COLUMN IF NOT EXISTS id BIGSERIAL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.alerts_sent'::regclass
      AND contype = 'p'
  ) THEN
    ALTER TABLE public.alerts_sent
      ADD CONSTRAINT alerts_sent_pk PRIMARY KEY (id);
  END IF;
END$$;

-- (c) performans index (son gönderimi hızlı almak için)
CREATE INDEX IF NOT EXISTS ix_alerts_sent_symbol_time
  ON public.alerts_sent(symbol, sent_at DESC);


-- 5) Hızlı kontroller (okuma sorguları)

-- View’lar var mı?
SELECT table_schema, table_name
FROM information_schema.views
WHERE table_schema = 'public'
  AND table_name IN ('v_stats_24h','v_latest_24h');

-- Toplam satır sayısı
SELECT COUNT(*) AS history_rows FROM public.binance_usdt_history;

-- En yeni 20 kayıt (UTC)
SELECT *
FROM public.v_latest_24h
ORDER BY scraped_at_utc DESC
LIMIT 20;

-- En yüksek hacim (quote_volume)
SELECT *
FROM public.v_latest_24h
ORDER BY quote_volume DESC NULLS LAST
LIMIT 20;

-- En yüksek z-skor
SELECT *
FROM public.v_latest_24h
ORDER BY vol_z DESC NULLS LAST
LIMIT 20;

-- En büyük mutlak % değişim
SELECT *
FROM public.v_latest_24h
ORDER BY ABS(price_change_percent) DESC NULLS LAST
LIMIT 20;

-- UTC ve TR'yi birlikte göster (artık view hazır veriyor)
SELECT
  symbol,
  scraped_at_utc,
  scraped_at_tr,
  price_change_percent,
  vol_z
FROM public.v_latest_24h
ORDER BY scraped_at_utc DESC
LIMIT 20;
