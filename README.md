# Web to Postgres (Binance → Postgres)

## Kurulum
1. `.env.example` → `.env` olarak kopyalayın ve değerleri doldurun.
2. Postgres'e şema yükleyin:
   ```bash
   psql -h $PG_HOST -U $PG_USER -d $PG_DB -f docs/setup.sql
