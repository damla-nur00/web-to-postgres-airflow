import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from importlib import import_module

def run_alert():
    # --- Buradan ENV'leri override ediyoruz ---
    os.environ.update({
        # DB: host makinedeki PostgreSQL (DBeaver'da baktığın DB)
        "PG_HOST": "host.docker.internal",
        "PG_DB":   "postgres",
        "PG_USER": "postgres",
        "PG_PASS": "1234",

        # Opsiyonel eşik ve davranışlar
        # "ALERT_PCT_THRESH": "10",     # default 10 (%)
        # "ALERT_VOLZ_THRESH": "2",     # default 2 (z-score)
        # "ALERT_COOLDOWN_HOURS": "6",  # default 6 saat
        # "ALERT_DRY_RUN": "1",         # 1 -> mail gönderme, sadece logla (test için ideal)

        # SMTP (boşsa script log’layıp atlar)
         "ALERT_EMAIL_TO": "damlahande4@gmail.com",
         "SMTP_HOST": "smtp.gmail.com",
         "SMTP_PORT": "587",
         "SMTP_USER": "damlahande4@gmail.com",
         "SMTP_PASS": "wbhoxdccnjxskrge",
    })

    # alert_1.py, /opt/airflow/src altında (PYTHONPATH'te), direkt import edebiliriz
    from alert_1 import main
    main()

with DAG(
    dag_id="alert_1",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["alerts", "binance"],
) as dag:
    PythonOperator(
        task_id="check_alerts",
        python_callable=run_alert,
    )

