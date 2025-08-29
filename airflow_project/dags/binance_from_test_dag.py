import os
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

def run_script():

    # ENV’leri burada set edebilirsin
    os.environ.update ({
       "PG_HOST": "host.docker.internal",
       "PG_DB": "postgres",
       "PG_USER": "postgres",
       "PG_PASS": "1234", })

    from binance_from_test import main
    main()

with DAG(
    dag_id="binance_from_test",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["binance","etl"],
):
    PythonOperator(
        task_id="run_test",
        python_callable=run_script,
    )

