from datetime import datetime, timedelta

from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore

from pipeline import SERIES, get_engine, processar_serie


def executar(tabela, codigo):
    processar_serie(get_engine(), tabela, codigo)


with DAG(
    dag_id="bcb_pipeline",
    description="Coleta diária de dólar e Selic do Banco Central",
    start_date=datetime(2024, 1, 1),
    schedule="0 8 * * *",          # todo dia às 08:00 (UTC)
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["bcb", "portfolio"],
) as dag:

    for tabela, codigo in SERIES.items():
        PythonOperator(
            task_id=f"carregar_{tabela}",
            python_callable=executar,
            op_kwargs={"tabela": tabela, "codigo": codigo},
        )