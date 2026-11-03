from datetime import datetime, timedelta

from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore

from pipeline import SERIES, get_engine, processar_serie

DBT = "/home/airflow/dbt_venv/bin/dbt"
DBT_ARGS = (
    "--project-dir /opt/airflow/dbt_project "
    "--profiles-dir /opt/airflow/dbt_project "
    "--target-path /tmp/dbt_target "
    "--log-path /tmp/dbt_logs"
)


def executar(tabela, codigo):
    processar_serie(get_engine(), tabela, codigo)


with DAG(
    dag_id="bcb_pipeline",
    description="Coleta diária de dólar e Selic do Banco Central + transformações dbt",
    start_date=datetime(2024, 1, 1),
    schedule="0 8 * * *",
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["bcb", "portfolio"],
) as dag:

    cargas = [
        PythonOperator(
            task_id=f"carregar_{tabela}",
            python_callable=executar,
            op_kwargs={"tabela": tabela, "codigo": codigo},
        )
        for tabela, codigo in SERIES.items()
    ]

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build {DBT_ARGS}",
    )

    cargas >> dbt_build