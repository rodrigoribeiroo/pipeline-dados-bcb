import os
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
DATA_INICIAL_PADRAO = date(2024, 1, 1)

# nome da tabela -> código da série no BCB
SERIES = {
    "cotacao_dolar": 1,
    "selic_meta": 432,
}


def get_engine():
    url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(url)


def criar_tabela(engine, tabela):
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {tabela} (
                data  DATE PRIMARY KEY,
                valor NUMERIC(12, 4) NOT NULL
            )
        """))


def ultima_data(engine, tabela):
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT MAX(data) FROM {tabela}")).scalar()


def extract(codigo, inicio, fim):
    params = {
        "formato": "json",
        "dataInicial": inicio.strftime("%d/%m/%Y"),
        "dataFinal": fim.strftime("%d/%m/%Y"),
    }
    resp = requests.get(BASE_URL.format(codigo=codigo), params=params, timeout=30)

    if resp.status_code == 404:
        # A API do BCB retorna 404 quando não há dados no período (fim de semana, feriado)
        return []

    resp.raise_for_status()
    return resp.json()


def transform(dados):
    df = pd.DataFrame(dados)
    if df.empty:
        return df
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y").dt.date
    df["valor"] = df["valor"].astype(float)
    return df


def load(engine, tabela, df):
    if df.empty:
        return 0
    sql = text(
        f"INSERT INTO {tabela} (data, valor) VALUES (:data, :valor) "
        "ON CONFLICT (data) DO NOTHING"
    )
    with engine.begin() as conn:
        conn.execute(sql, df.to_dict("records"))
    return len(df)


def processar_serie(engine, tabela, codigo):
    criar_tabela(engine, tabela)
    ultima = ultima_data(engine, tabela)
    inicio = DATA_INICIAL_PADRAO if ultima is None else ultima + timedelta(days=1)
    fim = date.today()

    if inicio > fim:
        print(f"{tabela}: já está atualizada")
        return

    dados = extract(codigo, inicio, fim)
    df = transform(dados)
    n = load(engine, tabela, df)
    print(f"{tabela}: {n} linhas processadas ({inicio} a {fim})")


def main():
    engine = get_engine()
    for tabela, codigo in SERIES.items():
        processar_serie(engine, tabela, codigo)


if __name__ == "__main__":
    main()