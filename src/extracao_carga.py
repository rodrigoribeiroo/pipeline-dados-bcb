import requests
import pandas as pd
from sqlalchemy import create_engine

URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados"  # dólar (venda)
params = {"formato": "json", "dataInicial": "01/01/2024", "dataFinal": "31/12/2024"}

resp = requests.get(URL, params=params, timeout=30)
resp.raise_for_status()

df = pd.DataFrame(resp.json())
df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
df["valor"] = df["valor"].astype(float)

engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/dados")
df.to_sql("cotacao_dolar", engine, if_exists="replace", index=False)
print(f"{len(df)} linhas carregadas")