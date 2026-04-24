#%%

import os
import re
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
import configparser
from datetime import datetime

DW_CONFIG_PATH = Path(r"E:\BI\config\config_datalake.ini")
dw = configparser.ConfigParser()
dw.read(DW_CONFIG_PATH, encoding="utf-8")

PG_HOST = dw.get("auth", "host", fallback=None)
PG_PORT = dw.get("auth", "port", fallback=None)
PG_DB = dw.get("auth", "db", fallback=None)
PG_USER = dw.get("auth", "user", fallback=None)
PG_PASS = dw.get("auth", "pwd", fallback=None)
SCHEMA = dw.get("auth", "schema", fallback="datalake")

caminho_base = r"E:\RPA\RPA_Itau"

dfs = []

padrao = re.compile(
    r"^relatorio_extrato_\d{8}_\d{8}_(?P<marca>.+)_(?P<data_arquivo>\d{8})_\d{6}\.xlsx$",
    re.IGNORECASE
)

for arquivo in os.listdir(caminho_base):
    caminho_arquivo = os.path.join(caminho_base, arquivo)

    if os.path.isfile(caminho_arquivo) and arquivo.lower().endswith(".xlsx"):
        match = padrao.match(arquivo)

        if match:
            try:
                marca = match.group("marca")
                data_arquivo = match.group("data_arquivo")

                df = pd.read_excel(
                    caminho_arquivo,
                    sheet_name="Extrato Detalhado",
                    header=4,
                    engine="openpyxl"
                )

                df.columns = (
                    df.columns
                    .astype(str)
                    .str.replace("\n", " ", regex=False)
                    .str.replace("\t", " ", regex=False)
                    .str.strip()
                )

                df = df.rename(columns={
                    "Valor (R$)": "Valor (R$)",
                    "Valor": "Valor (R$)",
                    "Saldo": "Saldo (R$)",
                    "Saldo (R$)": "Saldo (R$)"
                })

                colunas_map = {
                    "Data": "Data",
                    "Descrição": "Descrição",
                    "Valor (R$)": "Valor (R$)",
                    "Saldo (R$)": "Saldo (R$)",
                    "ID Transação": "ID Transação"
                }

                colunas_existentes = [c for c in colunas_map if c in df.columns]
                df = df[colunas_existentes]

                df = df[df["Descrição"].notna()]
                df = df[~df["Descrição"].str.contains("SALDO", case=False, na=False)]

                df["marca"] = marca
                df["data_arquivo"] = data_arquivo
                df["arquivo_origem"] = arquivo
                df['data_atualizacao'] = datetime.now()

                dfs.append(df)

            except Exception as e:
                print(f"Erro ao processar {arquivo}: {e}")

# 🔗 concat final
if dfs:
    df_final = pd.concat(dfs, ignore_index=True)
else:
    df_final = pd.DataFrame()


engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    pool_pre_ping=True
)

with engine.begin() as conn:
    conn.execute(
        text(f'TRUNCATE TABLE "{SCHEMA}"."itau_rpa_extratos"')
        
    )

df_final.to_sql(
    name="itau_rpa_extratos",
    con=engine,
    schema=SCHEMA,
    if_exists="append",
    index=False,
    chunksize=5000,
    method="multi"
)


