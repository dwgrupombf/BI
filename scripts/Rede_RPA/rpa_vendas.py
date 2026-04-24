
#%%

from sqlalchemy import create_engine, text
import os
import pandas as pd
from pathlib import Path
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

caminho_base = r"E:\RPA\RPA_Rede_2_0\downloads"

dfs = []

for pasta in os.listdir(caminho_base):
    caminho_pasta = os.path.join(caminho_base, pasta)

    if os.path.isdir(caminho_pasta):
        for arquivo in os.listdir(caminho_pasta):

            if "Rede_Rel_Vendas" in arquivo and arquivo.endswith((".xlsx", ".xls")):

                caminho_arquivo = os.path.join(caminho_pasta, arquivo)

                try:
            
                    df_raw = pd.read_excel(
                        caminho_arquivo,
                        header=None,
                        engine="openpyxl"
                    )

                    header_row = None

                    for i, row in df_raw.iterrows():
                        if "data da venda" in str(row.values).lower():
                            header_row = i
                            break

                    if header_row is None:
                        print(f"Cabeçalho não encontrado: {arquivo}")
                        continue

                    df = pd.read_excel(
                        caminho_arquivo,
                        skiprows=header_row,
                        engine="openpyxl"
                    )

                    df.columns = (
                        df.columns
                        .astype(str)
                        .str.strip()
                        .str.lower()
                    )

                    df["marca"] = pasta
                    df["arquivo_origem"] = arquivo
                    df['data_atualizacao'] = datetime.now()

                    if "hora da venda" in df.columns:
                        df["hora da venda"] = pd.to_datetime(
                            df["hora da venda"],
                            errors="coerce"
                        ).dt.time

                    dfs.append(df)

                except Exception as e:
                    print(f"Erro ao processar {caminho_arquivo}: {e}")

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
        text(f'TRUNCATE TABLE "{SCHEMA}"."rede_rpa_vendas"')
        
    )

df_final.to_sql(
    name="rede_rpa_vendas",
    con=engine,
    schema=SCHEMA,
    if_exists="append",
    index=False,
    chunksize=5000,
    method="multi"
)
# %%
