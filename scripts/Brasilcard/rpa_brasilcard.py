#%%

import os
import pandas as pd
import re
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

caminho_base = r"E:\RPA\RPA_Financeiras\saida_brasilcard"

dfs = []

padrao = re.compile(r"vendas_\d{8}_\d{6}\.xlsx")

colunas_desejadas = [
    "Período",
    "Data da venda",
    "Data de vencimento",
    "Número autorização",
    "Parcela(s)",
    "Cartão",
    "Nome",
    "Valor Bruto",
    "Valor Líquido",
    "Data de pagamento",
    "Modalidade"
]

for pasta in os.listdir(caminho_base):
    caminho_pasta = os.path.join(caminho_base, pasta)

    if os.path.isdir(caminho_pasta):

        for arquivo in os.listdir(caminho_pasta):

            if arquivo.endswith(".xlsx") and padrao.match(arquivo):

                caminho_arquivo = os.path.join(caminho_pasta, arquivo)

                try:
                    xls = pd.ExcelFile(caminho_arquivo, engine="openpyxl")

                    nome_aba = None
                    for aba in xls.sheet_names:
                        if aba.strip().lower() == "consolidado":
                            nome_aba = aba
                            break

                    if not nome_aba:
                        print(f"Aba 'Consolidado' não encontrada: {arquivo}")
                        continue

                    df = pd.read_excel(xls, sheet_name=nome_aba)

                    df.columns = df.columns.str.strip()

                    colunas_existentes = [col for col in colunas_desejadas if col in df.columns]
                    df = df[colunas_existentes]

                    if "Modalidade" in df.columns:
                        df = df[
                            df["Modalidade"].notna() &
                            (df["Modalidade"].astype(str).str.strip() != "")
                        ]

                    df["pv"] = pasta
                    df["arquivo_origem"] = arquivo
                    df["data_arquivo"] = arquivo.split("_")[1]
                    df['data_atualizacao'] = datetime.now()

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
        text(f'TRUNCATE TABLE "{SCHEMA}"."brasilcard_rpa_vendas"')
        
    )

df_final.to_sql(
    name="brasilcard_rpa_vendas",
    con=engine,
    schema=SCHEMA,
    if_exists="append",
    index=False,
    chunksize=5000,
    method="multi"
)