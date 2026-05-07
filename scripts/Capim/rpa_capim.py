#%%

import os
import pandas as pd
from sqlalchemy import create_engine, text
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

caminho_base = r"E:\RPA\RPA_Financeiras"
tabela = "capim_rpa_financiamento"

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    pool_pre_ping=True
)

# Buscar arquivos já carregados no banco
with engine.begin() as conn:
    arquivos_existentes = pd.read_sql(
        text(f'''
            SELECT DISTINCT "arquivo_origem"
            FROM "{SCHEMA}"."{tabela}"
            WHERE "arquivo_origem" IS NOT NULL
        '''),
        conn
    )

arquivos_existentes_set = set(arquivos_existentes["arquivo_origem"].astype(str))

dfs = []

colunas_desejadas = [
    "aba",
    "Nome",
    "CPF",
    "Data",
    "Valor",
    "Etapa",
    "Status"
]

for arquivo in os.listdir(caminho_base):
    caminho_arquivo = os.path.join(caminho_base, arquivo)

    if (
        os.path.isfile(caminho_arquivo)
        and arquivo.lower().endswith(".xlsx")
        and "financiamento_capim" in arquivo.lower()
        and arquivo not in arquivos_existentes_set
    ):
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

            df = pd.read_excel(
                xls,
                sheet_name=nome_aba,
                engine="openpyxl"
            )

            df.columns = df.columns.astype(str).str.strip()

            colunas_existentes = [
                col for col in colunas_desejadas
                if col in df.columns
            ]

            df = df[colunas_existentes]

            if df.empty:
                print(f"Arquivo sem linhas válidas: {arquivo}")
                continue

            df["arquivo_origem"] = arquivo
            df["data_atualizacao"] = datetime.now()

            dfs.append(df)

            print(f"✔ Processado: {arquivo} | Linhas: {len(df)}")

        except Exception as e:
            print(f"Erro ao processar {arquivo}: {e}")

if dfs:
    df_final = pd.concat(dfs, ignore_index=True)

    df_final.to_sql(
        name=tabela,
        con=engine,
        schema=SCHEMA,
        if_exists="append",
        index=False,
        chunksize=5000,
        method="multi"
    )

    print("\n🔥 Carga incremental concluída")
    print(f"Arquivos novos carregados: {len(dfs)}")
    print(f"Linhas inseridas: {len(df_final)}")

else:
    df_final = pd.DataFrame(
        columns=colunas_desejadas + ["arquivo_origem", "data_atualizacao"]
    )
    print("Nenhum arquivo novo encontrado para carregar.")

