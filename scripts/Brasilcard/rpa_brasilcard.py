#%%

import os
import re
import pandas as pd
from pathlib import Path
import configparser
from datetime import datetime
from sqlalchemy import create_engine, text

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
tabela = "brasilcard_rpa_vendas"

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    pool_pre_ping=True
)

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

padrao = re.compile(r"^vendas_\d{8}_\d{6}\.xlsx$", re.IGNORECASE)

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

            if (
                arquivo.lower().endswith(".xlsx")
                and padrao.match(arquivo)
                and arquivo not in arquivos_existentes_set
            ):

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

                    df.columns = df.columns.astype(str).str.strip()

                    colunas_existentes = [
                        col for col in colunas_desejadas 
                        if col in df.columns
                    ]

                    df = df[colunas_existentes]

                    if "Modalidade" in df.columns:
                        df = df[
                            df["Modalidade"].notna() &
                            (df["Modalidade"].astype(str).str.strip() != "")
                        ]

                    if df.empty:
                        print(f"Arquivo sem linhas válidas: {arquivo}")
                        continue

                    df["pv"] = pasta
                    df["arquivo_origem"] = arquivo
                    df["data_arquivo"] = arquivo.split("_")[1]
                    df["data_atualizacao"] = datetime.now()

                    dfs.append(df)

                    print(f"✔ Processado: {pasta}\\{arquivo} | Linhas: {len(df)}")

                except Exception as e:
                    print(f"Erro ao processar {caminho_arquivo}: {e}")

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
    df_final = pd.DataFrame()
    print("Nenhum arquivo novo encontrado para carregar.")

#%%
df_final