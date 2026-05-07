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

PG_HOST = dw.get("auth", "host")
PG_PORT = dw.get("auth", "port")
PG_DB = dw.get("auth", "db")
PG_USER = dw.get("auth", "user")
PG_PASS = dw.get("auth", "pwd")
SCHEMA = dw.get("auth", "schema", fallback="datalake")

caminho_base = r"E:\RPA\RPA_Itau\downloads"
tabela = "itau_rpa_extratos"

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    pool_pre_ping=True
)

with engine.begin() as conn:
    arquivos_existentes = pd.read_sql(
        text(f'SELECT DISTINCT "arquivo_origem" FROM "{SCHEMA}"."{tabela}"'),
        conn
    )

arquivos_existentes_set = set(arquivos_existentes["arquivo_origem"].astype(str))

dfs = []

padrao = re.compile(
    r"^extrato_PERIODO_\d+_\d{8}(?:_a)?_\d{8}_(?P<marca>.+)_(?P<data_arquivo>\d{8})_\d{6}\.xlsx$",
    re.IGNORECASE
)

for arquivo in os.listdir(caminho_base):
    caminho_arquivo = os.path.join(caminho_base, arquivo)

    if (
        os.path.isfile(caminho_arquivo)
        and arquivo.lower().endswith(".xlsx")
        and arquivo.lower().startswith("extrato_periodo_")
        and arquivo not in arquivos_existentes_set
    ):
        match = padrao.match(arquivo)

        if not match:
            print(f"Fora do padrão: {arquivo}")
            continue

        try:
            marca = match.group("marca").strip("_")
            data_arquivo = match.group("data_arquivo")

            xls = pd.ExcelFile(caminho_arquivo, engine="openpyxl")

            nome_aba = None

            for aba in xls.sheet_names:
                if aba.strip().lower() == "extrato detalhado":
                    nome_aba = aba
                    break

            if nome_aba is None:
                for aba in xls.sheet_names:
                    if "extrato" in aba.strip().lower():
                        nome_aba = aba
                        break

            if nome_aba is None:
                nome_aba = xls.sheet_names[0]

            print(f"Aba usada em {arquivo}: {nome_aba}")

            df = pd.read_excel(
                xls,
                sheet_name=nome_aba,
                header=4
            )

            df.columns = (
                df.columns
                .astype(str)
                .str.replace("\n", " ", regex=False)
                .str.replace("\t", " ", regex=False)
                .str.strip()
            )

            df = df.rename(columns={
                "Valor": "Valor (R$)",
                "Saldo": "Saldo (R$)"
            })

            colunas = [
                "Data",
                "Descrição",
                "Valor (R$)",
                "Saldo (R$)",
                "ID Transação"
            ]

            colunas_existentes = [c for c in colunas if c in df.columns]

            if not colunas_existentes:
                print(f"Nenhuma coluna desejada encontrada: {arquivo}")
                print("Colunas encontradas:", list(df.columns))
                continue

            df = df[colunas_existentes]

            if "Descrição" not in df.columns:
                print(f"Coluna Descrição não encontrada: {arquivo}")
                continue

            df = df[df["Descrição"].notna()]

            if df.empty:
                print(f"Arquivo sem registros válidos: {arquivo}")
                continue

            df["marca"] = marca
            df["data_arquivo"] = data_arquivo
            df["arquivo_origem"] = arquivo
            df["data_atualizacao"] = datetime.now()

            dfs.append(df)

            print(f"✔ Processado: {arquivo} | Linhas: {len(df)}")

        except Exception as e:
            print(f"Erro ao processar {arquivo}: {e}")

if dfs:
    df_final = pd.concat(dfs, ignore_index=True)

    if not df_final.empty:
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
        print("Arquivos processados, mas nenhum registro válido encontrado.")
else:
    df_final = pd.DataFrame()
    print("Nenhum arquivo novo encontrado para carregar.")

# %%
