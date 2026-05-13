#%%

from sqlalchemy import create_engine, text, bindparam
import pandas as pd
from pathlib import Path
import configparser
from datetime import datetime
import re
from openpyxl import load_workbook
from io import StringIO
from psycopg2 import sql

DW_CONFIG_PATH = Path(r"E:\BI\config\config_datalake.ini")
dw = configparser.ConfigParser()
dw.read(DW_CONFIG_PATH, encoding="utf-8")

PG_HOST = dw.get("auth", "host", fallback=None)
PG_PORT = dw.get("auth", "port", fallback=None)
PG_DB = dw.get("auth", "db", fallback=None)
PG_USER = dw.get("auth", "user", fallback=None)
PG_PASS = dw.get("auth", "pwd", fallback=None)
SCHEMA = dw.get("auth", "schema", fallback="datalake")

caminho_base = Path(r"E:\RPA\RPA_Rede_2_0\downloads")
tabela = "rede_rpa_vendas"

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    pool_pre_ping=True
)

def extrair_pv_nome_arquivo(nome_arquivo: str):
    match = re.search(r"^\d{2}_\d{4}_([0-9]+)_", nome_arquivo)
    return match.group(1) if match else None

def encontrar_linha_cabecalho(caminho_arquivo: Path, texto="data da venda", max_linhas=80):

    wb = load_workbook(
        filename=caminho_arquivo,
        read_only=True,
        data_only=True
    )

    try:
        ws = wb.active

        for idx, row in enumerate(
            ws.iter_rows(min_row=1, max_row=max_linhas, values_only=True),
            start=0
        ):
            conteudo_linha = " ".join(
                "" if valor is None else str(valor).strip().lower()
                for valor in row
            )

            if texto in conteudo_linha:
                return idx

    finally:
        wb.close()

    return None


def listar_arquivos_rede(caminho_base: Path):
    arquivos = []

    for caminho_arquivo in caminho_base.rglob("*"):
        if (
            caminho_arquivo.is_file()
            and "Rede_Rel_Vendas" in caminho_arquivo.name
            and caminho_arquivo.suffix.lower() in [".xlsx", ".xlsm"]
        ):
            arquivos.append(caminho_arquivo)

    return arquivos

def copy_df_to_postgres(df: pd.DataFrame, engine, schema: str, tabela: str):

    if df.empty:
        print("DataFrame vazio. Nada para carregar.")
        return

    buffer = StringIO()

    df.to_csv(
        buffer,
        index=False,
        header=True,
        sep="\t",
        na_rep="\\N"
    )

    buffer.seek(0)

    colunas = list(df.columns)

    raw_conn = engine.raw_connection()

    try:
        with raw_conn.cursor() as cursor:
            copy_sql = sql.SQL("""
                COPY {}.{} ({})
                FROM STDIN
                WITH (
                    FORMAT CSV,
                    HEADER TRUE,
                    DELIMITER E'\t',
                    NULL '\\N'
                )
            """).format(
                sql.Identifier(schema),
                sql.Identifier(tabela),
                sql.SQL(", ").join(sql.Identifier(col) for col in colunas)
            )

            cursor.copy_expert(copy_sql, buffer)

        raw_conn.commit()

    except Exception as e:
        raw_conn.rollback()
        raise e

    finally:
        raw_conn.close()

arquivos_encontrados = listar_arquivos_rede(caminho_base)
nomes_arquivos = [arquivo.name for arquivo in arquivos_encontrados]

print(f"Arquivos encontrados na pasta: {len(nomes_arquivos)}")

if nomes_arquivos:
    query_arquivos = text(f'''
        SELECT DISTINCT "arquivo_origem"
        FROM "{SCHEMA}"."{tabela}"
        WHERE "arquivo_origem" IN :arquivos
    ''').bindparams(bindparam("arquivos", expanding=True))

    with engine.begin() as conn:
        arquivos_carregados = pd.read_sql(
            query_arquivos,
            conn,
            params={"arquivos": nomes_arquivos}
        )

    arquivos_carregados_set = set(arquivos_carregados["arquivo_origem"].astype(str))
else:
    arquivos_carregados_set = set()


arquivos_novos = [
    arquivo for arquivo in arquivos_encontrados
    if arquivo.name not in arquivos_carregados_set
]

print(f"Arquivos novos para processar: {len(arquivos_novos)}")

dfs = []

data_carga = datetime.now()

for caminho_arquivo in arquivos_novos:
    arquivo = caminho_arquivo.name
    pasta = caminho_arquivo.parent.name

    try:
        header_row = encontrar_linha_cabecalho(caminho_arquivo)

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

        df = df.dropna(how="all")

        df["marca"] = pasta
        df["arquivo_origem"] = arquivo
        df["pv"] = extrair_pv_nome_arquivo(arquivo)
        df["data_atualizacao"] = data_carga

        if "hora da venda" in df.columns:
            df["hora da venda"] = pd.to_datetime(
                df["hora da venda"],
                errors="coerce"
            ).dt.time

        dfs.append(df)

        print(f"Novo arquivo processado: {arquivo} | PV: {df['pv'].iloc[0]} | Linhas: {len(df)}")

    except Exception as e:
        print(f"Erro ao processar {caminho_arquivo}: {e}")


if dfs:
    df_final = pd.concat(dfs, ignore_index=True)

    copy_df_to_postgres(
        df=df_final,
        engine=engine,
        schema=SCHEMA,
        tabela=tabela
    )

    print(f"\nCarga concluída.")
    print(f"Arquivos novos inseridos: {len(dfs)}")
    print(f"Linhas inseridas: {len(df_final)}")

else:
    df_final = pd.DataFrame()
    print("Nenhum arquivo novo encontrado para carregar.")



# %%
