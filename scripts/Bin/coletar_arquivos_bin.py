#%%
import json
import re
import subprocess
from datetime import datetime
from sqlalchemy import create_engine, text
from pathlib import Path
import configparser
import pandas as pd

LOG_PATH = Path(r"E:\BI\logs")
LOG_FILE = LOG_PATH / "log_bin.txt"
WINSCP_EXE = r"C:\Program Files (x86)\WinSCP\WinSCP.com"
DW_CONFIG_PATH = Path(r"E:\BI\config\config_datalake.ini")

dw = configparser.ConfigParser()
dw.read(DW_CONFIG_PATH, encoding="utf-8")

PG_HOST = dw.get("auth", "host", fallback=None)
PG_PORT = dw.get("auth", "port", fallback=None)
PG_DB = dw.get("auth", "db", fallback=None)
PG_USER = dw.get("auth", "user", fallback=None)
PG_PASS = dw.get("auth", "pwd", fallback=None)
SCHEMA = dw.get("auth", "schema", fallback="datalake")

INI_PATH = Path(r"E:\BI\config\config_bin.ini")
config = configparser.ConfigParser()
config.read(INI_PATH, encoding="utf-8")

HOST = config.get("auth", "host", fallback=None)
PORT = config.get("auth", "port", fallback=None)
USERNAME = config.get("auth", "user", fallback=None)
PASSWORD = config.get("auth", "pwd", fallback=None)
PRIVATE_KEY = r"E:\BI\config\bin_key_putty.ppk"
HOSTKEY = "*"
REMOTE_DIR = "/available"
# REMOTE_DIR = "/downloaded"

LOCAL_DOWNLOAD_DIR = Path(r"E:\BI\scripts\Bin\Json")
LOCAL_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

def write_log(message: str):
    LOG_PATH.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{ts} - {message}\n")

def identificar_tipo_edi(nome_arquivo: str) -> str | None:
    m = re.match(r"^EDI-([A-Z]+)-", nome_arquivo.upper())
    return m.group(1) if m else None


def extrair_metadados_nome(nome_arquivo: str) -> dict:
    partes = nome_arquivo.replace(".json", "").split("-")

    meta = {
        "nome_arquivo": nome_arquivo,
        "tipo_edi": None,
        "lote": None,
        "adquirente": None,
        "cnpj": None,
        "nsu": None,
        "data_arquivo": None,
    }

    if len(partes) >= 7 and partes[0] == "EDI":
        meta["tipo_edi"] = partes[1]
        meta["lote"] = partes[2]
        meta["adquirente"] = partes[3]
        meta["cnpj"] = partes[4]
        meta["nsu"] = partes[5]
        meta["data_arquivo"] = partes[6]

    return meta

def flatten_json_total(conteudo, meta: dict):
    if isinstance(conteudo, dict):
        df = pd.json_normalize(conteudo, sep="_")
    elif isinstance(conteudo, list):
        if len(conteudo) == 0:
            df = pd.DataFrame([{}])
        elif all(isinstance(x, dict) for x in conteudo):
            df = pd.json_normalize(conteudo, sep="_")
        else:
            df = pd.DataFrame({"valor_lista": [conteudo]})
    else:
        df = pd.DataFrame([{"conteudo_bruto": conteudo}])

    for k, v in meta.items():
        df[k] = v

    return df


def baixar_arquivos_localmente(pattern_remoto: str):
    script_file = LOCAL_DOWNLOAD_DIR / "winscp_download.txt"
    log_file = LOCAL_DOWNLOAD_DIR / "winscp_log.txt"

    script_content = f'''open sftp://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/ -privatekey="{PRIVATE_KEY}" -hostkey="{HOSTKEY}"
                        cd {REMOTE_DIR}
                        lcd "{LOCAL_DOWNLOAD_DIR}"
                        option transfer binary
                        get {pattern_remoto}
                        exit
                        '''

    script_file.write_text(script_content, encoding="utf-8")

    resultado = subprocess.run(
        [WINSCP_EXE, f"/script={script_file}", f"/log={log_file}"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace"
        
    )

    if resultado.returncode != 0:
        raise RuntimeError(
            f"Falha no download local.\n\nSTDOUT:\n{resultado.stdout}\n\nSTDERR:\n{resultado.stderr}"
        )

    print("Download concluído.")
    print(f"Pasta local: {LOCAL_DOWNLOAD_DIR}")
    write_log(f"SUCESSO | Download concluído com padrão {pattern_remoto}")


def processar_jsons_pasta_local(pasta: Path, pattern_local: str = "*.json"):
    grupos = {
        "R": [],
        "S": [],
        "PIX": [],
        "P": [],
        "VOUCHER": [],
        "OUTROS": [],
    }

    arquivos = list(pasta.glob(pattern_local))

    for arquivo in arquivos:
        try:
            with open(arquivo, "r", encoding="utf-8") as f:
                conteudo = json.load(f)

            tipo_edi = identificar_tipo_edi(arquivo.name)
            meta = extrair_metadados_nome(arquivo.name)
            df_tmp = flatten_json_total(conteudo, meta)

            if tipo_edi in grupos:
                grupos[tipo_edi].append(df_tmp)
            else:
                grupos["OUTROS"].append(df_tmp)

        except Exception as e:
            write_log(f"ERRO | Falha ao processar {arquivo.name}: {e}")

    resultado = {}
    for tipo, lista_dfs in grupos.items():
        resultado[tipo] = pd.concat(lista_dfs, ignore_index=True, sort=False) if lista_dfs else pd.DataFrame()

    return resultado


def serializar_colunas_complexas(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    df = df.copy()

    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: json.dumps(x, ensure_ascii=False)
            if isinstance(x, (list, dict))
            else x
        )

    return df

def delete_insert_por_nome_arquivo(engine, schema: str, tabela: str, df: pd.DataFrame):
    if df is None or df.empty:
        return 0

    if "nome_arquivo" not in df.columns:
        raise ValueError(f"O DataFrame da tabela {tabela} não possui a coluna nome_arquivo.")

    df = serializar_colunas_complexas(df)

    nomes = (
        df["nome_arquivo"]
        .dropna()
        .astype(str)
        .drop_duplicates()
        .tolist()
    )

    if not nomes:
        return 0

    delete_sql = text(f'''
        DELETE FROM "{schema}"."{tabela}"
        WHERE nome_arquivo = ANY(:nomes)
    ''')

    with engine.begin() as conn:
        conn.execute(delete_sql, {"nomes": nomes})

    df.to_sql(
        name=tabela,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )

    return len(df)


try:
    # dt_str = datetime.now().strftime("%Y%m%d")
    # dt_str = "20260408"
    # pattern_remoto = f"EDI-*-*-{dt_str}*.json"
    
    pattern_remoto = "EDI-*.json"
    pattern_local = pattern_remoto

    baixar_arquivos_localmente(pattern_remoto=pattern_remoto)

    dfs = processar_jsons_pasta_local(LOCAL_DOWNLOAD_DIR, pattern_local=pattern_local)

    df_r = dfs["R"]
    df_s = dfs["S"]
    df_pix = dfs["PIX"]
    df_p = dfs["P"]
    df_voucher = dfs["VOUCHER"]

    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}",
        pool_pre_ping=True
    )

    mapa_tabelas = {
        "bin_edi_r": df_r,
        "bin_edi_s": df_s,
        "bin_edi_pix": df_pix,
        "bin_edi_p": df_p,
        "bin_edi_voucher": df_voucher,
    }

    total_linhas_inseridas = 0

    for nome_tabela, df in mapa_tabelas.items():
        if df is None or df.empty:
            write_log(f"ATENCAO | DataFrame vazio, ignorado: {SCHEMA}.{nome_tabela}")
            continue

        qtd = delete_insert_por_nome_arquivo(engine, SCHEMA, nome_tabela, df)
     
        print(f"Tabela carregada: {SCHEMA}.{nome_tabela} -> {qtd} linhas")
        write_log(f"SUCESSO | Tabela carregada: {SCHEMA}.{nome_tabela} -> {qtd} linhas")

except Exception as e:
    write_log(f"ERRO | {type(e).__name__}: {e}")
    raise


# %%
