#%%

from pathlib import Path
import time
from sqlalchemy import create_engine, text
from datetime import timedelta, datetime, date
import configparser
import re
import requests
import pandas as pd
pd.set_option('display.max_columns', None)

DATA_INICIO = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
DATA_FIM = date.today().strftime("%Y-%m-%d") 

# DATA_INICIO = "2026-01-01"
# DATA_FIM = "2025-12-31" 

LOG_PATH = Path(r"E:\BI\logs")
LOG_FILE = LOG_PATH / "log_elosgate_exportacao_financeiro.txt"

INI_PATH = Path(r"E:\BI\config\config_elosgate.ini")
DW_CONFIG_PATH = Path(r"E:\BI\config\config_datalake.ini")
BASE_URL = "https://api.elosgate.com.br"

filiais = configparser.ConfigParser()
filiais.read(INI_PATH, encoding="utf-8")

dw = configparser.ConfigParser()
dw.read(DW_CONFIG_PATH, encoding="utf-8")

PG_HOST = dw.get("auth", "host", fallback=None)
PG_PORT = dw.get("auth", "port", fallback=None)
PG_DB = dw.get("auth", "db", fallback=None)
PG_USER = dw.get("auth", "user", fallback=None)
PG_PASS = dw.get("auth", "pwd", fallback=None)
SCHEMA  = dw.get("auth", "schema", fallback=None)

def write_log(message: str):
    LOG_PATH.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{ts} - {message}\n")

def get_token_elosgate(client_id: str, client_secret: str, base_url: str = BASE_URL, timeout: int = 30):
    url = base_url.rstrip("/") + "/api/v1/login"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "audience": "string",
        "grant_type": "string",
    }
    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "user-agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/122.0.0.0 Safari/537.36"),
        "origin": "https://api.elosgate.com.br",
        "referer": "https://api.elosgate.com.br/",
    }

    resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
    if not resp.ok:
        raise RuntimeError(f"Login falhou | status={resp.status_code} | body={resp.text}")
    return resp.json()

def solicitar_exportacao_financeiro(
        access_token: str
        , data_inicio: str
        , data_fim: str
        , grupo_nome: str = ""
        , sobrepor_existente: bool = True
        , estabelecimentos_inativo: bool = True
        , base_url: str = BASE_URL
        , timeout: int = 30
        ):

    url = base_url.rstrip("/") + "/api/v1/solicitacoes/exportacao-financeiro"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "user-agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/122.0.0.0 Safari/537.36"),
        "origin": "https://api.elosgate.com.br",
        "referer": "https://api.elosgate.com.br/",
    }
    payload = {
        "dataInicio": data_inicio,
        "dataFim": data_fim,
        "grupoNome": grupo_nome,
        "sobreporExistente": bool(sobrepor_existente),
        "estabelecimentosInativo": bool(estabelecimentos_inativo),
    }

    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    if not r.ok:
        raise RuntimeError(f"Solicitar exportação falhou | status={r.status_code} | body={r.text}")
    return r.json()

def obter_qtd_exportacao(access_token: str, id_exportacao: str, base_url: str = BASE_URL,
                        timeout: int = 30, tentativas: int = 25, espera_seg: int = 3):

    url = base_url.rstrip("/") + "/api/v1/consultas/consultar-exportacao-financeiro"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "user-agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/122.0.0.0 Safari/537.36"),
        "origin": "https://api.elosgate.com.br",
        "referer": "https://api.elosgate.com.br/",
    }

    ultima_mensagem = None

    for i in range(1, tentativas + 1):
        try:
            r = requests.get(
                url,
                headers=headers,
                params={"identificador": id_exportacao, "skip": 0, "take": 1},
                timeout=timeout
            )

            if r.status_code >= 500:
                ultima_mensagem = r.text
                time.sleep(espera_seg * min(i, 10))
                continue

            r.raise_for_status()
            j = r.json()
            ultima_mensagem = j.get("mensagem", "")

            m = re.search(r"Registros\s*:\s*(\d+)", ultima_mensagem or "")
            if m:
                return int(m.group(1)), ultima_mensagem

            if any(x in (ultima_mensagem or "").lower() for x in ["nenhum", "sem registro", "sem registros"]):
                return 0, ultima_mensagem

            if any(x in (ultima_mensagem or "").lower() for x in ["process", "processando", "aguarde"]):
                time.sleep(espera_seg * min(i, 10))
                continue

            time.sleep(espera_seg)

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            ultima_mensagem = str(e)
            time.sleep(espera_seg * min(i, 10))
            continue

    return None, ultima_mensagem

def baixar_exportacao_por_qtd(access_token: str, id_exportacao: str, qtd: int, data_ref: str,
                             base_url: str = BASE_URL, timeout: int = 30):

    url = base_url.rstrip("/") + "/api/v1/consultas/consultar-exportacao-financeiro"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "user-agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/122.0.0.0 Safari/537.36"),
        "origin": "https://api.elosgate.com.br",
        "referer": "https://api.elosgate.com.br/",
    }

    if qtd == 0:
        df = pd.DataFrame()
        df.insert(0, "data_ref", [])
        return df

    r = requests.get(
        url,
        headers=headers,
        params={"identificador": id_exportacao, "skip": 0, "take": qtd},
        timeout=timeout
    )
    r.raise_for_status()

    data = r.json()
    itens = data if isinstance(data, list) else (data.get("itens") or [])
    df = pd.json_normalize(itens)
    df.insert(0, "data_ref", data_ref)
    return df

def exportacao_financeiro_periodo(access_token: str, data_inicio: str, data_fim: str,
                                 base_url: str = BASE_URL, timeout: int = 30,
                                 take_sleep_dia: float = 0.8):

    ini = datetime.strptime(data_inicio, "%Y-%m-%d").date()
    fim = datetime.strptime(data_fim, "%Y-%m-%d").date()

    dfs = []
    erros = []

    d = ini
    while d <= fim:
        data_ref = d.strftime("%Y-%m-%d")
        print(f"\nSolicitando exportação do dia: {data_ref}")

        try:
            resp = solicitar_exportacao_financeiro(
                access_token=access_token,
                data_inicio=data_ref,
                data_fim=data_ref,
                base_url=base_url,
                timeout=timeout
            )

            id_exportacao = resp.get("idExportacao")
            if not id_exportacao:
                raise ValueError(f"Sem idExportacao: {resp}")

            qtd, msg = obter_qtd_exportacao(
                access_token=access_token,
                id_exportacao=id_exportacao,
                base_url=base_url,
                timeout=timeout
            )

            if qtd is None:
                erros.append({"data_ref": data_ref, "idExportacao": id_exportacao, "erro": "Não obtive qtd", "mensagem": msg})
                d += timedelta(days=1)
                continue

            print(f"Data: {data_ref} | Total: {qtd}")

            df_dia = baixar_exportacao_por_qtd(
                access_token=access_token,
                id_exportacao=id_exportacao,
                qtd=qtd,
                data_ref=data_ref,
                base_url=base_url,
                timeout=timeout
            )

            print(f"Recuperados {df_dia.shape[0]} registros | {df_dia.shape[1]} colunas.")
            dfs.append(df_dia)

        except Exception as e:
            erros.append({"data_ref": data_ref, "idExportacao": None, "erro": str(e), "mensagem": None})

        time.sleep(take_sleep_dia)
        d += timedelta(days=1)

    df_final = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    df_erros = pd.DataFrame(erros)
    return df_final, df_erros

def delete_e_inserir_financeiro_por_intervalo_e_marca(
        engine
        , df: pd.DataFrame
        , schema: str
        , mark: str,
        table: str = "elosgate_exportacao_financeiro"
        ):
    if df.empty:
        print("DF vazio. Nada a fazer.")
        return

    if "data_ref" not in df.columns:
        raise ValueError("DF precisa conter a coluna 'data_ref'.")

    if "marca" not in df.columns:
        raise ValueError("DF precisa conter a coluna 'marca'.")

    df = df[df["data_ref"].notna()].copy()
    df["data_ref"] = df["data_ref"].astype(str)

    data_min = df["data_ref"].min()
    data_max = df["data_ref"].max()

    print(f"Deletando marca={mark} no intervalo data_ref: {data_min} - {data_max}")

    delete_sql = text(f'''
        DELETE FROM "{schema}"."{table}"
        WHERE marca = :mark
          AND CAST(data_ref AS date)
              BETWEEN CAST(:data_min AS date) AND CAST(:data_max AS date)
    ''')

    with engine.begin() as conn:
        conn.execute(delete_sql, {"mark": mark, "data_min": data_min, "data_max": data_max})

    print("Inserindo novos dados.")

    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=5000,
        method="multi"
    )

    print("Insert concluído.")

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    pool_pre_ping=True
)

for section in filiais.sections():
    try:
        MARK = filiais.get(section, "mark")
        CLIENT = filiais.get(section, "client")
        SECRET = filiais.get(section, "secret")

        print(f"\n=== MARCA: {MARK} ({section}) ===")

        token = get_token_elosgate(client_id=CLIENT, client_secret=SECRET).get("access_token")
        if not token:
            raise RuntimeError("Resposta do login sem access_token")

        df_fin, df_erros = exportacao_financeiro_periodo(token, DATA_INICIO, DATA_FIM)

        if not df_fin.empty:
            if "marca" in df_fin.columns:
                df_fin["marca"] = MARK
            else:
                df_fin.insert(0, "marca", MARK)

            delete_e_inserir_financeiro_por_intervalo_e_marca(
                engine=engine,
                df=df_fin,
                schema=SCHEMA,
                mark=MARK,
                table="elosgate_exportacao_financeiro"
            )

        write_log(f"SUCESSO | marca={MARK} | data_inicio={DATA_INICIO} data_fim={DATA_FIM} | linhas={df_fin.shape[0]} colunas={df_fin.shape[1]} | tabela={SCHEMA}.elosgate_exportacao_financeiro")

        if not df_erros.empty:
            write_log(f"ATENCAO | marca={MARK} | falhas_dias={df_erros.shape[0]} | exemplo_erro={df_erros.iloc[0].to_dict()}")

    except Exception as e:
        write_log(f"ERRO | marca={filiais.get(section, 'mark', fallback=section)} | {type(e).__name__}: {e}")
        continue

# %%