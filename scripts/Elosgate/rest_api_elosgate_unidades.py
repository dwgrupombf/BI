#%%

from pathlib import Path
from sqlalchemy import create_engine, text
from datetime import datetime
import configparser
import requests
import pandas as pd
pd.set_option('display.max_columns', None)

LOG_PATH = Path(r"E:\BI\logs")
LOG_FILE = LOG_PATH / "log_elosgate_unidades.txt"
INI_PATH = Path(r"E:\BI\config\config_elosgate.ini")
DW_CONFIG_PATH = Path(r"E:\BI\config\config_datalake.ini")
BASE_URL = "https://api.elosgate.com.br"

filiais = configparser.ConfigParser()
filiais.read(INI_PATH, encoding="utf-8")

dw = configparser.ConfigParser()
dw.read(DW_CONFIG_PATH, encoding="utf-8")
PG_HOST = dw.get("auth", "host", fallback=None)
PG_PORT = dw.get("auth", "port", fallback=None)
PG_DB   = dw.get("auth", "db", fallback=None)
PG_USER = dw.get("auth", "user", fallback=None)
PG_PASS = dw.get("auth", "pwd", fallback=None)
SCHEMA  = dw.get("auth", "schema", fallback=None)

def write_log(message: str):
    LOG_PATH.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{ts} - {message}\n")

def get_token_elosgate(
    client_id: str,
    client_secret: str,
    base_url: str = BASE_URL,
    timeout: int = 30
):
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

def listar_unidades_elosgate(
    access_token: str,
    mark: str,
    base_url: str = BASE_URL,
    skip: int = 0,
    take: int = 1000,
    timeout: int = 30
):
    url = base_url.rstrip("/") + "/api/v1/consultas/listar-unidades"

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

    params = {"skip": skip, "take": take}
    r = requests.get(url, headers=headers, params=params, timeout=timeout)

    if not r.ok:
        raise RuntimeError(f"Listar unidades falhou | status={r.status_code} | body={r.text}")

    data = r.json()

    itens = None
    if isinstance(data, list):
        itens = data
    elif isinstance(data, dict):
        for key in ("itens", "items", "content", "data", "result"):
            v = data.get(key)
            if isinstance(v, list):
                itens = v
                break

    if itens is None:
        raise ValueError(f"Formato inesperado de resposta: {data}")

    df = pd.json_normalize(itens)
    df.insert(0, "marca", mark)  
    return df

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    pool_pre_ping=True
)

for section in filiais.sections():
    MARK   = filiais.get(section, "mark")
    CLIENT = filiais.get(section, "client")
    SECRET = filiais.get(section, "secret")
  
    try:
        token = get_token_elosgate(client_id=CLIENT, client_secret=SECRET).get("access_token")
        df_unidades = listar_unidades_elosgate(token, MARK)
        print(f"Coletados dados de {MARK} | linhas={df_unidades.shape[0]} | colunas={df_unidades.shape[1]}")

        with engine.begin() as conn:
            conn.execute(
                text(f'DELETE FROM "{SCHEMA}"."elosgate_unidades" WHERE marca = :m'),
                {"m": MARK}
            )

        df_unidades.to_sql(
            name="elosgate_unidades",
            con=engine,
            schema=SCHEMA,
            if_exists="append",
            index=False,
            chunksize=5000,
            method="multi"
        )

        write_log(f"SUCESSO | marca={MARK} | registros={df_unidades.shape[0]}")

    except Exception as e:
        write_log(f"ERRO | marca={MARK} | {type(e).__name__}: {e}")
        continue  

# %%