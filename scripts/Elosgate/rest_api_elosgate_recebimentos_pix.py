#%%

from pathlib import Path
from sqlalchemy import create_engine, text
from datetime import timedelta, datetime, date
import configparser
import requests
import pandas as pd
pd.set_option('display.max_columns', None)

DATA_INICIO = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
DATA_FIM = date.today().strftime("%Y-%m-%d") 

# DATA_INICIO = "2022-05-01"
# DATA_FIM = "2022-05-09"

LOG_PATH = Path(r"E:\BI\logs")
LOG_FILE = LOG_PATH / "log_elosgate_recebimentos_pix.txt"
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
SCHEMA = dw.get("auth", "schema", fallback=None)

def write_log(message: str) -> None:
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

def recebimentos_pix_elosgate(
        access_token: str
        , data_ref: str
        , base_url: str = BASE_URL
        , timeout: int = 30
        ):
    
    url = base_url.rstrip("/") + f"/api/v1/consultas/recebimentos-pix/{data_ref}"
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

    r = requests.get(url, headers=headers, timeout=timeout)
    if not r.ok:
        raise RuntimeError(f"Recebimentos PIX falhou | data_ref={data_ref} | status={r.status_code} | body={r.text}")

    data = r.json()

    if isinstance(data, list):
        return pd.json_normalize(data)

    if isinstance(data, dict):
        for key in ("itens", "items", "content", "data", "result", "recebimentos"):
            v = data.get(key)
            if isinstance(v, list):
                return pd.json_normalize(v)
        return pd.json_normalize([data])

    raise ValueError(f"Formato inesperado de resposta: {data}")

def recebimentos_pix_periodo(
        access_token: str
        , data_inicio: str
        , data_fim: str
        , mark: str
        , base_url: str = BASE_URL
        , timeout: int = 30
        ):
    
    ini = datetime.strptime(data_inicio, "%Y-%m-%d").date()
    fim = datetime.strptime(data_fim, "%Y-%m-%d").date()

    dfs, erros = [], []
    d = ini
    while d <= fim:
        print(f"Dados de {d} | Marca {mark}") 
        data_ref = d.strftime("%Y-%m-%d")
        try:
            df_dia = recebimentos_pix_elosgate(
                access_token=access_token
                , data_ref=data_ref
                , base_url=base_url
                , timeout=timeout
                )
            
            df_dia.insert(0, "data_ref", data_ref)
            df_dia.insert(0, "marca", mark)  
            dfs.append(df_dia)

        except Exception as e:
            erros.append({"marca": mark, "data_ref": data_ref, "erro": str(e)})
        d += timedelta(days=1)

    if erros:
        write_log(f"ALERTA | marca={mark} | falhas_em_datas={len(erros)} | exemplo={erros[0]['data_ref']}")

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def delete_e_inserir_pix_por_intervalo_e_marca(
            engine
        , df: pd.DataFrame
        , schema: str
        , table: str = "elosgate_recebimentos_pix"
        ):
    if df.empty:
        return 0

    for c in ("data_ref", "marca"):
        if c not in df.columns:
            raise ValueError(f"DF precisa conter a coluna '{c}'.")

    df = df[df["data_ref"].notna() & df["marca"].notna()].copy()
    df["data_ref"] = df["data_ref"].astype(str)

    mark = str(df["marca"].iloc[0])
    data_min = df["data_ref"].min()
    data_max = df["data_ref"].max()

    delete_sql = text(f"""
        DELETE FROM "{schema}"."{table}"
        WHERE marca = :mark
          AND CAST(data_ref AS date) BETWEEN CAST(:data_min AS date) AND CAST(:data_max AS date)
    """)

    with engine.begin() as conn:
        conn.execute(delete_sql, {"mark": mark, "data_min": data_min, "data_max": data_max})

    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=5000,
        method="multi"
    )

    return df.shape[0]

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    pool_pre_ping=True
)

for section in filiais.sections():
    MARK = filiais.get(section, "mark", fallback=section)
    CLIENT = filiais.get(section, "client", fallback=None)
    SECRET = filiais.get(section, "secret", fallback=None)

    if not CLIENT or not SECRET:
        write_log(f"ERRO | marca={MARK} | config incompleto (client/secret ausente) | section={section}")
        continue

    try:
        token = get_token_elosgate(client_id=CLIENT, client_secret=SECRET).get("access_token")
        if not token:
            raise RuntimeError("Resposta do login sem access_token")

        df_pix = recebimentos_pix_periodo(
            access_token=token,
            data_inicio=DATA_INICIO,
            data_fim=DATA_FIM,
            mark=MARK
        )

        linhas = delete_e_inserir_pix_por_intervalo_e_marca(
            engine=engine,
            df=df_pix,
            schema=SCHEMA,
            table="elosgate_recebimentos_pix"
        )

        write_log(f"SUCESSO | marca={MARK} | linhas={linhas} | periodo={DATA_INICIO} -> {DATA_FIM}")

    except Exception as e:
        write_log(f"ERRO | marca={MARK} | {type(e).__name__}: {e}")
        continue

# %%