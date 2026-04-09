#%%

from estabelecimentos import get_usuarios, ContaEstabelecimento
import requests
import json
import math
import pandas as pd
pd.set_option("display.max_columns", 400)

from pandas import json_normalize
from datetime import datetime, date
import configparser
from pathlib import Path
import calendar
from sqlalchemy import create_engine, text, inspect

# =========================
# INPUT (dinâmico)
# =========================
# formato: "dd-mm-aaaa"
DATA_INICIAL = "01-01-2026"
DATA_FINAL   = "01-12-2026"

# =========================
# CONFIG API REDE
# =========================
BASE_URL = "https://api.userede.com.br/redelabs"

# MANTENHA AQUI OS MESMOS VALORES QUE VOCÊ JÁ USA HOJE
CLIENT_ID = "b7d9801d-098b-4ecd-b054-03500c9c50fa"
CLIENT_SECRET = "23i4HnQDaR"

# =========================
# CONFIG DW (Postgres)
# =========================
DW_CONFIG_PATH = Path(r"E:\BI\config\config_datalake.ini")
dw = configparser.ConfigParser()
dw.read(DW_CONFIG_PATH, encoding="utf-8")

PG_HOST = dw.get("auth", "host", fallback=None)
PG_PORT = dw.get("auth", "port", fallback=None)
PG_DB   = dw.get("auth", "db", fallback=None)
PG_USER = dw.get("auth", "user", fallback=None)
PG_PASS = dw.get("auth", "pwd", fallback=None)
SCHEMA  = dw.get("auth", "schema", fallback="datalake")

TBL_VENDAS     = "rede_vendas"
TBL_RECEBIDOS  = "rede_recebidos"
TBL_RECEBIVEIS = "rede_recebiveis"

# =========================
# TOKEN
# =========================
def gerar_token(username: str, password: str) -> str:
    url = f"{BASE_URL}/oauth/token"

    response = requests.post(
        url,
        auth=(CLIENT_ID, CLIENT_SECRET),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "password",
            "username": username,
            "password": password
        },
        timeout=30
    )
    response.raise_for_status()
    return response.json()["access_token"]

def parse_ddmmyyyy(s: str) -> date:
    return datetime.strptime(s, "%d-%m-%Y").date()

def month_periods(data_ini_ddmmyyyy: str, data_fim_ddmmyyyy: str):

    ini = parse_ddmmyyyy(data_ini_ddmmyyyy)
    fim = parse_ddmmyyyy(data_fim_ddmmyyyy)

    cur = ini.replace(day=1)
    end = fim.replace(day=1)

    out = []
    while cur <= end:
        last_day = calendar.monthrange(cur.year, cur.month)[1]
        start_date = cur.strftime("%Y-%m-%d")
        end_date = cur.replace(day=last_day).strftime("%Y-%m-%d")
        ano_mes = cur.strftime("%Y-%m")
        out.append((start_date, end_date, ano_mes))

        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1, day=1)
        else:
            cur = cur.replace(month=cur.month + 1, day=1)

    return out

# =========================
# REQUEST BASE
# =========================
def fazer_get(url: str, params: dict, auth_state: dict, extra_headers: dict | None = None):
    headers = {"Accept": "application/json", **(extra_headers or {})}
    headers["Authorization"] = f"Bearer {auth_state['token']}"

    resp = requests.get(url, headers=headers, params=params, timeout=30)

    if resp.status_code == 401:
        print(f"Token expirado para {auth_state['username']}. Renovando token e repetindo requisição...")

        auth_state["token"] = gerar_token(
                                    auth_state["username"],
                                    auth_state["password"]
                                )
        headers["Authorization"] = f"Bearer {auth_state['token']}"

        resp = requests.get(url, headers=headers, params=params, timeout=30)

        if resp.status_code == 401:
            print(f"Token renovado para {auth_state['username']}, mas a API continuou retornando 401.")

    return resp

# =========================
# NORMALIZAÇÃO
# =========================
def normalizar_lista(
    lista: list,
    origem: str,
    ano_mes: str,
    conta: ContaEstabelecimento
) -> pd.DataFrame:
    if not lista:
        return pd.DataFrame()

    df = json_normalize(lista, sep="_")
    df["origem"] = origem
    df["ano_mes"] = ano_mes
    df["pv"] = str(conta.pv)
    df["subsidiary"] = str(conta.subsidiary)
    df["usuario_api"] = conta.email
    df["data_carga"] = datetime.now().replace(microsecond=0)
    return df

def serializar_colunas_complexas(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    df = df.copy()

    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: json.dumps(x, ensure_ascii=False)
            if isinstance(x, (dict, list))
            else x
        )

    return df

def limpar_valores_invalidos(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    df = df.copy()
    df = df.replace({pd.NA: None})

    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: None if isinstance(x, float) and (math.isnan(x) or math.isinf(x)) else x
        )

    return df

# =========================
# CONSULTAS API
# =========================
def consultar_vendas_mes(
    auth_state: dict,
    start_date: str,
    end_date: str,
    conta: ContaEstabelecimento
):
    url = f"{BASE_URL}/merchant-statement/v1/sales"
    params = {
        "parentCompanyNumber": conta.pv,
        "subsidiaries": conta.subsidiary,
        "startDate": start_date,
        "endDate": end_date,
        "size": 50
    }

    resp = fazer_get(
        url=url,
        params=params,
        auth_state=auth_state
    )

    if resp.status_code != 200:
        return [], {
            "tipo": "sales",
            "status_http": resp.status_code,
            "resposta": resp.text
        }

    data = resp.json()
    transactions = data.get("content", {}).get("transactions", [])
    return transactions, None

def consultar_recebidos_mes(
    auth_state: dict,
    start_date: str,
    end_date: str,
    conta: ContaEstabelecimento
):
    url = f"{BASE_URL}/merchant-statement/v1/payments"
    params = {
        "parentCompanyNumber": conta.pv,
        "subsidiaries": conta.subsidiary,
        "startDate": start_date,
        "endDate": end_date,
        "size": 50
    }

    resp = fazer_get(
        url=url,
        params=params,
        auth_state=auth_state
    )

    if resp.status_code != 200:
        return [], {
            "tipo": "payments",
            "status_http": resp.status_code,
            "resposta": resp.text
        }

    data = resp.json()
    payments = data.get("content", {}).get("payments", [])
    return payments, None

def consultar_recebiveis_mes(
    auth_state: dict,
    start_date: str,
    end_date: str,
    conta: ContaEstabelecimento
):
    url = f"{BASE_URL}/merchant-statement/v1/receivables/daily"
    params = {
        "startDate": start_date,
        "endDate": end_date,
        "size": 50
    }

    resp = fazer_get(
        url=url,
        params=params,
        auth_state=auth_state,
        extra_headers={
            "merchant-id": str(conta.pv),
            "Content-Type": "application/json"
        }
    )

    if resp.status_code != 200:
        return [], {
            "tipo": "receivables",
            "status_http": resp.status_code,
            "resposta": resp.text
        }

    data = resp.json()

    if "content" in data and isinstance(data["content"], dict):
        for _, valor in data["content"].items():
            if isinstance(valor, list):
                return valor, None
        return [data["content"]], None

    for _, valor in data.items():
        if isinstance(valor, list):
            return valor, None

    return [data], None

# =========================
# DW: APOIO
# =========================
def tabela_existe(engine, schema: str, table: str) -> bool:
    insp = inspect(engine)
    return insp.has_table(table, schema=schema)

def delete_mes_pv(engine, schema: str, table: str, ano_mes: str, pv: str) -> int:

    sql = text(f'''
        DELETE FROM "{schema}"."{table}"
        WHERE ano_mes = :ano_mes
          AND pv = :pv
    ''')

    with engine.begin() as conn:
        result = conn.execute(sql, {"ano_mes": ano_mes, "pv": str(pv)})
        return result.rowcount if result.rowcount is not None else 0

def insert_mes(engine, schema: str, table: str, df: pd.DataFrame):
    if df is None or df.empty:
        return 0

    df = serializar_colunas_complexas(df)
    df = limpar_valores_invalidos(df)

    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=5000,
        method="multi"
    )

    return len(df)

def processar_carga_mes(
    engine,
    schema: str,
    table: str,
    auth_state: dict,
    conta: ContaEstabelecimento,
    start_date: str,
    end_date: str,
    ano_mes: str,
    nome_logico: str,
    origem_normalizacao: str,
    func_consulta,
    logs: list
):
    try:
        lista, erro = func_consulta(auth_state, start_date, end_date, conta)

        if erro:
            logs.append({
                "etapa": nome_logico,
                "ano_mes": ano_mes,
                "usuario": conta.email,
                "pv": conta.pv,
                "tipo": erro.get("tipo"),
                "status_http": erro.get("status_http"),
                "resposta": erro.get("resposta"),
                "mensagem": "Erro na consulta da API"
            })
            print(f"{nome_logico} | {erro.get("status_http")}: erro na API para {ano_mes} | usuário={conta.email} | pv={conta.pv}")
            return

        df = normalizar_lista(lista, origem_normalizacao, ano_mes, conta)

        if df.empty:
            print(f"{nome_logico}: sem dados em {ano_mes} | usuário={conta.email} | pv={conta.pv}")
            return

        tabela_ja_existe = tabela_existe(engine, schema, table)

        if tabela_ja_existe:
            deletados = delete_mes_pv(engine, schema, table, ano_mes, conta.pv)
            print(f"{nome_logico}: {deletados} registro(s) apagado(s) de {ano_mes} | pv={conta.pv}")
        else:
            print(f"{nome_logico}: tabela {schema}.{table} ainda não existe. Será criada no primeiro insert.")

        inseridos = insert_mes(engine, schema, table, df)
        print(f"{nome_logico}: {inseridos} registro(s) inserido(s) em {ano_mes} | pv={conta.pv}")

    except Exception as e:
        logs.append({
            "etapa": nome_logico,
            "ano_mes": ano_mes,
            "usuario": conta.email,
            "pv": conta.pv,
            "tipo": "exception",
            "mensagem": str(e)
        })
        print(f"{nome_logico}: falhou em {ano_mes} | usuário={conta.email} | pv={conta.pv} -> {e}")


if __name__ == "__main__":
    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}",
        pool_pre_ping=True
    )

    logs = []
    periodos = month_periods(DATA_INICIAL, DATA_FINAL)
    usuarios = get_usuarios()

    for usuario in usuarios:
        print(f"\n######## USUÁRIO: {usuario.email} ########")

        try:
            # token = gerar_token(usuario.email, usuario.senha)
            auth_state = {
                "username": usuario.email,
                "password": usuario.senha,
                "token": gerar_token(usuario.email, usuario.senha)
            }
            print(f"Token inicial gerado com sucesso para {usuario.email}.")
        except Exception as e:
            logs.append({
                "etapa": "TOKEN",
                "usuario": usuario.email,
                "pv": None,
                "tipo": "exception",
                "mensagem": str(e)
            })
            print(f"Falha ao gerar token para {usuario.email}: {e}")
            continue

        for conta in usuario.contas():
            print(f"\n==== ESTABELECIMENTO {conta.pv} ====")

            for start_date, end_date, ano_mes in periodos:
                print(f"\n=== Processando {ano_mes} ({start_date} -> {end_date}) ===")

                processar_carga_mes(
                    engine=engine,
                    schema=SCHEMA,
                    table=TBL_VENDAS,
                    auth_state=auth_state,
                    conta=conta,
                    start_date=start_date,
                    end_date=end_date,
                    ano_mes=ano_mes,
                    nome_logico="VENDAS",
                    origem_normalizacao="sales",
                    func_consulta=consultar_vendas_mes,
                    logs=logs
                )

                processar_carga_mes(
                    engine=engine,
                    schema=SCHEMA,
                    table=TBL_RECEBIDOS,
                    auth_state=auth_state,
                    conta=conta,
                    start_date=start_date,
                    end_date=end_date,
                    ano_mes=ano_mes,
                    nome_logico="RECEBIDOS",
                    origem_normalizacao="payments",
                    func_consulta=consultar_recebidos_mes,
                    logs=logs
                )

                processar_carga_mes(
                    engine=engine,
                    schema=SCHEMA,
                    table=TBL_RECEBIVEIS,
                    auth_state=auth_state,
                    conta=conta,
                    start_date=start_date,
                    end_date=end_date,
                    ano_mes=ano_mes,
                    nome_logico="RECEBÍVEIS",
                    origem_normalizacao="receivables",
                    func_consulta=consultar_recebiveis_mes,
                    logs=logs
                )

    if logs:
        print("\n=== FALHAS REGISTRADAS ===")
        df_logs = pd.DataFrame(logs)
        print(df_logs.head(200))
    else:
        print("\nTudo OK!")
# %%
