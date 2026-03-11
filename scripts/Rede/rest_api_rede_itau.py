#%%
import requests
from pandas import json_normalize, DataFrame, concat, set_option
set_option('display.max_columns', None)

''' Sandbox '''
# PV = "0817b0c0-d038-4868-a0ec-d4382919ce41"
# KEY = "8Padm9J6Kf"
# BASE_URL = "https://rl7-sandbox-api.useredecloud.com.br"

''' Produção '''
PV = "102863458"
KEY = "563854059a144f7bae0b9f2376e034ad"
BASE_URL = "https://api.userede.com.br/redelabs"

def get_token(url_token: str, client_id: str, client_secret: str, timeout: int = 30):
    
    url = url_token.rstrip("/") + "/oauth2/token"

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = {
        "grant_type": "client_credentials"
    }
    
    resp = requests.post(
        url,
        headers=headers,
        data=data,
        auth=(client_id, client_secret), # Obs.: Basic Auth (requests monta Authorization: Basic base64(id:secret))
        timeout=timeout
    )

    resp.raise_for_status()
    return resp.json()

token_json = get_token(
    url_token=BASE_URL,
    client_id=PV,
    client_secret=KEY,
)

access_token = token_json["access_token"]

access_token


#%%

def consultar_vendas(
    base_url: str,
    access_token: str,
    parentCompanyNumber: str,
    subsidiaries: str,
    startDate: str,   
    endDate: str,   
    timeout: int = 30
):

    url = base_url.rstrip("/") + "/merchant-statement/v1/sales"

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}",
    }

    params = {
        "parentCompanyNumber": parentCompanyNumber,
        "subsidiaries": subsidiaries,
        "startDate": startDate,
        "endDate": endDate,
    }

    r = requests.get(url, headers=headers, params=params, timeout=timeout)

    if not r.ok:
        print("STATUS:", r.status_code)
        print("BODY:", r.text)

    r.raise_for_status()
    data = r.json()

    # Normalmente vem assim: {"content": {"transactions": [ ... ]}}
    transactions = (data.get("content") or {}).get("transactions")

    # fallback: se a API mudar e já retornar lista direto
    if transactions is None and isinstance(data, list):
        transactions = data

    if not isinstance(transactions, list):
        raise ValueError(f"Resposta inesperada: {data}")

    df = json_normalize(transactions)

    df.insert(0, "parentCompanyNumber", parentCompanyNumber)
    df.insert(1, "subsidiaries", subsidiaries)
    df.insert(2, "startDate", startDate)
    df.insert(3, "endDate", endDate)

    return df

df = consultar_vendas(
    base_url=BASE_URL,
    access_token=access_token,
    parentCompanyNumber=PV,
    subsidiaries=PV,
    startDate="2025-12-01",
    endDate="2025-12-31"
)

df

# %%

def explode_tracking(df: DataFrame, tracking_col: str = "tracking"):
    df2 = df.copy()

    # garante que tracking é lista (se vier None, vira lista vazia)
    df2[tracking_col] = df2[tracking_col].apply(lambda x: x if isinstance(x, list) else [])

    # 1) explode: cada item da lista vira uma linha
    df2 = df2.explode(tracking_col, ignore_index=True)

    # 2) normaliza o dict de tracking em colunas
    tracking_norm = json_normalize(df2[tracking_col])

    # 3) remove a coluna original e concatena as novas colunas
    df2 = df2.drop(columns=[tracking_col]).reset_index(drop=True)
    df_final = concat([df2, tracking_norm], axis=1)

    return df_final


df_tracking = explode_tracking(df)
df_tracking.head()

#%%


df_tracking