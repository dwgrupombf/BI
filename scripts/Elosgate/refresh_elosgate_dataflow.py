#%%

import requests
import time
from datetime import datetime
import configparser
from urllib.parse import quote_plus

# ============================================================
# CONFIGURAÇÕES
# ============================================================

credential_path = r"E:\BI\config"

WORKSPACE_NAME = "MBF"

DATAFLOWS = [ 
    "elosgate_dimensoes"
    , "elosgate_fatos"
    ]

DATASETS = [ "" ]

SLEEP_BETWEEN_CHECKS = 30 

BASE_URL = "https://api.powerbi.com/v1.0/myorg/"

# ============================================================
# AUTENTICAÇÃO
# ============================================================

config = configparser.ConfigParser()
config.read(fr"{credential_path}\config_pbi.ini")
CLIENT_ID = quote_plus(config["credentials"]["client_id"])
CLIENT_SECRET = quote_plus(config["credentials"]["client_secret"])
TENANT_ID = config["credentials"]["tenant_id"]

def get_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "resource": "https://analysis.windows.net/powerbi/api",
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    return r.json()["access_token"]


def get_headers():
    return {"Authorization": f"Bearer {get_token()}"}


# ============================================================
# WORKSPACE
# ============================================================

def get_workspace_id(headers):
    r = requests.get(BASE_URL + "groups", headers=headers)
    r.raise_for_status()

    for ws in r.json()["value"]:
        if ws["name"] == WORKSPACE_NAME:
            print(f"✓ Workspace encontrado: {WORKSPACE_NAME}")
            return ws["id"]

    raise Exception("Workspace não encontrado")


# ============================================================
# DATAFLOWS
# ============================================================

def refresh_dataflow(headers, workspace_id, dataflow_name, payload):
    r = requests.get(
        BASE_URL + f"groups/{workspace_id}/dataflows", headers=headers
    )
    r.raise_for_status()

    for df in r.json()["value"]:
        if df["name"] == dataflow_name:
            refresh_url = BASE_URL + f"groups/{workspace_id}/dataflows/{df['objectId']}/refreshes"
            rr = requests.post(refresh_url, headers=headers, json=payload)
            rr.raise_for_status()

            print(f"▶ Dataflow iniciado: {dataflow_name}")
            return df["objectId"]

    raise Exception(f"Dataflow '{dataflow_name}' não encontrado")


def wait_dataflow_finish(headers, workspace_id, dataflow_id, name):
    print(f"⏳ Aguardando Dataflow finalizar: {name}")

    while True:
        r = requests.get(
            BASE_URL + f"groups/{workspace_id}/dataflows/{dataflow_id}/transactions",
            headers=headers,
        )
        r.raise_for_status()

        status = r.json()["value"][0]["status"]

        print(f"   Status {name}: {status}")

        if status == "Success":
            print(f"✓ Dataflow finalizado: {name}")
            break
        elif status == "Failed":
            raise Exception(f"Dataflow falhou: {name}")
        else:
            time.sleep(SLEEP_BETWEEN_CHECKS)


# ============================================================
# DATASETS
# ============================================================

def refresh_dataset(headers, workspace_id, dataset_name):
    r = requests.get(
        BASE_URL + f"groups/{workspace_id}/datasets", headers=headers
    )
    r.raise_for_status()

    for ds in r.json()["value"]:
        if ds["name"] == dataset_name:
            if not ds["isRefreshable"]:
                print(f"⚠ Dataset não refreshable: {dataset_name}")
                return

            refresh_url = BASE_URL + f"groups/{workspace_id}/datasets/{ds['id']}/refreshes"
            rr = requests.post(refresh_url, headers=headers)
            rr.raise_for_status()

            print(f"▶ Dataset iniciado: {dataset_name}")
            return

    raise Exception(f"Dataset '{dataset_name}' não encontrado")


# ============================================================
# MAIN
# ============================================================

def main():
    start = time.time()
    print("==============================================")
    print("🔄 Início atualização Power BI")
    print("Data/Hora:", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    print("==============================================\n")

    headers = get_headers()
    workspace_id = get_workspace_id(headers)

    # -------------------------
    # DATAFLOWS
    # -------------------------

    payload = {"notifyOption": "MailOnFailure"}

    for df_name in DATAFLOWS:
        df_id = refresh_dataflow(headers, workspace_id, df_name, payload)
        # wait_dataflow_finish(headers, workspace_id, df_id, df_name)
        print()

    # -------------------------
    # DATASETS
    # -------------------------
    # for ds_name in DATASETS:
    #     refresh_dataset(headers, workspace_id, ds_name)
    #     print()

    end = time.time()
    elapsed = time.strftime("%H:%M:%S", time.gmtime(end - start))

    print("==============================================")
    print(f"✅ Processo concluído com sucesso")
    print(f"⏱ Tempo total: {elapsed}")
    print("==============================================")


if __name__ == "__main__":
    main()




# %%
