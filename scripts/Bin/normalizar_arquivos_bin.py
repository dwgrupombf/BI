#%%
import json
import configparser
from pathlib import Path
from sqlalchemy import create_engine, text
import pandas as pd

DW_CONFIG_PATH = Path(r"E:\BI\config\config_datalake.ini")

dw = configparser.ConfigParser()
dw.read(DW_CONFIG_PATH, encoding="utf-8")

PG_HOST = dw.get("auth", "host", fallback=None)
PG_PORT = dw.get("auth", "port", fallback=None)
PG_DB = dw.get("auth", "db", fallback=None)
PG_USER = dw.get("auth", "user", fallback=None)
PG_PASS = dw.get("auth", "pwd", fallback=None)
SCHEMA = dw.get("auth", "schema", fallback="datalake")

PASTA_JSON = Path(r"E:\BI\scripts\Bin\Json")

METADATA_COLS = [
    "nome_arquivo",
    "tipo_edi",
    "lote",
    "adquirente",
    "cnpj",
    "estabelecimento",
    "data_arquivo",
]


EDI_CONFIG = {
    "PIX": {
        "glob": "EDI-PIX-*.json",
        "dicts": {
            "fileHeader": "bin_edi_pix_file_header",
        },
        "lists": {
            "clientHeaders": "bin_edi_pix_client_headers",
            "pixTransactions": "bin_edi_pix_transactions",
            "pixPSPTransactions": "bin_edi_pix_psp_transactions",
        },
        "special": None,
    },
    "P": {
        "glob": "EDI-P-*.json",
        "dicts": {
            "fileHeader": "bin_edi_p_file_header",
            "financeFileTrailer": "bin_edi_p_finance_file_trailer",
        },
        "lists": {
            "clientHeaders": "bin_edi_p_client_headers",
            "debitFinanceSummary": "bin_edi_p_debit_finance_summary",
            "creditFinanceSummary": "bin_edi_p_credit_finance_summary",
            "installmentFinanceSummary": "bin_edi_p_installment_finance_summary",
            "financeSuspendedTransactions": "bin_edi_p_finance_suspended_transactions",
            "intraCountSummary": "bin_edi_p_intra_count_summary",
            "financeAdjustments": "bin_edi_p_finance_adjustments",
            "chargebackReceipt": "bin_edi_p_chargeback_receipt",
            "financeSummaryAdvancement": "bin_edi_p_finance_summary_advancement",
            "financeAdvancementFileTrailer": "bin_edi_p_finance_advancement_file_trailer",
            "financeVoucherFlagsSummary": "bin_edi_p_finance_voucher_flags_summary",
            "financeClientFileTrailers": "bin_edi_p_finance_client_file_trailers",
        },
        "special": None,
    },
    "R": {
        "glob": "EDI-R-*.json",
        "dicts": {
            "fileHeader": "bin_edi_r_file_header",
            "fileTrailerRU": "bin_edi_r_file_trailer_ru",
        },
        "lists": {
            "clientHeaders": "bin_edi_r_client_headers",
            "receivableUnitTrailers": "bin_edi_r_receivable_unit_trailers",
        },
        "special": "R",
    },
    "S": {
        "glob": "EDI-S-*.json",
        "dicts": {
            "fileHeader": "bin_edi_s_file_header",
            "finalFileTrailer": "bin_edi_s_final_file_trailer",
        },
        "lists": {
            "clientHeaders": "bin_edi_s_client_headers",
            "debitSalesSummary": "bin_edi_s_debit_sales_summary",
            "salesInstallmentTransaction": "bin_edi_s_sales_installment_transaction",
            "salesVoucherFlagsTransaction": "bin_edi_s_sales_voucher_flags_transaction",
            "salesInstallmentAcceleration": "bin_edi_s_sales_installment_acceleration",
            "salesCancelInfo": "bin_edi_s_sales_cancel_info",
            "salesChargeBackTransactions": "bin_edi_s_sales_chargeback_transactions",
            "clientFileTrailers": "bin_edi_s_client_file_trailers",
        },
        "special": "S",
    },
    "VOUCHER": {
        "glob": "EDI-VOUCHER-*.json",
        "dicts": {
            "fileHeader": "bin_edi_voucher_file_header",
            "fileTrailer": "bin_edi_voucher_file_trailer",
        },
        "lists": {
            "clientHeaders": "bin_edi_voucher_client_headers",
            "vanTransactions": "bin_edi_voucher_van_transactions",
            "clientTrailers": "bin_edi_voucher_client_trailers",
        },
        "special": None,
    },
}

def extrair_metadados_nome(nome_arquivo: str) -> dict:
    partes = nome_arquivo.replace(".json", "").split("-")

    meta = {
        "nome_arquivo": nome_arquivo,
        "tipo_edi": None,
        "lote": None,
        "adquirente": None,
        "cnpj": None,
        "estabelecimento": None,
        "data": None,
    }

    if len(partes) >= 7 and partes[0] == "EDI":
        meta["tipo_edi"] = partes[1]
        meta["lote"] = partes[2]
        meta["adquirente"] = partes[3]
        meta["cnpj"] = partes[4]
        meta["estabelecimento"] = partes[5]

        data_raw = partes[6]

        try:
            meta["data"] = pd.to_datetime(data_raw[:8], format="%Y%m%d").date()
        except Exception:
            meta["data"] = None

    return meta

def normalize_dict_only_metadata(conteudo_dict: dict, meta: dict):
    if not conteudo_dict:
        return pd.DataFrame()

    df = pd.json_normalize(conteudo_dict, sep="_")
    for k, v in meta.items():
        df[k] = v
    return df


def explode_dict_column_only_nested(df: pd.DataFrame, col: str):
    if col not in df.columns:
        return pd.DataFrame()

    cols_keep = [c for c in METADATA_COLS if c in df.columns]
    cols_keep.append(col)

    tmp = df[cols_keep].copy()

    def preparar(x):
        if isinstance(x, list) and len(x) > 0:
            return x
        return [None]

    tmp[col] = tmp[col].apply(preparar)
    tmp = tmp.explode(col, ignore_index=True)

    mask = tmp[col].notna()

    detalhe = pd.DataFrame(index=tmp.index)
    if mask.any():
        detalhe_valid = pd.json_normalize(tmp.loc[mask, col], sep="_")
        detalhe_valid.index = tmp.loc[mask].index
        detalhe = detalhe.combine_first(detalhe_valid)

    base = tmp.drop(columns=[col])

    return pd.concat([base, detalhe], axis=1)


def serializar_colunas_complexas(df: pd.DataFrame):
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
        print(f"DataFrame vazio, ignorado: {schema}.{tabela}")
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
        print(f"Sem nome_arquivo para deletar/inserir em {schema}.{tabela}")
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


def juntar(lista):
    return pd.concat(lista, ignore_index=True, sort=False) if lista else pd.DataFrame()

def processar_edi_generico(tipo: str, pasta: Path):
    cfg = EDI_CONFIG[tipo]
    arquivos = sorted(pasta.glob(cfg["glob"]))

    print(f"Total de arquivos {tipo}: {len(arquivos)}")

    acumuladores = {tabela: [] for tabela in list(cfg["dicts"].values()) + list(cfg["lists"].values())}

    for i, arquivo in enumerate(arquivos, start=1):
        try:
            with open(arquivo, "r", encoding="utf-8") as f:
                conteudo = json.load(f)

            meta = extrair_metadados_nome(arquivo.name)

            # dicts
            for campo_json, nome_tabela in cfg["dicts"].items():
                df_tmp = normalize_dict_only_metadata(conteudo.get(campo_json, {}), meta)
                if not df_tmp.empty:
                    acumuladores[nome_tabela].append(df_tmp)

            # listas raiz
            df_base_tmp = pd.DataFrame([conteudo])
            for k, v in meta.items():
                df_base_tmp[k] = v

            for campo_json, nome_tabela in cfg["lists"].items():
                df_tmp = explode_dict_column_only_nested(df_base_tmp, campo_json)
                if not df_tmp.empty:
                    acumuladores[nome_tabela].append(df_tmp)

            if i % 20 == 0 or i == len(arquivos):
                print(f"{tipo}: processados {i}/{len(arquivos)}")

        except Exception as e:
            print(f"Erro ao ler {arquivo.name}: {e}")

    return {nome_tabela: juntar(lista) for nome_tabela, lista in acumuladores.items()}

def processar_edi_r_especial(pasta: Path):
    arquivos = sorted(pasta.glob(EDI_CONFIG["R"]["glob"]))

    dfs_receivable_units = []
    dfs_payment_ru = []
    dfs_contracts_negotiated_ru = []
    dfs_detail_events_deductions_ru = []

    for arquivo in arquivos:
        try:
            with open(arquivo, "r", encoding="utf-8") as f:
                conteudo = json.load(f)

            meta = extrair_metadados_nome(arquivo.name)

            receivable_units = conteudo.get("receivableUnits", [])
            if receivable_units:
                for ru in receivable_units:
                    ru_base = dict(ru)
                    payment_ru = ru_base.pop("paymentRU", [])
                    contracts_negotiated_ru = ru_base.pop("contractsNegotiatedRU", [])
                    detail_events_deductions_ru = ru_base.pop("detailEventsDeductionsRU", [])

                    df_ru = pd.json_normalize(ru_base, sep="_")
                    for k, v in meta.items():
                        df_ru[k] = v
                    dfs_receivable_units.append(df_ru)

                    if isinstance(payment_ru, list) and payment_ru:
                        df_payment = pd.json_normalize(payment_ru, sep="_")
                        df_payment["idUr_pai"] = ru.get("idUr")
                        df_payment["receivableUnitKey_pai"] = ru.get("receivableUnitKey")
                        for k, v in meta.items():
                            df_payment[k] = v
                        dfs_payment_ru.append(df_payment)

                    if isinstance(contracts_negotiated_ru, list) and contracts_negotiated_ru:
                        df_contracts = pd.json_normalize(contracts_negotiated_ru, sep="_")
                        df_contracts["idUr_pai"] = ru.get("idUr")
                        df_contracts["receivableUnitKey_pai"] = ru.get("receivableUnitKey")
                        for k, v in meta.items():
                            df_contracts[k] = v
                        dfs_contracts_negotiated_ru.append(df_contracts)

                    if isinstance(detail_events_deductions_ru, list) and detail_events_deductions_ru:
                        df_detail = pd.json_normalize(detail_events_deductions_ru, sep="_")
                        df_detail["idUr_pai"] = ru.get("idUr")
                        df_detail["receivableUnitKey_pai"] = ru.get("receivableUnitKey")
                        for k, v in meta.items():
                            df_detail[k] = v
                        dfs_detail_events_deductions_ru.append(df_detail)

        except Exception as e:
            print(f"Erro R especial ao ler {arquivo.name}: {e}")

    return {
        "bin_edi_r_receivable_units": juntar(dfs_receivable_units),
        "bin_edi_r_payment_ru": juntar(dfs_payment_ru),
        "bin_edi_r_contracts_negotiated_ru": juntar(dfs_contracts_negotiated_ru),
        "bin_edi_r_detail_events_deductions_ru": juntar(dfs_detail_events_deductions_ru),
    }

def processar_edi_s_especial(pasta: Path):
    arquivos = sorted(pasta.glob(EDI_CONFIG["S"]["glob"]))

    dfs_credit_sales_summary = []
    dfs_credit_sales_receipt = []

    for arquivo in arquivos:
        try:
            with open(arquivo, "r", encoding="utf-8") as f:
                conteudo = json.load(f)

            meta = extrair_metadados_nome(arquivo.name)

            credit_sales_summary = conteudo.get("creditSalesSummary", [])
            if isinstance(credit_sales_summary, list) and credit_sales_summary:
                for css in credit_sales_summary:
                    css_base = dict(css)
                    credit_sales_receipt = css_base.pop("creditSalesReceipt", [])

                    df_css = pd.json_normalize(css_base, sep="_")
                    for k, v in meta.items():
                        df_css[k] = v
                    dfs_credit_sales_summary.append(df_css)

                    if isinstance(credit_sales_receipt, list) and credit_sales_receipt:
                        df_receipt = pd.json_normalize(credit_sales_receipt, sep="_")
                        df_receipt["salesSummaryNumber_pai"] = css.get("salesSummaryNumber")
                        df_receipt["salesDate_pai"] = css.get("salesDate")
                        for k, v in meta.items():
                            df_receipt[k] = v
                        dfs_credit_sales_receipt.append(df_receipt)

        except Exception as e:
            print(f"Erro S especial ao ler {arquivo.name}: {e}")

    return {
        "bin_edi_s_credit_sales_summary": juntar(dfs_credit_sales_summary),
        "bin_edi_s_credit_sales_receipt": juntar(dfs_credit_sales_receipt),
    }


def executar_tipo(tipo: str, engine, pasta: Path):
    print(f"\n=== PROCESSANDO {tipo} ===")

    resultado = processar_edi_generico(tipo, pasta)

    if EDI_CONFIG[tipo]["special"] == "R":
        resultado.update(processar_edi_r_especial(pasta))

    if EDI_CONFIG[tipo]["special"] == "S":
        resultado.update(processar_edi_s_especial(pasta))

    for nome_tabela, df in resultado.items():
        qtd = delete_insert_por_nome_arquivo(engine, SCHEMA, nome_tabela, df)
        print(f"Tabela carregada: {SCHEMA}.{nome_tabela} -> {qtd} linhas")


if __name__ == "__main__":
    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}",
        pool_pre_ping=True
    )

    for tipo in ["PIX", "P", "R", "S", "VOUCHER"]:
        executar_tipo(tipo, engine, PASTA_JSON)

# %%