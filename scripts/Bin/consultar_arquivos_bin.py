#%%

import subprocess
from datetime import datetime
from pathlib import Path
import configparser

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
SCHEMA = dw.get("auth", "schema", fallback=None)

INI_PATH = Path(r"E:\BI\config\config_bin.ini")
config = configparser.ConfigParser()
config.read(INI_PATH, encoding="utf-8")

host = config.get("auth", "host", fallback=None)
port = config.get("auth", "port", fallback=None)
username = config.get("auth", "user", fallback=None)
password = config.get("auth", "pwd", fallback=None)
private_key = r"E:\BI\config\bin_key_putty.ppk"
hostkey = "*"

hoje_str = datetime.now().strftime("%Y%m%d")

LOCAL_DOWNLOAD_DIR = Path(r"E:\BI\scripts\Bin\Json")
LOCAL_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

winscp_exe = r"C:\Program Files (x86)\WinSCP\WinSCP.com"

tmp_dir = Path(r"E:\BI\scripts\Bin")
tmp_dir.mkdir(parents=True, exist_ok=True)

script_file = tmp_dir / "teste_conexao_sftp.txt"

script_content = f"""open sftp://{username}:{password}@{host}:{port}/ -privatekey="{private_key}" -hostkey="{hostkey}"
                    cd /available
                    ls 
                    exit
                    """

script_file.write_text(script_content, encoding="utf-8")

resultado = subprocess.run(
    [winscp_exe, f"/script={script_file}"],
    capture_output=True,
    text=True,
    encoding="utf-8",
    errors="replace"
)

print("===== STDOUT =====")
print(resultado.stdout)

print("===== RETURN CODE =====")
print(resultado.returncode)



# %%
