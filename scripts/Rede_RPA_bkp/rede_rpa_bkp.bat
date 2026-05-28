@echo off
setlocal

REM Pasta de origem
set "ORIGEM=E:\RPA\RPA_Rede_2_0\downloads"

REM Pasta de destino
set "DESTINO=E:\BI\bkp\rede_rpa"

REM Pasta de log
set "LOG_DIR=E:\BI\logs\bkp_rede_rpa"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%DESTINO%" mkdir "%DESTINO%"

set "LOG_FILE=%LOG_DIR%\copia_arquivos.log"

echo ============================================== >> "%LOG_FILE%"
echo Inicio da copia: %date% %time% >> "%LOG_FILE%"
echo Origem: %ORIGEM% >> "%LOG_FILE%"
echo Destino: %DESTINO% >> "%LOG_FILE%"
echo ============================================== >> "%LOG_FILE%"

robocopy "%ORIGEM%" "%DESTINO%" *.* /E /XC /XN /XO /R:3 /W:5 /LOG+:"%LOG_FILE%" /TEE

echo Fim da copia: %date% %time% >> "%LOG_FILE%"
echo. >> "%LOG_FILE%"

endlocal