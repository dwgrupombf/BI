@echo off
title RPA Rede - Iniciando todos os robos
cd /d D:\www\RPA_Rede

echo ============================================================
echo  Iniciando todos os RPAs...
echo ============================================================
echo.

echo [1/5] Iniciando listar_estabelecimentos.py...
start "RPA - Listar Estabelecimentos" cmd /k "cd /d E:\BI\scripts\Rede_RPA && python rpa_listar_estabelecimentos.py"
timeout /t 3 /nobreak >nul

echo [2/5] Iniciando rpa_taxas.py...
start "RPA - Taxas" cmd /k "cd /d E:\BI\scripts\Rede_RPA && python rpa_taxas.py"
timeout /t 3 /nobreak >nul

echo [3/5] Iniciando rpa_vendas.py...
start "RPA - Vendas (~4h)" cmd /k "cd /d E:\BI\scripts\Rede_RPA && python rpa_vendas.py"
timeout /t 3 /nobreak >nul

echo [4/5] Iniciando rpa_recebidos.py...
start "RPA - Recebidos (~4h)" cmd /k "cd /d E:\BI\scripts\Rede_RPA && python rpa_recebidos.py"
timeout /t 3 /nobreak >nul

echo [5/5] Iniciando rpa_a_receber.py...
start "RPA - A Receber (~24h)" cmd /k "cd /d E:\BI\scripts\Rede_RPA && python rpa_a_receber.py"

echo.
echo ============================================================
echo  Todos os RPAs foram iniciados!
echo  Cada um roda em sua propria janela.
echo.
echo  Previsao de conclusao:
echo  - Listar Estabelecimentos: ~5 min
echo  - Taxas:                   ~15 min
echo  - Vendas:                  ~4h
echo  - Recebidos:               ~4h
echo  - A Receber:               ~24h
echo ============================================================
echo.
pause