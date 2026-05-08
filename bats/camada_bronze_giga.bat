@echo off

set "KNIME_EXE=E:\KNIME\knime.exe"
set "WF_DIR=E:\BI\knime\giga"
set "PY_SCRIPT=E:\BI\scripts\Giga\refresh_giga_dataflow.py"

"%KNIME_EXE%" ^
  -nosave ^
  -consoleLog ^
  -nosplash ^
  -reset ^
  -application org.knime.product.KNIME_BATCH_APPLICATION ^
  -workflowDir="%WF_DIR%"

REM python "%PY_SCRIPT%"

