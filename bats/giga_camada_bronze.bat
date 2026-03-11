@echo off
setlocal

set "KNIME_EXE=C:\Users\Wiser\AppData\Local\Programs\KNIME\knime.exe"
set "WF_DIR=E:\BI\knime\camada_bronze_GIGA"

"%KNIME_EXE%" ^
  -nosplash ^
  -consoleLog ^
  -application org.knime.product.KNIME_BATCH_APPLICATION ^
  -workflowDir="%WF_DIR%" ^
  -reset

echo ExitCode=%ERRORLEVEL%
exit /b %ERRORLEVEL%