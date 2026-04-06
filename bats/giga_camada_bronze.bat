@echo off
set "KNIME_EXE="E:\KNIME\knime.exe""
set "WF_DIR=E:\BI\knime\camada_bronze_giga"

"%KNIME_EXE%" ^
  -nosave ^
  -consoleLog ^
  -nosplash ^
  -reset ^
  -application org.knime.product.KNIME_BATCH_APPLICATION ^
  -workflowDir="%WF_DIR%"
