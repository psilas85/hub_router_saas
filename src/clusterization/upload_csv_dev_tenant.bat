@echo off
set CSV_PATH=D:\Users\psila\Documents\cluster_router\clusterization\tenants\dev_tenant\input\dados_input.csv
set TENANT_ID=dev_tenant
python upload_csv_cli.py --csv "%CSV_PATH%" --tenant %TENANT_ID%
pause
