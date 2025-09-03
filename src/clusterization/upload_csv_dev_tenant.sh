#!/bin/bash
CSV_PATH="D:/Users/psila/Documents/cluster_router/clusterization/tenants/dev_tenant/input/dados_input.csv"
TENANT_ID="dev_tenant"
python3 upload_csv_cli.py --csv "$CSV_PATH" --tenant "$TENANT_ID"
