from pathlib import Path

base_tenants_dir = Path("tenants")

for tenant_dir in base_tenants_dir.iterdir():
    input_csv = tenant_dir / "input" / "dados_input.csv"
    if input_csv.exists():
        tenant_id = tenant_dir.name
        # Windows
        bat = Path(f"upload_csv_{tenant_id}.bat")
        bat.write_text(f"""@echo off
set CSV_PATH={input_csv.resolve()}
set TENANT_ID={tenant_id}
python upload_csv_cli.py --csv "%CSV_PATH%" --tenant %TENANT_ID%
pause
""", encoding="utf-8")

        # Linux/Mac
        sh = Path(f"upload_csv_{tenant_id}.sh")
        sh.write_text(f"""#!/bin/bash
CSV_PATH="{input_csv.resolve().as_posix()}"
TENANT_ID="{tenant_id}"
python3 upload_csv_cli.py --csv "$CSV_PATH" --tenant "$TENANT_ID"
""", encoding="utf-8")
        print(f"✅ Atalhos criados para {tenant_id}")
