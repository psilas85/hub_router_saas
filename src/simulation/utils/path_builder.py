#hub_router_1.0.1/src/simulation/utils/path_builder.py

import os

def build_output_path(base_dir, tenant_id, envio_data, tipo):
    """
    tipo: maps | tables | graphs | reports
    """
    path = os.path.join(
        base_dir,
        tenant_id,
        str(envio_data),
        tipo
    )
    os.makedirs(path, exist_ok=True)
    return path