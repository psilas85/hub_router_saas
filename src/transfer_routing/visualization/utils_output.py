import os

def caminho_output(tenant_id: str, tipo: str) -> str:
    """
    Retorna o caminho de saída padronizado (maps ou relatorios) para o tenant.
    Cria a pasta se não existir.
    """
    base = os.path.join("output", tenant_id, tipo)
    os.makedirs(base, exist_ok=True)
    return base
