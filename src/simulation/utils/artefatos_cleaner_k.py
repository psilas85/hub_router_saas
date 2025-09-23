import os

def limpar_artefatos_k(output_dir: str, tenant_id: str, data_inicial: str, data_final: str, logger=None):
    """
    Remove somente arquivos de k_fixo de um tenant
    que contenham a data inicial, final ou a combinação inicial_final no nome.
    """
    output_path = os.path.join(output_dir, tenant_id)
    if not os.path.isdir(output_path):
        return

    padroes = [data_inicial, data_final, f"{data_inicial}_{data_final}"]

    for f in os.listdir(output_path):
        if f.startswith("k_fixo") and any(p in f for p in padroes):
            try:
                os.remove(os.path.join(output_path, f))
                msg = f"🗑️ Removido artefato de k_fixo: {f}"
                print(msg)
                if logger:
                    logger.info(msg)
            except Exception as e:
                msg = f"⚠️ Erro ao remover {f}: {e}"
                print(msg)
                if logger:
                    logger.error(msg)
