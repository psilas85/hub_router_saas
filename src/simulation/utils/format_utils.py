# simulation/utils/format_utils.py

def formatar(valor, casas=2):
    """
    Formata valores numéricos com casas decimais, retornando 'N/A' se None.
    """
    return f"{valor:.{casas}f}" if valor is not None else "N/A"
