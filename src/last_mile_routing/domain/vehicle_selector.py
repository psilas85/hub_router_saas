def selecionar_veiculo(peso_total, custos_veiculos, centro_cidade, cidades, peso_leve_max, restricao_veiculo_leve_municipio):
    """
    Seleciona o veículo adequado baseado no peso total, na restrição de município
    e nos dados do banco.
    """

    # 🔧 Normaliza os nomes
    faixas = [
        {**f, "veiculo": f["veiculo"].strip().capitalize()}
        for f in custos_veiculos
    ]

    # 🚦 Classifica veículos como leves ou pesados
    faixas_leves = [f for f in faixas if f["peso_maximo_kg"] <= peso_leve_max]
    faixas_pesados = [f for f in faixas if f["peso_minimo_kg"] > peso_leve_max]

    entregas_fora_do_municipio = any(cidade != centro_cidade for cidade in cidades)

    if restricao_veiculo_leve_municipio and entregas_fora_do_municipio:
        # 🚫 Só permite veículos pesados
        for faixa in faixas_pesados:
            if faixa["peso_minimo_kg"] <= peso_total <= faixa["peso_maximo_kg"]:
                return faixa["veiculo"]
        return faixas_pesados[-1]["veiculo"]  # fallback
    else:
        # ✔️ Permite qualquer, priorizando o mais leve possível
        for faixa in faixas:
            if faixa["peso_minimo_kg"] <= peso_total <= faixa["peso_maximo_kg"]:
                return faixa["veiculo"]
        return faixas[-1]["veiculo"]  # fallback
