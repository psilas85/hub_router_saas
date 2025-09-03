from clusterization.simulacao_avulsa.simulador_clusterizacao import ClusterSimulationService
import pandas as pd
import os
from datetime import datetime

# Input de teste
input_simulacao = {
    "centros_clusters": [
        "Rua Major Barreto, 1499 - Centro, Itapajé - CE",
        "Rua João Batista Rios, 1929 - Centro, Itarema - CE",
        "Av. Lúcia Saboia, 452 - Centro, Sobral - CE",
        "Rua Romeu Martins, 154 - Centro, Canindé - CE",
        "Avenida Prefeito José Jaques, 1334 - Centro, Tianguá - CE",
        "Rua Sabino Roberto, 2919 - Centro, Limoeiro do Norte - CE",
        "Avenida Plácido Castelo, 1773 - Centro, Quixadá - CE",
        "Rua Monsenhor Furtado, 416 - Centro, Santa Quitéria - CE",
        "Rua Epitácio Pessoa, 66 - Centro, Iguatu - CE",
        "Rua João Crispim, 1705 - Parque Santa Maria, Fortaleza - CE"
    ],
    "data_inicio": "2025-03-02",
    "data_fim": "2025-03-31"
}

servico = ClusterSimulationService()
resultados_por_data = servico.executar_simulacao(input_simulacao)

output_path = os.path.join(os.path.dirname(__file__), "simulacao_output")
os.makedirs(output_path, exist_ok=True)

data_execucao = datetime.now().strftime("%Y%m%d_%H%M%S")

todos_resultados = []

for data_str, df_resultado in resultados_por_data.items():
    print(f"\n📦 Processando resultados do dia {data_str}...")

    df_resultado = df_resultado.loc[:, ~df_resultado.columns.duplicated()]
    if "destino_latitude" in df_resultado.columns and "lat" not in df_resultado.columns:
        df_resultado["lat"] = df_resultado["destino_latitude"]
    if "destino_longitude" in df_resultado.columns and "lon" not in df_resultado.columns:
        df_resultado["lon"] = df_resultado["destino_longitude"]
    for col in ["cluster", "centro_lat", "centro_lon"]:
        if col not in df_resultado.columns:
            df_resultado[col] = None

    if df_resultado.empty:
        print(f"\n⚠️ Nenhuma entrega para {data_str}. Pulando.")
        continue

    falhas_entregas = df_resultado[(df_resultado['lat'].isna()) | (df_resultado['lon'].isna())]
    if not falhas_entregas.empty:
        print(f"\n⚠️ Falha ao geocodificar entregas do dia {data_str}:")
        print(falhas_entregas[['cte_numero', 'cte_cidade', 'cte_cep']])

    falhas_centros = df_resultado[(df_resultado['centro_lat'].isna()) | (df_resultado['centro_lon'].isna())]
    if not falhas_centros.empty:
        print(f"\n⚠️ Falha ao geocodificar centros do dia {data_str}:")
        print(falhas_centros[['cte_numero', 'cluster_endereco']])

    resumo = df_resultado.groupby("cluster").agg({
        "cte_numero": "count",
        "cte_peso": "sum",
        "cte_volumes": "sum",
        "cte_valor_nf": "sum",
        "cte_valor_frete": "sum"
    }).rename(columns={
        "cte_numero": "qtde_entregas",
        "cte_peso": "peso_total",
        "cte_volumes": "volumes_totais",
        "cte_valor_nf": "valor_nf_total",
        "cte_valor_frete": "valor_frete_total"
    })

    print(f"\n📊 Resumo {data_str}:")
    print(resumo)

    todos_resultados.append(df_resultado)

# Consolidado final
df_consolidado = pd.concat(todos_resultados, ignore_index=True)

resumo_geral = df_consolidado.groupby("cluster").agg({
    "cte_numero": "count",
    "cte_peso": "sum",
    "cte_volumes": "sum",
    "cte_valor_nf": "sum",
    "cte_valor_frete": "sum"
}).rename(columns={
    "cte_numero": "qtde_entregas",
    "cte_peso": "peso_total",
    "cte_volumes": "volumes_totais",
    "cte_valor_nf": "valor_nf_total",
    "cte_valor_frete": "valor_frete_total"
})


csv_entregas = f"{output_path}/entregas_clusterizadas_{data_execucao}.csv"
csv_resumo = f"{output_path}/resumo_clusters_{data_execucao}.csv"

df_consolidado.to_csv(csv_entregas, index=False)
resumo_geral.to_csv(csv_resumo)

print(f"\n✅ Simulação finalizada!")
print(f"📁 Resultados consolidados salvos em:\n{csv_entregas}\n{csv_resumo}")

servico.db.fechar_conexao()
