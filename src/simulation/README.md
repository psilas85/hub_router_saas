📦 Visão Geral

O módulo simulation do HubRouter é responsável por executar a simulação completa da rede logística, desde a clusterização até a escolha do ponto ótimo de configuração da malha.

🚀 Fluxo Geral

Clusterização das entregas (KMeans + heurísticas).

Transfer Routing (middle-mile) com Savings Algorithm.

Last-Mile Routing com subclusterização dinâmica.

Cálculo de custos (transfer, last-mile, clusters).

Definição do ponto ótimo (k_clusters) com heurística de inflexão.

Persistência de resultados no banco e geração de mapas, gráficos e relatórios.

📊 Estruturas de Dados

Entradas: entregas (CTEs), hubs, parâmetros operacionais.

Saídas:

Custos totais e por componente.

Rotas (transferências e last-mile).

Mapas e relatórios.

Registro consolidado em resultados_simulacao.

⚙️ Configurações Principais

entregas_por_subcluster: nº máx. de entregas por subcluster last-mile (default=25).

tempo_maximo_roteirizacao: tempo limite de rota last-mile (default=600 min).

tempo_parada_leve / tempo_parada_pesada: tempos fixos por entrega.

tempo_por_volume: tempo de descarga (0,4 min/unidade).

raio_hub_km: raio para cluster especial do hub central (default=80 km).

fundir_clusters_pequenos: habilita fusão de clusters com poucas entregas.

📂 Outputs

Banco de Dados: clusterizações, rotas, resumos e resultados.

Mapas: .html e .png por k.

Relatórios: resumo final consolidado.

🏆 Ponto Ótimo

Avaliado por trade-off entre custo de transferência e last-mile.

Escolhido com base em menor custo total e heurística de inflexão de tendência.