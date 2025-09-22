flowchart TD

    A[📥 Entradas<br/>CTEs + Hubs + Parâmetros] --> B[📊 Clusterização]
    B --> B1[Definição k_inicial<br/>Método do Elbow (KMeans/Inércia)]
    B1 --> B2[Lista de k a testar<br/>Variações ±5]
    B2 --> C[Clusterização entregas por k]

    C --> D[🚛 Roteirização Middle-Mile<br/>Savings Algorithm]
    D --> D1[Expandir pontos por capacidade do veículo]
    D1 --> D2[Obter rotas reais (cache/API)]
    D2 --> D3[Calcular distância, tempo, veículo]
    D3 --> E[💰 Custo de Transferência]

    C --> F[🚐 Roteirização Last-Mile<br/>Subclusterização dinâmica]
    F --> F1[KMeans define subclusters (25 entregas/subcluster)]
    F1 --> F2[Subdividir por veículo e tempo máximo]
    F2 --> F3[Sequenciar entregas (ferradura)]
    F3 --> F4[Obter rotas reais (cache/API)]
    F4 --> F5[Somar paradas + descarga]
    F5 --> G[💰 Custo de Last-Mile]

    C --> H[📦 Custo de Clusters<br/>Fixo + Variável por entrega]

    E --> I[🔢 Consolidação de custos<br/>Transfer + Last-Mile + Clusters]
    G --> I
    H --> I

    I --> J{Ponto ótimo?}
    J -- Não --> B2
    J -- Sim --> K[🏆 Persistência no banco<br/>is_ponto_otimo=True]

    K --> L[🗺️ Visualizações<br/>Mapas (Cluster, Transfer, Last-Mile)]
    K --> M[📊 Gráficos de custos]
    K --> N[📑 Relatório final]

    L & M & N --> O[✅ Saída Final<br/>Resultado completo por envio_data]


🔹 Etapas em detalhe
1. Clusterização

Usa KMeans com k_inicial definido pelo Elbow geométrico.

Gera lista de k em torno do inicial (até ±5).

Centros ajustados com heurística ponderada por entregas e refinados para centro urbano via Nominatim/cache.

Clusters pequenos podem ser fundidos (min_entregas_cluster).

Clusters próximos ao hub (raio) recebem ID especial 9999.

2. Transfer Routing (Middle-Mile)

Algoritmo: Savings (Clarke-Wright modificado).

Regras:

Veículos definidos a partir do peso agregado (consulta ao banco).

Respeita tempo máximo (tempo_maximo_transferencia).

Considera tempos de parada e descarga.

Saídas:

Distância total / parcial.

Tempo total / parcial.

Rotas completas em JSON + sequências de coordenadas.

3. Last-Mile Routing

Subclusterização inicial: 25 entregas/subcluster (ajustável).

Se rota excede tempo máximo → subdivisão incremental (k_sub++).

Sequenciamento: heurística ferradura.

Roteirização real: Google Maps/OSRM com cache.

Restrições:

Tempo máximo (hard limit, ou aceitação com warning se permitir_rotas_excedentes=True).

Tempos de parada (10 ou 20 min conforme peso).

Tempo de descarga (0,4 min por volume).

Saídas:

Rotas completas com sequência de entregas.

Distância, tempo, veículo, coordenadas.

4. Custos

Transferência:

Custo = distância × tarifa/km.

Last-Mile:

Custo = distância × tarifa/km + entregas × tarifa/entrega.

Clusters:

Custo fixo diário + variável por entrega.

5. Definição do Ponto Ótimo

Sempre executa baseline k=1 (centralizado no hub).

Testa variações de k até k_max.

Critério:

Menor custo total (transfer + last-mile + cluster).

Heurística de inflexão: identifica quando a curva de custos muda de tendência (queda → subida, ou vice-versa).

Persistência:

Marca is_ponto_otimo=True no banco.

Salva no resultados_simulacao.

6. Saídas

Tabelas persistidas no banco:

entregas_clusterizadas

resumo_clusters

rotas_transferencias / resumo_transferencias

rotas_last_mile / resumo_rotas_last_mile

resultados_simulacao

Outputs gerados:

Mapas (cluster, transfer, last-mile).

Gráficos de custos por k.

Relatório final consolidado.