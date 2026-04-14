#hub_router_1.0.1/src/simulation/domain/simulation_service.py
import math
import uuid
import numpy as np
import pandas as pd
from geopy.distance import geodesic
from sklearn.cluster import KMeans
from collections import namedtuple

import math

from simulation.config import UF_BOUNDS
from simulation.utils.heuristics import avaliar_parada_heuristica
from simulation.domain.entities import SimulationParams

class SimulationService:
    @staticmethod
    def _arredondar_k_operacional(valor):
        if valor is None:
            return None
        return max(1, int(math.floor(float(valor) + 0.5)))

    def __init__(self, tenant_id, envio_data, simulation_db, logger, hub_id=None):
        self.tenant_id = tenant_id
        self.envio_data = envio_data
        self.simulation_db = simulation_db
        self.logger = logger
        self.simulation_id = str(uuid.uuid4())
        self.hub_id = hub_id


    def gerar_simulation_id(self):
        self.logger.info(f"🆔 Simulation ID gerado: {self.simulation_id}")
        return self.simulation_id



    def preparar_dados_operacionais_iniciais(self, df_entregas, params: SimulationParams):
        df = df_entregas.copy()

        cobertura_minima_tempo = params.cobertura_minima_tempo
        cobertura_minima_peso = params.cobertura_minima_peso
        cobertura_minima_volumes = params.cobertura_minima_volumes
        cobertura_minima_prazo = params.cobertura_minima_prazo

        total_registros = max(len(df), 1)

        if "cte_peso" in df.columns:
            df["peso_operacional"] = pd.to_numeric(df["cte_peso"], errors="coerce")
        else:
            df["peso_operacional"] = np.nan

        if "cte_prazo_min" in df.columns:
            df["prazo_operacional_min"] = pd.to_numeric(df["cte_prazo_min"], errors="coerce")
        else:
            df["prazo_operacional_min"] = np.nan

        tempo_direto = pd.to_numeric(
            df.get("cte_tempo_atendimento_min"),
            errors="coerce",
        ) if "cte_tempo_atendimento_min" in df.columns else pd.Series(np.nan, index=df.index)

        volumes = pd.to_numeric(
            df.get("cte_volumes"),
            errors="coerce",
        ) if "cte_volumes" in df.columns else pd.Series(np.nan, index=df.index)

        pesos = pd.to_numeric(
            df.get("cte_peso"),
            errors="coerce",
        ) if "cte_peso" in df.columns else pd.Series(np.nan, index=df.index)

        tempo_parada_leve = params.tempo_parada_leve
        tempo_parada_pesada = params.tempo_parada_pesada
        tempo_por_volume = params.tempo_por_volume
        limite_peso_parada = params.limite_peso_parada

        paradas_estimadas = np.where(
            pesos.fillna(0) > limite_peso_parada,
            tempo_parada_pesada,
            tempo_parada_leve,
        )
        tempo_estimado = pd.Series(paradas_estimadas, index=df.index) + (
            volumes.fillna(0) * tempo_por_volume
        )

        usa_tempo_direto = tempo_direto.notna() & (tempo_direto >= 0)
        pode_estimar_tempo = (~usa_tempo_direto) & volumes.notna() & (volumes >= 0)

        df["tempo_operacional_min"] = np.where(
            usa_tempo_direto,
            tempo_direto,
            np.where(pode_estimar_tempo, tempo_estimado, np.nan),
        )
        df["fonte_tempo_operacional"] = np.where(
            usa_tempo_direto,
            "tempo_atendimento_direto",
            np.where(pode_estimar_tempo, "tempo_estimado_por_volume", "tempo_indisponivel"),
        )

        cobertura_tempo_direto = float(usa_tempo_direto.mean())
        cobertura_tempo_total = float(pd.notna(df["tempo_operacional_min"]).mean())
        cobertura_peso = float(pd.notna(df["peso_operacional"]).mean())
        cobertura_volumes = float(volumes.notna().mean())
        cobertura_prazo = float(pd.notna(df["prazo_operacional_min"]).mean())

        if cobertura_tempo_direto >= cobertura_minima_tempo:
            modo_avaliacao = "full_time_direct"
        elif cobertura_tempo_total >= cobertura_minima_tempo and cobertura_volumes >= cobertura_minima_volumes:
            modo_avaliacao = "full_time_estimated"
        elif cobertura_peso >= cobertura_minima_peso:
            modo_avaliacao = "weight_only"
        else:
            modo_avaliacao = "count_only"

        df["modo_avaliacao_operacional"] = modo_avaliacao

        metadata = {
            "modo_avaliacao": modo_avaliacao,
            "cobertura_tempo_direto": cobertura_tempo_direto,
            "cobertura_tempo_total": cobertura_tempo_total,
            "cobertura_peso": cobertura_peso,
            "cobertura_volumes": cobertura_volumes,
            "cobertura_prazo": cobertura_prazo,
            "total_entregas": total_registros,
        }

        self.logger.info(
            "🧪 Preparação operacional inicial "
            f"modo={modo_avaliacao} "
            f"tempo_direto={cobertura_tempo_direto:.1%} "
            f"tempo_total={cobertura_tempo_total:.1%} "
            f"peso={cobertura_peso:.1%} "
            f"volumes={cobertura_volumes:.1%} "
            f"prazo={cobertura_prazo:.1%}"
        )

        return df, metadata

    def separar_outliers_geograficos(self, df_entregas, params: SimulationParams):
        if df_entregas.empty or len(df_entregas) < 3:
            return df_entregas, pd.DataFrame(columns=df_entregas.columns), {
                "qtd_outliers": 0,
                "limite_km": None,
            }

        usar_outlier = params.usar_outlier
        if not usar_outlier:
            return df_entregas, pd.DataFrame(columns=df_entregas.columns), {
                "qtd_outliers": 0,
                "limite_km": None,
            }

        df = df_entregas.copy()
        coordenadas = df[["latitude", "longitude"]].apply(
            pd.to_numeric,
            errors="coerce",
        )
        mascara_validas = coordenadas.notna().all(axis=1)
        df_validas = df.loc[mascara_validas].copy()
        if len(df_validas) < 3:
            return df_entregas, pd.DataFrame(columns=df_entregas.columns), {
                "qtd_outliers": 0,
                "limite_km": None,
            }

        coords_rad = np.radians(
            df_validas[["latitude", "longitude"]].astype(float).values
        )
        lat = coords_rad[:, 0][:, np.newaxis]
        lon = coords_rad[:, 1][:, np.newaxis]
        dlat = lat - lat.T
        dlon = lon - lon.T
        a = (
            np.sin(dlat / 2.0) ** 2
            + np.cos(lat) * np.cos(lat.T) * np.sin(dlon / 2.0) ** 2
        )
        matriz_distancias = 6371.0 * 2.0 * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))
        np.fill_diagonal(matriz_distancias, np.inf)
        distancias_vizinho_proximo = matriz_distancias.min(axis=1)

        q1, q3 = np.quantile(distancias_vizinho_proximo, [0.25, 0.75])
        iqr = max(float(q3 - q1), 0.0)
        multiplicador_iqr = params.multiplicador_iqr
        quantil_referencia = params.quantil_referencia
        distancia_minima_km = params.distancia_minima_km
        limite_iqr = q3 + (multiplicador_iqr * iqr)
        limite_quantil = float(
            np.quantile(distancias_vizinho_proximo, quantil_referencia)
        )
        limite_outlier_parametrizado = params.distancia_outlier_km
        if limite_outlier_parametrizado is not None:
            limite_outlier_km = float(limite_outlier_parametrizado)
        else:
            limite_outlier_km = max(distancia_minima_km, limite_iqr, limite_quantil)

        df_validas["dist_vizinho_mais_proximo_km"] = distancias_vizinho_proximo
        df_outliers = df_validas[
            df_validas["dist_vizinho_mais_proximo_km"] > limite_outlier_km
        ].copy()

        percentual_maximo = params.percentual_maximo
        qtd_maxima_outliers = int(np.floor(len(df_validas) * percentual_maximo))
        qtd_maxima_outliers = max(1, qtd_maxima_outliers)
        k_minimo = 2
        qtd_maxima_por_base = max(0, len(df_validas) - k_minimo)
        qtd_outliers_permitida = min(qtd_maxima_outliers, qtd_maxima_por_base)

        if len(df_outliers) > qtd_outliers_permitida > 0:
            df_outliers = df_outliers.nlargest(
                qtd_outliers_permitida,
                "dist_vizinho_mais_proximo_km",
            ).copy()
        elif qtd_outliers_permitida == 0:
            df_outliers = df_outliers.iloc[0:0].copy()

        if df_outliers.empty:
            return df_entregas, pd.DataFrame(columns=df_entregas.columns), {
                "qtd_outliers": 0,
                "limite_km": round(float(limite_outlier_km), 2),
            }

        indices_outliers = df_outliers.index.tolist()
        df_clusterizavel = df.loc[~df.index.isin(indices_outliers)].copy()
        df_outliers = df.loc[df.index.isin(indices_outliers)].copy()
        df_outliers["dist_vizinho_mais_proximo_km"] = df_validas.loc[
            indices_outliers,
            "dist_vizinho_mais_proximo_km",
        ]

        resumo_outliers = {
            str(row.get("cte_numero")): round(
                float(row["dist_vizinho_mais_proximo_km"]),
                2,
            )
            for _, row in df_outliers.head(10).iterrows()
        }
        self.logger.warning(
            "🛰️ Outliers geográficos separados antes da clusterização "
            f"qtd={len(df_outliers)} limite_km={limite_outlier_km:.2f} "
            f"amostra={resumo_outliers}"
        )

        return df_clusterizavel, df_outliers, {
            "qtd_outliers": int(len(df_outliers)),
            "limite_km": round(float(limite_outlier_km), 2),
        }

    def materializar_clusters_outliers(self, df_outliers):
        if df_outliers is None or df_outliers.empty:
            return pd.DataFrame(columns=[])

        df = df_outliers.copy().reset_index(drop=True)
        df["cluster"] = [f"OUTLIER_{idx + 1}" for idx in range(len(df))]
        df["centro_lat"] = pd.to_numeric(df["latitude"], errors="coerce").astype(float)
        df["centro_lon"] = pd.to_numeric(df["longitude"], errors="coerce").astype(float)
        df["cluster_endereco"] = df.apply(
            lambda row: (
                f"Centro isolado ({float(row['latitude']):.5f}, "
                f"{float(row['longitude']):.5f})"
            ),
            axis=1,
        )
        if "cte_cidade" in df.columns:
            df["cluster_cidade"] = df["cte_cidade"].fillna("OUTLIER GEOGRAFICO")
        else:
            df["cluster_cidade"] = "OUTLIER GEOGRAFICO"
        df["cluster_tipo"] = "outlier_geografico"

        self.logger.info(
            "🧭 Clusters especiais de outlier materializados "
            f"qtd={len(df)} ids={df['cluster'].tolist()}"
        )

        return df

    def repartir_cluster_hub_central(self, df_clusterizado, params: SimulationParams):
        if df_clusterizado.empty or "cluster" not in df_clusterizado.columns:
            return df_clusterizado

        df = df_clusterizado.copy()
        mascara_hub = df["cluster"].astype(str).eq("9999")
        if not mascara_hub.any():
            return df

        df_hub = df.loc[mascara_hub].copy()
        if len(df_hub) <= 1:
            return df

        coordenadas_validas = df_hub[["latitude", "longitude"]].dropna()
        if len(coordenadas_validas) <= 1:
            return df

        entregas_por_rota = params.entregas_por_rota
        tempo_limite = params.tempo_max_roteirizacao
        fator_ocupacao_hub = params.fator_ocupacao_hub

        k_por_entregas = int(np.ceil(len(df_hub) / entregas_por_rota))

        tempo_total_hub = float(
            pd.to_numeric(df_hub.get("tempo_operacional_min"), errors="coerce").fillna(0).sum()
        )
        capacidade_tempo_subcluster = tempo_limite * fator_ocupacao_hub if tempo_limite > 0 else 0
        k_por_tempo = (
            int(np.ceil(tempo_total_hub / capacidade_tempo_subcluster))
            if capacidade_tempo_subcluster > 0
            else 1
        )

        k_hub = max(1, k_por_entregas, k_por_tempo)
        if k_hub <= 1:
            return df

        k_hub = min(k_hub, len(coordenadas_validas))
        if k_hub <= 1:
            return df

        labels = KMeans(
            n_clusters=k_hub,
            random_state=42,
            n_init="auto",
        ).fit_predict(coordenadas_validas[["latitude", "longitude"]].values)

        df_hub.loc[coordenadas_validas.index, "cluster_hub_subgrupo"] = labels
        if df_hub["cluster_hub_subgrupo"].isna().any():
            df_hub["cluster_hub_subgrupo"] = df_hub["cluster_hub_subgrupo"].fillna(0)

        df_hub["cluster"] = df_hub["cluster_hub_subgrupo"].astype(int).map(
            lambda idx: f"9999_{idx + 1}"
        )
        df_hub["cluster_cidade"] = df_hub.get("cluster_cidade", "HUB CENTRAL").fillna("HUB CENTRAL")
        df_hub["hub_cluster_principal"] = "9999"

        df_restante = df.loc[~mascara_hub].copy()
        df_resultado = pd.concat([df_restante, df_hub], ignore_index=True)

        resumo_subgrupos = df_hub.groupby("cluster").size().to_dict()
        self.logger.info(
            "🧩 Cluster do hub central repartido em subgrupos "
            f"k_hub={k_hub} entregas={len(df_hub)} resumo={resumo_subgrupos}"
        )

        return df_resultado

    def calcular_faixa_operacional_k(self, df_entregas, params: SimulationParams):
        k_min_param = 1
        k_max_param = 50

        coordenadas_validas = df_entregas[["latitude", "longitude"]].dropna()
        total_entregas = len(coordenadas_validas)
        if total_entregas == 0:
            raise ValueError("❌ Nenhuma entrega com coordenadas válidas disponível para calcular faixa de k.")

        minimo_alvo = params.min_entregas_por_cluster_alvo
        maximo_alvo = params.max_entregas_por_cluster_alvo
        if minimo_alvo is None and maximo_alvo is None:
            k_min_final = min(k_min_param, total_entregas)
            k_max_final = min(k_max_param, total_entregas)
            if k_min_final > k_max_final:
                k_min_final = k_max_final
            return {
                "k_min": k_min_final,
                "k_max": k_max_final,
                "origem": "parametros_legados",
            }

        if minimo_alvo is None:
            minimo_alvo = 1
        if maximo_alvo is None:
            maximo_alvo = max(minimo_alvo, minimo_alvo)

        minimo_alvo = max(1, int(minimo_alvo))
        maximo_alvo = max(minimo_alvo, int(maximo_alvo))

        k_min_operacional = self._arredondar_k_operacional(total_entregas / maximo_alvo)
        k_max_operacional = self._arredondar_k_operacional(total_entregas / minimo_alvo)
        k_max_operacional = max(1, k_max_operacional)

        k_min_final = max(k_min_param, k_min_operacional)
        k_max_final = min(k_max_param, k_max_operacional, total_entregas)

        if k_min_final > k_max_final:
            k_ajustado = max(1, min(k_min_final, total_entregas, k_max_param))
            self.logger.warning(
                "⚠️ Faixa operacional de k ficou inconsistente; "
                f"ajustando para valor único k={k_ajustado}. "
                f"total_entregas={total_entregas} min_alvo={minimo_alvo} max_alvo={maximo_alvo}"
            )
            k_min_final = k_ajustado
            k_max_final = k_ajustado

        self.logger.info(
            "🧮 Faixa operacional de k calculada por entregas totais: "
            f"k_min={k_min_final} k_max={k_max_final} "
            f"total_entregas={total_entregas} min_alvo={minimo_alvo} max_alvo={maximo_alvo}"
        )

        return {
            "k_min": k_min_final,
            "k_max": k_max_final,
            "origem": "faixa_operacional_entregas",
        }


    def obter_k_inicial(self, df_entregas, k_min, k_max, params=None) -> int:
        lat_col = [col for col in df_entregas.columns if col.strip().lower() == "latitude"]
        lon_col = [col for col in df_entregas.columns if col.strip().lower() == "longitude"]

        if not lat_col or not lon_col:
            raise ValueError("❌ Colunas 'latitude' e 'longitude' não encontradas no DataFrame.")

        coordenadas = df_entregas[[lat_col[0], lon_col[0]]].dropna().values
        if len(coordenadas) < k_min:
            raise ValueError(f"❌ Apenas {len(coordenadas)} entregas válidas para k_min={k_min}.")

        k_base_operacional = k_min
        if params:
            candidatos_operacionais = [k_min]
            detalhes_candidatos = {"k_min": k_min}

            alvo_entregas = max(1, int(getattr(params, "alvo_entregas_por_cluster", getattr(params, "min_entregas_por_cluster_alvo", 25))))
            k_por_entregas = int(np.ceil(len(coordenadas) / alvo_entregas))
            candidatos_operacionais.append(k_por_entregas)
            detalhes_candidatos["k_entregas"] = k_por_entregas

            if "peso_operacional" in df_entregas.columns:
                peso_total = pd.to_numeric(df_entregas["peso_operacional"], errors="coerce").fillna(0).sum()
                alvo_peso = float(getattr(params, "alvo_peso_por_cluster", getattr(params, "peso_max_kg", 0)) or 0)
                if alvo_peso > 0 and peso_total > 0:
                    k_por_peso = int(np.ceil(peso_total / alvo_peso))
                    candidatos_operacionais.append(k_por_peso)
                    detalhes_candidatos["k_peso"] = k_por_peso

            if "tempo_operacional_min" in df_entregas.columns:
                tempo_total = pd.to_numeric(df_entregas["tempo_operacional_min"], errors="coerce").fillna(0).sum()
                alvo_tempo_base = float(getattr(params, "alvo_tempo_por_cluster", getattr(params, "tempo_maximo_roteirizacao", 0)) or 0)
                rotas_alvo_por_cluster = max(1, int(getattr(params, "rotas_alvo_por_cluster", getattr(params, "limite_critico_rotas_precheck", 4)) or 4))
                fator_ocupacao_cluster = float(getattr(params, "fator_ocupacao_cluster_inicial", 0.85) or 0.85)
                alvo_tempo = alvo_tempo_base * rotas_alvo_por_cluster * fator_ocupacao_cluster
                if alvo_tempo > 0 and tempo_total > 0:
                    k_por_tempo = int(np.ceil(tempo_total / alvo_tempo))
                    candidatos_operacionais.append(k_por_tempo)
                    detalhes_candidatos["k_tempo"] = k_por_tempo
                    detalhes_candidatos["rotas_alvo_por_cluster"] = rotas_alvo_por_cluster
                    detalhes_candidatos["fator_ocupacao_cluster_inicial"] = round(fator_ocupacao_cluster, 2)

            k_base_operacional = max(candidatos_operacionais)
            k_base_operacional = max(k_min, min(k_base_operacional, k_max, len(coordenadas)))
            self.logger.info(
                "🧮 Piso operacional de k calculado: "
                f"{k_base_operacional} | componentes={detalhes_candidatos}"
            )

        # Elbow tradicional
        custos = []
        k_inicio_busca = max(k_min, k_base_operacional)
        k_limite_superior = min(k_max, len(coordenadas))
        k_vals = list(range(k_inicio_busca, k_limite_superior + 1))
        for k in k_vals:
            kmeans = KMeans(n_clusters=k, random_state=42, n_init="auto").fit(coordenadas)
            custos.append(kmeans.inertia_)
            self.logger.info(f"📉 Elbow: k={k}, inércia={kmeans.inertia_:.2f}")

        if len(custos) < 3:
            return k_inicio_busca

        # Cotovelo geométrico
        x1, y1 = k_vals[0], custos[0]
        x2, y2 = k_vals[-1], custos[-1]
        def distancia_do_ponto(x0, y0):
            numerador = abs((y2 - y1) * x0 - (x2 - x1) * y0 + x2*y1 - y2*x1)
            denominador = ((y2 - y1)**2 + (x2 - x1)**2)**0.5
            return numerador / denominador
        distancias = [distancia_do_ponto(k, c) for k, c in zip(k_vals, custos)]
        k_elbow = k_vals[int(np.argmax(distancias))]

        k_final = max(k_elbow, k_base_operacional)
        self.logger.info(f"🔍 k_inicial escolhido com método do cotovelo: {k_final}")
        return k_final



    def gerar_lista_k(self, k_inicial, k_min, k_max, variacao_maxima=5):
        """
        Gera uma lista de k_clusters em torno do k_inicial, com prioridade ao valor sugerido pela heurística.
        Ex: se k_inicial=5, gera: [5, 4, 6, 3, 7, 2, 8...]
        """
        lista_k = [k_inicial]

        for delta in range(1, variacao_maxima + 1):
            if k_inicial - delta >= k_min:
                lista_k.append(k_inicial - delta)
            if k_inicial + delta <= k_max:
                lista_k.append(k_inicial + delta)

        # Remove duplicatas mantendo ordem
        lista_k_final = list(dict.fromkeys(lista_k))
        self.logger.info(f"🧮 Lista de k gerada para testes: {lista_k_final}")
        return lista_k_final

    def gerar_cenarios_explicitos(self, df_entregas, params: SimulationParams):

        import math

        total_entregas = len(df_entregas)

        if total_entregas == 0:
            return []

        # 🔴 FAIXA OPERACIONAL DE K
        minimo_alvo = max(1, int(params.min_entregas_por_cluster_alvo or 1))
        maximo_alvo = max(minimo_alvo, int(params.max_entregas_por_cluster_alvo or total_entregas))

        k_min = max(1, math.ceil(total_entregas / maximo_alvo))
        k_max = max(1, math.floor(total_entregas / minimo_alvo))

        if k_min > k_max:
            k_min = k_max

        self.logger.info(
            f"[K DEBUG] total={total_entregas} | "
            f"min_cluster={minimo_alvo} | max_cluster={maximo_alvo} | "
            f"k_min={k_min} | k_max={k_max}"
        )

        # 🔴 CENÁRIO K0 (SEMPRE PRESENTE)
        cenarios = [
            {
                "tipo": "k_zero",
                "identificador": "k=0",
                "k_clusters": 0,
            }
        ]

        # 🔴 RANGE COMPLETO DE K
        lista_k = list(range(k_min, k_max + 1))

        usa_cluster_hub = not params.desativar_cluster_hub

        if usa_cluster_hub:
            # ajuste para considerar hub como cluster adicional
            lista_k = [k - 1 for k in lista_k if k >= 2]
        else:
            lista_k = [k for k in lista_k if k >= 1]

        # 🔴 (OPCIONAL) PRIORIZAR K MAIS PROVÁVEL
        try:
            k_inicial = getattr(params, "k_inicial", None)
            if k_inicial:
                lista_k = sorted(lista_k, key=lambda x: abs(x - k_inicial))
        except Exception:
            pass

        # 🔴 MONTA CENÁRIOS
        cenarios_k = [
            {
                "tipo": "k_numero",
                "identificador": f"k={k}",
                "k_clusters": k,
                "algoritmo": getattr(params, "algoritmo_clusterizacao", "kmeans"),
            }
            for k in lista_k
        ]

        cenarios.extend(cenarios_k)

        self.logger.info(
            f"🧭 Cenários finais gerados: {[c['identificador'] for c in cenarios]}"
        )

        return cenarios



    def verificar_ponto_inflexao_com_tendencia(self, lista_custos: list[float], janela: int = 2) -> bool:
        """
        Nova heurística: considera ponto ótimo se após uma queda consistente, houver
        pelo menos dois aumentos consecutivos nos custos (ou vice-versa).
        """
        if len(lista_custos) < (janela * 2 + 1):
            return False  # precisa de pelo menos janela*2 + 1 valores para avaliar

        # Últimos valores relevantes
        anteriores = lista_custos[-(janela * 2 + 1):-janela]
        posteriores = lista_custos[-janela:]

        # Diferenças
        delta_antes = [anteriores[i+1] - anteriores[i] for i in range(len(anteriores) - 1)]
        delta_depois = [posteriores[i+1] - posteriores[i] for i in range(len(posteriores) - 1)]

        # Tendência anterior de queda
        tendencia_queda = all(d < 0 for d in delta_antes)
        tendencia_subida = all(d > 0 for d in delta_antes)

        # Tendência posterior oposta
        confirmacao_subida = all(d > 0 for d in delta_depois)
        confirmacao_queda = all(d < 0 for d in delta_depois)

        if (tendencia_queda and confirmacao_subida) or (tendencia_subida and confirmacao_queda):
            self.logger.info(f"🧠 Ponto ótimo confirmado por tendência: antes={delta_antes}, depois={delta_depois}")
            return True

        return False


    def simulacao_ja_existente(self) -> bool:
        """
        Verifica se já existem dados para tenant_id e envio_data em qualquer das tabelas da simulação.
        """
        tabelas = [
            'detalhes_rotas',
            'detalhes_transferencias',
            'entregas_clusterizadas',
            'resultados_simulacao',
            'resumo_clusters',
            'resumo_transferencias',
            'rotas_last_mile',
            'rotas_transferencias'
        ]

        cursor = self.simulation_db.cursor()
        for tabela in tabelas:
            try:
                cursor.execute(f"""
                    SELECT 1 FROM {tabela}
                    WHERE tenant_id = %s AND envio_data = %s
                    LIMIT 1
                """, (self.tenant_id, self.envio_data))
                if cursor.fetchone():
                    self.logger.warning(f"⚠️ Dados existentes detectados na tabela '{tabela}' para envio_data={self.envio_data}")
                    cursor.close()
                    return True
            except Exception as e:
                self.logger.error(f"❌ Erro ao verificar existência na tabela '{tabela}': {e}")

        cursor.close()
        return False


    def limpar_simulacoes_anteriores(self):
        self.logger.info(f"♻️ Limpando dados de simulações anteriores para envio_data = {self.envio_data}, tenant_id = {self.tenant_id}...")
        cursor = self.simulation_db.cursor()

        tabelas = [
            'detalhes_rotas',
            'detalhes_transferencias',
            'entregas_clusterizadas',
            'resultados_simulacao',
            'resumo_clusters',
            'resumo_transferencias',
            'rotas_last_mile',
            'rotas_transferencias'
        ]

        for tabela in tabelas:
            try:
                cursor.execute(
                    f"DELETE FROM {tabela} WHERE tenant_id = %s AND envio_data = %s",
                    (self.tenant_id, self.envio_data)
                )
                self.logger.info(f"🧹 Dados apagados da tabela {tabela}")
            except Exception as e:
                self.logger.warning(f"⚠️ Erro ao apagar dados de {tabela}: {e}")

        self.simulation_db.commit()
        cursor.close()
        self.logger.info("✅ Dados antigos removidos com sucesso.")

    def buscar_hub_central(self):
        if not self.hub_id:
            raise Exception("❌ Nenhum hub central informado. Informe --hub-id.")

        cursor = self.simulation_db.cursor()
        cursor.execute("""
            SELECT nome, latitude, longitude, cidade
            FROM hubs
            WHERE tenant_id = %s AND hub_id = %s
        """, (self.tenant_id, self.hub_id))
        row = cursor.fetchone()
        cursor.close()

        if not row:
            raise Exception(f"❌ Hub central com hub_id={self.hub_id} não encontrado para este tenant.")

        Hub = namedtuple("Hub", ["nome", "latitude", "longitude", "cidade"])
        return Hub(*row)



    def gerar_range_k(total_entregas: int, min_cluster: int, max_cluster: int, max_cenarios: int = 20):

        if total_entregas <= 0:
            return []

        min_cluster = max(1, int(min_cluster))
        max_cluster = max(min_cluster, int(max_cluster))

        k_min = math.ceil(total_entregas / max_cluster)
        k_max = math.floor(total_entregas / min_cluster)

        if k_min > k_max:
            k_min = k_max

        k_values = list(range(k_min, k_max + 1))

        # 🔴 proteção contra explosão
        if len(k_values) > max_cenarios:
            step = math.ceil(len(k_values) / max_cenarios)
            k_values = k_values[::step]

        return k_values