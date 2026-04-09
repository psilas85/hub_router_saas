#simulation/domain/clusterization_service.py

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from scipy.spatial.distance import cdist
import uuid

from simulation.infrastructure.simulation_database_writer import salvar_resumo_clusters_em_db
from simulation.infrastructure.cache_coordinates import buscar_coordenadas
from simulation.utils.helpers import encontrar_centro_mais_denso, ajustar_para_centro_urbano, log_coordenadas


def coordenadas_sao_validas(lat, lon):
    return lat is not None and lon is not None and -35 <= lat <= 5 and -75 <= lon <= -30


class ClusterizationService:
    def __init__(self, clusterization_db, simulation_db, logger, tenant_id):
        self.clusterization_db = clusterization_db
        self.simulation_db = simulation_db
        self.logger = logger
        self.tenant_id = tenant_id

    def carregar_entregas(self, tenant_id, envio_data):
        self.logger.info("📥 Carregando entregas do clusterization_db...")

        query = """
        SELECT
            cte_numero, remetente_cnpj, cte_rua, cte_bairro, cte_complemento,
            cte_cidade, cte_uf, cte_cep, cte_nf, cte_volumes, cte_peso,
            cte_valor_nf, cte_valor_frete, envio_data, endereco_completo,
            transportadora, remetente_nome, destinatario_nome, destinatario_cnpj,
            destino_latitude AS latitude, destino_longitude AS longitude,
            remetente_cidade, remetente_uf, doc_min, data_processamento, tenant_id
        FROM entregas
        WHERE tenant_id = %s AND envio_data = %s
        """
        df = pd.read_sql(query, self.clusterization_db, params=(tenant_id, envio_data))

        self.logger.info(f"📋 Colunas do DataFrame carregado: {df.columns.tolist()}")
        self.logger.info(f"🔢 Total de entregas carregadas: {len(df)}")

        colunas_esperadas = ['cte_peso', 'cte_volumes', 'cte_valor_nf', 'cte_valor_frete']
        faltando = [col for col in colunas_esperadas if col not in df.columns]
        if faltando:
            self.logger.warning(f"⚠️ Colunas esperadas ausentes: {faltando}")
        else:
            self.logger.info("✅ Colunas adicionais carregadas com sucesso.")

        return df

    def clusterizar(self, df_entregas, k, tenant_id, envio_data, simulation_id):
        self.logger.info(f"📊 Realizando clusterização com KMeans (k={k})...")

        df_entregas = df_entregas.copy()
        indices_coordenadas_validas = df_entregas[["latitude", "longitude"]].dropna().index
        mascara_coordenadas_validas = df_entregas.index.isin(indices_coordenadas_validas)
        df_validas = df_entregas.loc[mascara_coordenadas_validas].copy()

        if df_validas.empty:
            raise ValueError("❌ Nenhuma entrega com coordenadas válidas disponível para clusterização.")

        if len(df_validas) < k:
            raise ValueError(
                f"❌ Quantidade insuficiente de entregas válidas para k={k}: {len(df_validas)}"
            )

        coordenadas_validas = df_validas[["latitude", "longitude"]].values
        modelo = KMeans(n_clusters=k, n_init='auto', random_state=42)
        df_validas['cluster'] = modelo.fit_predict(coordenadas_validas)

        for cluster_id in range(k):
            df_cluster = df_validas[df_validas['cluster'] == cluster_id]
            if df_cluster.empty:
                continue

            centro_lat, centro_lon = encontrar_centro_mais_denso(df_cluster)
            endereco, cidade = ajustar_para_centro_urbano(centro_lat, centro_lon, self.simulation_db, self.tenant_id)


            if cidade == "Fora da UF":
                self.logger.warning(f"⚠️ Cluster {cluster_id}: coordenada fora da UF → ({centro_lat:.5f}, {centro_lon:.5f})")

            self.logger.info(f"📍 Buscando coordenadas para Cluster {cluster_id}: {endereco}")
            lat, lon = buscar_coordenadas(endereco, self.tenant_id, self.simulation_db, self.logger)

            if not coordenadas_sao_validas(lat, lon):
                self.logger.warning(f"⚠️ Coordenadas inválidas. Usando ponto denso para Cluster {cluster_id}.")
                lat, lon = centro_lat, centro_lon

            log_coordenadas(self.logger, cluster_id, lat, lon)

            df_validas.loc[df_validas['cluster'] == cluster_id, 'centro_lat'] = lat
            df_validas.loc[df_validas['cluster'] == cluster_id, 'centro_lon'] = lon
            df_validas.loc[df_validas['cluster'] == cluster_id, 'cluster_endereco'] = endereco
            df_validas.loc[df_validas['cluster'] == cluster_id, 'cluster_cidade'] = cidade

        self.logger.info(f"✅ Clusterização finalizada para k={k} com {k} clusters.")
        return df_validas

    def ajustar_centros_dos_clusters(self, df_clusterizado):
        self.logger.info("📍 Ajustando centros dos clusters (fixo = média ponderada por entregas)...")

        novos_centros = []
        for cluster_id in sorted(df_clusterizado['cluster'].unique()):
            df_cluster = df_clusterizado[df_clusterizado['cluster'] == cluster_id]

            if df_cluster.empty:
                self.logger.warning(f"⚠️ Cluster {cluster_id} sem entregas.")
                continue

            # 📍 Centro calculado = média simples das coordenadas (ponderado por entregas)
            centro_lat = df_cluster["latitude"].mean()
            centro_lon = df_cluster["longitude"].mean()

            if pd.isna(centro_lat) or pd.isna(centro_lon):
                self.logger.warning(f"⚠️ Cluster {cluster_id} sem coordenadas válidas.")
                continue

            # 🔄 Ajuste para centro urbano
            endereco_urbano, cidade = ajustar_para_centro_urbano(
                centro_lat, centro_lon, self.simulation_db, self.tenant_id
            )

            self.logger.info(f"📍 Buscando coordenadas para cluster {cluster_id}: {endereco_urbano}")
            lat, lon = buscar_coordenadas(endereco_urbano, self.tenant_id, self.simulation_db, self.logger)

            if not coordenadas_sao_validas(lat, lon):
                self.logger.warning(f"⚠️ Coordenadas inválidas. Usando centro calculado.")
                lat, lon = centro_lat, centro_lon

            log_coordenadas(self.logger, cluster_id, lat, lon, prefixo="Cluster ajustado")
            novos_centros.append({
                'cluster': cluster_id,
                'cluster_cidade': cidade,
                'centro_lat': lat,
                'centro_lon': lon
            })

        # 🔁 Atualiza DataFrame com os novos centros
        df_centros = pd.DataFrame(novos_centros)
        df_clusterizado = df_clusterizado.drop(columns=[
            "centro_lat", "centro_lon", "cluster_endereco", "cluster_cidade"
        ], errors='ignore')
        df_clusterizado = df_clusterizado.merge(df_centros, on="cluster", how="left")

        return df_clusterizado


    def fundir_clusters_pequenos(self, df_clusterizado, min_entregas: int = 10):
        self.logger.info(f"🔁 Fundindo clusters com menos de {min_entregas} entregas...")

        if 'k_clusters' not in df_clusterizado.columns:
            k_inferido = df_clusterizado['cluster'].nunique()
            df_clusterizado['k_clusters'] = k_inferido
            self.logger.warning(f"⚠️ k_clusters ausente. Atribuído: {k_inferido}")

        if not all(col in df_clusterizado.columns for col in ['cluster', 'centro_lat', 'centro_lon']):
            self.logger.error("❌ Colunas obrigatórias ausentes.")
            return df_clusterizado

        contagem = df_clusterizado['cluster'].value_counts().sort_index()
        centros = df_clusterizado.groupby('cluster')[['centro_lat', 'centro_lon']].mean()

        clusters_pequenos = contagem[contagem < min_entregas].index.tolist()
        clusters_validos = contagem[contagem >= min_entregas].index.tolist()

        if not clusters_validos:
            self.logger.warning("⚠️ Nenhum cluster com entregas suficientes.")
            return df_clusterizado

        substituicoes = {}
        for cluster_id in clusters_pequenos:
            try:
                origem = centros.loc[[cluster_id]].values.astype(float)
                destinos = centros.loc[clusters_validos].values.astype(float)
                distancias = cdist(origem, destinos, metric='euclidean')[0]
                cluster_destino = clusters_validos[np.argmin(distancias)]
                substituicoes[cluster_id] = cluster_destino
                self.logger.info(f"🔄 Cluster {cluster_id} fundido com {cluster_destino}")
            except Exception as e:
                self.logger.error(f"❌ Erro ao fundir cluster {cluster_id}: {e}")

        df_clusterizado['cluster'] = df_clusterizado['cluster'].apply(lambda x: substituicoes.get(x, x))
        return self.ajustar_centros_dos_clusters(df_clusterizado)

    def salvar_clusterizacao_em_db(self, df_clusterizado, simulation_id, envio_data, k_clusters):
        colunas_obrigatorias = [
            "tenant_id", "envio_data", "cte_numero", "cluster",
            "cluster_cidade", "centro_lat", "centro_lon",
            "simulation_id", "k_clusters", "is_ponto_otimo",
            "cte_peso", "cte_volumes", "cte_valor_nf", "cte_valor_frete"
        ]

        for col in colunas_obrigatorias:
            if col not in df_clusterizado.columns:
                raise ValueError(f"❌ Coluna obrigatória '{col}' ausente no DataFrame.")

        # 🔁 Evita duplicação de registros por segurança
        df_clusterizado = df_clusterizado.drop_duplicates(
            subset=["tenant_id", "envio_data", "simulation_id", "k_clusters", "cluster", "cte_numero"]
        )

        self.logger.info("🧹 Limpando clusterizações e resumos anteriores para esse simulation_id e k_clusters...")
        cursor = self.simulation_db.cursor()
        for tabela in ["entregas_clusterizadas", "resumo_clusters"]:
            cursor.execute(f"""
                DELETE FROM {tabela}
                WHERE tenant_id = %s AND envio_data = %s AND simulation_id = %s AND k_clusters = %s
            """, (self.tenant_id, envio_data, simulation_id, k_clusters))
            self.logger.info(f"🗑️ Registros apagados de {tabela}")
        self.simulation_db.commit()

        self.logger.info("💾 Salvando clusterização no banco...")
        df_clusterizado["k_clusters"] = pd.to_numeric(
            df_clusterizado["k_clusters"], errors="coerce"
        ).fillna(k_clusters).astype(int)
        df_clusterizado["cte_volumes"] = pd.to_numeric(
            df_clusterizado["cte_volumes"], errors="coerce"
        ).fillna(0).clip(-2147483648, 2147483647).astype(int)
        df_clusterizado["cluster"] = df_clusterizado["cluster"].astype(str) # <-- conversão para string
        df_clusterizado["id"] = [str(uuid.uuid4()) for _ in range(len(df_clusterizado))]
        df_clusterizado["created_at"] = pd.Timestamp.now()

        insert_query = """
            INSERT INTO entregas_clusterizadas (
                id, tenant_id, envio_data, cte_numero, cluster,
                cluster_cidade, centro_lat, centro_lon,
                created_at, simulation_id, k_clusters, is_ponto_otimo,
                cte_peso, cte_volumes, cte_valor_nf, cte_valor_frete,
                latitude, longitude
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """


        for _, row in df_clusterizado.iterrows():
            payload = (
                row["id"],
                row["tenant_id"],
                row["envio_data"],
                row["cte_numero"],
                str(row["cluster"]),
                row["cluster_cidade"],
                row["centro_lat"],
                row["centro_lon"],
                row["created_at"],
                row["simulation_id"],
                int(row["k_clusters"]),
                row["is_ponto_otimo"],
                row["cte_peso"],
                int(row["cte_volumes"]),
                row["cte_valor_nf"],
                row["cte_valor_frete"],
                row.get("latitude"),
                row.get("longitude")
            )
            try:
                cursor.execute(insert_query, payload)
            except Exception as e:
                self.simulation_db.rollback()
                self.logger.error(
                    "❌ Falha ao inserir em entregas_clusterizadas | "
                    f"cte_numero={row.get('cte_numero')} | "
                    f"k_clusters={row.get('k_clusters')} | "
                    f"cte_volumes={row.get('cte_volumes')} | erro={e}"
                )
                cursor.close()
                raise


        self.simulation_db.commit()
        self.logger.info("✅ Clusterização salva com sucesso.")

        # 🧾 Construção do resumo de clusters
        df_clusterizado["cluster"] = df_clusterizado["cluster"].astype(str)

        df_resumo_clusters = (
            df_clusterizado.groupby([
                "tenant_id", "envio_data", "simulation_id",
                "k_clusters", "cluster", "centro_lat", "centro_lon"
            ])
            .agg(
                peso_total_kg=('cte_peso', 'sum'),
                volumes_total=('cte_volumes', 'sum'),
                valor_total_nf=('cte_valor_nf', 'sum'),
                qde_ctes=('cte_numero', 'nunique')
            )
            .reset_index()
        )

        # Adiciona cluster_cidade ao resumo
        df_cluster_cidade = df_clusterizado[["cluster", "cluster_cidade"]].drop_duplicates(subset=["cluster"])
        df_resumo_clusters = df_resumo_clusters.merge(df_cluster_cidade, on="cluster", how="left")
        df_resumo_clusters["created_at"] = pd.Timestamp.now()

        try:
            salvar_resumo_clusters_em_db(self.simulation_db, df_resumo_clusters, self.logger)
        except Exception as e:
            self.logger.error(
                "❌ Falha ao salvar resumo_clusters | "
                f"simulation_id={simulation_id} | envio_data={envio_data} | "
                f"k_clusters={k_clusters} | erro={e}"
            )
            raise



    @staticmethod
    def atribuir_entregas_proximas_ao_hub_central(df_entregas, hubs, raio_km=80.0):
        """
        Atribui cluster '9999' (texto) às entregas dentro do raio de um hub central.
        """
        import numpy as np
        from geopy.distance import geodesic

        df_entregas = df_entregas.copy()
        df_entregas['cluster'] = pd.Series([None] * len(df_entregas), dtype='object')

        for hub in hubs:
            hub_coord = (hub["latitude"], hub["longitude"])
            for i, entrega in df_entregas.iterrows():
                if pd.isnull(entrega['latitude']) or pd.isnull(entrega['longitude']):
                    continue
                entrega_coord = (entrega['latitude'], entrega['longitude'])
                distancia = geodesic(entrega_coord, hub_coord).km
                if distancia <= raio_km:
                    df_entregas.at[i, 'cluster'] = '9999'
                    df_entregas.at[i, 'cluster_cidade'] = 'HUB CENTRAL'
                    df_entregas.at[i, 'centro_lat'] = hub["latitude"]
                    df_entregas.at[i, 'centro_lon'] = hub["longitude"]

        df_hub = df_entregas[df_entregas['cluster'] == '9999'].copy()
        df_restante = df_entregas[df_entregas['cluster'] != '9999'].copy()

        return df_hub, df_restante



