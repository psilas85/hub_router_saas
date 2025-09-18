# domain/simulation_service.py
import uuid
import numpy as np
from sklearn.cluster import KMeans
from collections import namedtuple

from simulation.config import UF_BOUNDS
from simulation.utils.heuristics import avaliar_parada_heuristica

class SimulationService:
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


    def obter_k_inicial(self, df_entregas, k_min, k_max) -> int:
        lat_col = [col for col in df_entregas.columns if col.strip().lower() == "latitude"]
        lon_col = [col for col in df_entregas.columns if col.strip().lower() == "longitude"]

        if not lat_col or not lon_col:
            raise ValueError("❌ Colunas 'latitude' e 'longitude' não encontradas no DataFrame.")

        coordenadas = df_entregas[[lat_col[0], lon_col[0]]].dropna().values
        if len(coordenadas) < k_min:
            raise ValueError(f"❌ Apenas {len(coordenadas)} entregas válidas para k_min={k_min}.")

        # Elbow tradicional
        custos = []
        k_vals = list(range(k_min, min(k_max + 1, len(coordenadas))))
        for k in k_vals:
            kmeans = KMeans(n_clusters=k, random_state=42, n_init="auto").fit(coordenadas)
            custos.append(kmeans.inertia_)
            self.logger.info(f"📉 Elbow: k={k}, inércia={kmeans.inertia_:.2f}")

        if len(custos) < 3:
            return k_min

        # Cotovelo geométrico
        x1, y1 = k_vals[0], custos[0]
        x2, y2 = k_vals[-1], custos[-1]
        def distancia_do_ponto(x0, y0):
            numerador = abs((y2 - y1) * x0 - (x2 - x1) * y0 + x2*y1 - y2*x1)
            denominador = ((y2 - y1)**2 + (x2 - x1)**2)**0.5
            return numerador / denominador
        distancias = [distancia_do_ponto(k, c) for k, c in zip(k_vals, custos)]
        k_elbow = k_vals[int(np.argmax(distancias))]

        k_final = k_elbow
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
