#hub_router_1.0.1/src/data_input/application/data_input_distributed_use_case.py

import os
import math
import uuid
import logging
import pandas as pd

from rq import Queue
from redis import Redis

from data_input.workers.data_input_subjob import processar_subjob
from data_input.application.dataframe_builder import DataFrameBuilder
from data_input.utils.address_normalizer import normalize_address


logger = logging.getLogger(__name__)


class DataInputDistributedUseCase:
    """
    Fluxo distribuído otimizado:

    1. Lê arquivo
    2. Faz build completo UMA VEZ
    3. Gera addr_norm
    4. Deduplica globalmente os endereços
    5. Divide apenas os endereços únicos em subjobs
    6. Salva o DataFrame completo pré-processado em arquivo temporário
    7. O job pai depois consolida os resultados dos subjobs
    """

    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.redis_conn = Redis(host="redis", port=6379)
        self.queue = Queue("data_input_subjobs", connection=self.redis_conn)

        # Bom ponto de partida para t3.xlarge
        self.chunk_size = int(os.getenv("DATA_INPUT_GEOCODE_CHUNK_SIZE", "300"))

    def _load_file(self, filepath: str) -> pd.DataFrame:
        ext = os.path.splitext(filepath)[1].lower()

        if ext in [".xlsx", ".xls"]:
            return pd.read_excel(filepath)

        if ext == ".csv":
            try:
                return pd.read_csv(filepath, sep=";")
            except Exception:
                return pd.read_csv(filepath)

        raise ValueError(f"Formato de arquivo não suportado: {ext}")

    def execute(self, filepath: str) -> dict:
        logger.info(f"📂 Lendo arquivo de input: {filepath}")

        # ---------------------------------------------------------
        # LOAD
        # ---------------------------------------------------------
        df = self._load_file(filepath)

        logger.info(f"📊 Linhas carregadas: {len(df)}")

        # ---------------------------------------------------------
        # BUILD GLOBAL
        # ---------------------------------------------------------
        builder = DataFrameBuilder()
        df = builder.build(df)
        df["tenant_id"] = self.tenant_id

        # ---------------------------------------------------------
        # NORMALIZA ENDEREÇO GLOBALMENTE
        # ---------------------------------------------------------
        df["addr_norm"] = df["endereco_completo"].apply(normalize_address)

        # remove linhas sem endereço normalizado útil para geocode
        df_geo = df[df["addr_norm"].notna()].copy()

        # ---------------------------------------------------------
        # DEDUP GLOBAL DE ENDEREÇOS
        # ---------------------------------------------------------
        colunas_geo = [
            "addr_norm",
            "cte_rua",
            "cte_cidade",
            "cte_uf",
            "cte_cep",
            "endereco_completo",
        ]

        df_unique = (
            df_geo[colunas_geo]
            .drop_duplicates(subset=["addr_norm"])
            .reset_index(drop=True)
        )

        total_linhas = len(df)
        total_enderecos_unicos = len(df_unique)

        logger.info(
            f"🧹 Deduplicação global concluída | linhas={total_linhas} | enderecos_unicos={total_enderecos_unicos}"
        )

        # ---------------------------------------------------------
        # SALVA DATAFRAME COMPLETO PRÉ-PROCESSADO
        # ---------------------------------------------------------
        run_id = str(uuid.uuid4())
        tmp_dir = f"/tmp/data_input/{self.tenant_id}"
        os.makedirs(tmp_dir, exist_ok=True)

        preprocessed_path = os.path.join(
            tmp_dir, f"data_input_preprocessed_{run_id}.pkl"
        )

        df.to_pickle(preprocessed_path)

        logger.info(f"💾 DataFrame pré-processado salvo em: {preprocessed_path}")

        # ---------------------------------------------------------
        # CHUNKS DOS ENDEREÇOS ÚNICOS
        # ---------------------------------------------------------
        chunks = [
            df_unique.iloc[i:i + self.chunk_size].copy()
            for i in range(0, len(df_unique), self.chunk_size)
        ]

        subjobs = []

        for i, chunk in enumerate(chunks):
            payload = {
                "chunk_id": i,
                "tenant_id": self.tenant_id,
                "data": chunk.to_dict(orient="records"),
            }

            subjob = self.queue.enqueue(
                processar_subjob,
                payload,
                job_timeout=3600,
                result_ttl=86400,
                failure_ttl=86400,
            )

            subjobs.append(subjob.id)

        logger.info(f"🚀 {len(subjobs)} subjobs criados")

        return {
            "run_id": run_id,
            "preprocessed_path": preprocessed_path,
            "subjobs": subjobs,
            "total_linhas": total_linhas,
            "total_enderecos_unicos": total_enderecos_unicos,
            "chunk_size": self.chunk_size,
        }