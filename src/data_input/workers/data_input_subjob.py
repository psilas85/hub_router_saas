#hub_router_1.0.1/src/data_input/workers/data_input_subjob.py

import logging
import pandas as pd

from rq import get_current_job

from data_input.infrastructure.db import Database
from data_input.infrastructure.database_reader import DatabaseReader
from data_input.infrastructure.database_writer import DatabaseWriter
from data_input.application.geocode_batch_service import GeocodeBatchService


logger = logging.getLogger(__name__)


def processar_subjob(payload: dict):
    """
    Subjob faz apenas geocoding dos endereços únicos.
    Não faz validation nem persistência em entregas.
    Isso evita duplicidade de lógica e melhora a consistência final.
    """
    job = get_current_job()

    db = None

    try:
        chunk_id = payload["chunk_id"]
        tenant_id = payload["tenant_id"]
        data = payload["data"]

        logger.info(
            f"🚀 Subjob iniciado | chunk_id={chunk_id} | tenant={tenant_id} | linhas={len(data)}"
        )

        df = pd.DataFrame(data)

        if df.empty:
            logger.info(f"⚠ Subjob {chunk_id} recebeu chunk vazio")
            return {
                "chunk_id": chunk_id,
                "processed": 0,
                "results": [],
            }

        db = Database()
        db.conectar()

        reader = DatabaseReader(db.conexao)
        writer = DatabaseWriter(db.conexao)

        # Aqui writer é importante para persistir cache em localizacoes
        geo = GeocodeBatchService(reader)

        # geocode_batch já usa endereco_completo / addr_norm / cache / nominatim / google
        df = geo.execute(df)

        resultado = df[
            [
                "endereco_completo",
                "destino_latitude",
                "destino_longitude",
                "geocode_source",
            ]
        ].copy()

        # garante serialização segura
        resultado = resultado.astype(object).where(pd.notna(resultado), None)

        if job:
            job.meta["progress"] = 100
            job.meta["step"] = f"Chunk {chunk_id} concluído"
            job.save_meta()

        logger.info(
            f"✅ Subjob finalizado | chunk_id={chunk_id} | processados={len(resultado)}"
        )

        return {
            "chunk_id": chunk_id,
            "processed": len(resultado),
            "results": resultado.to_dict(orient="records"),
        }

    except Exception as e:
        logger.error(f"❌ Erro no subjob: {e}", exc_info=True)
        raise

    finally:
        try:
            if db and db.conexao:
                db.conexao.close()
        except Exception:
            pass