# hub_router/src/data_input/workers/data_input_job.py

import os
import time
import logging
from datetime import datetime

import pandas as pd
from redis import Redis
from rq import get_current_job
from rq.job import Job

from data_input.application.data_input_distributed_use_case import DataInputDistributedUseCase
from data_input.application.validation_service import ValidationService
from data_input.domain.entities import Entrega
from data_input.infrastructure.database_writer import DatabaseWriter
from data_input.utils.address_normalizer import normalize_address
from data_input.infrastructure.db_connection import get_connection_context

logger = logging.getLogger(__name__)

def salvar_historico(
    tenant_id,
    job_id,
    status,
    arquivo,
    total,
    validos,
    invalidos,
    mensagem
):

    try:
        with get_connection_context() as conn:
            with conn.cursor() as cur:

                cur.execute(
                    """
                    INSERT INTO historico_data_input
                    (tenant_id, job_id, arquivo, status, total_processados, validos, invalidos, mensagem, criado_em)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        tenant_id,
                        job_id,
                        arquivo,
                        status,
                        total,
                        validos,
                        invalidos,
                        mensagem,
                        datetime.utcnow(),
                    ),
                )

    except Exception as e:
        logger.error(f"❌ erro ao salvar histórico: {e}", exc_info=True)


def _persistir_entregas_validas(df_valid: pd.DataFrame):

    if df_valid.empty:
        logger.info("⚠ Nenhuma entrega válida para persistir.")
        return

    with get_connection_context() as conn:

        writer = DatabaseWriter(conn)

        campos_entrega = Entrega.__init__.__code__.co_varnames[1:]

        colunas_presentes = [col for col in df_valid.columns if col in campos_entrega]
        df_valid = df_valid[colunas_presentes].copy()

        records = df_valid.astype(object).where(pd.notna(df_valid), None).to_dict("records")
        entregas = [Entrega(**r) for r in records]

        writer.inserir_dados_entregas(entregas)
        writer.atualizar_data_processamento_lote(entregas)

        logger.info(f"✅ Entregas persistidas: {len(entregas)}")

def persistir_cache_localizacoes(df, writer):

    from data_input.utils.address_normalizer import normalize_address
    import logging

    if df is None or df.empty:
        logging.info("📊 CACHE | nenhum novo endereço para persistir")
        return

    df = df.copy()

    df["addr_norm"] = df["endereco_completo"].apply(normalize_address)

    df = df[
        [
            "endereco_completo",
            "addr_norm",
            "destino_latitude",
            "destino_longitude",
            "geocode_source",
        ]
    ].drop_duplicates()

    # validações finais
    df = df[
        df["destino_latitude"].notna() &
        df["destino_longitude"].notna()
    ]

    logging.info(f"📊 CACHE | persistindo={len(df)} registros")

    for row in df.itertuples():

        endereco = row.addr_norm
        lat = row.destino_latitude
        lon = row.destino_longitude
        origem = row.geocode_source

        if not endereco or lat is None or lon is None:
            continue

        writer.inserir_localizacao(
            endereco,
            float(lat),
            float(lon),
            origem.lower()
        )

def processar_data_input(
    tenant_id,
    file_path,
    modo_forcar=False,
    limite_peso_kg=None
):
    import os
    import time
    import pandas as pd
    from redis import Redis
    from rq.job import Job
    from rq import get_current_job

    job = get_current_job()

    logger.info(f"🚀 Data Input distribuído iniciado | tenant={tenant_id}")

    preprocessed_path = None

    try:
        # ---------------------------------------------------------
        # PREP + DISTRIBUIÇÃO
        # ---------------------------------------------------------
        use_case = DataInputDistributedUseCase(tenant_id)
        orchestrator = use_case.execute(file_path)

        preprocessed_path = orchestrator["preprocessed_path"]
        subjobs = orchestrator["subjobs"]
        total_linhas = orchestrator["total_linhas"]
        total_enderecos_unicos = orchestrator["total_enderecos_unicos"]

        logger.info(
            f"📦 Subjobs criados: {len(subjobs)} | linhas={total_linhas} | únicos={total_enderecos_unicos}"
        )

        # ---------------------------------------------------------
        # 🔥 AGUARDA SUBJOBS (RETRY-SAFE + FETCH MANY)
        # ---------------------------------------------------------
        redis_conn = Redis(host="redis", port=6379)

        timeout_segundos = max(900, len(subjobs) * 180)

        start_time = time.time()
        processed_jobs = set()
        results_by_chunk = {}
        last_status = {}

        while True:

            if time.time() - start_time > timeout_segundos:
                raise Exception(
                    f"⏰ Timeout aguardando subjobs | timeout={timeout_segundos}s"
                )

            finished = 0

            jobs = Job.fetch_many(subjobs, connection=redis_conn)

            for j in jobs:

                jid = j.id
                status = j.get_status()

                # log só quando muda (evita spam)
                if last_status.get(jid) != status:
                    logger.info(f"🔍 Subjob {jid} status={status}")
                    last_status[jid] = status

                if j.is_finished:
                    finished += 1

                    if jid not in processed_jobs:
                        processed_jobs.add(jid)

                        if j.result and isinstance(j.result, dict):
                            chunk_id = j.result.get("chunk_id")
                            results = j.result.get("results", [])

                            # 🔒 evita duplicação por retry
                            results_by_chunk[chunk_id] = results

                elif j.is_failed:
                    raise Exception(f"❌ Subjob {jid} falhou")

            progress = int((finished / len(subjobs)) * 70) if subjobs else 70

            if job:
                job.meta["progress"] = progress
                job.meta["step"] = f"Geocodificando ({finished}/{len(subjobs)})"
                job.save_meta()

            if finished == len(subjobs):
                break

            time.sleep(0.2)

        # ---------------------------------------------------------
        # 🔥 CONSOLIDA RESULTADOS (ORDENADO)
        # ---------------------------------------------------------
        logger.info("🔗 Consolidando resultados de geocode")

        geocode_results = []

        for chunk_id in sorted(results_by_chunk.keys()):
            geocode_results.extend(results_by_chunk[chunk_id])

        df_geo = pd.DataFrame(geocode_results)

        if not df_geo.empty:

            # -----------------------------------------------------
            # NORMALIZA CHAVE
            # -----------------------------------------------------
            df_geo["addr_norm"] = df_geo["endereco_completo"].apply(normalize_address)

            # -----------------------------------------------------
            # 🔥 PRIORIDADE DE FONTE (DETERMINÍSTICO)
            # -----------------------------------------------------
            priority = {
                "google": 3,
                "nominatim_structured": 2,
                "nominatim": 1,
                "cache": 0,
                "falha": -1,
            }

            df_geo["priority"] = df_geo["geocode_source"].map(priority).fillna(0)

            df_geo = (
                df_geo
                .sort_values("priority", ascending=False)
                .drop_duplicates(subset=["addr_norm"], keep="first")
                .drop(columns=["priority"])
                .reset_index(drop=True)
            )

        # ---------------------------------------------------------
        # 🔥 CARREGA DATAFRAME ORIGINAL
        # ---------------------------------------------------------
        df = pd.read_pickle(preprocessed_path)

        # garante chave normalizada
        if "addr_norm" not in df.columns:
            df["addr_norm"] = df["endereco_completo"].apply(normalize_address)

        # ---------------------------------------------------------
        # 🔥 APLICA GEOCODE (MAP AO INVÉS DE MERGE)
        # ---------------------------------------------------------
        if not df_geo.empty:

            # cria dicionário para lookup rápido
            geo_map = df_geo.set_index("addr_norm").to_dict(orient="index")

            df["destino_latitude"] = df["addr_norm"].map(
                lambda x: geo_map.get(x, {}).get("destino_latitude")
            )

            df["destino_longitude"] = df["addr_norm"].map(
                lambda x: geo_map.get(x, {}).get("destino_longitude")
            )

            df["geocode_source"] = df["addr_norm"].map(
                lambda x: geo_map.get(x, {}).get("geocode_source", "falha")
            )

        else:
            df["destino_latitude"] = None
            df["destino_longitude"] = None
            df["geocode_source"] = "falha"

        # ---------------------------------------------------------
        # 🔥 PROTEÇÃO EXTRA (evita lixo)
        # ---------------------------------------------------------
        df["destino_latitude"] = pd.to_numeric(df["destino_latitude"], errors="coerce")
        df["destino_longitude"] = pd.to_numeric(df["destino_longitude"], errors="coerce")

        # ---------------------------------------------------------
        # LOG DE SOURCES (debug forte)
        # ---------------------------------------------------------
        if "geocode_source" in df.columns:
            logger.info(
                f"📊 GEO SOURCES: {df['geocode_source'].value_counts(dropna=False).to_dict()}"
            )

        # ---------------------------------------------------------
        # VALIDAÇÃO FINAL
        # ---------------------------------------------------------
        if job:
            job.meta["progress"] = 80
            job.meta["step"] = "Validando dados"
            job.save_meta()

        validator = ValidationService()
        df_valid, df_invalid = validator.execute(df)

        # ---------------------------------------------------------
        # 🔥 REGRA DE LIMITE DE PESO (COM DEFAULT)
        # ---------------------------------------------------------
        DEFAULT_LIMITE_PESO = 15000  # kg

        if limite_peso_kg is None:
            limite_peso_kg = DEFAULT_LIMITE_PESO
            logger.info(f"⚖ Limite de peso não informado. Usando default: {limite_peso_kg} kg")
        else:
            logger.info(f"⚖ Limite de peso informado: {limite_peso_kg} kg")

        if "cte_peso" in df_valid.columns and not df_valid.empty:

            # -------------------------------------------------
            # 🔍 DEBUG PESO
            # -------------------------------------------------
            df_valid = df_valid.copy()

            df_valid["cte_peso_raw"] = df_valid["cte_peso"]
            df_valid["cte_peso"] = pd.to_numeric(df_valid["cte_peso"], errors="coerce")

            logger.info("📊 PESO DEBUG:")
            logger.info(df_valid["cte_peso"].describe())

            logger.info(
                f"📊 PESO AMOSTRA:\n{df_valid[['cte_numero','cte_peso_raw','cte_peso']].head(10)}"
            )

            # -------------------------------------------------
            # 🔥 FILTRO
            # -------------------------------------------------
            df_peso_excedido = df_valid[df_valid["cte_peso"] > limite_peso_kg]

            logger.info(f"📊 Registros acima do limite: {len(df_peso_excedido)}")

            if not df_peso_excedido.empty:

                logger.warning(
                    f"⚠ Excedidos (sample):\n"
                    f"{df_peso_excedido[['cte_numero','cte_peso_raw','cte_peso']].head(10)}"
                )

                df_peso_excedido = df_peso_excedido.copy()
                df_peso_excedido["motivo"] = f"Peso acima do limite ({limite_peso_kg} kg)"

                # -------------------------------------------------
                # 🔥 CONSOLIDA INVALIDOS
                # -------------------------------------------------
                if isinstance(df_invalid, pd.DataFrame) and not df_invalid.empty:
                    df_invalid = pd.concat([df_invalid, df_peso_excedido], ignore_index=True)
                else:
                    df_invalid = df_peso_excedido.copy()

                # -------------------------------------------------
                # 🔥 REMOVE DOS VALIDOS
                # -------------------------------------------------
                df_valid = df_valid[df_valid["cte_peso"] <= limite_peso_kg]

        else:
            logger.warning("⚠ Coluna cte_peso ausente ou df_valid vazio")

        # ---------------------------------------------------------
        # 🔥 GARANTE CONSISTÊNCIA FINAL
        # ---------------------------------------------------------
        df_valid = df_valid.reset_index(drop=True)
        df_invalid = df_invalid.reset_index(drop=True) if df_invalid is not None else pd.DataFrame()
        if not df_invalid.empty and "motivo" not in df_invalid.columns:
            df_invalid["motivo"] = "Erro de validação"

        # ---------------------------------------------------------
        # 🔥 REPROCESSAMENTO DE INVALIDOS (NOVO)
        # ---------------------------------------------------------
        from data_input.application.reprocess_invalids_service import ReprocessInvalidsService
        from data_input.infrastructure.database_reader import DatabaseReader
        from data_input.application.geocode_batch_service import GeocodeBatchService

        logger.info(f"♻ Iniciando reprocessamento de inválidos: {len(df_invalid)}")

        if not df_invalid.empty:

            with get_connection_context() as conn:

                reader = DatabaseReader(conn)
                writer = DatabaseWriter(conn)

                geo = GeocodeBatchService(reader)

                reprocessor = ReprocessInvalidsService(
                    geolocation_service=geo,
                    database_writer=writer
                )

                df_recuperados, df_invalid = reprocessor.execute(df_invalid)

                if not df_recuperados.empty:
                    logger.info(f"♻ Recuperados: {len(df_recuperados)}")

                    df_valid = pd.concat([df_valid, df_recuperados], ignore_index=True)

                else:
                    logger.info("♻ Nenhum inválido recuperado")

        # -----------------------------------------
        # CACHE (SÓ VALIDOS)
        # -----------------------------------------
        with get_connection_context() as conn:

            writer_cache = DatabaseWriter(conn)

            df_cache_novo = df_valid[
                df_valid["geocode_source"].isin([
                    "cache",
                    "nominatim",
                    "nominatim_structured",
                    "google",
                    "google_override"
                ])
            ].copy()

            df_cache_novo["geocode_source_norm"] = (
                df_cache_novo["geocode_source"]
                .fillna("")
                .astype(str)
                .str.strip()
                .str.lower()
            )

            df_cache_novo = df_cache_novo[
                df_cache_novo["destino_latitude"].notna() &
                df_cache_novo["destino_longitude"].notna()
            ]

            logger.info(
                f"📊 CACHE DEBUG | candidatos_para_cache={len(df_cache_novo)} | "
                f"sources={df_cache_novo['geocode_source_norm'].value_counts(dropna=False).to_dict()}"
            )

            persistir_cache_localizacoes(df_cache_novo, writer_cache)

        # -----------------------------------------
        # LOG FINAL DE VALIDAÇÃO
        # -----------------------------------------
        total_valid = len(df_valid)
        total_invalid = len(df_invalid)
        total = total_valid + total_invalid

        logger.info(
            f"📊 Validação concluída | total={total} | validos={total_valid} | invalidos={total_invalid}"
        )

        # ---------------------------------------------------------
        # OVERWRITE POR DATA (modo_forcar)
        # ---------------------------------------------------------
        if modo_forcar and not df_valid.empty:

            datas = list(df_valid["envio_data"].dropna().unique())

            logger.warning(f"🔥 modo_forcar ativo | limpando datas: {datas}")

            with get_connection_context() as conn:
                with conn.cursor() as cur:

                    placeholders = ",".join(["%s"] * len(datas))

                    query = f"""
                        DELETE FROM entregas
                        WHERE tenant_id = %s
                        AND envio_data IN ({placeholders})
                    """

                    cur.execute(query, (tenant_id, *datas))
                    conn.commit()

            logger.info(f"🧹 Dados antigos removidos | datas={len(datas)}")

        # ---------------------------------------------------------
        # PERSISTÊNCIA FINAL (SÓ VALIDOS)
        # ---------------------------------------------------------
        if job:
            job.meta["progress"] = 90
            job.meta["step"] = "Persistindo entregas válidas"
            job.save_meta()

        # -----------------------------------------
        # DEBUG CRÍTICO
        # -----------------------------------------
        logger.info(f"[DEBUG] df_valid linhas: {len(df_valid)}")

        if "cte_numero" in df_valid.columns:
            logger.info(f"[DEBUG] ctes únicos: {df_valid['cte_numero'].nunique()}")
        else:
            logger.warning("[DEBUG] coluna cte_numero NÃO encontrada")

        # -----------------------------------------
        # PERSISTÊNCIA DE ENTREGAS
        # -----------------------------------------
        _persistir_entregas_validas(df_valid)

        # ---------------------------------------------------------
        # HISTÓRICO
        # ---------------------------------------------------------
        salvar_historico(
            tenant_id=tenant_id,
            job_id=job.id if job else None,
            status="done",
            arquivo=os.path.basename(file_path),
            total=total,
            validos=total_valid,
            invalidos=total_invalid,
            mensagem="Processamento concluído",
        )

        if job:
            job.meta["progress"] = 100
            job.meta["step"] = "Concluído"
            job.meta["total_processados"] = total
            job.meta["validos"] = total_valid
            job.meta["invalidos"] = total_invalid

            job.meta["result"] = {
                "total_processados": total,
                "validos": total_valid,
                "invalidos": total_invalid,
            }

            job.save_meta()

        logger.info("✅ Data Input distribuído concluído com sucesso")

        # -----------------------------------------
        # 🔥 PREPARA DATAFRAMES PARA OUTPUT
        # -----------------------------------------

        df_valid_output = df_valid.copy()
        df_invalid_output = df_invalid.copy()

        # garante motivo nos inválidos
        if not df_invalid_output.empty and "motivo" not in df_invalid_output.columns:
            df_invalid_output["motivo"] = "Erro de validação"

        # -----------------------------------------
        # 🔥 SALVA EXCEL COMPLETO
        # -----------------------------------------

        output_dir = f"/app/src/data_input/tenants/{tenant_id}/output"
        os.makedirs(output_dir, exist_ok=True)

        output_path = os.path.join(output_dir, f"{job.id}.xlsx")

        with pd.ExcelWriter(output_path) as writer:
            df_valid_output.to_excel(writer, sheet_name="validos", index=False)
            df_invalid_output.to_excel(writer, sheet_name="invalidos", index=False)

        logger.info(f"📁 OUTPUT COMPLETO salvo em: {output_path}")

        return {
            "status": "done",
            "tenant_id": tenant_id,
            "job_id": job.id,
            "mensagem": "Processamento concluído",
            "total_processados": total,
            "validos": total_valid,
            "invalidos": total_invalid,
        }

    except Exception as e:
        logger.error(f"❌ erro no data_input: {e}", exc_info=True)

        salvar_historico(
            tenant_id=tenant_id,
            job_id=job.id if job else None,
            status="error",
            arquivo=os.path.basename(file_path),
            total=0,
            validos=0,
            invalidos=0,
            mensagem=str(e),
        )

        if job:
            job.meta["step"] = "Erro"
            job.meta["progress"] = 100
            job.save_meta()

        raise

    finally:
        try:
            if preprocessed_path and os.path.exists(preprocessed_path):
                os.remove(preprocessed_path)
                logger.info(f"🧹 Arquivo temporário removido: {preprocessed_path}")
        except Exception as e:
            logger.warning(f"⚠ Não foi possível remover arquivo temporário: {e}")