#clusterization/main_clusterization.py

import logging
import argparse
import pandas as pd
from datetime import datetime, timedelta

from clusterization.config import UF_BOUNDS
from clusterization.infrastructure.database_connection import conectar_banco, fechar_conexao
from clusterization.infrastructure.database_reader import DatabaseReader
from clusterization.infrastructure.database_writer import DatabaseWriter
from clusterization.domain.clustering_service import ClusteringService
from clusterization.domain.centro_urbano_service import CentroUrbanoService
from clusterization.application.clusterization_use_case import ClusterizationUseCase
from clusterization.domain.geolocalizacao_service import GeolocalizacaoService

# === CONFIGURAÇÃO DE LOG ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main_clusterization")

# === PARSER DE ARGUMENTOS ===
parser = argparse.ArgumentParser(description="🚀 Executar clusterização de entregas.")

parser.add_argument("--tenant", required=True, help="Tenant ID")
parser.add_argument("--data_inicial", required=True, help="Data inicial no formato YYYY-MM-DD")
parser.add_argument(
    "--data_final",
    required=False,
    help="Data final no formato YYYY-MM-DD (opcional, default = data_inicial)"
)
parser.add_argument("--k_min", type=int, default=2, help="Valor mínimo de clusters (default=2)")
parser.add_argument("--k_max", type=int, default=50, help="Valor máximo de clusters (default=50)")
parser.add_argument("--min_entregas_por_cluster", type=int, default=25,
                    help="Quantidade mínima de entregas por cluster (default=25)")
parser.add_argument("--fundir_clusters_pequenos", action="store_true",
                    help="Se definido, irá fundir clusters pequenos com menos entregas que o mínimo.")
parser.add_argument(
    "--desativar-cluster-hub-central",
    action="store_true",
    help="Se definido, desativa a criação do cluster Hub Central para entregas próximas."
)
parser.add_argument(
    "--raio-cluster-hub-central",
    type=float,
    default=80.0,
    help="Raio em km para definir entregas que serão atribuídas ao cluster Hub Central."
)

parser.add_argument(
    "--modo-forcar",
    action="store_true",
    help="Força reprocessamento limpando dados antigos para data e tenant."
)

args = parser.parse_args()

# === PARÂMETROS ===
tenant_id = args.tenant
data_inicial = datetime.strptime(args.data_inicial, "%Y-%m-%d").date()

# Se não passar data_final, assume igual à inicial
if args.data_final:
    data_final = datetime.strptime(args.data_final, "%Y-%m-%d").date()
else:
    data_final = data_inicial

if data_final < data_inicial:
    logger.error("❌ Data final não pode ser anterior à data inicial.")
    exit(1)

datas_envio = [data_inicial + timedelta(days=i) for i in range((data_final - data_inicial).days + 1)]

logger.info(
    f"🔧 Parâmetros definidos: tenant_id={tenant_id}, "
    f"datas={datas_envio}, "
    f"k_min={args.k_min}, k_max={args.k_max}, "
    f"min_entregas_por_cluster={args.min_entregas_por_cluster}, "
    f"fundir_clusters_pequenos={args.fundir_clusters_pequenos}, "
    f"modo_forcar={args.modo_forcar}"
)

# === CONEXÃO COM BANCO ===
conexao = conectar_banco()
reader = DatabaseReader(conexao)
writer = DatabaseWriter(conexao)
geo_service = GeolocalizacaoService(reader, writer)
centro_service = CentroUrbanoService(reader)


# === LEITURA DE DADOS ===
df_entregas = reader.buscar_entregas(datas_envio, tenant_id)

if df_entregas.empty:
    logger.error("❌ Nenhum dado encontrado no banco para as datas informadas.")
    fechar_conexao(conexao)
    exit(1)

# === PREENCHER LATITUDE E LONGITUDE ===

for idx, row in df_entregas.iterrows():
    if pd.isna(row["destino_latitude"]) or pd.isna(row["destino_longitude"]):
        endereco = f'{row["cte_cidade"]} {row["cte_cep"]}'
        resultado = geo_service.buscar_coordenadas(endereco_original=endereco, uf_esperada=row["cte_uf"])
        if resultado:
            df_entregas.at[idx, "destino_latitude"] = resultado[0]
            df_entregas.at[idx, "destino_longitude"] = resultado[1]
        else:
            logger.warning(f"⚠️ Não foi possível obter coordenadas para {endereco}")

# === VALIDAÇÃO DE COLUNAS ===
COLUNAS_OBRIGATORIAS = [
    "id_entrega", "cte_numero", "transportadora", "envio_data",
    "cte_cidade", "cte_uf", "cte_cep", "cte_volumes", "cte_peso",
    "cte_valor_nf", "cte_valor_frete", "destino_latitude", "destino_longitude"
]

for col in COLUNAS_OBRIGATORIAS:
    if col not in df_entregas.columns:
        df_entregas[col] = pd.NA

# === PROCESSAMENTO POR DATA ===
for envio_data in datas_envio:
    logger.info(f"📌 Processando envio_data = {envio_data}")

    existe_resumo = writer.existe_resumo_clusterizacao(envio_data, tenant_id)

    if existe_resumo:
        if args.modo_forcar:
            logger.info(f"♻️ Modo forçar ativo: limpando dados antigos para {envio_data} e tenant {tenant_id}...")
            writer.excluir_clusterizacao_por_data_tenant(envio_data, tenant_id)
            logger.info("✅ Dados antigos removidos com sucesso.")
        else:
            logger.info(f"⚠️ Dados já existem para {envio_data} e tenant {tenant_id} e modo-forcar não ativado. Pulando processamento.")
            continue

    df_envio = df_entregas[df_entregas["envio_data"] == envio_data]


    # Aqui você chama o uso do caso de clusterização para processar essa data
    use_case = ClusterizationUseCase(
        clustering_service=ClusteringService(UF_BOUNDS, random_state=42, max_clusters=15, logger=logger),
        k_min=args.k_min,
        k_max=args.k_max,
        min_entregas_por_cluster=args.min_entregas_por_cluster,
        fundir_clusters_pequenos=args.fundir_clusters_pequenos,
        usar_cluster_hub_central=not args.desativar_cluster_hub_central,
        raio_cluster_hub_central_km=args.raio_cluster_hub_central,
        centro_service=centro_service
    )

    df_clusterizado, df_centros, df_outliers = use_case.executar(df_envio)

    if df_clusterizado["id_entrega"].isna().any():
        qtd_nulos = df_clusterizado["id_entrega"].isna().sum()
        logger.warning(f"⚠ {qtd_nulos} registros com 'id_entrega' nulo serão descartados.")
        df_clusterizado = df_clusterizado[df_clusterizado["id_entrega"].notna()]

    # Ajustar centros urbanos
    df_clusterizado = centro_service.ajustar_centros(df_clusterizado)
    df_clusterizado["tenant_id"] = tenant_id

    resumo = (
    df_clusterizado
    .groupby("cluster")
    .agg(
        quantidade_entregas=("id_entrega", "count"),
        peso_total_kg=("cte_peso", "sum"),
        quantidade_volumes=("cte_volumes", "sum"),
        cte_valor_nf_total=("cte_valor_nf", "sum"),
        cte_valor_frete_total=("cte_valor_frete", "sum")
    )
    .reset_index()
)

df_resumo = pd.merge(resumo, df_centros, on="cluster", how="left")
df_resumo["distancia_media_km"] = 0
df_resumo["tempo_estimado_min"] = 0

if "cte_valor_nf_total" not in df_resumo.columns:
    df_resumo["cte_valor_nf_total"] = resumo["cte_valor_nf_total"].fillna(0)
if "cte_valor_frete_total" not in df_resumo.columns:
    df_resumo["cte_valor_frete_total"] = resumo["cte_valor_frete_total"].fillna(0)

writer.salvar_clusterizacao(df_clusterizado)

writer.salvar_resumo_clusters(
    df_resumo.assign(envio_data=envio_data, tenant_id=tenant_id),
    envio_data,
    tenant_id
)

logger.info(f"✅ Clusterização concluída com sucesso para {envio_data} ({tenant_id})")

fechar_conexao(conexao)
logger.info("🏁 Processo de clusterização finalizado para todas as datas.")

