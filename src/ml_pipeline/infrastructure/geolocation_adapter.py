# hub_router_1.0.1/src/ml_pipeline/infrastructure/geolocation_adapter.py

import logging
from clusterization.domain.geolocalizacao_service import GeolocalizacaoService
from clusterization.infrastructure.database_reader import DatabaseReader
from clusterization.infrastructure.database_writer import DatabaseWriter
from clusterization.infrastructure.database_connection import conectar_banco


class GeolocationAdapter:
    """
    Adapter para expor um método simples get_latlon(cidade, uf),
    compatível com o StructureOptimizer do ml_pipeline.
    """
    def __init__(self):
        # 🔑 Usa conexão padrão já configurada via .env
        conn = conectar_banco()

        self.geo = GeolocalizacaoService(
            reader=DatabaseReader(conn),
            writer=DatabaseWriter(conn)
        )

    def get_latlon(self, cidade: str, uf: str):
        endereco = f"{cidade}, {uf}, Brasil"
        logging.debug(f"🔎 GeolocationAdapter: buscando coordenadas para {endereco}")
        coords = self.geo.buscar_coordenadas(endereco, uf_esperada=uf)
        if not coords:
            logging.warning(f"⚠️ Coordenadas não encontradas para {cidade}/{uf}")
            return (None, None)
        return coords
