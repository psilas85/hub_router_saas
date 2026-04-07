#hub_router_1.0.1/src/data_input/application/geo_validator.py

from data_input.config.config import UF_BOUNDS


class GeoValidator:

    def is_generic_location(self, lat, lon):
        """
        Detecta se são coordenadas genéricas de centro de cidade.
        Ex: Sé de SP (-23.5505, -46.6333), Centro RJ (-22.9068, -43.1729)
        """
        import math

        # Centros de cidades conhecidas (Brasil)
        GENERIC_CENTERS = {
            'SP': (-23.5505, -46.6333),
            'RJ': (-22.9068, -43.1729),
            'BH': (-19.9167, -43.9345),
            'CWB': (-25.4284, -49.2733),
            'POA': (-30.0331, -51.2304),
            'BA': (-12.9714, -38.5014),
            'CE': (-3.7319, -38.5267),
            'PE': (-8.0476, -34.8770),
            'GO': (-15.7939, -48.0694),
            'SC': (-27.2423, -49.6439),
        }

        # Threshold: ±0.001 graus ≈ ±100-110 metros
        THRESHOLD = 0.001

        for city, (center_lat, center_lon) in GENERIC_CENTERS.items():
            distance = math.sqrt(
                (lat - center_lat) ** 2 +
                (lon - center_lon) ** 2
            )

            if distance < THRESHOLD:
                return True

        return False

    def validar_ponto(self, lat, lon, uf):

        # ---------------------------
        # básico
        # ---------------------------
        if lat is None or lon is None:
            return "falha"

        try:
            lat = float(lat)
            lon = float(lon)
        except:
            return "falha"

        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return "falha"

        # 🔥 NOVO: Rejeita coordenadas genéricas
        if self.is_generic_location(lat, lon):
            return "falha"

        # ---------------------------
        # UF
        # ---------------------------
        if not uf:
            return "falha"

        uf = str(uf).upper().strip()

        bounds = UF_BOUNDS.get(uf)

        if not bounds:
            return "falha"

        # ---------------------------
        # UF bounds
        # ---------------------------
        if not (
            bounds["lat_min"] <= lat <= bounds["lat_max"]
            and bounds["lon_min"] <= lon <= bounds["lon_max"]
        ):
            return "fora_uf"

        return "ok"

