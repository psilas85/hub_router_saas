#last_mile_routing/domain/route_calculator.py

from last_mile_routing.infrastructure.geolocation_service import calcular_distancia_km, GoogleDirectionsService
from last_mile_routing.infrastructure.cache_route import save_cache
from last_mile_routing.infrastructure.osrm_service import OSRMRouteService


class RotaCalculator:
    def __init__(self, tenant_id, cache, api_key, osrm_url="http://osrm:5000"):
        self.tenant_id = tenant_id
        self.cache = cache or {}
        self.osrm_service = OSRMRouteService(osrm_url)
        self.gmaps_service = GoogleDirectionsService(api_key)

    def calcular_rota(self, pontos):
        """
        Calcula distância geodésica simples entre os pontos (sem consulta de rota real).
        """
        total_distancia = 0
        sequencia = []

        for i in range(len(pontos) - 1):
            origem = pontos[i]
            destino = pontos[i + 1]
            chave = f"{origem}-{destino}"

            if chave in self.cache:
                item_cache = self.cache[chave]
                distancia = item_cache["distancia"] if isinstance(item_cache, dict) else item_cache
                print(f"📦 Cache encontrado (distância geodésica) para {chave}")
            else:
                distancia = calcular_distancia_km(origem, destino)
                self.cache[chave] = {
                    "rota": None,
                    "distancia": distancia,
                    "tempo": None
                }
                print(f"➕ Cache inicializado com distância geodésica para {chave}")

            total_distancia += distancia
            sequencia.append((origem, destino, distancia))

        return total_distancia, sequencia

    def obter_tracado_rota(self, origem, destinos):
        """
        Retorna a rota completa usando:
        1️⃣ Cache → 2️⃣ OSRM → 3️⃣ Google Directions
        """
        pontos = [origem] + destinos
        rota_completa = []
        distancia_total = 0
        tempo_total = 0

        for i in range(len(pontos) - 1):
            origem_p = pontos[i]
            destino_p = pontos[i + 1]
            chave = f"{origem_p}-{destino_p}"

            print(f"🔍 Checando cache para {chave}...")
            # 1️⃣ Cache
            if chave in self.cache and self.cache[chave].get("rota") is not None:
                print(f"📦 Cache encontrado para {chave}")
                rota = self.cache[chave]["rota"]
                distancia = self.cache[chave]["distancia"]
                tempo = self.cache[chave]["tempo"]

            else:
                # 2️⃣ OSRM
                print(f"🗺 Tentando via OSRM para {chave}...")
                rota, distancia, tempo = self.osrm_service.consultar_rota(origem_p, destino_p)

                # 3️⃣ Google Directions se OSRM falhar
                if distancia == 0:
                    print(f"🌍 Tentando via Google Maps para {chave}...")
                    rota, distancia, tempo = self.gmaps_service.consultar_rota(origem_p, destino_p)

                # Salva no cache
                self.cache[chave] = {
                    "rota": rota,
                    "distancia": distancia,
                    "tempo": tempo
                }
                print(f"💾 Cache atualizado para {chave} (distância: {distancia:.2f} km, tempo: {tempo:.2f} min)")

            rota_completa.extend(rota)
            distancia_total += distancia
            tempo_total += tempo

        return rota_completa, distancia_total, tempo_total

    def salvar_cache(self):
        save_cache(self.tenant_id, self.cache)
