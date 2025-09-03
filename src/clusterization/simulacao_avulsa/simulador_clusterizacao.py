import os
import time
import requests
import logging
import pandas as pd
from dotenv import load_dotenv
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from geopy.distance import geodesic

from clusterization.infrastructure.db import Database
from clusterization.config import UF_BOUNDS, CLUSTERING_CONFIG
from clusterization.visualization.cluster_plotter import ClusterPlotter
from clusterization.simulacao_avulsa.cache_centros import CentroCache


load_dotenv()
logging.basicConfig(level=logging.INFO)

class ClusterSimulationService:
    def __init__(self):
        self.db = Database()
        self.db.conectar()
        self.geolocator = Nominatim(user_agent="cluster_simulador")
        self.GOOGLE_MAPS_API_KEY = os.getenv("GMAPS_API_KEY")
        self.cache_centros = CentroCache()

    def buscar_entregas(self, data_inicio, data_fim):
        conn = self.db.conexao
        query = """
            SELECT * FROM entregas
            WHERE envio_data BETWEEN %s AND %s
        """
        df = pd.read_sql(query, conn, params=(data_inicio, data_fim))
        df = df.rename(columns={
            "destino_latitude": "lat",
            "destino_longitude": "lon"
        })
        for col in ["cluster", "centro_lat", "centro_lon"]:
            if col not in df.columns:
                df[col] = None
        return df

    def buscar_datas_unicas(self, data_inicio, data_fim):
        conn = self.db.conexao
        query = """
            SELECT DISTINCT envio_data FROM entregas
            WHERE envio_data BETWEEN %s AND %s
            ORDER BY envio_data
        """
        datas = pd.read_sql(query, conn, params=(data_inicio, data_fim))
        datas["envio_data"] = pd.to_datetime(datas["envio_data"])
        return datas["envio_data"].dt.date.tolist()

    def buscar_lat_lon_com_fallback(self, endereco, tentativas=3):
        for tentativa in range(tentativas):
            try:
                location = self.geolocator.geocode(endereco, timeout=10)
                if location:
                    logging.info(f"[Nominatim] {endereco} → {location.latitude}, {location.longitude}")
                    return location.latitude, location.longitude
            except GeocoderTimedOut:
                logging.warning(f"[Nominatim Timeout] {endereco}")
                time.sleep(2)
            except Exception as e:
                logging.error(f"[Nominatim Erro] {e}")
                break

        logging.warning(f"[Fallback Google] {endereco}")
        return self.buscar_lat_lon_google(endereco)

    def buscar_lat_lon_google(self, endereco):
        if not self.GOOGLE_MAPS_API_KEY:
            logging.error("API key do Google Maps ausente.")
            return None, None

        url = f"https://maps.googleapis.com/maps/api/geocode/json?address={endereco}&key={self.GOOGLE_MAPS_API_KEY}"
        try:
            response = requests.get(url, timeout=10)
            data = response.json()
            if data["status"] == "OK":
                location = data["results"][0]["geometry"]["location"]
                logging.info(f"[Google Maps] {endereco} → {location['lat']}, {location['lng']}")
                return location["lat"], location["lng"]
            else:
                logging.warning(f"[Google Maps Falha] {endereco}")
                return None, None
        except Exception as e:
            logging.error(f"[Google Maps Erro] {e}")
            return None, None

    def geocodificar_centros(self, enderecos):
        centros_geolocalizados = []
        for endereco in enderecos:
            # Verifica cache primeiro
            coordenadas = self.cache_centros.obter(endereco)
            if coordenadas:
                lat, lon = coordenadas
                logging.info(f"[CACHE] {endereco} → {lat}, {lon}")
            else:
                lat, lon = self.buscar_lat_lon_com_fallback(endereco)
                if lat is not None and lon is not None:
                    self.cache_centros.atualizar(endereco, lat, lon)

            centros_geolocalizados.append({
                "endereco": endereco,
                "lat": lat,
                "lon": lon
            })

        return centros_geolocalizados


    def atribuir_entregas_a_centros(self, entregas_df, centros_geolocalizados):
        resultados = []
        for _, entrega in entregas_df.iterrows():
            if pd.isnull(entrega["lat"]) or pd.isnull(entrega["lon"]):
                continue
            entrega_coord = (entrega["lat"], entrega["lon"])
            menor_distancia = float("inf")
            centro_mais_proximo = None
            cluster_id = None

            for idx, centro in enumerate(centros_geolocalizados):
                centro_coord = (centro["lat"], centro["lon"])
                distancia = geodesic(entrega_coord, centro_coord).km
                if distancia < menor_distancia:
                    menor_distancia = distancia
                    centro_mais_proximo = centro
                    cluster_id = idx

            entrega_dict = entrega.to_dict()
            entrega_dict.update({
                "cluster": cluster_id,
                "cluster_endereco": centro_mais_proximo["endereco"],
                "centro_lat": centro_mais_proximo["lat"],
                "centro_lon": centro_mais_proximo["lon"]
            })
            resultados.append(entrega_dict)

        return pd.DataFrame(resultados)

    def plotar_mapa_simulacao(self, df_resultado, envio_data, output_dir="simulacao_output"):
        try:
            plotter = ClusterPlotter(self.db, output_dir)
            df_resultado["envio_data"] = pd.to_datetime(envio_data).date()
            df_resultado["destino_latitude"] = df_resultado["lat"]
            df_resultado["destino_longitude"] = df_resultado["lon"]

            plotter.generate_map(df_resultado, df_resultado["envio_data"].iloc[0])
        except Exception as e:
            logging.error(f"❌ Erro ao gerar mapa de simulação: {e}")

    def executar_simulacao(self, input_simulacao):
        centros = input_simulacao["centros_clusters"]
        data_inicio = input_simulacao["data_inicio"]
        data_fim = input_simulacao["data_fim"]

        centros_geo = self.geocodificar_centros(centros)
        datas_unicas = self.buscar_datas_unicas(data_inicio, data_fim)

        output_dir = os.path.join(os.path.dirname(__file__), "simulacao_output")
        os.makedirs(output_dir, exist_ok=True)

        resultados_por_data = {}

        for data in datas_unicas:
            logging.info(f"🔄 Processando data: {data}")
            entregas_dia = self.buscar_entregas(data, data)
            if entregas_dia.empty:
                logging.warning(f"⚠️ Nenhuma entrega encontrada para {data}")
                continue

            resultado_dia = self.atribuir_entregas_a_centros(entregas_dia, centros_geo)
            self.plotar_mapa_simulacao(resultado_dia, data)

            nome_arquivo = os.path.join(output_dir, f"entregas_clusterizadas_{data}.csv")
            resultado_dia.to_csv(nome_arquivo, index=False)

            resultados_por_data[str(data)] = resultado_dia

        return resultados_por_data
