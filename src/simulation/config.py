import logging
import psycopg2
import os
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

def conectar_db_simulation():
    return psycopg2.connect(
        host=os.getenv("SIM_DB_HOST", "localhost"),
        port=os.getenv("SIM_DB_PORT", 5432),
        dbname=os.getenv("SIM_DB_NAME", "simulation_db"),
        user=os.getenv("SIM_DB_USER", "postgres"),
        password=os.getenv("SIM_DB_PASSWORD", "postgres")
    )

def setup_logger():
    logger = logging.getLogger("simulation_logger")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        console_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger

# Limites geográficos por UF (latitude e longitude)
UF_BOUNDS = {
    'AC': {'lat_min': -11.14, 'lat_max': -7.33, 'lon_min': -73.83, 'lon_max': -66.65},
    'AL': {'lat_min': -10.57, 'lat_max': -8.02, 'lon_min': -38.50, 'lon_max': -35.05},
    'AP': {'lat_min': 0.02, 'lat_max': 4.47, 'lon_min': -54.03, 'lon_max': -49.95},
    'AM': {'lat_min': -10.73, 'lat_max': 2.22, 'lon_min': -73.99, 'lon_max': -56.10},
    'BA': {'lat_min': -17.95, 'lat_max': -7.53, 'lon_min': -46.65, 'lon_max': -37.00},
    'CE': {'lat_min': -7.88, 'lat_max': -2.75, 'lon_min': -41.53, 'lon_max': -37.35},
    'DF': {'lat_min': -16.03, 'lat_max': -15.60, 'lon_min': -48.20, 'lon_max': -47.30},
    'ES': {'lat_min': -21.19, 'lat_max': -17.90, 'lon_min': -41.89, 'lon_max': -39.20},
    'GO': {'lat_min': -18.04, 'lat_max': -12.99, 'lon_min': -53.96, 'lon_max': -46.50},
    'MA': {'lat_min': -9.68, 'lat_max': 0.15, 'lon_min': -48.45, 'lon_max': -41.82},
    'MT': {'lat_min': -17.58, 'lat_max': -7.12, 'lon_min': -61.00, 'lon_max': -50.25},
    'MS': {'lat_min': -24.99, 'lat_max': -17.51, 'lon_min': -58.53, 'lon_max': -52.63},
    'MG': {'lat_min': -22.90, 'lat_max': -14.12, 'lon_min': -51.10, 'lon_max': -39.86},
    'PA': {'lat_min': -9.37, 'lat_max': 1.29, 'lon_min': -56.39, 'lon_max': -48.06},
    'PB': {'lat_min': -8.32, 'lat_max': -6.02, 'lon_min': -38.47, 'lon_min': -34.79},
    'PR': {'lat_min': -26.72, 'lat_max': -22.55, 'lon_min': -54.63, 'lon_max': -48.04},
    'PE': {'lat_min': -9.53, 'lat_max': -7.33, 'lon_min': -42.06, 'lon_max': -34.78},
    'PI': {'lat_min': -10.93, 'lat_max': -2.82, 'lon_min': -45.98, 'lon_max': -40.66},
    'RJ': {'lat_min': -23.37, 'lat_max': -20.06, 'lon_min': -44.81, 'lon_max': -40.96},
    'RN': {'lat_min': -6.69, 'lat_max': -4.95, 'lon_min': -38.60, 'lon_max': -34.98},
    'RS': {'lat_min': -33.75, 'lat_max': -27.08, 'lon_min': -57.64, 'lon_max': -49.66},
    'RO': {'lat_min': -13.63, 'lat_max': -8.21, 'lon_min': -65.00, 'lon_max': -60.63},
    'RR': {'lat_min': 0.45, 'lat_max': 5.27, 'lon_min': -64.90, 'lon_max': -59.76},
    'SC': {'lat_min': -29.38, 'lat_max': -25.84, 'lon_min': -53.87, 'lon_max': -48.59},
    'SP': {'lat_min': -25.38, 'lat_max': -19.76, 'lon_min': -53.10, 'lon_max': -44.18},
    'SE': {'lat_min': -11.58, 'lat_max': -9.67, 'lon_min': -38.23, 'lon_max': -36.41},
    'TO': {'lat_min': -13.50, 'lat_max': -5.23, 'lon_min': -50.68, 'lon_max': -45.00}
}