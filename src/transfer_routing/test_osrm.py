# test_osrm_fallback.py

import psycopg2
from transfer_routing.infrastructure.geolocation import get_route

# Config do banco de teste
DB_CONFIG = {
    "dbname": "routing_db_utf8",
    "user": "postgres",
    "password": "Psilas@85",
    "host": "postgres",  # ✅ aqui em vez de "localhost"
    "port": 5432
}


# Config de teste
TENANT_ID = "teste-osrm"
ORIGEM = (-3.7319, -38.5267)   # Fortaleza, CE
DESTINO = (-3.8777, -38.6259)  # Pacatuba, CE

def main():
    conn = psycopg2.connect(**DB_CONFIG)

    print("\n🧹 Limpando cache para rota de teste...")
    with conn.cursor() as cur:
        cur.execute("DELETE FROM cache_rotas WHERE origem LIKE %s AND destino LIKE %s", 
                    (f"{ORIGEM[0]:.6f}%", f"{DESTINO[0]:.6f}%"))
        conn.commit()

    print("\n🚀 Primeira execução (deve chamar OSRM)...")
    distancia, tempo, rota = get_route(ORIGEM, DESTINO, TENANT_ID, conn)
    print(f"Distância: {distancia:.2f} km | Tempo: {tempo:.2f} min | Coordenadas: {len(rota.get('coordenadas', []))} pontos")

    print("\n🚀 Segunda execução (deve pegar do cache)...")
    distancia, tempo, rota = get_route(ORIGEM, DESTINO, TENANT_ID, conn)
    print(f"Distância: {distancia:.2f} km | Tempo: {tempo:.2f} min | Coordenadas: {len(rota.get('coordenadas', []))} pontos")

    conn.close()

if __name__ == "__main__":
    main()
