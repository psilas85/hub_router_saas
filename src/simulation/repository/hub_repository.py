#repository/hub_repository.py

class HubRepository:
    def __init__(self, db):
        self.db = db

    def adicionar_hub(self, tenant_id, nome, endereco, latitude, longitude):
        cursor = self.db.cursor()
        cursor.execute("""
            INSERT INTO hubs (tenant_id, nome, endereco, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, endereco) DO NOTHING
        """, (tenant_id, nome, endereco, latitude, longitude))
        self.db.commit()
        cursor.close()

    def remover_hub(self, tenant_id, endereco):
        cursor = self.db.cursor()
        cursor.execute("""
            DELETE FROM hubs WHERE tenant_id = %s AND endereco = %s
        """, (tenant_id, endereco))
        self.db.commit()
        cursor.close()

    def listar_hubs(self, tenant_id):
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT id, nome, endereco, latitude, longitude
            FROM hubs
            WHERE tenant_id = %s
            ORDER BY nome
        """, (tenant_id,))
        hubs = cursor.fetchall()
        cursor.close()
        return hubs
