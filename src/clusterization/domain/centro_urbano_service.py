import pandas as pd
from clusterization.infrastructure.database_reader import DatabaseReader


class CentroUrbanoService:
    def __init__(self, reader: DatabaseReader):
        self.reader = reader

    def ajustar_centros(self, df_clusterizado: pd.DataFrame) -> pd.DataFrame:
        """
        Para cada cluster, busca um endereço central urbano e ajusta lat/lon e nome da cidade.
        Para cluster hub_central (9999), mantém as informações do hub.
        """
        if df_clusterizado.empty or 'cluster' not in df_clusterizado.columns:
            raise ValueError("DataFrame inválido ou sem coluna 'cluster'.")

        for cluster_id in df_clusterizado['cluster'].unique():
            cluster_df = df_clusterizado[df_clusterizado['cluster'] == cluster_id]

            centro_lat = cluster_df['centro_lat'].mean()
            centro_lon = cluster_df['centro_lon'].mean()

            if cluster_id == 9999:
                # Cluster do Hub Central já tem o endereço e coordenadas definidas
                cluster_endereco = "HUB_CENTRAL"
                novo_lat = centro_lat
                novo_lon = centro_lon
            else:
                cluster_endereco, (novo_lat, novo_lon) = self.reader.buscar_centro_urbano(centro_lat, centro_lon)

                if not cluster_endereco:
                    # fallback: mantém as coordenadas médias
                    cluster_endereco = "Centro desconhecido"
                    novo_lat = centro_lat
                    novo_lon = centro_lon

            df_clusterizado.loc[df_clusterizado["cluster"] == cluster_id, "cluster_cidade"] = cluster_endereco
            df_clusterizado.loc[df_clusterizado["cluster"] == cluster_id, "centro_lat"] = novo_lat
            df_clusterizado.loc[df_clusterizado["cluster"] == cluster_id, "centro_lon"] = novo_lon

        return df_clusterizado

    def buscar_hub_central(self, tenant_id: str) -> tuple:
        """
        Retorna latitude e longitude do hub central do tenant.
        """
        resultado = self.reader.buscar_hub_central(tenant_id)
        if resultado:
            latitude, longitude = resultado
            return latitude, longitude
        else:
            raise ValueError(f"❌ Hub Central não encontrado para tenant '{tenant_id}'")
