# hub_router/src/data_input/application/geocode_batch_service.py

import pandas as pd

from data_input.services.geolocation_service import GeolocationService


class GeocodeBatchService:

    def __init__(self, reader):
        self.service = GeolocationService(reader=reader)

    def execute(self, df):

        df = df.copy()

        if "geocode_source" not in df.columns:
            df["geocode_source"] = None

        # -----------------------------------------
        # GEOCODE DIRETO (CACHE POR ENDEREÇO JÁ ESTÁ DENTRO DO SERVICE)
        # -----------------------------------------
        df = self.service.geocode_batch(df)

        return df