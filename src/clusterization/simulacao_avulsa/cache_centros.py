import json
import os

class CentroCache:
    def __init__(self, caminho_arquivo=None):
        pasta_base = os.path.dirname(__file__)
        self.caminho = caminho_arquivo or os.path.join(pasta_base, "centros_cache.json")
        self.dados = self._carregar_cache()

    def _carregar_cache(self):
        if os.path.exists(self.caminho):
            with open(self.caminho, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def salvar(self):
        with open(self.caminho, "w", encoding="utf-8") as f:
            json.dump(self.dados, f, ensure_ascii=False, indent=2)

    def obter(self, endereco):
        return self.dados.get(endereco)

    def atualizar(self, endereco, lat, lon):
        self.dados[endereco] = [lat, lon]
        self.salvar()
