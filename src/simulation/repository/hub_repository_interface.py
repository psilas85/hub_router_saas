#repository/hub_repository.py

from abc import ABC, abstractmethod

class HubRepositoryInterface(ABC):
    @abstractmethod
    def inserir_hub(self, tenant_id: str, nome: str, endereco: str, lat: float, lon: float):
        pass

    @abstractmethod
    def remover_hub(self, tenant_id: str, nome: str):
        pass
