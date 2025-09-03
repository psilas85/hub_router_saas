from dataclasses import dataclass
from typing import Optional
from datetime import date

@dataclass
class Entrega:
    cte_numero: Optional[str] = None
    remetente_cnpj: Optional[str] = None
    cte_rua: Optional[str] = None
    cte_bairro: Optional[str] = None
    cte_complemento: Optional[str] = None
    cte_cidade: Optional[str] = None
    cte_uf: Optional[str] = None
    cte_cep: Optional[str] = None
    cte_nf: Optional[str] = None
    cte_volumes: Optional[float] = None
    cte_peso: Optional[float] = None
    cte_valor_nf: Optional[float] = None
    cte_valor_frete: Optional[float] = None
    envio_data: Optional[date] = None
    endereco_completo: Optional[str] = None
    transportadora: Optional[str] = None
    remetente_nome: Optional[str] = None
    destinatario_nome: Optional[str] = None
    destinatario_cnpj: Optional[str] = None
    destino_latitude: Optional[float] = None
    destino_longitude: Optional[float] = None
    remetente_cidade: Optional[str] = None
    remetente_uf: Optional[str] = None
    doc_min: Optional[str] = None
    tenant_id: Optional[str] = None