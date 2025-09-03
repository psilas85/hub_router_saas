#authentication/domain/entities.py

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Tenant:
    id: str
    nome: str
    cnpj: str
    plano: str  # 'basic' ou 'simulation'
    ativo: bool
    criado_em: datetime

@dataclass
class Usuario:
    id: str
    tenant_id: str
    nome: str
    email: str
    senha_hash: str
    role: str  # 'admin', 'operador', etc.
    ativo: bool
    criado_em: datetime

@dataclass
class UsuarioToken:
    id: str
    tenant_id: str
    role: str
    plano: str
