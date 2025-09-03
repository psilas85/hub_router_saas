# authentication/infrastructure/auth_repository.py

import uuid
from datetime import datetime
from authentication.domain.entities import Usuario, Tenant
from typing import Optional


class AuthRepository:
    def __init__(self, conn):
        if conn is None:
            raise ValueError("❌ Conexão com o banco de dados falhou e é None.")
        self.conn = conn


    def buscar_usuario_por_email(self, email: str) -> Optional[Usuario]:

        query = """
        SELECT id, tenant_id, nome, email, senha_hash, role, ativo, criado_em
        FROM usuarios
        WHERE email = %s
        LIMIT 1
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (email,))
            row = cur.fetchone()
            if not row:
                return None
            return Usuario(*row)

    def criar_usuario(self, usuario: Usuario) -> None:
        query = """
        INSERT INTO usuarios (id, tenant_id, nome, email, senha_hash, role, ativo, criado_em)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (
                usuario.id,
                usuario.tenant_id,
                usuario.nome,
                usuario.email,
                usuario.senha_hash,
                usuario.role,
                usuario.ativo,
                usuario.criado_em,
            ))
        self.conn.commit()

    def buscar_tenant_por_id(self, tenant_id: str) -> Optional[Tenant]:

        query = """
        SELECT id, nome, cnpj, plano, ativo, criado_em
        FROM tenants
        WHERE id = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (tenant_id,))
            row = cur.fetchone()
            if not row:
                return None
            return Tenant(*row)
