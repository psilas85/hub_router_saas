// src/pages/Users/UsersPage.tsx
import { useEffect, useMemo, useState } from "react";
import { useAuthStore } from "@/store/authStore";
import api from "@/services/api";

interface Usuario {
  id: string;
  nome: string;
  email: string;
  role: string;
  tenant_id?: string | null;
}

interface Tenant {
  id: string;
  nome: string;
  cnpj: string;
  plano: string;
  ativo: boolean;
  criado_em: string;
}

type Role =
  | "hub_admin"
  | "hub_operacional"
  | "cliente_admin"
  | "cliente_operacional";

const labelRole = (r: Role) =>
  r === "hub_admin"
    ? "Hub Admin"
    : r === "hub_operacional"
      ? "Hub Operacional"
      : r === "cliente_admin"
        ? "Cliente Admin"
        : "Cliente Operacional";

export default function UsersPage() {
  const { usuario: me, tenant: myTenant } = useAuthStore();

  const isHubAdmin = me?.role === "hub_admin";
  const isClienteAdmin = me?.role === "cliente_admin";

  const [usuarios, setUsuarios] = useState<Usuario[]>([]);
  const [tenants, setTenants] = useState<Tenant[]>([]);
  const [loading, setLoading] = useState(false);

  // criação
  const [creating, setCreating] = useState(false);
  const [form, setForm] = useState({
    nome: "",
    email: "",
    senha: "",
    role: (isHubAdmin ? "hub_admin" : "cliente_operacional") as Role,
    tenant_id: isClienteAdmin ? myTenant?.id || "" : "",
  });

  // edição
  const [editOpen, setEditOpen] = useState(false);
  const [editSaving, setEditSaving] = useState(false);
  const [editUserId, setEditUserId] = useState<string | null>(null);
  const [edit, setEdit] = useState<{
    nome: string;
    email: string;
    senha: string;
    role: Role;
    tenant_id: string;
  }>({
    nome: "",
    email: "",
    senha: "",
    role: "cliente_operacional",
    tenant_id: "",
  });

  const [deletingId, setDeletingId] = useState<string | null>(null);

  const allowedCreateRoles = useMemo<Role[]>(() => {
    if (isHubAdmin)
      return [
        "hub_admin",
        "hub_operacional",
        "cliente_admin",
        "cliente_operacional",
      ];
    if (isClienteAdmin) return ["cliente_operacional"];
    return [] as Role[];
  }, [isHubAdmin, isClienteAdmin]);

  async function fetchUsuarios() {
    try {
      setLoading(true);
      const { data } = await api.get<Usuario[]>("/auth/list");
      setUsuarios(data);
    } catch (err) {
      console.error("Erro ao carregar usuários:", err);
    } finally {
      setLoading(false);
    }
  }

  async function fetchTenants() {
    if (!isHubAdmin) return;
    try {
      const { data } = await api.get<Tenant[]>("/auth/tenants");
      setTenants(data);
    } catch (err) {
      console.error("Erro ao carregar tenants:", err);
    }
  }

  useEffect(() => {
    fetchUsuarios();
  }, []);

  useEffect(() => {
    fetchTenants();
  }, [isHubAdmin]);

  // ---------- CREATE ----------
  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!isHubAdmin && !isClienteAdmin) {
      alert("❌ Sem permissão para criar usuários.");
      return;
    }
    if (isClienteAdmin && !myTenant?.id) {
      alert("❌ Tenant não encontrado no seu perfil.");
      return;
    }
    try {
      setCreating(true);
      const payload = {
        ...form,
        tenant_id: isHubAdmin ? form.tenant_id : myTenant?.id || "",
      };
      await api.post("/auth/register", payload);
      alert("✅ Usuário cadastrado com sucesso!");
      setForm({
        nome: "",
        email: "",
        senha: "",
        role: isHubAdmin ? "hub_admin" : "cliente_operacional",
        tenant_id: isClienteAdmin ? myTenant?.id || "" : "",
      });
      await fetchUsuarios();
    } catch (err: any) {
      console.error(err);
      alert(
        "❌ Erro ao cadastrar usuário: " +
        (err.response?.data?.detail || err.message)
      );
    } finally {
      setCreating(false);
    }
  };

  // ---------- EDIT ----------
  const openEdit = (u: Usuario) => {
    if (isClienteAdmin && u.tenant_id !== myTenant?.id) {
      alert("❌ Você não pode editar usuários de outro tenant.");
      return;
    }
    setEditUserId(u.id);
    setEdit({
      nome: u.nome,
      email: u.email,
      senha: "",
      role: u.role as Role,
      tenant_id: u.tenant_id || "",
    });
    setEditOpen(true);
  };

  const handleEditSave = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!editUserId) return;
    try {
      setEditSaving(true);
      const payload: any = {
        nome: edit.nome,
        email: edit.email,
        role: edit.role,
      };
      if (edit.senha) payload.senha = edit.senha;
      if (isHubAdmin) payload.tenant_id = edit.tenant_id;

      await api.put(`/auth/${editUserId}`, payload);
      alert("✅ Usuário atualizado com sucesso!");
      setEditOpen(false);
      setEditUserId(null);
      await fetchUsuarios();
    } catch (err: any) {
      console.error(err);
      alert(
        "❌ Erro ao atualizar usuário: " +
        (err.response?.data?.detail || err.message)
      );
    } finally {
      setEditSaving(false);
    }
  };

  // ---------- DELETE ----------
  const handleDelete = async (u: Usuario) => {
    if (u.id === me?.id) {
      alert("❌ Você não pode remover a si mesmo.");
      return;
    }
    if (!confirm(`Remover o usuário "${u.nome}"?`)) return;

    try {
      setDeletingId(u.id);
      await api.delete(`/auth/${u.id}`);
      alert("🗑️ Usuário removido.");
      await fetchUsuarios();
    } catch (err: any) {
      console.error(err);
      alert(
        "❌ Erro ao remover usuário: " +
        (err.response?.data?.detail || err.message)
      );
    } finally {
      setDeletingId(null);
    }
  };

  return (
    <div className="max-w-6xl mx-auto p-6">
      <h1 className="text-xl font-bold mb-4">👥 Gestão de Usuários</h1>

      {/* Badge de diagnóstico */}
      <div className="mb-3 text-xs text-gray-600">
        <span className="px-2 py-1 rounded bg-gray-100">
          role: {me?.role ? labelRole(me.role as Role) : "-"} | tenant:{" "}
          {myTenant?.id || "-"}
        </span>
      </div>

      {/* Formulário de criação */}
      {(isHubAdmin || isClienteAdmin) && (
        <form
          onSubmit={handleCreate}
          className="space-y-3 bg-white p-4 rounded-lg shadow mb-6"
        >
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <input
              type="text"
              placeholder="Nome"
              value={form.nome}
              onChange={(e) => setForm({ ...form, nome: e.target.value })}
              className="w-full border rounded p-2"
              required
            />
            <input
              type="email"
              placeholder="Email"
              value={form.email}
              onChange={(e) => setForm({ ...form, email: e.target.value })}
              className="w-full border rounded p-2"
              required
            />
            <input
              type="password"
              placeholder="Senha"
              value={form.senha}
              onChange={(e) => setForm({ ...form, senha: e.target.value })}
              className="w-full border rounded p-2"
              required
            />

            {/* Roles com labels amigáveis */}
            <select
              value={form.role}
              onChange={(e) =>
                setForm({ ...form, role: e.target.value as Role })
              }
              className="w-full border rounded p-2"
            >
              {allowedCreateRoles.map((r) => (
                <option key={r} value={r}>
                  {labelRole(r)}
                </option>
              ))}
            </select>

            {isHubAdmin && (
              <select
                value={form.tenant_id}
                onChange={(e) =>
                  setForm({ ...form, tenant_id: e.target.value })
                }
                className="w-full border rounded p-2 md:col-span-2"
              >
                <option value="">Selecione um Tenant (opcional para HUB)</option>
                {tenants.map((t) => (
                  <option key={t.id} value={t.id}>
                    {t.nome} ({t.plano})
                  </option>
                ))}
              </select>
            )}
          </div>

          <button
            type="submit"
            disabled={creating}
            className="bg-emerald-600 text-white px-4 py-2 rounded hover:bg-emerald-700 disabled:opacity-60"
          >
            {creating ? "Criando..." : "Criar Usuário"}
          </button>
        </form>
      )}

      {/* Listagem */}
      <div className="mt-2">
        <div className="flex items-center justify-between mb-2">
          <h2 className="font-semibold">Usuários existentes</h2>
          <button
            onClick={fetchUsuarios}
            disabled={loading}
            className="text-sm bg-emerald-600 text-white px-3 py-1.5 rounded hover:bg-emerald-700 disabled:opacity-60"
          >
            {loading ? "Atualizando..." : "Atualizar"}
          </button>
        </div>

        {loading ? (
          <p>Carregando...</p>
        ) : usuarios.length === 0 ? (
          <p className="text-gray-600">Nenhum usuário encontrado.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full border-collapse border">
              <thead>
                <tr className="bg-emerald-100 text-left">
                  <th className="border px-3 py-2">Nome</th>
                  <th className="border px-3 py-2">Email</th>
                  <th className="border px-3 py-2">Role</th>
                  <th className="border px-3 py-2">Tenant</th>
                  {(isHubAdmin || isClienteAdmin) && (
                    <th className="border px-3 py-2 w-44">Ações</th>
                  )}
                </tr>
              </thead>
              <tbody>
                {usuarios.map((u) => {
                  const canAct =
                    isHubAdmin || (isClienteAdmin && u.tenant_id === myTenant?.id);
                  return (
                    <tr
                      key={u.id}
                      className="odd:bg-white even:bg-emerald-50/40"
                    >
                      <td className="border px-3 py-1">{u.nome}</td>
                      <td className="border px-3 py-1">{u.email}</td>
                      <td className="border px-3 py-1">
                        {labelRole(u.role as Role)}
                      </td>
                      <td className="border px-3 py-1">{u.tenant_id ?? "-"}</td>
                      {(isHubAdmin || isClienteAdmin) && (
                        <td className="border px-3 py-1">
                          <div className="flex gap-2">
                            <button
                              className="px-2 py-1 rounded bg-sky-600 text-white hover:bg-sky-700 disabled:opacity-60"
                              disabled={!canAct}
                              onClick={() => openEdit(u)}
                            >
                              Editar
                            </button>
                            <button
                              className="px-2 py-1 rounded bg-red-600 text-white hover:bg-red-700 disabled:opacity-60"
                              disabled={
                                !canAct || deletingId === u.id || u.id === me?.id
                              }
                              onClick={() => handleDelete(u)}
                            >
                              {deletingId === u.id
                                ? "Removendo..."
                                : "Remover"}
                            </button>
                          </div>
                        </td>
                      )}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Modal de edição */}
      {editOpen && (
        <div className="fixed inset-0 bg-black/40 z-40 flex items-center justify-center p-4">
          <div className="w-full max-w-lg bg-white rounded-xl shadow-lg p-5">
            <h3 className="text-lg font-semibold mb-3">Editar Usuário</h3>
            <form onSubmit={handleEditSave} className="space-y-3">
              <input
                type="text"
                placeholder="Nome"
                value={edit.nome}
                onChange={(e) => setEdit({ ...edit, nome: e.target.value })}
                className="w-full border rounded p-2"
                required
              />
              <input
                type="email"
                placeholder="Email"
                value={edit.email}
                onChange={(e) => setEdit({ ...edit, email: e.target.value })}
                className="w-full border rounded p-2"
                required
              />
              <input
                type="password"
                placeholder="Nova senha (opcional)"
                value={edit.senha}
                onChange={(e) => setEdit({ ...edit, senha: e.target.value })}
                className="w-full border rounded p-2"
              />

              {/* Roles com labels amigáveis */}
              <select
                value={edit.role}
                onChange={(e) =>
                  setEdit({ ...edit, role: e.target.value as Role })
                }
                className="w-full border rounded p-2"
              >
                {(isHubAdmin
                  ? ([
                    "hub_admin",
                    "hub_operacional",
                    "cliente_admin",
                    "cliente_operacional",
                  ] as Role[])
                  : (["cliente_operacional", "cliente_admin"] as Role[])
                ).map((r) => (
                  <option key={r} value={r}>
                    {labelRole(r)}
                  </option>
                ))}
              </select>

              {isHubAdmin && (
                <select
                  value={edit.tenant_id}
                  onChange={(e) =>
                    setEdit({ ...edit, tenant_id: e.target.value })
                  }
                  className="w-full border rounded p-2"
                >
                  <option value="">Sem tenant (hub)</option>
                  {tenants.map((t) => (
                    <option key={t.id} value={t.id}>
                      {t.nome} ({t.plano})
                    </option>
                  ))}
                </select>
              )}

              <div className="flex justify-end gap-2 pt-2">
                <button
                  type="button"
                  className="px-3 py-2 rounded border"
                  onClick={() => {
                    setEditOpen(false);
                    setEditUserId(null);
                  }}
                  disabled={editSaving}
                >
                  Cancelar
                </button>
                <button
                  type="submit"
                  className="px-3 py-2 rounded bg-emerald-600 text-white hover:bg-emerald-700 disabled:opacity-60"
                  disabled={editSaving}
                >
                  {editSaving ? "Salvando..." : "Salvar alterações"}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}
