// src/pages/Tenants/TenantsPage.tsx
import { useEffect, useMemo, useState } from "react";
import { useAuthStore } from "@/store/authStore";
import api from "@/services/api";

type Plano = "basic" | "simulation";

interface Tenant {
    id: string;
    nome: string;
    cnpj: string;
    plano: Plano;
    ativo: boolean;
    criado_em: string;
}

export default function TenantsPage() {
    const { usuario, tenant: myTenant } = useAuthStore();
    const isHubAdmin = usuario?.role === "hub_admin";

    console.log("🔎 [TenantsPage] usuario:", usuario);
    console.log("🔎 [TenantsPage] role:", usuario?.role);
    console.log("🔎 [TenantsPage] isHubAdmin:", isHubAdmin);

    const [tenants, setTenants] = useState<Tenant[]>([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    // criação
    const [saving, setSaving] = useState(false);
    const [form, setForm] = useState<{ nome: string; cnpj: string; plano: Plano }>({
        nome: "",
        cnpj: "",
        plano: "basic",
    });

    // edição
    const [editOpen, setEditOpen] = useState(false);
    const [editSaving, setEditSaving] = useState(false);
    const [edit, setEdit] = useState<{
        id: string;
        nome: string;
        cnpj: string;
        plano: Plano;
        ativo: boolean;
    } | null>(null);

    // remoção
    const [deletingId, setDeletingId] = useState<string | null>(null);

    // helpers CNPJ
    const cnpjLimpo = (v: string) => v.replace(/\D/g, "");
    const cnpjValidoCreate = useMemo(() => cnpjLimpo(form.cnpj).length === 14, [form.cnpj]);
    const cnpjValidoEdit = useMemo(() => (edit ? cnpjLimpo(edit.cnpj).length === 14 : true), [edit]);

    async function fetchTenants() {
        if (!isHubAdmin) return;
        try {
            setLoading(true);
            setError(null);
            const { data } = await api.get<Tenant[]>("/auth/tenants");
            setTenants(data);
        } catch (err: any) {
            console.error(err);
            setError(err.response?.data?.detail || err.message || "Erro ao carregar tenants");
        } finally {
            setLoading(false);
        }
    }

    useEffect(() => {
        fetchTenants();
    }, [isHubAdmin]);

    // CREATE
    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        if (!cnpjValidoCreate) {
            alert("CNPJ inválido. Informe 14 dígitos (com ou sem máscara).");
            return;
        }
        try {
            setSaving(true);
            setError(null);
            await api.post("/auth/tenants", {
                nome: form.nome,
                cnpj: cnpjLimpo(form.cnpj),
                plano: form.plano,
            });
            alert("✅ Tenant criado com sucesso!");
            setForm({ nome: "", cnpj: "", plano: "basic" }); // limpa tudo
            await fetchTenants();
        } catch (err: any) {
            console.error(err);
            const msg = err.response?.data?.detail || err.message || "Erro ao criar tenant";
            setError(msg);
            alert("❌ " + msg);
        } finally {
            setSaving(false);
        }
    };

    // OPEN EDIT
    const openEdit = (t: Tenant) => {
        setEdit({
            id: t.id,
            nome: t.nome,
            cnpj: t.cnpj,
            plano: t.plano,
            ativo: t.ativo,
        });
        setEditOpen(true);
    };

    // SAVE EDIT
    const handleEditSave = async (e: React.FormEvent) => {
        e.preventDefault();
        if (!edit) return;
        if (!cnpjValidoEdit) {
            alert("CNPJ inválido na edição. Informe 14 dígitos.");
            return;
        }
        try {
            setEditSaving(true);
            await api.put(`/auth/tenants/${edit.id}`, {
                nome: edit.nome,
                cnpj: cnpjLimpo(edit.cnpj),
                plano: edit.plano,
                ativo: edit.ativo,
            });
            alert("✅ Tenant atualizado com sucesso!");
            setEditOpen(false);
            setEdit(null);
            await fetchTenants();
        } catch (err: any) {
            console.error(err);
            const msg = err.response?.data?.detail || err.message || "Erro ao atualizar tenant";
            alert("❌ " + msg);
        } finally {
            setEditSaving(false);
        }
    };

    // DELETE
    const handleDelete = async (t: Tenant) => {
        if (!confirm(`Remover tenant "${t.nome}"? Esta ação é irreversível.`)) return;
        try {
            setDeletingId(t.id);
            await api.delete(`/auth/tenants/${t.id}`);
            alert("🗑️ Tenant removido.");
            await fetchTenants();
        } catch (err: any) {
            console.error(err);
            const msg = err.response?.data?.detail || err.message || "Erro ao remover tenant";
            alert("❌ " + msg);
        } finally {
            setDeletingId(null);
        }
    };

    if (!isHubAdmin) {
        return (
            <p className="p-6 text-red-600">
                ❌ Apenas Hub Admin pode acessar esta página
            </p>
        );
    }

    return (
        <div className="max-w-6xl mx-auto p-6">
            <div className="flex items-center justify-between mb-4">
                <h1 className="text-xl font-bold">🏢 Gestão de Tenants</h1>
                <button
                    onClick={fetchTenants}
                    disabled={loading}
                    className="bg-emerald-600 text-white px-3 py-2 rounded hover:bg-emerald-700 disabled:opacity-60"
                >
                    {loading ? "Atualizando..." : "Atualizar"}
                </button>
            </div>

            {/* Badge de diagnóstico */}
            <div className="mb-3 text-xs text-gray-600">
                <span className="px-2 py-1 rounded bg-gray-100">
                    role: {usuario?.role || "-"} | tenant: {myTenant?.id || "-"}
                </span>
            </div>

            {/* Formulário de criação */}
            <form onSubmit={handleSubmit} className="space-y-3 bg-white p-4 rounded-lg shadow">
                <input
                    type="text"
                    placeholder="Nome da empresa"
                    value={form.nome}
                    onChange={(e) => setForm({ ...form, nome: e.target.value })}
                    className="w-full border rounded p-2"
                    required
                />

                <input
                    type="text"
                    placeholder="CNPJ (apenas números ou com máscara)"
                    value={form.cnpj}
                    onChange={(e) => setForm({ ...form, cnpj: e.target.value })}
                    className="w-full border rounded p-2"
                    required
                />
                {!cnpjValidoCreate && form.cnpj && (
                    <p className="text-xs text-amber-600">CNPJ deve ter 14 dígitos.</p>
                )}

                <select
                    value={form.plano}
                    onChange={(e) => setForm({ ...form, plano: e.target.value as Plano })}
                    className="w-full border rounded p-2"
                >
                    <option value="basic">Basic</option>
                    <option value="simulation">Simulation</option>
                </select>

                <button
                    type="submit"
                    disabled={saving}
                    className="bg-emerald-600 text-white px-4 py-2 rounded hover:bg-emerald-700 disabled:opacity-60"
                >
                    {saving ? "Criando..." : "Criar Tenant"}
                </button>
            </form>

            {/* Listagem */}
            <div className="mt-6">
                <div className="flex items-center gap-3 mb-2">
                    <h2 className="font-semibold">Tenants existentes</h2>
                    {error && <span className="text-sm text-red-600">{error}</span>}
                </div>

                {loading ? (
                    <p>Carregando...</p>
                ) : tenants.length === 0 ? (
                    <p className="text-gray-600">Nenhum tenant encontrado.</p>
                ) : (
                    <div className="overflow-x-auto">
                        <table className="min-w-full border-collapse border">
                            <thead>
                                <tr className="bg-emerald-100 text-left">
                                    <th className="border px-3 py-2">Nome</th>
                                    <th className="border px-3 py-2">CNPJ</th>
                                    <th className="border px-3 py-2">Plano</th>
                                    <th className="border px-3 py-2">Ativo</th>
                                    <th className="border px-3 py-2">Criado em</th>
                                    <th className="border px-3 py-2 w-44">Ações</th>
                                </tr>
                            </thead>
                            <tbody>
                                {tenants.map((t) => (
                                    <tr key={t.id} className="odd:bg-white even:bg-emerald-50/40">
                                        <td className="border px-3 py-1">{t.nome}</td>
                                        <td className="border px-3 py-1">{t.cnpj}</td>
                                        <td className="border px-3 py-1">{t.plano}</td>
                                        <td className="border px-3 py-1">{t.ativo ? "✅" : "❌"}</td>
                                        <td className="border px-3 py-1">
                                            {t.criado_em ? new Date(t.criado_em).toLocaleString() : "-"}
                                        </td>
                                        <td className="border px-3 py-1">
                                            <div className="flex gap-2">
                                                <button
                                                    className="px-2 py-1 rounded bg-sky-600 text-white hover:bg-sky-700"
                                                    onClick={() => openEdit(t)}
                                                >
                                                    Editar
                                                </button>
                                                <button
                                                    className="px-2 py-1 rounded bg-red-600 text-white hover:bg-red-700 disabled:opacity-60"
                                                    disabled={deletingId === t.id}
                                                    onClick={() => handleDelete(t)}
                                                >
                                                    {deletingId === t.id ? "Removendo..." : "Remover"}
                                                </button>
                                            </div>
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                )}
            </div>

            {/* Modal de edição */}
            {editOpen && edit && (
                <div className="fixed inset-0 bg-black/40 z-40 flex items-center justify-center p-4">
                    <div className="w-full max-w-lg bg-white rounded-xl shadow-lg p-5">
                        <h3 className="text-lg font-semibold mb-3">Editar Tenant</h3>
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
                                type="text"
                                placeholder="CNPJ (apenas números ou com máscara)"
                                value={edit.cnpj}
                                onChange={(e) => setEdit({ ...edit, cnpj: e.target.value })}
                                className="w-full border rounded p-2"
                                required
                            />
                            {!cnpjValidoEdit && edit.cnpj && (
                                <p className="text-xs text-amber-600">CNPJ deve ter 14 dígitos.</p>
                            )}

                            <select
                                value={edit.plano}
                                onChange={(e) => setEdit({ ...edit, plano: e.target.value as Plano })}
                                className="w-full border rounded p-2"
                            >
                                <option value="basic">Basic</option>
                                <option value="simulation">Simulation</option>
                            </select>

                            <label className="flex items-center gap-2 text-sm">
                                <input
                                    type="checkbox"
                                    checked={edit.ativo}
                                    onChange={(e) => setEdit({ ...edit, ativo: e.target.checked })}
                                />
                                Ativo
                            </label>

                            <div className="flex justify-end gap-2 pt-2">
                                <button
                                    type="button"
                                    className="px-3 py-2 rounded border"
                                    onClick={() => {
                                        setEditOpen(false);
                                        setEdit(null);
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
