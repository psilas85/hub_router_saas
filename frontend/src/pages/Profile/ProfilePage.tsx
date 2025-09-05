// src/pages/Profile/ProfilePage.tsx
import { useAuthGuard } from "@/hooks/useAuthGuard";
import { useApi } from "@/hooks/useApi";
import { useAuthStore } from "@/store/authStore";
import { LogOut, Save } from "lucide-react";
import { useState } from "react";
import axios from "axios";

export default function ProfilePage() {
    const { usuario, tenant } = useAuthGuard();
    const { logout } = useAuthStore();

    const { data, loading, error, execute } = useApi<{ user: any }>({
        url: "/auth/me",
    });

    const [nome, setNome] = useState(usuario?.nome ?? "");
    const [senha, setSenha] = useState("");
    const [saving, setSaving] = useState(false);
    const [msg, setMsg] = useState<string | null>(null);

    const handleLogout = () => {
        logout();
        window.location.href = "/login";
    };

    const handleSave = async () => {
        try {
            setSaving(true);
            setMsg(null);

            await axios.put("/auth/me", {
                nome,
                ...(senha ? { senha } : {}),
            });

            setMsg("✅ Alterações salvas com sucesso!");
            setSenha("");

            await execute(); // 🔑 reexecuta a chamada para atualizar os dados
        } catch (err: any) {
            console.error(err);
            setMsg("❌ Erro ao salvar alterações.");
        } finally {
            setSaving(false);
        }
    };

    return (
        <div className="p-6 max-w-3xl mx-auto">
            {/* Cabeçalho */}
            <div className="flex justify-between items-center mb-8">
                <h1 className="text-3xl font-bold text-gray-800">Meu Perfil</h1>
                <button
                    onClick={handleLogout}
                    className="flex items-center gap-2 bg-red-600 text-white px-4 py-2 rounded-xl hover:bg-red-700 transition"
                >
                    <LogOut size={18} />
                    Sair
                </button>
            </div>

            {/* Loading e Erro */}
            {loading && (
                <div className="text-center py-6 text-gray-500 animate-pulse">
                    Carregando informações...
                </div>
            )}
            {error && (
                <div className="text-center py-6 text-red-600">
                    ⚠️ Erro ao carregar: {String(error)}
                </div>
            )}

            {/* Dados e edição */}
            {data && (
                <div className="space-y-6">
                    {/* Card principal */}
                    <div className="bg-white shadow rounded-2xl p-6">
                        <div className="flex items-center gap-6 mb-6">
                            {/* Avatar */}
                            <div className="h-16 w-16 rounded-full bg-emerald-600 flex items-center justify-center text-white text-xl font-bold">
                                {usuario?.nome?.[0] ?? "U"}
                            </div>
                            <div>
                                <p className="text-xl font-semibold text-gray-800">{usuario?.nome}</p>
                                <p className="text-gray-600">{usuario?.email}</p>
                                <span className="inline-block mt-2 px-3 py-1 text-sm rounded-full bg-emerald-100 text-emerald-700">
                                    {usuario?.role}
                                </span>
                            </div>
                        </div>

                        {/* Form de edição */}
                        <div className="space-y-4">
                            <div>
                                <label className="block text-sm font-medium text-gray-700">Nome</label>
                                <input
                                    type="text"
                                    value={nome}
                                    onChange={(e) => setNome(e.target.value)}
                                    className="w-full mt-1 p-3 border rounded-lg focus:ring-2 focus:ring-emerald-500"
                                />
                            </div>

                            <div>
                                <label className="block text-sm font-medium text-gray-700">Nova senha</label>
                                <input
                                    type="password"
                                    value={senha}
                                    onChange={(e) => setSenha(e.target.value)}
                                    placeholder="Deixe em branco para não alterar"
                                    className="w-full mt-1 p-3 border rounded-lg focus:ring-2 focus:ring-emerald-500"
                                />
                            </div>

                            {msg && <p className="text-sm text-gray-700">{msg}</p>}

                            <button
                                onClick={handleSave}
                                disabled={saving}
                                className="flex items-center gap-2 bg-emerald-600 text-white px-5 py-2 rounded-xl hover:bg-emerald-700 transition disabled:opacity-50"
                            >
                                <Save size={18} />
                                {saving ? "Salvando..." : "Salvar alterações"}
                            </button>
                        </div>
                    </div>

                    {/* Tenant */}
                    <div className="bg-white shadow rounded-2xl p-6">
                        <h2 className="text-lg font-semibold mb-3">Tenant</h2>
                        <p>
                            <strong>Nome:</strong> {tenant?.nome ?? "-"}
                        </p>
                        <p>
                            <strong>Plano:</strong> {tenant?.plano ?? "-"}
                        </p>
                    </div>

                    {/* Dados brutos da API */}
                    <div className="bg-white shadow rounded-2xl p-6">
                        <h2 className="text-lg font-semibold mb-3">/auth/me (debug)</h2>
                        <pre className="bg-gray-50 p-3 rounded text-sm overflow-x-auto text-gray-700">
                            {JSON.stringify(data, null, 2)}
                        </pre>
                    </div>
                </div>
            )}
        </div>
    );
}
