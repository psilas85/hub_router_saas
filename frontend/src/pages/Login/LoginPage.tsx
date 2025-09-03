// src/pages/Login/LoginPage.tsx

// src/pages/Login/LoginPage.tsx
import { useState } from "react";
import { useAuthStore } from "@/store/authStore";
import { useNavigate } from "react-router-dom";

export default function LoginPage() {
    const [email, setEmail] = useState("");
    const [senha, setSenha] = useState("");
    const [errors, setErrors] = useState<{ email?: string; senha?: string }>({});
    const [loading, setLoading] = useState(false);

    const login = useAuthStore((s) => s.login);
    const navigate = useNavigate();

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        let newErrors: typeof errors = {};

        if (!email.includes("@")) newErrors.email = "Informe um email válido";
        if (senha.length < 6) newErrors.senha = "Senha deve ter pelo menos 6 caracteres";

        setErrors(newErrors);
        if (Object.keys(newErrors).length > 0) return;

        try {
            setLoading(true);
            await login(email, senha);
            navigate("/"); // ✅ redireciona após login
        } catch (err: any) {
            console.error("❌ Erro no login:", err);

            if (err?.response?.status === 401) {
                setErrors({ email: "Usuário ou senha inválidos" });
            } else {
                alert("⚠️ Erro inesperado no login. Verifique sua conexão ou tente novamente.");
                setErrors({ email: "Erro inesperado" });
            }
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="flex h-screen">
            {/* Coluna Esquerda */}
            <div className="hidden md:flex w-1/2 bg-emerald-600 text-white flex-col items-center justify-center p-8">
                <img src="/hubrouter_logo.png" alt="HubRouter Logo" className="h-24 mb-6" />
                <h1 className="text-3xl font-bold mb-2">HubRouter</h1>
                <p className="text-lg text-center max-w-md">
                    Inteligência Artificial para redes logísticas
                </p>
            </div>

            {/* Coluna Direita */}
            <div className="w-full md:w-1/2 flex items-center justify-center bg-white">
                <div className="w-full max-w-md p-8 border rounded-lg shadow-lg">
                    <h2 className="text-2xl font-bold text-gray-800 mb-6 text-center">
                        Acessar Conta
                    </h2>

                    <form className="space-y-4" onSubmit={handleSubmit}>
                        {/* Email */}
                        <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">Email</label>
                            <input
                                type="email"
                                value={email}
                                onChange={(e) => setEmail(e.target.value)}
                                placeholder="seu@email.com"
                                className={`w-full p-3 border rounded-md focus:ring-2 focus:outline-none ${errors.email ? "border-red-500 focus:ring-red-500" : "focus:ring-emerald-500"
                                    }`}
                            />
                            {errors.email && <p className="text-red-500 text-sm mt-1">{errors.email}</p>}
                        </div>

                        {/* Senha */}
                        <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">Senha</label>
                            <input
                                type="password"
                                value={senha}
                                onChange={(e) => setSenha(e.target.value)}
                                placeholder="********"
                                className={`w-full p-3 border rounded-md focus:ring-2 focus:outline-none ${errors.senha ? "border-red-500 focus:ring-red-500" : "focus:ring-emerald-500"
                                    }`}
                            />
                            {errors.senha && <p className="text-red-500 text-sm mt-1">{errors.senha}</p>}
                        </div>

                        {/* Esqueci senha */}
                        <div className="text-right">
                            <a href="#" className="text-sm text-emerald-600 hover:underline">
                                Esqueci minha senha
                            </a>
                        </div>

                        {/* Botão login */}
                        <button
                            type="submit"
                            disabled={loading}
                            className="w-full bg-emerald-600 text-white p-3 rounded-md hover:bg-emerald-700 transition disabled:opacity-50"
                        >
                            {loading ? "Entrando..." : "Entrar"}
                        </button>

                        {/* Botão criar conta */}
                        <button
                            type="button"
                            className="w-full border border-emerald-600 text-emerald-600 p-3 rounded-md hover:bg-emerald-50 transition"
                        >
                            Criar Conta
                        </button>
                    </form>
                </div>
            </div>
        </div>
    );
}
