// src/pages/Login/LoginPage.tsx
import { useState } from "react";
import { useAuthStore } from "@/store/authStore";
import { useNavigate } from "react-router-dom";
import { Mail, Lock } from "lucide-react";
import { motion } from "framer-motion";

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
            navigate("/");
        } catch (err: any) {
            console.error("❌ Erro no login:", err);
            if (err?.response?.status === 401) {
                setErrors({ email: "Usuário ou senha inválidos" });
            } else {
                setErrors({ email: "Erro inesperado. Tente novamente." });
            }
        } finally {
            setLoading(false);
        }
    };

    return (
        <motion.div
            className="flex h-screen bg-gray-50"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ duration: 0.8 }}
        >
            {/* Coluna Esquerda */}
            <motion.div
                className="hidden md:flex w-1/2 bg-gradient-to-br from-emerald-500 via-emerald-600 to-emerald-800 text-white items-center justify-center relative overflow-hidden"
                initial={{ x: -80, opacity: 0 }}
                animate={{ x: 0, opacity: 1 }}
                transition={{ duration: 0.9, ease: 'easeOut' }}
            >
                <div className="absolute inset-0 bg-white/10 backdrop-blur-sm" />
                <div className="relative z-10 text-center px-8">
                    <img
                        src="/hubrouter_logo.png"
                        alt="HubRouter Logo"
                        className="h-40 mx-auto mb-8 drop-shadow-lg"
                    />
                    <h1 className="text-5xl font-extrabold mb-4 tracking-tight">
                        HubRouter
                    </h1>
                    <p className="text-lg opacity-90 max-w-md mx-auto leading-relaxed">
                        Inteligência Artificial para redes logísticas
                    </p>
                </div>
            </motion.div>

            {/* Coluna Direita */}
            <motion.div
                className="w-full md:w-1/2 flex items-center justify-center p-6"
                initial={{ x: 80, opacity: 0 }}
                animate={{ x: 0, opacity: 1 }}
                transition={{ duration: 0.9, ease: 'easeOut' }}
            >
                <div className="w-full max-w-md bg-white rounded-3xl shadow-lg hover:shadow-xl p-10 transition-all">
                    <h2 className="text-3xl font-bold text-gray-800 mb-8 text-center">
                        Acessar Conta
                    </h2>

                    <form className="space-y-5" onSubmit={handleSubmit}>
                        {/* Email */}
                        <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">
                                Email
                            </label>
                            <div className="relative">
                                <Mail className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" size={18} />
                                <input
                                    type="email"
                                    value={email}
                                    onChange={(e) => setEmail(e.target.value)}
                                    placeholder="seu@email.com"
                                    className={`w-full pl-10 pr-3 p-3 border rounded-lg focus:ring-2 focus:outline-none ${errors.email
                                            ? 'border-red-500 focus:ring-red-500'
                                            : 'border-gray-300 focus:ring-emerald-500'
                                        }`}
                                />
                            </div>
                            {errors.email && (
                                <p className="text-red-500 text-sm mt-1">{errors.email}</p>
                            )}
                        </div>

                        {/* Senha */}
                        <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">
                                Senha
                            </label>
                            <div className="relative">
                                <Lock className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" size={18} />
                                <input
                                    type="password"
                                    value={senha}
                                    onChange={(e) => setSenha(e.target.value)}
                                    placeholder="********"
                                    className={`w-full pl-10 pr-3 p-3 border rounded-lg focus:ring-2 focus:outline-none ${errors.senha
                                            ? 'border-red-500 focus:ring-red-500'
                                            : 'border-gray-300 focus:ring-emerald-500'
                                        }`}
                                />
                            </div>
                            {errors.senha && (
                                <p className="text-red-500 text-sm mt-1">{errors.senha}</p>
                            )}
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
                            className="w-full bg-gradient-to-r from-emerald-600 to-emerald-700 text-white p-3 rounded-lg font-medium hover:from-emerald-700 hover:to-emerald-800 transition-all shadow-md disabled:opacity-50"
                        >
                            {loading ? 'Entrando...' : 'Entrar'}
                        </button>
                    </form>
                </div>
            </motion.div>
        </motion.div>

    );
}
