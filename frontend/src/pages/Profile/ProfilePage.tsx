// ProfilePage.tsx

import { useAuthGuard } from "@/hooks/useAuthGuard";
import { useApi } from "@/hooks/useApi";
import { useAuthStore } from "@/store/authStore";

export default function ProfilePage() {
    const { usuario, tenant } = useAuthGuard();
    const { logout } = useAuthStore();

    const { data, loading, error } = useApi<{ user: any }>({
        url: "/auth/me",
    });

    return (
        <div className="p-6">
            <div className="flex justify-between items-center mb-6">
                <h1 className="text-2xl font-bold">Perfil</h1>
                <button
                    onClick={() => {
                        logout();
                        window.location.href = "/login";
                    }}
                    className="bg-red-600 text-white px-4 py-2 rounded hover:bg-red-700"
                >
                    Logout
                </button>
            </div>

            {loading && <p>Carregando...</p>}
            {error && <p className="text-red-500">Erro: {String(error)}</p>}

            {data && (
                <div className="space-y-2">
                    <p><strong>Nome:</strong> {usuario?.nome}</p>
                    <p><strong>Email:</strong> {usuario?.email}</p>
                    <p><strong>Role:</strong> {usuario?.role}</p>
                    <p><strong>Tenant:</strong> {tenant?.nome} ({tenant?.plano})</p>

                    <hr className="my-4" />

                    <h2 className="text-lg font-semibold">/auth/me:</h2>
                    <pre className="bg-gray-100 p-3 rounded text-sm overflow-x-auto">
                        {JSON.stringify(data, null, 2)}
                    </pre>
                </div>
            )}
        </div>
    );
}
