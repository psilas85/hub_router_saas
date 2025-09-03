// src/pages/Home/HomePage.tsx
import { useAuthGuard } from "@/hooks/useAuthGuard";

export default function HomePage() {
    const { usuario, tenant } = useAuthGuard();

    // texto amigável para o tenant
    const tenantInfo =
        tenant?.nome && tenant?.plano
            ? `${tenant.nome} (${tenant.plano})`
            : usuario?.role === "hub_admin"
                ? "— Hub Admin (sem tenant vinculado)"
                : "—";

    return (
        <div className="mx-auto max-w-7xl px-4 py-6">
            <h1 className="text-2xl md:text-3xl font-bold text-gray-900 mb-4">
                Bem-vindo(a), {usuario?.nome}!
            </h1>

            <section className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
                <dl className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                    <div>
                        <dt className="text-sm font-medium text-gray-500">Email</dt>
                        <dd className="mt-1 text-gray-900">{usuario?.email}</dd>
                    </div>
                    <div>
                        <dt className="text-sm font-medium text-gray-500">Role</dt>
                        <dd className="mt-1 text-gray-900">{usuario?.role}</dd>
                    </div>
                    <div>
                        <dt className="text-sm font-medium text-gray-500">Tenant</dt>
                        <dd className="mt-1 text-gray-900">{tenantInfo}</dd>
                    </div>
                </dl>
            </section>
        </div>
    );
}
