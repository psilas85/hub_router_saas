// hub_router_1.0.1/frontend/src/pages/middle_mile/CostsPage.tsx
import { useState } from "react";
import api from "@/services/api";
import toast from "react-hot-toast";
import { Loader2, FileText, Table } from "lucide-react";

type Artefatos = {
    tenant_id: string;
    envio_data: string;
    csv_url?: string | null;
    json_url?: string | null;
    pdf_url?: string | null;
    json_dados?: any[];
};

export default function CostsTransferPage() {
    const [data, setData] = useState<string>("");
    const [loading, setLoading] = useState(false);
    const [artefatos, setArtefatos] = useState<Artefatos | null>(null);

    const canRun = !!data;

    async function processarCustos() {
        if (!canRun) {
            toast.error("Informe a data.");
            return;
        }
        try {
            setLoading(true);

            // 1️⃣ Primeiro processa os custos (sempre forçar no backend)
            await api.post("/costs_transfer/processar", null, {
                params: { data_inicial: data, data_final: data },
            });

            // 2️⃣ Depois busca os artefatos gerados
            const { data: resp } = await api.get<Artefatos>(
                "/costs_transfer/visualizar",
                { params: { data } }
            );

            setArtefatos({
                ...resp,
                json_dados: Array.isArray(resp.json_dados) ? resp.json_dados : [],
            });
            toast.success("Custos processados com sucesso!");
        } catch (e) {
            console.error(e);
            setArtefatos(null);
            toast.error("Erro ao processar custos.");
        } finally {
            setLoading(false);
        }
    }

    return (
        <div className="max-w-7xl mx-auto p-6">
            <h1 className="text-2xl font-semibold text-gray-800 mb-6">
                Middle-Mile • Custeio
            </h1>

            {/* Formulário */}
            <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-4 mb-6">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Data
                        </label>
                        <input
                            type="date"
                            value={data}
                            onChange={(e) => setData(e.target.value)}
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 focus:ring-2 focus:ring-emerald-500"
                        />
                    </div>
                </div>

                <div className="mt-4 flex flex-wrap gap-3">
                    <button
                        onClick={processarCustos}
                        disabled={!canRun || loading}
                        className="bg-emerald-600 hover:bg-emerald-700 text-white px-4 py-2 rounded-lg flex items-center gap-2 disabled:opacity-60"
                    >
                        {loading ? (
                            <>
                                <Loader2 className="w-4 h-4 animate-spin" /> Processando...
                            </>
                        ) : (
                            "Processar Custos"
                        )}
                    </button>
                </div>
            </div>

            {/* Resultado */}
            {artefatos && (
                <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-4">
                    <div className="flex items-center gap-3 mb-4">
                        {artefatos.csv_url && (
                            <a
                                href={artefatos.csv_url}
                                target="_blank"
                                rel="noreferrer"
                                className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg flex items-center gap-2"
                                download
                            >
                                <Table className="w-4 h-4" /> Baixar CSV
                            </a>
                        )}
                        {artefatos.pdf_url && (
                            <a
                                href={artefatos.pdf_url}
                                target="_blank"
                                rel="noreferrer"
                                className="bg-emerald-600 hover:bg-emerald-700 text-white px-4 py-2 rounded-lg flex items-center gap-2"
                                download
                            >
                                <FileText className="w-4 h-4" /> Baixar PDF
                            </a>
                        )}
                    </div>

                    {/* Tabela interativa */}
                    {artefatos.json_dados && artefatos.json_dados.length > 0 ? (
                        <div className="overflow-x-auto">
                            <table className="min-w-full border border-gray-200 divide-y divide-gray-200">
                                <thead className="bg-gray-50">
                                    <tr>
                                        <th className="px-3 py-2 text-left text-sm font-medium text-gray-600 border-b">
                                            Rota
                                        </th>
                                        <th className="px-3 py-2 text-left text-sm font-medium text-gray-600 border-b">
                                            Hub
                                        </th>
                                        <th className="px-3 py-2 text-left text-sm font-medium text-gray-600 border-b">
                                            Veículo
                                        </th>
                                        <th className="px-3 py-2 text-right text-sm font-medium text-gray-600 border-b">
                                            Peso (kg)
                                        </th>
                                        <th className="px-3 py-2 text-right text-sm font-medium text-gray-600 border-b">
                                            Frete (R$)
                                        </th>
                                        <th className="px-3 py-2 text-right text-sm font-medium text-gray-600 border-b">
                                            Distância (km)
                                        </th>
                                        <th className="px-3 py-2 text-right text-sm font-medium text-gray-600 border-b">
                                            Custo (R$)
                                        </th>
                                        <th className="px-3 py-2 text-right text-sm font-medium text-gray-600 border-b">
                                            % Custo
                                        </th>
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-gray-100">
                                    {artefatos.json_dados.map((row, i) => (
                                        <tr key={i} className="hover:bg-gray-50">
                                            <td className="px-3 py-2 border-b">{row.rota_transf}</td>
                                            <td className="px-3 py-2 border-b">{row.hub_central_nome}</td>
                                            <td className="px-3 py-2 border-b">{row.tipo_veiculo}</td>
                                            <td className="px-3 py-2 border-b text-right">
                                                {row.cte_peso.toLocaleString("pt-BR")}
                                            </td>
                                            <td className="px-3 py-2 border-b text-right">
                                                {row.cte_valor_frete.toLocaleString("pt-BR", {
                                                    style: "currency",
                                                    currency: "BRL",
                                                })}
                                            </td>
                                            <td className="px-3 py-2 border-b text-right">
                                                {row.distancia_total.toLocaleString("pt-BR")}
                                            </td>
                                            <td className="px-3 py-2 border-b text-right">
                                                {row.custo_transferencia_total.toLocaleString("pt-BR", {
                                                    style: "currency",
                                                    currency: "BRL",
                                                })}
                                            </td>
                                            <td className="px-3 py-2 border-b text-right">
                                                {row.percentual_custo.toFixed(2)}%
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>

                                {/* Totais */}
                                <tfoot className="bg-gray-100 font-semibold">
                                    {(() => {
                                        const totalPeso = artefatos.json_dados.reduce((acc, r) => acc + (r.cte_peso || 0), 0);
                                        const totalFrete = artefatos.json_dados.reduce((acc, r) => acc + (r.cte_valor_frete || 0), 0);
                                        const totalCusto = artefatos.json_dados.reduce((acc, r) => acc + (r.custo_transferencia_total || 0), 0);
                                        const percentualTotal = totalFrete > 0 ? (totalCusto / totalFrete) * 100 : 0;

                                        return (
                                            <tr>
                                                <td colSpan={3} className="px-3 py-2 border-t text-right">
                                                    Totais:
                                                </td>
                                                <td className="px-3 py-2 border-t text-right">
                                                    {totalPeso.toLocaleString("pt-BR")}
                                                </td>
                                                <td className="px-3 py-2 border-t text-right">
                                                    {totalFrete.toLocaleString("pt-BR", {
                                                        style: "currency",
                                                        currency: "BRL",
                                                    })}
                                                </td>
                                                <td className="px-3 py-2 border-t text-right">-</td>
                                                <td className="px-3 py-2 border-t text-right">
                                                    {totalCusto.toLocaleString("pt-BR", {
                                                        style: "currency",
                                                        currency: "BRL",
                                                    })}
                                                </td>
                                                <td className="px-3 py-2 border-t text-right">
                                                    {percentualTotal.toFixed(2)}%
                                                </td>
                                            </tr>
                                        );
                                    })()}
                                </tfoot>
                            </table>
                        </div>
                    ) : (
                        <p className="text-sm text-gray-500">Nenhum dado encontrado.</p>
                    )}
                </div>
            )}
        </div>
    );
}
