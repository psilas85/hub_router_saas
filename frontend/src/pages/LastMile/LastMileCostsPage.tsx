// src/pages/LastMile/LastMileCostsPage.tsx
import { useState } from "react";
import { lmProcessCosts, lmVisualizeCosts } from "@/services/lastMileApi";
import type { VisualizeCostsResponse } from "@/services/lastMileApi";
import toast from "react-hot-toast";
import { Loader2, Calculator, FileSpreadsheet, FileText } from "lucide-react";

function todayISO() {
    const d = new Date();
    return d.toISOString().slice(0, 10);
}

export default function LastMileCostsPage() {
    const [data, setData] = useState("");
    const [dataVisualizar, setDataVisualizar] = useState("");
    const [loading, setLoading] = useState(false);
    const [artefatos, setArtefatos] = useState<VisualizeCostsResponse | null>(null);

    const processar = async () => {
        if (!data) {
            toast.error("Selecione uma data.");
            return;
        }
        setLoading(true);
        try {
            await lmProcessCosts(data, data);

            // 👇 já busca o resumo atualizado após processar
            const r = await lmVisualizeCosts(data);
            setArtefatos(r);

            toast.success("Custos processados e carregados!");
        } catch (err) {
            console.error(err);
            toast.error("Erro ao processar custos.");
        } finally {
            setLoading(false);
        }
    };

    const visualizar = async () => {
        if (!dataVisualizar) {
            toast.error("Selecione a data para visualizar relatórios.");
            return;
        }
        setLoading(true);
        try {
            const r = await lmVisualizeCosts(dataVisualizar);
            setArtefatos(r);
            if (!r?.json_dados || r.json_dados.length === 0) {
                toast("Nenhum dado encontrado para esta data.", { icon: "⚠️" });
            } else {
                toast.success("Artefatos carregados!");
            }
        } catch (err) {
            console.error(err);
            toast.error("Erro ao buscar artefatos.");
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="p-6 max-w-7xl mx-auto">
            <h1 className="text-2xl font-semibold mb-6 flex items-center gap-2">
                <Calculator className="w-6 h-6 text-emerald-600" />
                Last-Mile • Custeio
            </h1>

            {/* Processar custos (uma única data) */}
            <div className="bg-white rounded-2xl shadow p-4 grid grid-cols-1 md:grid-cols-3 gap-3 mb-4">
                <div>
                    <label className="text-sm text-gray-600">Data</label>
                    <input
                        type="date"
                        className="input"
                        value={data}
                        onChange={(e) => setData(e.target.value)}
                        max={todayISO()}
                    />
                </div>
                <div className="flex items-end gap-2 md:col-span-2">
                    <button
                        className="btn flex items-center gap-2"
                        onClick={processar}
                        disabled={loading}
                    >
                        {loading ? (
                            <>
                                <Loader2 className="w-4 h-4 animate-spin" /> Processando…
                            </>
                        ) : (
                            <>
                                <Calculator className="w-4 h-4" /> Processar Custos
                            </>
                        )}
                    </button>
                </div>
            </div>

            {/* Visualizar relatórios (por data) */}
            <div className="bg-white rounded-2xl shadow p-4 grid grid-cols-1 md:grid-cols-5 gap-3 mb-4">
                <div>
                    <label className="text-sm text-gray-600">
                        Visualizar relatórios — Data
                    </label>
                    <input
                        type="date"
                        className="input"
                        value={dataVisualizar}
                        onChange={(e) => setDataVisualizar(e.target.value)}
                        max={todayISO()}
                    />
                </div>
                <div className="flex items-end gap-2 md:col-span-2">
                    <button
                        className="btn-secondary flex items-center gap-2"
                        onClick={visualizar}
                        disabled={loading}
                    >
                        {loading ? (
                            <>
                                <Loader2 className="w-4 h-4 animate-spin" /> Buscando…
                            </>
                        ) : (
                            <>
                                <FileSpreadsheet className="w-4 h-4" /> Gerar Detalhes/Resumo/PDF
                            </>
                        )}
                    </button>
                </div>
            </div>

            {/* Resultado */}
            {artefatos && artefatos.json_dados && artefatos.json_dados.length > 0 && (
                <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-4 mt-4">
                    <h2 className="font-semibold mb-3">
                        Artefatos {artefatos.envio_data}
                    </h2>

                    <div className="flex gap-3 mb-4">
                        {artefatos.detalhes_url && (
                            <a
                                href={artefatos.detalhes_url}
                                className="btn flex items-center gap-2"
                                target="_blank"
                                rel="noreferrer"
                            >
                                <FileSpreadsheet className="w-4 h-4" /> Baixar Detalhes CSV
                            </a>
                        )}
                        {artefatos.resumo_url && (
                            <a
                                href={artefatos.resumo_url}
                                className="btn flex items-center gap-2"
                                target="_blank"
                                rel="noreferrer"
                            >
                                <FileSpreadsheet className="w-4 h-4" /> Baixar Resumo CSV
                            </a>
                        )}
                        {artefatos.pdf_url && (
                            <a
                                href={artefatos.pdf_url}
                                className="btn flex items-center gap-2"
                                target="_blank"
                                rel="noreferrer"
                            >
                                <FileText className="w-4 h-4" /> Baixar PDF
                            </a>
                        )}
                    </div>

                    {/* Tabela de resumo com rodapé */}
                    <div className="overflow-x-auto">
                        <table className="min-w-full border border-gray-200 divide-y divide-gray-200">
                            <thead className="bg-gray-50">
                                <tr>
                                    <th className="px-3 py-2 text-left text-sm font-medium text-gray-600 border-b">
                                        Veículo
                                    </th>
                                    <th className="px-3 py-2 text-right text-sm font-medium text-gray-600 border-b">
                                        Entregas
                                    </th>
                                    <th className="px-3 py-2 text-right text-sm font-medium text-gray-600 border-b">
                                        Peso (kg)
                                    </th>
                                    <th className="px-3 py-2 text-right text-sm font-medium text-gray-600 border-b">
                                        Distância (km)
                                    </th>
                                    <th className="px-3 py-2 text-right text-sm font-medium text-gray-600 border-b">
                                        Custo (R$)
                                    </th>
                                    <th className="px-3 py-2 text-right text-sm font-medium text-gray-600 border-b">
                                        Frete (R$)
                                    </th>
                                    <th className="px-3 py-2 text-right text-sm font-medium text-gray-600 border-b">
                                        % Custo
                                    </th>
                                </tr>
                            </thead>
                            <tbody className="divide-y divide-gray-100">
                                {artefatos.json_dados.map((row: any, i: number) => (
                                    <tr key={i} className="hover:bg-gray-50">
                                        <td className="px-3 py-2 border-b">{row.veiculo}</td>
                                        <td className="px-3 py-2 border-b text-right">
                                            {row.quantidade_entregas}
                                        </td>
                                        <td className="px-3 py-2 border-b text-right">
                                            {row.peso_total_kg?.toLocaleString("pt-BR")}
                                        </td>
                                        <td className="px-3 py-2 border-b text-right">
                                            {row.distancia_total_km?.toLocaleString("pt-BR")}
                                        </td>
                                        <td className="px-3 py-2 border-b text-right">
                                            {row.custo_entrega_total?.toLocaleString("pt-BR", {
                                                style: "currency",
                                                currency: "BRL",
                                            })}
                                        </td>
                                        <td className="px-3 py-2 border-b text-right">
                                            {row.cte_frete_total?.toLocaleString("pt-BR", {
                                                style: "currency",
                                                currency: "BRL",
                                            })}
                                        </td>
                                        <td className="px-3 py-2 border-b text-right">
                                            {row.percentual_custo
                                                ? row.percentual_custo.toFixed(2) + "%"
                                                : "-"}
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                            <tfoot className="bg-gray-100 font-semibold">
                                {(() => {
                                    const totalEntregas = artefatos.json_dados.reduce(
                                        (acc, r) => acc + (r.quantidade_entregas || 0),
                                        0
                                    );
                                    const totalPeso = artefatos.json_dados.reduce(
                                        (acc, r) => acc + (r.peso_total_kg || 0),
                                        0
                                    );
                                    const totalDist = artefatos.json_dados.reduce(
                                        (acc, r) => acc + (r.distancia_total_km || 0),
                                        0
                                    );
                                    const totalCusto = artefatos.json_dados.reduce(
                                        (acc, r) => acc + (r.custo_entrega_total || 0),
                                        0
                                    );
                                    const totalFrete = artefatos.json_dados.reduce(
                                        (acc, r) => acc + (r.cte_frete_total || 0),
                                        0
                                    );
                                    const percentualTotal =
                                        totalFrete > 0 ? (totalCusto / totalFrete) * 100 : 0;

                                    return (
                                        <tr>
                                            <td className="px-3 py-2 border-t text-right">Totais:</td>
                                            <td className="px-3 py-2 border-t text-right">
                                                {totalEntregas}
                                            </td>
                                            <td className="px-3 py-2 border-t text-right">
                                                {totalPeso.toLocaleString("pt-BR")}
                                            </td>
                                            <td className="px-3 py-2 border-t text-right">
                                                {totalDist.toLocaleString("pt-BR")}
                                            </td>
                                            <td className="px-3 py-2 border-t text-right">
                                                {totalCusto.toLocaleString("pt-BR", {
                                                    style: "currency",
                                                    currency: "BRL",
                                                })}
                                            </td>
                                            <td className="px-3 py-2 border-t text-right">
                                                {totalFrete.toLocaleString("pt-BR", {
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
                </div>
            )}
        </div>
    );
}
