// src/components/LastMileCostsResumoTable.tsx
interface ResumoRow {
    veiculo: string;
    qtd_rotas: number;
    entregas: number;
    peso_total: number;
    distancia_km: number;
    custo_total: number;
    percentual: number; // % sobre o total
}

interface Props {
    data: ResumoRow[];
}

export default function LastMileCostsResumoTable({ data }: Props) {
    if (!data || data.length === 0) {
        return (
            <div className="p-4 text-sm text-gray-500 italic">
                Nenhum resumo disponível para exibir.
            </div>
        );
    }

    // Totais para rodapé
    const totalEntregas = data.reduce((acc, r) => acc + r.entregas, 0);
    const totalPeso = data.reduce((acc, r) => acc + r.peso_total, 0);
    const totalDistancia = data.reduce((acc, r) => acc + r.distancia_km, 0);
    const totalCusto = data.reduce((acc, r) => acc + r.custo_total, 0);

    return (
        <div className="overflow-x-auto bg-white rounded-2xl shadow">
            <table className="w-full text-sm text-left border-collapse">
                <thead className="bg-gray-50 text-gray-700 text-xs uppercase">
                    <tr>
                        <th className="px-4 py-2">Veículo</th>
                        <th className="px-4 py-2 text-center">Rotas</th>
                        <th className="px-4 py-2 text-center">Entregas</th>
                        <th className="px-4 py-2 text-center">Peso Total (kg)</th>
                        <th className="px-4 py-2 text-center">Distância (km)</th>
                        <th className="px-4 py-2 text-center">Custo (R$)</th>
                        <th className="px-4 py-2 text-center">% Custo Total</th>
                    </tr>
                </thead>
                <tbody>
                    {data.map((r, i) => (
                        <tr key={i} className="border-b last:border-0">
                            <td className="px-4 py-2">{r.veiculo}</td>
                            <td className="px-4 py-2 text-center">{r.qtd_rotas}</td>
                            <td className="px-4 py-2 text-center">{r.entregas}</td>
                            <td className="px-4 py-2 text-center">{r.peso_total.toLocaleString("pt-BR")}</td>
                            <td className="px-4 py-2 text-center">{r.distancia_km.toFixed(1)}</td>
                            <td className="px-4 py-2 text-center">R$ {r.custo_total.toFixed(2)}</td>
                            <td className="px-4 py-2 text-center">{r.percentual.toFixed(1)}%</td>
                        </tr>
                    ))}
                </tbody>
                <tfoot className="bg-gray-50 font-semibold">
                    <tr>
                        <td className="px-4 py-2">Totais</td>
                        <td className="px-4 py-2 text-center">–</td>
                        <td className="px-4 py-2 text-center">{totalEntregas}</td>
                        <td className="px-4 py-2 text-center">{totalPeso.toLocaleString("pt-BR")}</td>
                        <td className="px-4 py-2 text-center">{totalDistancia.toFixed(1)}</td>
                        <td className="px-4 py-2 text-center">R$ {totalCusto.toFixed(2)}</td>
                        <td className="px-4 py-2 text-center">100%</td>
                    </tr>
                </tfoot>
            </table>
        </div>
    );
}
