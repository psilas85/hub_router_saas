//hub_router_1.0.1/frontend/src/pages/Data_input/DataInputPage.tsx

import { useState, useEffect } from "react";
import api from "@/services/api";
import toast from "react-hot-toast";
import { Loader2, FileSpreadsheet, CheckCircle2, XCircle } from "lucide-react";

type Resultado = {
    status: string;
    tenant_id?: string;   // 👈 agora opcional
    mensagem?: string;
    job_id?: string;
    total_processados?: number;
    validos?: number;
    invalidos?: number;
    progress?: number;    // 👈 já aproveitei pra incluir o progress
    step?: string;        // 👈 e o step do backend
    result?: {
        total_processados: number;
        validos: number;
        invalidos: number;
    };
};


export default function DataInputPage() {
    const [file, setFile] = useState<File | null>(null);
    const [modoForcar, setModoForcar] = useState(false);
    const [limitePeso, setLimitePeso] = useState<number | "">("");
    const [loading, setLoading] = useState(false);
    const [jobId, setJobId] = useState<string | null>(null);
    const [resultado, setResultado] = useState<Resultado | null>(null);

    const limparEstado = () => {
        setFile(null);
        setModoForcar(false);
        setLimitePeso("");
        setJobId(null);
        setResultado(null);
    };

    const processar = async () => {
        if (!file) {
            toast.error("Selecione um arquivo .csv primeiro.");
            return;
        }

        try {
            setLoading(true);
            setResultado(null);

            const formData = new FormData();
            formData.append("file", file);

            const params = new URLSearchParams();
            params.append("modo_forcar", String(modoForcar));
            if (limitePeso !== "") params.append("limite_peso_kg", String(limitePeso));

            const { data } = await api.post<Resultado>(
                `/data_input/upload?${params.toString()}`,
                formData,
                { headers: { "Content-Type": "multipart/form-data" } }
            );

            if (data.job_id) {
                setJobId(data.job_id);
                toast.success("Arquivo enviado! Processamento iniciado.");
            } else {
                toast.error("Não foi possível iniciar o processamento.");
            }

            setFile(null);
            setModoForcar(false);
        } catch (err: any) {
            const detail =
                err?.response?.data?.detail || err.message || "Erro desconhecido";
            toast.error(typeof detail === "string" ? detail : JSON.stringify(detail));
        } finally {
            setLoading(false);
        }
    };

    // 🔁 Polling de status do job
    useEffect(() => {
        if (!jobId) return;

        const interval = setInterval(async () => {
            try {
                const { data } = await api.get<Resultado>(`/data_input/status/${jobId}`);

                if (data.status === "done" || data.status === "error") {
                    const normalizado: Resultado = {
                        status: data.status,
                        tenant_id: data.tenant_id,
                        job_id: data.job_id,
                        mensagem: data.mensagem,
                        total_processados:
                            data.total_processados ?? data.result?.total_processados ?? 0,
                        validos: data.validos ?? data.result?.validos ?? 0,
                        invalidos: data.invalidos ?? data.result?.invalidos ?? 0,
                    };

                    setResultado(normalizado);
                    clearInterval(interval);

                    if (data.status === "done") {
                        toast.success("Processamento concluído!");
                    } else {
                        toast.error("Erro no processamento.");
                    }
                } else {
                    // Atualiza progresso enquanto está em execução
                    setResultado((prev) => ({
                        ...prev,
                        status: data.status,
                        progress: data.progress,
                        step: data.step,
                    }));
                }
            } catch (err) {
                console.error(err);
            }
        }, 3000);

        return () => clearInterval(interval);
    }, [jobId]);

    return (
        <div className="max-w-3xl mx-auto p-6">
            <h2 className="text-xl font-semibold mb-4 flex items-center gap-2">
                <FileSpreadsheet className="w-5 h-5 text-emerald-600" />
                Data Input
            </h2>

            {/* Card de upload */}
            <div className="bg-white rounded-lg shadow p-5 space-y-4">
                <div className="flex flex-col gap-2">
                    <label className="text-sm font-medium text-gray-700">
                        Arquivo (.csv)
                    </label>
                    <input
                        type="file"
                        accept=".csv"
                        onChange={(e) => setFile(e.target.files?.[0] || null)}
                        className="block w-full text-sm text-gray-600
              file:mr-4 file:py-2 file:px-4
              file:rounded-md file:border-0
              file:text-sm file:font-semibold
              file:bg-emerald-600 file:text-white
              hover:file:bg-emerald-700"
                    />
                    {file && (
                        <span className="text-xs text-gray-500">
                            Selecionado: <strong>{file.name}</strong> (
                            {Math.ceil(file.size / 1024)} KB)
                        </span>
                    )}
                </div>

                <label className="flex items-center gap-2 text-sm">
                    <input
                        type="checkbox"
                        checked={modoForcar}
                        onChange={(e) => setModoForcar(e.target.checked)}
                        className="h-4 w-4 text-emerald-600 border-gray-300 rounded"
                    />
                    Forçar reprocessamento
                </label>

                <div className="flex flex-col gap-1">
                    <label className="text-sm font-medium text-gray-700 flex items-center gap-2">
                        Limite de peso (kg){" "}
                        <span
                            className="text-gray-400 cursor-help"
                            title="Define o peso máximo permitido por CTE. Padrão 15000kg."
                        >
                            ❓
                        </span>
                    </label>
                    <input
                        type="number"
                        placeholder="15000"
                        value={limitePeso}
                        onChange={(e) =>
                            setLimitePeso(e.target.value === "" ? "" : Number(e.target.value))
                        }
                        className="w-full border rounded px-3 py-2 text-sm"
                    />
                </div>

                <div className="flex gap-2">
                    <button
                        disabled={!file || loading}
                        onClick={processar}
                        className="bg-emerald-600 text-white px-4 py-2 rounded-lg flex items-center gap-2 hover:bg-emerald-700 disabled:opacity-50"
                    >
                        {loading ? (
                            <>
                                <Loader2 className="w-4 h-4 animate-spin" /> Enviando...
                            </>
                        ) : (
                            "Processar Data Input"
                        )}
                    </button>
                    <button
                        type="button"
                        onClick={limparEstado}
                        className="px-4 py-2 rounded-lg border text-gray-700 hover:bg-gray-50"
                        disabled={loading}
                    >
                        Limpar
                    </button>
                </div>
            </div>

            {/* Card de progresso */}
            {resultado?.status === "processing" && (
                <div className="mt-6 bg-white rounded-lg shadow p-5">
                    <h3 className="font-semibold mb-3">Processando...</h3>
                    <p className="text-sm text-gray-600 mb-2">
                        {resultado.step || "Em andamento"}
                    </p>
                    <div className="w-full bg-gray-200 rounded-full h-3">
                        <div
                            className="bg-emerald-600 h-3 rounded-full transition-all duration-500"
                            style={{ width: `${resultado.progress || 0}%` }}
                        />
                    </div>
                    <p className="text-xs text-gray-500 mt-1">
                        {resultado.progress ?? 0}% concluído
                    </p>
                </div>
            )}

            {/* Card de resultado */}
            {resultado &&
                (resultado.status === "done" || resultado.status === "error") && (
                    <div className="mt-6 bg-white rounded-lg shadow p-5">
                        <h3 className="font-semibold mb-3">Resultado</h3>
                        <p className="text-sm text-gray-700 mb-3">
                            {resultado.mensagem || "Resumo do processamento"}
                        </p>

                        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 text-sm">
                            <div className="border rounded p-3 flex flex-col items-center">
                                <span className="text-gray-500">Total processados</span>
                                <span className="text-lg font-semibold flex items-center gap-1">
                                    <FileSpreadsheet className="w-4 h-4 text-gray-500" />
                                    {resultado.total_processados ?? "—"}
                                </span>
                            </div>
                            <div className="border rounded p-3 flex flex-col items-center">
                                <span className="text-gray-500">Válidos</span>
                                <span className="text-lg font-semibold text-emerald-700 flex items-center gap-1">
                                    <CheckCircle2 className="w-4 h-4" />
                                    {resultado.validos ?? "—"}
                                </span>
                            </div>
                            <div className="border rounded p-3 flex flex-col items-center">
                                <span className="text-gray-500">Inválidos</span>
                                <span className="text-lg font-semibold text-rose-700 flex items-center gap-1">
                                    <XCircle className="w-4 h-4" />
                                    {resultado.invalidos ?? "—"}
                                </span>
                            </div>
                        </div>
                    </div>
                )}
        </div>
    );
}
