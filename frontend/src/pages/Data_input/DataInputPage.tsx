// hub_router_1.0.1/frontend/src/pages/Data_input/DataInputPage.tsx
import { useState, useEffect, useMemo } from "react";
import api from "@/services/api";
import toast from "react-hot-toast";
import {
    Loader2,
    FileSpreadsheet,
    CheckCircle2,
    XCircle,
    AlertCircle,
    RefreshCw,
    Download,
    Info,
    Clock3,
} from "lucide-react";
import { useProcessing } from "@/context/ProcessingContext";

type Resultado = {
    status: string;
    tenant_id?: string;
    mensagem?: string;
    job_id?: string;
    total_processados?: number;
    validos?: number;
    invalidos?: number;
    progress?: number;
    step?: string;
    error?: string;
    result?: {
        status?: string;
        tenant_id?: string;
        mensagem?: string;
        job_id?: string;
        total_processados?: number;
        validos?: number;
        invalidos?: number;
    };
};

type Historico = {
    job_id: string;
    arquivo: string;
    criado_em: string;
    status: string;
    total_processados: number;
    validos: number;
    invalidos: number;
    mensagem?: string;
};

const STORAGE_KEY = "dataInputJobId";
const DEFAULT_LIMITE_PESO = 15000;

export default function DataInputPage() {
    const [file, setFile] = useState<File | null>(null);
    const [modoForcar, setModoForcar] = useState(false);
    const [limitePeso, setLimitePeso] = useState<number | "">(DEFAULT_LIMITE_PESO);

    const [uploading, setUploading] = useState(false);
    const [jobId, setJobId] = useState<string | null>(null);
    const [resultado, setResultado] = useState<Resultado | null>(null);
    const [historico, setHistorico] = useState<Historico[]>([]);
    const [carregandoHistorico, setCarregandoHistorico] = useState(false);
    const [downloadLoadingId, setDownloadLoadingId] = useState<string | null>(null);

    const { startProcessing, stopProcessing } = useProcessing();

    const isProcessing = useMemo(
        () => resultado?.status === "processing" || (!!jobId && resultado?.status !== "done" && resultado?.status !== "error"),
        [jobId, resultado]
    );

    const limitePesoAplicado =
        limitePeso === "" || Number.isNaN(Number(limitePeso))
            ? DEFAULT_LIMITE_PESO
            : Number(limitePeso);

    const limparEstado = () => {
        setFile(null);
        setModoForcar(false);
        setLimitePeso(DEFAULT_LIMITE_PESO);
        setJobId(null);
        setResultado(null);
        localStorage.removeItem(STORAGE_KEY);
    };

    const carregarHistorico = async () => {
        try {
            setCarregandoHistorico(true);
            const { data } = await api.get<Historico[]>("/data_input/historico?limit=10");
            setHistorico(data);
        } catch (err) {
            console.error(err);
            toast.error("Erro ao carregar histórico.");
        } finally {
            setCarregandoHistorico(false);
        }
    };

    const normalizarResultadoFinal = (data: Resultado): Resultado => {
        if (data.result) {
            return {
                status: data.result.status || data.status || "done",
                tenant_id: data.result.tenant_id ?? data.tenant_id,
                job_id: data.result.job_id ?? data.job_id,
                mensagem: data.result.mensagem ?? data.mensagem,
                total_processados: data.result.total_processados ?? 0,
                validos: data.result.validos ?? 0,
                invalidos: data.result.invalidos ?? 0,
                progress: 100,
                step: "Concluído",
            };
        }

        return {
            status: data.status,
            tenant_id: data.tenant_id,
            job_id: data.job_id,
            mensagem: data.mensagem,
            total_processados: data.total_processados ?? 0,
            validos: data.validos ?? 0,
            invalidos: data.invalidos ?? 0,
            progress: data.progress ?? (data.status === "done" ? 100 : 0),
            step: data.step ?? (data.status === "done" ? "Concluído" : "Em andamento"),
            error: data.error,
        };
    };

    const consultarStatusJob = async (id: string, silent = false) => {
        try {
            const { data } = await api.get<Resultado>(`/data_input/status/${id}`);

            if (data.status === "done") {
                const finalizado = normalizarResultadoFinal(data);
                setResultado(finalizado);
                setJobId(null);
                localStorage.removeItem(STORAGE_KEY);
                stopProcessing();
                await carregarHistorico();

                if (!silent) {
                    toast.success(
                        `Processamento concluído! (${finalizado.validos ?? 0} válidos / ${finalizado.invalidos ?? 0} inválidos)`
                    );
                }

                return { finished: true };
            }

            if (data.status === "error") {
                setResultado({
                    status: "error",
                    mensagem: "Erro no processamento.",
                    error: data.error,
                    progress: 100,
                    step: "Falha no processamento",
                });
                setJobId(null);
                localStorage.removeItem(STORAGE_KEY);
                stopProcessing();

                if (!silent) {
                    toast.error("Erro no processamento.");
                }

                return { finished: true };
            }

            if (data.status === "not_found") {
                setResultado({
                    status: "error",
                    mensagem: "Job não encontrado.",
                    error: "Job não encontrado no backend.",
                    progress: 0,
                    step: "Job não encontrado",
                });
                setJobId(null);
                localStorage.removeItem(STORAGE_KEY);
                stopProcessing();

                if (!silent) {
                    toast.error("Job não encontrado.");
                }

                return { finished: true };
            }

            setResultado((prev) => ({
                ...prev,
                status: "processing",
                job_id: id,
                progress: data.progress ?? prev?.progress ?? 0,
                step: data.step ?? prev?.step ?? "Em andamento",
                total_processados: data.total_processados ?? prev?.total_processados ?? 0,
                validos: data.validos ?? prev?.validos ?? 0,
                invalidos: data.invalidos ?? prev?.invalidos ?? 0,
                mensagem: data.mensagem ?? prev?.mensagem,
            }));

            return { finished: false };
        } catch (err) {
            console.error(err);
            if (!silent) {
                toast.error("Erro ao consultar status do job.");
            }
            return { finished: false };
        }
    };

    const processar = async () => {
        if (!file) {
            toast.error("Selecione um arquivo .xlsx primeiro.");
            return;
        }

        try {
            setUploading(true);
            setResultado({
                status: "processing",
                progress: 5,
                step: "Enviando arquivo...",
                total_processados: 0,
                validos: 0,
                invalidos: 0,
            });

            startProcessing();

            const formData = new FormData();
            formData.append("file", file);

            const params = new URLSearchParams();
            params.append("modo_forcar", String(modoForcar));
            params.append("limite_peso_kg", String(limitePesoAplicado));

            const { data } = await api.post<Resultado>(
                `/data_input/upload?${params.toString()}`,
                formData,
                { headers: { "Content-Type": "multipart/form-data" } }
            );

            if (!data.job_id) {
                toast.error("Não foi possível iniciar o processamento.");
                stopProcessing();
                setResultado(null);
                return;
            }

            setJobId(data.job_id);
            localStorage.setItem(STORAGE_KEY, data.job_id);

            setResultado((prev) => ({
                ...prev,
                status: "processing",
                job_id: data.job_id,
                tenant_id: data.tenant_id,
                progress: prev?.progress ?? 10,
                step: "Arquivo enviado. Processamento iniciado...",
            }));

            toast.success("Arquivo enviado. Processamento iniciado.");

            setFile(null);
            setModoForcar(false);
        } catch (err: any) {
            const detail =
                err?.response?.data?.detail || err?.message || "Erro desconhecido";
            toast.error(typeof detail === "string" ? detail : JSON.stringify(detail));
            setResultado({
                status: "error",
                mensagem: "Erro ao enviar arquivo.",
                error: typeof detail === "string" ? detail : JSON.stringify(detail),
            });
            stopProcessing();
        } finally {
            setUploading(false);
        }
    };

    const baixarResultado = async (id: string) => {
        try {
            setDownloadLoadingId(id);

            const response = await api.get(`/data_input/download/${id}`, {
                responseType: "blob",
            });

            const blob = new Blob([response.data]);
            const url = window.URL.createObjectURL(blob);

            const a = document.createElement("a");
            a.href = url;
            a.download = `resultado_${id}.xlsx`;
            document.body.appendChild(a);
            a.click();
            a.remove();

            window.URL.revokeObjectURL(url);
        } catch (err: any) {
            const detail =
                err?.response?.data?.detail || err?.message || "Erro ao baixar arquivo";
            toast.error(typeof detail === "string" ? detail : "Erro ao baixar arquivo");
        } finally {
            setDownloadLoadingId(null);
        }
    };

    useEffect(() => {
        if (!jobId) return;

        const interval = setInterval(async () => {
            const { finished } = await consultarStatusJob(jobId, true);
            if (finished) {
                clearInterval(interval);
            }
        }, 2000);

        return () => clearInterval(interval);
    }, [jobId]);

    useEffect(() => {
        const savedJobId = localStorage.getItem(STORAGE_KEY);

        if (savedJobId) {
            setJobId(savedJobId);
            startProcessing();

            consultarStatusJob(savedJobId, true).catch((err) => {
                console.error("Erro ao restaurar status:", err);
            });
        }

        carregarHistorico();
    }, []);

    const statusHistoricoBadge = (status: string) => {
        if (status === "done") {
            return <span className="text-emerald-700 font-medium">✅ Concluído</span>;
        }
        if (status === "processing") {
            return <span className="text-amber-600 font-medium">⏳ Processando</span>;
        }
        return <span className="text-rose-700 font-medium">❌ Erro</span>;
    };

    return (
        <div className="max-w-6xl mx-auto p-6 space-y-6">
            <div className="flex items-center gap-2">
                <FileSpreadsheet className="w-6 h-6 text-emerald-600" />
                <div>
                    <h1 className="text-2xl font-semibold text-gray-900">Data Input</h1>
                    <p className="text-sm text-gray-500">
                        Envie a planilha, acompanhe o processamento e baixe o resultado.
                    </p>
                </div>
            </div>

            <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-5 space-y-5">
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    <div className="space-y-3">
                        <label className="text-sm font-medium text-gray-700 block">
                            Arquivo (.xlsx)
                        </label>

                        <input
                            type="file"
                            accept=".xlsx"
                            onChange={(e) => setFile(e.target.files?.[0] || null)}
                            className="block w-full text-sm text-gray-600
                file:mr-4 file:py-2 file:px-4
                file:rounded-md file:border-0
                file:text-sm file:font-semibold
                file:bg-emerald-600 file:text-white
                hover:file:bg-emerald-700"
                            disabled={uploading || isProcessing}
                        />

                        {file ? (
                            <div className="rounded-lg border border-emerald-100 bg-emerald-50 px-3 py-2 text-sm text-gray-700">
                                <div>
                                    Arquivo: <strong>{file.name}</strong>
                                </div>
                                <div className="text-xs text-gray-500 mt-1">
                                    Tamanho: {Math.ceil(file.size / 1024)} KB
                                </div>
                            </div>
                        ) : (
                            <div className="rounded-lg border border-dashed border-gray-200 px-3 py-4 text-sm text-gray-500">
                                Nenhum arquivo selecionado.
                            </div>
                        )}
                    </div>

                    <div className="space-y-4">
                        <label className="flex items-center gap-2 text-sm text-gray-700">
                            <input
                                type="checkbox"
                                checked={modoForcar}
                                onChange={(e) => setModoForcar(e.target.checked)}
                                className="h-4 w-4 rounded border-gray-300 text-emerald-600"
                                disabled={uploading || isProcessing}
                            />
                            Forçar reprocessamento
                        </label>

                        <div className="space-y-2">
                            <label className="text-sm font-medium text-gray-700 flex items-center gap-2">
                                Limite de peso (kg)
                                <span
                                    className="text-gray-400"
                                    title="CT-es acima desse peso serão marcados como inválidos. Valor padrão: 15000 kg."
                                >
                                    <Info className="w-4 h-4" />
                                </span>
                            </label>

                            <input
                                type="number"
                                min={0}
                                step="0.01"
                                placeholder="15000"
                                value={limitePeso}
                                onChange={(e) =>
                                    setLimitePeso(e.target.value === "" ? "" : Number(e.target.value))
                                }
                                className="w-full border rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-emerald-500"
                                disabled={uploading || isProcessing}
                            />

                            <p className="text-xs text-gray-500">
                                CT-es acima desse peso serão marcados como inválidos.
                            </p>
                            <p className="text-xs text-gray-500">
                                Limite aplicado: <strong>{limitePesoAplicado.toLocaleString("pt-BR")} kg</strong>
                            </p>
                        </div>
                    </div>
                </div>

                <div className="flex flex-wrap gap-3">
                    <button
                        disabled={!file || uploading || isProcessing}
                        onClick={processar}
                        className="bg-emerald-600 text-white px-4 py-2 rounded-lg flex items-center gap-2 hover:bg-emerald-700 disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                        {uploading ? (
                            <>
                                <Loader2 className="w-4 h-4 animate-spin" />
                                Enviando...
                            </>
                        ) : isProcessing ? (
                            <>
                                <Loader2 className="w-4 h-4 animate-spin" />
                                Processando...
                            </>
                        ) : (
                            "Processar Data Input"
                        )}
                    </button>

                    <button
                        type="button"
                        onClick={limparEstado}
                        className="px-4 py-2 rounded-lg border text-gray-700 hover:bg-gray-50 disabled:opacity-50"
                        disabled={uploading}
                    >
                        Limpar
                    </button>

                    <button
                        type="button"
                        onClick={carregarHistorico}
                        className="px-4 py-2 rounded-lg border text-gray-700 hover:bg-gray-50 disabled:opacity-50 flex items-center gap-2"
                        disabled={carregandoHistorico}
                    >
                        {carregandoHistorico ? (
                            <Loader2 className="w-4 h-4 animate-spin" />
                        ) : (
                            <RefreshCw className="w-4 h-4" />
                        )}
                        Atualizar histórico
                    </button>
                </div>
            </div>

            {resultado?.status === "processing" && (
                <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-5 space-y-4">
                    <div className="flex items-center gap-2">
                        <Clock3 className="w-5 h-5 text-amber-500" />
                        <h3 className="font-semibold text-gray-900">Processando</h3>
                    </div>

                    <div>
                        <p className="text-sm text-gray-600 mb-2">
                            {resultado.step || "Em andamento"}
                        </p>

                        <div className="w-full bg-gray-200 rounded-full h-3 overflow-hidden">
                            <div
                                className="bg-emerald-600 h-3 rounded-full transition-all duration-500"
                                style={{ width: `${resultado.progress || 0}%` }}
                            />
                        </div>

                        <p className="text-xs text-gray-500 mt-2">
                            {resultado.progress ?? 0}% concluído
                        </p>
                    </div>

                    <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                        <div className="border rounded-lg p-4 flex flex-col items-center bg-gray-50">
                            <span className="text-sm text-gray-500">Total</span>
                            <span className="text-xl font-semibold text-gray-900">
                                {resultado.total_processados ?? 0}
                            </span>
                        </div>

                        <div className="border rounded-lg p-4 flex flex-col items-center bg-emerald-50">
                            <span className="text-sm text-emerald-700">Válidos</span>
                            <span className="text-xl font-semibold text-emerald-700">
                                {resultado.validos ?? 0}
                            </span>
                        </div>

                        <div className="border rounded-lg p-4 flex flex-col items-center bg-rose-50">
                            <span className="text-sm text-rose-700">Inválidos</span>
                            <span className="text-xl font-semibold text-rose-700">
                                {resultado.invalidos ?? 0}
                            </span>
                        </div>
                    </div>
                </div>
            )}

            {resultado && (resultado.status === "done" || resultado.status === "error") && (
                <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-5 space-y-4">
                    <div className="flex items-center gap-2">
                        {resultado.status === "done" ? (
                            <CheckCircle2 className="w-5 h-5 text-emerald-600" />
                        ) : (
                            <AlertCircle className="w-5 h-5 text-rose-600" />
                        )}
                        <h3 className="font-semibold text-gray-900">Resultado</h3>
                    </div>

                    <p className="text-sm text-gray-700">
                        {resultado.mensagem ||
                            (resultado.status === "done"
                                ? "Processamento concluído com sucesso."
                                : "O processamento falhou.")}
                    </p>

                    {resultado.error && (
                        <div className="rounded-lg border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700 whitespace-pre-wrap break-words">
                            {resultado.error}
                        </div>
                    )}

                    <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                        <div className="border rounded-lg p-4 flex flex-col items-center">
                            <span className="text-sm text-gray-500">Total processados</span>
                            <span className="text-lg font-semibold flex items-center gap-1 text-gray-900">
                                <FileSpreadsheet className="w-4 h-4 text-gray-500" />
                                {resultado.total_processados ?? 0}
                            </span>
                        </div>

                        <div className="border rounded-lg p-4 flex flex-col items-center">
                            <span className="text-sm text-emerald-700">Válidos</span>
                            <span className="text-lg font-semibold text-emerald-700 flex items-center gap-1">
                                <CheckCircle2 className="w-4 h-4" />
                                {resultado.validos ?? 0}
                            </span>
                        </div>

                        <div className="border rounded-lg p-4 flex flex-col items-center">
                            <span className="text-sm text-rose-700">Inválidos</span>
                            <span className="text-lg font-semibold text-rose-700 flex items-center gap-1">
                                <XCircle className="w-4 h-4" />
                                {resultado.invalidos ?? 0}
                            </span>
                        </div>
                    </div>

                    {resultado.status === "done" && resultado.job_id && (
                        <div className="pt-2">
                            <button
                                type="button"
                                onClick={() => baixarResultado(resultado.job_id!)}
                                className="px-4 py-2 rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 flex items-center gap-2"
                                disabled={downloadLoadingId === resultado.job_id}
                            >
                                {downloadLoadingId === resultado.job_id ? (
                                    <Loader2 className="w-4 h-4 animate-spin" />
                                ) : (
                                    <Download className="w-4 h-4" />
                                )}
                                Baixar resultado
                            </button>
                        </div>
                    )}
                </div>
            )}

            <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-5">
                <div className="flex items-center justify-between gap-3 mb-4">
                    <h3 className="font-semibold text-gray-900">Últimos processamentos</h3>
                    <span className="text-xs text-gray-500">
                        {historico.length} registro(s)
                    </span>
                </div>

                {carregandoHistorico ? (
                    <div className="py-8 flex items-center justify-center text-gray-500">
                        <Loader2 className="w-5 h-5 animate-spin mr-2" />
                        Carregando histórico...
                    </div>
                ) : historico.length === 0 ? (
                    <div className="py-8 text-sm text-gray-500 text-center">
                        Nenhum processamento encontrado.
                    </div>
                ) : (
                    <div className="overflow-x-auto">
                        <table className="w-full text-sm border border-gray-200 rounded-lg overflow-hidden">
                            <thead className="bg-gray-50">
                                <tr>
                                    <th className="p-3 text-left">Data</th>
                                    <th className="p-3 text-left">Arquivo</th>
                                    <th className="p-3 text-left">Status</th>
                                    <th className="p-3 text-center">Total</th>
                                    <th className="p-3 text-center">Válidos</th>
                                    <th className="p-3 text-center">Inválidos</th>
                                    <th className="p-3 text-center">Ação</th>
                                </tr>
                            </thead>
                            <tbody>
                                {historico.map((h) => (
                                    <tr key={h.job_id} className="border-t">
                                        <td className="p-3 whitespace-nowrap">
                                            {new Date(h.criado_em).toLocaleString("pt-BR")}
                                        </td>
                                        <td className="p-3">
                                            <div className="max-w-[280px] truncate" title={h.arquivo}>
                                                {h.arquivo}
                                            </div>
                                        </td>
                                        <td className="p-3">{statusHistoricoBadge(h.status)}</td>
                                        <td className="p-3 text-center">{h.total_processados ?? 0}</td>
                                        <td className="p-3 text-center text-emerald-700 font-medium">
                                            {h.validos ?? 0}
                                        </td>
                                        <td className="p-3 text-center text-rose-700 font-medium">
                                            {h.invalidos ?? 0}
                                        </td>
                                        <td className="p-3 text-center">
                                            {h.status === "done" ? (
                                                <button
                                                    onClick={() => baixarResultado(h.job_id)}
                                                    className="text-emerald-600 hover:underline inline-flex items-center gap-1 disabled:opacity-50"
                                                    disabled={downloadLoadingId === h.job_id}
                                                >
                                                    {downloadLoadingId === h.job_id ? (
                                                        <Loader2 className="w-4 h-4 animate-spin" />
                                                    ) : (
                                                        <Download className="w-4 h-4" />
                                                    )}
                                                    Download
                                                </button>
                                            ) : (
                                                <span className="text-gray-400">—</span>
                                            )}
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                )}
            </div>
        </div>
    );
}