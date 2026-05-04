// frontend/src/services/exploratoryApi.ts
import api from "@/services/api";

export interface EDAParams {
    data_inicial: string;
    data_final: string;
    granularidade: "diaria" | "mensal" | "anual";
}

export interface ResumoDados {
    totais: {
        total_entregas: number;
        total_peso: number;
        total_volumes: number;
        total_valor_nf: number;
        total_valor_frete: number;
    };
    cobertura_datas: {
        data_minima: string | null;
        data_maxima: string | null;
        dias_cobertos: number;
    };
    nulos_pct: Record<string, number>;
}

export interface OutlierIQR {
    coluna: string;
    total_observacoes: number;
    outliers: number;
    percentual: number;
    lim_inf: number;
    lim_sup: number;
}

export interface ZeradoItem {
    coluna: string;
    zerados: number;
    nulos: number;
    pct_zerados: number;
    pct_nulos: number;
}

export interface QualidadeDados {
    outliers_iqr: OutlierIQR[];
    zerados: ZeradoItem[];
    campos_criticos_faltando: { campo: string; faltando: number; pct: number }[];
}

export interface SerieTemporal {
    periodo: string;
    qtd_entregas: number;
    total_peso: number;
    total_volumes: number;
    total_valor_nf: number;
    total_valor_frete: number;
}

export interface TemporalDados {
    granularidade: string;
    series: SerieTemporal[];
}

export interface BinHistograma {
    bin_label: string;
    count: number;
}

export interface DistribuicaoDados {
    peso: BinHistograma[];
    valor_nf: BinHistograma[];
    valor_frete: BinHistograma[];
    volumes: BinHistograma[];
    frete_sobre_nf: BinHistograma[];
}

export interface RankingDestinatario {
    destinatario_nome: string;
    cte_cidade: string;
    cte_uf: string;
    qtd_entregas?: number;
    valor_total_nf?: number;
}

export interface RankingCidade {
    cte_cidade: string;
    cte_uf: string;
    qtd_entregas: number;
    valor_total_nf: number;
}

export interface RankingsDados {
    top_frequencia: RankingDestinatario[];
    top_valor_nf: RankingDestinatario[];
    top_cidades: RankingCidade[];
}

export interface PontoGeo {
    lat: number;
    lon: number;
    valor_nf: number;
    destinatario_nome: string;
    cidade: string;
}

export interface GeograficoDados {
    pontos: PontoGeo[];
    total_com_coordenadas: number;
    total_sem_coordenadas: number;
}

export interface CorrelacaoDados {
    variaveis: string[];
    matriz: { var_x: string; var_y: string; r: number }[];
}

export interface ConcentracaoFimMes {
    periodo: string;
    total_entregas: number;
    entregas_ultimos_5uteis: number;
    entregas_resto: number;
    pct_ultimos_5uteis: number;
}

export interface ConcentracaoDados {
    fim_mes: ConcentracaoFimMes[];
    dia_semana: { dia: string; qtd_entregas: number }[];
    dia_mes: { dia: number; qtd_entregas: number }[];
}

export type AnalysisName =
    | "resumo"
    | "qualidade"
    | "temporal"
    | "distribuicao"
    | "rankings"
    | "geografico"
    | "correlacao"
    | "concentracao";

async function fetchEda<T>(analysis: AnalysisName, params: EDAParams): Promise<T> {
    const { data } = await api.get<T>(`/exploratory/eda/${analysis}`, { params });
    return data;
}

export function fetchResumo(params: EDAParams) {
    return fetchEda<ResumoDados>("resumo", params);
}
export function fetchQualidade(params: EDAParams) {
    return fetchEda<QualidadeDados>("qualidade", params);
}
export function fetchTemporal(params: EDAParams) {
    return fetchEda<TemporalDados>("temporal", params);
}
export function fetchDistribuicao(params: EDAParams) {
    return fetchEda<DistribuicaoDados>("distribuicao", params);
}
export function fetchRankings(params: EDAParams) {
    return fetchEda<RankingsDados>("rankings", params);
}
export function fetchGeografico(params: EDAParams) {
    return fetchEda<GeograficoDados>("geografico", params);
}
export function fetchCorrelacao(params: EDAParams) {
    return fetchEda<CorrelacaoDados>("correlacao", params);
}
export function fetchConcentracao(params: EDAParams) {
    return fetchEda<ConcentracaoDados>("concentracao", params);
}
