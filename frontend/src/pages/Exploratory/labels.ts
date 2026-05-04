// frontend/src/pages/Exploratory/labels.ts

export const FIELD_LABELS: Record<string, string> = {
    cte_peso: "Peso (kg)",
    cte_valor_nf: "Valor da NF (R$)",
    cte_valor_frete: "Receita de frete (R$)",
    cte_volumes: "Volumes",
    destino_latitude: "Latitude destino",
    destino_longitude: "Longitude destino",
    destinatario_nome: "Destinatário",
    cte_cidade: "Cidade",
    cte_uf: "UF",
    cte_numero: "Nº CTE",
    remetente_cidade: "Cidade remetente",
};

export function fieldLabel(key: string): string {
    return FIELD_LABELS[key] ?? key;
}

export interface TipContent {
    comercial: string;
    tecnico?: string;
}

export const INFO_TIPS: Record<string, TipContent> = {
    resumo_totais: {
        comercial: "Soma acumulada de todas as entregas realizadas no período selecionado.",
    },
    resumo_nulos: {
        comercial: "Campos sem informação que podem comprometer relatórios e roteirizações.",
        tecnico: "Percentual calculado sobre o total de registros. Campos geográficos nulos impedem a plotagem no mapa.",
    },
    outliers_iqr: {
        comercial: "Entregas com valores muito fora do padrão — podem indicar erros de digitação ou eventos excepcionais.",
        tecnico: "Método IQR (Intervalo Interquartil): limites calculados como Q1 − 1,5×IQR e Q3 + 1,5×IQR. Valores fora desses limites são marcados como outliers.",
    },
    zerados: {
        comercial: "Campos preenchidos com zero quando deveriam ter um valor positivo (ex: peso zerado numa entrega).",
        tecnico: "Zerado e nulo têm tratamentos distintos: zero é um valor inválido no contexto logístico; nulo indica ausência de dado.",
    },
    campos_criticos: {
        comercial: "Campos essenciais para o funcionamento das análises. Registros faltando nesses campos ficam de fora dos cálculos.",
    },
    temporal: {
        comercial: "Evolução das entregas ao longo do tempo. Útil para identificar sazonalidade, crescimento e períodos de baixo volume.",
    },
    distribuicao: {
        comercial: "Mostra como os valores se distribuem: se há muitas entregas leves, se existe concentração numa faixa, etc.",
        tecnico: "Histograma com 30 intervalos (bins) de largura uniforme. Calculado via numpy.histogram apenas para valores > 0.",
    },
    frete_sobre_nf: {
        comercial: "Percentual da receita de frete em relação ao valor da mercadoria. Ex: 10% significa que o frete vale 10% do valor do produto transportado.",
        tecnico: "Calculado como (cte_valor_frete ÷ cte_valor_nf) × 100. Apenas registros com ambos > 0 são considerados. Valores típicos em transportadoras: 5–25%.",
    },
    pareto: {
        comercial: "A linha mostra o percentual acumulado de entregas à medida que as faixas avançam. Onde a linha cruza 80% revela em quais faixas está concentrada a maior parte do volume.",
        tecnico: "Princípio de Pareto (regra 80/20): em geral 80% do volume está concentrado em 20% das faixas de valor. A linha vermelha tracejada marca o limiar de 80%.",
    },
    rankings: {
        comercial: "Clientes e cidades que mais movimentaram volume e valor no período — base para negociação de contratos e priorização de rotas.",
    },
    geografico: {
        comercial: "Distribuição espacial das entregas. Ajuda a identificar regiões com alta concentração e oportunidades de otimização de rota.",
        tecnico: "Quando o volume supera 5.000 pontos, uma amostra aleatória é exibida. A cor indica a faixa de valor da NF: verde = baixo, laranja = médio, vermelho = alto.",
    },
    correlacao: {
        comercial: "O quanto duas variáveis 'andam juntas'. Ex: quando o peso aumenta, o frete também sobe proporcionalmente?",
        tecnico: "Correlação de Pearson (r): varia de −1 a +1. |r| ≥ 0,7 = forte; 0,3–0,7 = moderada; < 0,3 = fraca. Azul = correlação negativa, vermelho = positiva.",
    },
    concentracao_fim_mes: {
        comercial: "Mede o quanto a operação se concentra nos últimos dias do mês — fenômeno comum em empresas com fechamento mensal.",
        tecnico: "Considera os últimos 5 dias úteis de cada mês. Percentual acima de 35% indica concentração relevante que pode sobrecarregar a operação.",
    },
    concentracao_dia_semana: {
        comercial: "Distribuição de entregas por dia da semana — identifica sobrecargas em dias específicos.",
    },
    concentracao_dia_mes: {
        comercial: "Padrão de entregas por dia do mês — revela ciclos internos como fechamento quinzenal ou mensal.",
    },
};
