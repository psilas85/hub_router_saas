// frontend/src/utils/format.ts

export const fmtBR = (v: number) =>
    new Intl.NumberFormat("pt-BR").format(v);

export const fmtMoeda = (v: number) =>
    new Intl.NumberFormat("pt-BR", {
        style: "currency",
        currency: "BRL",
        maximumFractionDigits: 2,
    }).format(v);

export const fmtCompact = (v: number) =>
    new Intl.NumberFormat("pt-BR", {
        notation: "compact",
        compactDisplay: "short",
        maximumFractionDigits: 1,
    }).format(v);

export const fmtPerc = (v: number, digits = 1) =>
    `${v.toFixed(digits).replace(".", ",")}%`;

// Novo helper para minutos → "Xh Ym"
export const fmtTempoMin = (min: number): string => {
    const h = Math.floor(min / 60);
    const m = min % 60;
    return `${h}h ${m}m`;
};
