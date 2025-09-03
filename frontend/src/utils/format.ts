// frontend/src/utils/format.ts

export const fmtBR = new Intl.NumberFormat("pt-BR");

export const fmtMoeda = new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL",
    maximumFractionDigits: 2,
});

export const fmtCompact = new Intl.NumberFormat("pt-BR", {
    notation: "compact",
    compactDisplay: "short",
    maximumFractionDigits: 1,
});

export const fmtPerc = (v: number, digits = 1) =>
    `${v.toFixed(digits).replace(".", ",")}%`;

