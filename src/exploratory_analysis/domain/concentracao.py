# exploratory_analysis/domain/concentracao.py

import pandas as pd
import pandas.tseries.offsets as offsets

DIAS_PT = {
    "Monday": "Segunda",
    "Tuesday": "Terça",
    "Wednesday": "Quarta",
    "Thursday": "Quinta",
    "Friday": "Sexta",
    "Saturday": "Sábado",
    "Sunday": "Domingo",
}


def _ultimos_dias_uteis_do_mes(data: pd.Timestamp) -> set:
    fim_mes = data + offsets.MonthEnd(0)
    inicio_mes = data.replace(day=1)
    dias_uteis = pd.bdate_range(inicio_mes, fim_mes)
    return set(dias_uteis[-5:])


def calcular(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"fim_mes": [], "dia_semana": [], "dia_mes": []}

    df = df.copy()
    df["envio_data"] = pd.to_datetime(df["envio_data"], errors="coerce")
    df = df.dropna(subset=["envio_data"])

    df["mes"] = df["envio_data"].dt.to_period("M")
    df["fim_mes_util"] = df["envio_data"].apply(
        lambda d: d in _ultimos_dias_uteis_do_mes(d)
    )

    resultado_mes = df.groupby("mes").agg(
        total_entregas=("cte_numero", "count"),
        entregas_ultimos_5uteis=("fim_mes_util", "sum"),
    ).reset_index()
    resultado_mes["mes"] = resultado_mes["mes"].astype(str)
    resultado_mes["entregas_resto"] = (
        resultado_mes["total_entregas"] - resultado_mes["entregas_ultimos_5uteis"]
    )
    resultado_mes["pct_ultimos_5uteis"] = (
        resultado_mes["entregas_ultimos_5uteis"] / resultado_mes["total_entregas"] * 100
    ).round(1)
    fim_mes = resultado_mes.rename(columns={"mes": "periodo"})[
        ["periodo", "total_entregas", "entregas_ultimos_5uteis", "entregas_resto", "pct_ultimos_5uteis"]
    ].to_dict(orient="records")

    ordem = list(DIAS_PT.keys())
    df["dia_semana_en"] = df["envio_data"].dt.day_name()
    contagem_semana = df["dia_semana_en"].value_counts().reindex(ordem).fillna(0)
    dia_semana = [
        {"dia": DIAS_PT[k], "qtd_entregas": int(v)}
        for k, v in contagem_semana.items()
    ]

    df["dia_mes"] = df["envio_data"].dt.day
    contagem_dia_mes = df["dia_mes"].value_counts().sort_index()
    dia_mes = [
        {"dia": int(k), "qtd_entregas": int(v)}
        for k, v in contagem_dia_mes.items()
    ]

    return {
        "fim_mes": fim_mes,
        "dia_semana": dia_semana,
        "dia_mes": dia_mes,
    }
