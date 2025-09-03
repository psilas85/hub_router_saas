#hub_router_1.0.1/src/ml_pipeline/planning/hub_candidates.py

import pandas as pd
from typing import List, Tuple

class HubCandidates:
    """
    Seleciona hubs candidatos por densidade histórica de entregas.
    """
    def select(self, hist: pd.DataFrame, top_n: int=20) -> List[Tuple[str,str]]:
        agg = (hist.groupby(["cidade","uf"])["entregas"]
                  .sum().reset_index().sort_values("entregas", ascending=False))
        return list(agg.head(top_n)[["cidade","uf"]].itertuples(index=False, name=None))
