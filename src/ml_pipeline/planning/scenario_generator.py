#hub_router_1.0.1/src/ml_pipeline/planning/scenario_generator.py

class ScenarioGenerator:
    def list_scenarios(self, base=True, baixo=True, alto=True):
        out = []
        if baixo: out.append("baixo")
        if base:  out.append("base")
        if alto:  out.append("alto")
        return out
