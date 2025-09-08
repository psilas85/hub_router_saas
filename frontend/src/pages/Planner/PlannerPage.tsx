// frontend/src/pages/Planner/PlannerPage.tsx

import { useState } from "react";
import TrainCompareTab from "./TrainCompareTab";
import PlanTab from "./PlanTab";
import PlanV2Tab from "./PlanV2Tab"; // ✅ import adicionado
import WhatIfTab from "./WhatIfTab";
import ResultsTab from "./ResultsTab";
import { PlannerProvider } from "@/context/PlannerContext";
import { BarChart3, ClipboardList, LineChart, HelpCircle } from "lucide-react";

const tabs = [
    { key: "compare", label: "1. Treinamento", icon: BarChart3 },
    { key: "plan", label: "2. Planejamento (Histórico)", icon: ClipboardList },
    { key: "planv2", label: "3. Planejamento (ML)", icon: LineChart },
    { key: "results", label: "4. Resultados", icon: LineChart },
    { key: "whatif", label: "5. Testar Cenários", icon: HelpCircle },
] as const;

export default function PlannerPage() {
    const [active, setActive] = useState<typeof tabs[number]["key"]>("compare");

    return (
        <PlannerProvider>
            <div className="p-6 space-y-6">
                {/* Header */}
                <div className="flex items-center justify-between">
                    <h1 className="text-2xl font-semibold">Planner • HubRouter</h1>
                </div>

                {/* Tabs */}
                <div role="tablist" className="flex gap-2 border-b overflow-x-auto scrollbar-thin">
                    {tabs.map((t) => {
                        const Icon = t.icon;
                        return (
                            <button
                                key={t.key}
                                role="tab"
                                aria-selected={active === t.key}
                                onClick={() => setActive(t.key)}
                                className={`flex items-center gap-2 px-4 py-2 -mb-px rounded-t-xl border transition-colors ${active === t.key
                                        ? "bg-white border-b-white text-emerald-700 font-medium shadow-sm"
                                        : "bg-gray-100 hover:bg-gray-200 border-transparent text-gray-600"
                                    }`}
                            >
                                <Icon className="w-4 h-4" />
                                {t.label}
                            </button>
                        );
                    })}
                </div>

                {/* Content */}
                <div className="bg-white border rounded-xl p-4 shadow-sm">
                    {active === "compare" && <TrainCompareTab />}
                    {active === "plan" && <PlanTab />}
                    {active === "planv2" && <PlanV2Tab />}
                    {active === "results" && <ResultsTab />}
                    {active === "whatif" && <WhatIfTab />}
                </div>
            </div>
        </PlannerProvider>
    );
}
