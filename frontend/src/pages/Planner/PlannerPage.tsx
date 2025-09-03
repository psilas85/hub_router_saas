// frontend/src/pages/Planner/PlannerPage.tsx
import { useState } from "react";
import TrainCompareTab from "./TrainCompareTab";
import PlanTab from "./PlanTab";
import WhatIfTab from "./WhatIfTab";
import ResultsTab from "./ResultsTab";
import { PlannerProvider } from "@/context/PlannerContext";

const tabs = [
    { key: "compare", label: "1. Comparar Modelos" },
    { key: "plan", label: "2. Planejamento" },
    { key: "results", label: "3. Resultados" },
    { key: "whatif", label: "4. What-if" },
] as const;

export default function PlannerPage() {
    const [active, setActive] = useState<typeof tabs[number]["key"]>("compare");

    return (
        <PlannerProvider>
            <div className="p-6 space-y-6">
                <div className="flex items-center justify-between">
                    <h1 className="text-2xl font-semibold">Planner • HubRouter</h1>
                </div>

                <div className="flex gap-2 border-b">
                    {tabs.map((t) => (
                        <button
                            key={t.key}
                            onClick={() => setActive(t.key)}
                            className={`px-4 py-2 -mb-px rounded-t-xl ${active === t.key
                                    ? "bg-white border border-b-white"
                                    : "bg-gray-100 hover:bg-gray-200 border border-transparent"
                                }`}
                        >
                            {t.label}
                        </button>
                    ))}
                </div>

                <div className="bg-white border rounded-xl p-4 shadow-sm">
                    {active === "compare" && <TrainCompareTab />}
                    {active === "plan" && <PlanTab />}
                    {active === "results" && <ResultsTab />}
                    {active === "whatif" && <WhatIfTab />}
                </div>
            </div>
        </PlannerProvider>
    );
}
