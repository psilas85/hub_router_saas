// hub_router_1.0.1/frontend/src/App.tsx
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import HomePage from "@/pages/Home/HomePage";
import LoginPage from "@/pages/Login/LoginPage";
import DataInputPage from "@/pages/Data_input/DataInputPage";
import ClusterizationPage from "@/pages/Clusterization/ClusterizationPage";
import ProfilePage from "@/pages/Profile/ProfilePage";
import UsersPage from "@/pages/Users/UsersPage";
import TenantsPage from "@/pages/Tenants/TenantsPage";
import ProtectedRoute from "@/routes/ProtectedRoute";
import Layout from "@/components/Layout";

// Middle-Mile
import VehiclesPage from "@/pages/middle_mile/VehiclesPage";
import MmRoutingPage from "@/pages/middle_mile/RoutingPage";
import CostsPage from "@/pages/middle_mile/CostsPage";

// Last-Mile
import LastMileVehiclesPage from "@/pages/LastMile/LastMileVehiclesPage";
import LastMileRoutingPage from "@/pages/LastMile/LastMileRoutingPage";
import LastMileCostsPage from "@/pages/LastMile/LastMileCostsPage";

// Outros módulos
import SimulationPage from "@/pages/Simulation/SimulationPage";
import PlannerPage from "@/pages/Planner/PlannerPage";
import ExploratoryDashboardPage from "@/pages/Exploratory/ExploratoryDashboardPage";


export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        {/* Público */}
        <Route path="/login" element={<LoginPage />} />

        {/* Protegidas */}
        {/* Página inicial agora abre a Home protegida */}
        <Route
          path="/"
          element={
            <ProtectedRoute>
              <Layout>
                <HomePage />
              </Layout>
            </ProtectedRoute>
          }
        />

        {/* Exploratory (Streamlit via iframe) */}
        <Route
          path="/exploratory_analysis_ui"
          element={
            <ProtectedRoute>
              <Layout>
                <ExploratoryDashboardPage />
              </Layout>
            </ProtectedRoute>
          }
        />
        {/* Alias curto para EDA */}
        <Route path="/eda" element={<Navigate to="/exploratory_analysis_ui" replace />} />


        <Route
          path="/data-input"
          element={
            <ProtectedRoute>
              <Layout>
                <DataInputPage />
              </Layout>
            </ProtectedRoute>
          }
        />
        <Route
          path="/clusterization"
          element={
            <ProtectedRoute>
              <Layout>
                <ClusterizationPage />
              </Layout>
            </ProtectedRoute>
          }
        />

        {/* Middle-Mile */}
        <Route
          path="/middle-mile/vehicles"
          element={
            <ProtectedRoute>
              <Layout>
                <VehiclesPage />
              </Layout>
            </ProtectedRoute>
          }
        />
        <Route
          path="/middle-mile/routing"
          element={
            <ProtectedRoute>
              <Layout>
                <MmRoutingPage />
              </Layout>
            </ProtectedRoute>
          }
        />
        <Route
          path="/middle-mile/costs"
          element={
            <ProtectedRoute>
              <Layout>
                <CostsPage />
              </Layout>
            </ProtectedRoute>
          }
        />

        {/* Last-Mile */}
        <Route
          path="/last-mile/vehicles"
          element={
            <ProtectedRoute>
              <Layout>
                <LastMileVehiclesPage />
              </Layout>
            </ProtectedRoute>
          }
        />
        <Route
          path="/last-mile/routing"
          element={
            <ProtectedRoute>
              <Layout>
                <LastMileRoutingPage />
              </Layout>
            </ProtectedRoute>
          }
        />
        <Route
          path="/last-mile/costs"
          element={
            <ProtectedRoute>
              <Layout>
                <LastMileCostsPage />
              </Layout>
            </ProtectedRoute>
          }
        />

        {/* Outros módulos */}
        <Route
          path="/simulation"
          element={
            <ProtectedRoute>
              <Layout>
                <SimulationPage />
              </Layout>
            </ProtectedRoute>
          }
        />
        <Route
          path="/planner"
          element={
            <ProtectedRoute>
              <Layout>
                <PlannerPage />
              </Layout>
            </ProtectedRoute>
          }
        />
        <Route
          path="/users"
          element={
            <ProtectedRoute>
              <Layout>
                <UsersPage />
              </Layout>
            </ProtectedRoute>
          }
        />
        <Route
          path="/tenants"
          element={
            <ProtectedRoute>
              <Layout>
                <TenantsPage />
              </Layout>
            </ProtectedRoute>
          }
        />
        <Route
          path="/profile"
          element={
            <ProtectedRoute>
              <Layout>
                <ProfilePage />
              </Layout>
            </ProtectedRoute>
          }
        />

        {/* Default */}
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </BrowserRouter>
  );
}
