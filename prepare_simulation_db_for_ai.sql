-- ========================================================
-- PREPARAÇÃO DO SIMULATION_DB PARA IA
-- ========================================================

-- Criar extensões necessárias
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ========================================================
-- 1️⃣ SCHEMA AI (Machine Learning)
-- ========================================================
CREATE SCHEMA IF NOT EXISTS ai;

-- 1.1 Features Diárias
CREATE TABLE IF NOT EXISTS ai.features_diarias (
    feature_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL,
    envio_data DATE NOT NULL,
    json_features JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_features_diarias_tenant_data
    ON ai.features_diarias (tenant_id, envio_data);

-- 1.2 Previsões Diárias
CREATE TABLE IF NOT EXISTS ai.predicoes_diarias (
    predicao_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL,
    envio_data_prevista DATE NOT NULL,
    modelo_nome TEXT NOT NULL,
    json_predicoes JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_predicoes_diarias_tenant_data
    ON ai.predicoes_diarias (tenant_id, envio_data_prevista);

-- 1.3 Avaliações de Modelo
CREATE TABLE IF NOT EXISTS ai.avaliacoes_modelo (
    avaliacao_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    modelo_nome TEXT NOT NULL,
    data_treino DATE NOT NULL,
    periodo_avaliado DATERANGE NOT NULL,
    mae NUMERIC,
    rmse NUMERIC,
    acuracia NUMERIC,
    comentario TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_avaliacoes_modelo_nome
    ON ai.avaliacoes_modelo (modelo_nome, data_treino);

-- ========================================================
-- 2️⃣ AJUSTES NO SCHEMA PUBLIC
-- ========================================================

-- 2.1 Hubs
ALTER TABLE public.hubs
    ADD COLUMN IF NOT EXISTS capacidade_peso NUMERIC,
    ADD COLUMN IF NOT EXISTS capacidade_volume NUMERIC,
    ADD COLUMN IF NOT EXISTS status_operacional TEXT,
    ADD COLUMN IF NOT EXISTS data_criacao DATE DEFAULT CURRENT_DATE;

-- 2.2 Cluster Costs
ALTER TABLE public.cluster_costs
    ADD COLUMN IF NOT EXISTS custo_fixo NUMERIC,
    ADD COLUMN IF NOT EXISTS custo_variavel_km NUMERIC,
    ADD COLUMN IF NOT EXISTS custo_variavel_entrega NUMERIC;

-- 2.3 Resumo Clusters
ALTER TABLE public.resumo_clusters
    ADD COLUMN IF NOT EXISTS gargalo_potencial BOOLEAN;

-- 2.4 Garantir tenant_id e envio_data nas tabelas-chave
DO $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN 
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'
          AND table_name IN (
              'resumo_clusters',
              'resumo_rotas_last_mile',
              'resumo_transferencias',
              'cluster_costs',
              'rotas_last_mile',
              'rotas_transferencias',
              'detalhes_rotas',
              'detalhes_transferencias',
              'entregas_clusterizadas'
          )
    LOOP
        EXECUTE format('ALTER TABLE public.%I ADD COLUMN IF NOT EXISTS tenant_id TEXT;', rec.table_name);
        EXECUTE format('ALTER TABLE public.%I ADD COLUMN IF NOT EXISTS envio_data DATE;', rec.table_name);
    END LOOP;
END$$;

-- 2.5 Veículos
ALTER TABLE public.veiculos_last_mile
    ADD COLUMN IF NOT EXISTS custo_operacional_km NUMERIC,
    ADD COLUMN IF NOT EXISTS restricoes TEXT;

ALTER TABLE public.veiculos_transferencia
    ADD COLUMN IF NOT EXISTS custo_operacional_km NUMERIC,
    ADD COLUMN IF NOT EXISTS restricoes TEXT;
