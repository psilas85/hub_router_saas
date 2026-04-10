DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'entregas'
          AND column_name = 'cte_tempo_atendimento'
    ) THEN
        ALTER TABLE public.entregas
            RENAME COLUMN cte_tempo_atendimento TO cte_tempo_atendimento_min;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'entregas'
          AND column_name = 'cte_prazo'
    ) THEN
        ALTER TABLE public.entregas
            RENAME COLUMN cte_prazo TO cte_prazo_min;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'entregas_tempo_atendimento_min_chk'
    ) THEN
        ALTER TABLE public.entregas
            ADD CONSTRAINT entregas_tempo_atendimento_min_chk
            CHECK (
                cte_tempo_atendimento_min IS NULL
                OR cte_tempo_atendimento_min >= 0
            );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'entregas_prazo_min_chk'
    ) THEN
        ALTER TABLE public.entregas
            ADD CONSTRAINT entregas_prazo_min_chk
            CHECK (
                cte_prazo_min IS NULL
                OR cte_prazo_min >= 0
            );
    END IF;
END $$;

COMMENT ON COLUMN public.entregas.cte_tempo_atendimento_min
IS 'Tempo de atendimento da entrega em minutos.';

COMMENT ON COLUMN public.entregas.cte_prazo_min
IS 'Prazo operacional da entrega em minutos a partir da liberacao do veiculo.';