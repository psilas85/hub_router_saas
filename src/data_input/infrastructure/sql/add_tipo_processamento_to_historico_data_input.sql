ALTER TABLE public.historico_data_input
ADD COLUMN IF NOT EXISTS tipo_processamento text;

UPDATE public.historico_data_input
SET tipo_processamento = COALESCE(tipo_processamento, 'padrao');
