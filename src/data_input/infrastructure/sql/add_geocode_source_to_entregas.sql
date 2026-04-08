ALTER TABLE public.entregas
ADD COLUMN IF NOT EXISTS geocode_source text;
