CREATE TABLE IF NOT EXISTS public.stock_prices (
    symbol TEXT NOT NULL,
    ts TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    open NUMERIC(18,6),
    high NUMERIC(18,6),
    low NUMERIC(18,6),
    close NUMERIC(18,6),
    volume BIGINT,
    meta JSONB,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    CONSTRAINT stock_prices_pk PRIMARY KEY (symbol, ts)
);

CREATE INDEX IF NOT EXISTS stock_prices_symbol_ts_idx
    ON public.stock_prices (symbol, ts);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_set_updated_at ON public.stock_prices;
CREATE TRIGGER trg_set_updated_at
BEFORE UPDATE ON public.stock_prices
FOR EACH ROW EXECUTE FUNCTION set_updated_at();


