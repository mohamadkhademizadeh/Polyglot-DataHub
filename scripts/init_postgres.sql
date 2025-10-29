-- PostgreSQL schema (idempotent)

CREATE TABLE IF NOT EXISTS customers (
  id SERIAL PRIMARY KEY,
  email TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
  id BIGSERIAL PRIMARY KEY,
  customer_id INT NOT NULL REFERENCES customers(id),
  total_cents INT NOT NULL,
  currency TEXT NOT NULL DEFAULT 'USD',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_items (
  id BIGSERIAL PRIMARY KEY,
  order_id BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  sku TEXT NOT NULL,
  qty INT NOT NULL,
  unit_price_cents INT NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox (
  id BIGSERIAL PRIMARY KEY,
  aggregate_type TEXT NOT NULL,   -- 'order_item'
  aggregate_id BIGINT NOT NULL,   -- order_items.id
  event_type TEXT NOT NULL,       -- 'ORDER_ITEM_CREATED'
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed BOOLEAN NOT NULL DEFAULT false
);

CREATE OR REPLACE FUNCTION emit_order_item_outbox() RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO outbox(aggregate_type, aggregate_id, event_type, payload)
  VALUES ('order_item', NEW.id, 'ORDER_ITEM_CREATED',
          jsonb_build_object('order_id', NEW.order_id, 'sku', NEW.sku, 'qty', NEW.qty));
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_order_items_outbox ON order_items;
CREATE TRIGGER trg_order_items_outbox
AFTER INSERT ON order_items
FOR EACH ROW
EXECUTE PROCEDURE emit_order_item_outbox();
