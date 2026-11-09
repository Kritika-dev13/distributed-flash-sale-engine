-- Schema for the persistent order store.
-- Redis is the source of truth for *live* stock; PostgreSQL is the durable ledger.
-- There is intentionally NO "stock = stock - n" UPDATE here: that would reintroduce a
-- hot-row lock. Sold units are derived from the append-only orders table (see product_sales).

CREATE TABLE products (
    id              TEXT PRIMARY KEY,
    name            TEXT        NOT NULL,
    price_cents     INTEGER     NOT NULL CHECK (price_cents >= 0),
    total_stock     INTEGER     NOT NULL CHECK (total_stock >= 0),
    sale_starts_at  TIMESTAMPTZ NOT NULL,
    sale_ends_at    TIMESTAMPTZ NOT NULL
);

CREATE TYPE order_status AS ENUM ('CONFIRMED', 'FAILED');

CREATE TABLE orders (
    id               BIGSERIAL PRIMARY KEY,
    order_id         UUID         NOT NULL UNIQUE,          -- generated at the edge, used as payment idempotency key
    user_id          TEXT         NOT NULL,
    product_id       TEXT         NOT NULL REFERENCES products(id),
    quantity         INTEGER      NOT NULL CHECK (quantity > 0),
    amount_cents     BIGINT       NOT NULL CHECK (amount_cents >= 0),
    status           order_status NOT NULL,
    failure_reason   TEXT,
    idempotency_key  TEXT         NOT NULL,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (user_id, product_id, idempotency_key)           -- DB-level backstop for duplicate checkouts
);
CREATE INDEX orders_product_status_idx ON orders (product_id, status);

CREATE TABLE payments (
    payment_id    TEXT PRIMARY KEY,
    order_id      UUID        NOT NULL UNIQUE REFERENCES orders(order_id),
    amount_cents  BIGINT      NOT NULL,
    status        TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_events (
    id          BIGSERIAL PRIMARY KEY,
    order_id    UUID        NOT NULL,
    event_type  TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Reconciliation / pre-warm helper: units durably sold per product.
CREATE VIEW product_sales AS
SELECT product_id,
       COALESCE(SUM(quantity) FILTER (WHERE status = 'CONFIRMED'), 0)::int AS units_sold
FROM orders
GROUP BY product_id;

INSERT INTO products (id, name, price_cents, total_stock, sale_starts_at, sale_ends_at)
VALUES ('sku-flash-001', 'Limited Edition Sneaker', 4999, 1000,
        now() - interval '1 hour', now() + interval '1 day');
