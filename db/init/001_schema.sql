CREATE TABLE customers(
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  country TEXT
);

CREATE TABLE orders(
  id SERIAL PRIMARY KEY,
  customer_id INT REFERENCES customers(id),
  order_date DATE NOT NULL,
  total_amount NUMERIC(12,2) NOT NULL
);

CREATE TABLE items(
  id SERIAL PRIMARY KEY,
  order_id INT REFERENCES orders(id),
  sku TEXT,
  product_name TEXT,
  qty INT NOT NULL,
  unit_price NUMERIC(12,2) NOT NULL
);

CREATE INDEX idx_orders_customer_date ON orders(customer_id, order_date);
CREATE INDEX idx_items_order ON items(order_id);
