INSERT INTO customers(name,country) VALUES
 ('Alpha Ltd','IL'),
 ('Beta Inc','US'),
 ('Gamma GmbH','DE'),
 ('Delta SARL','FR');

INSERT INTO orders(customer_id, order_date, total_amount) VALUES
 (1,'2025-09-01',120.00),
 (1,'2025-09-03',250.00),
 (2,'2025-09-02',500.00),
 (3,'2025-09-05',75.00),
 (4,'2025-09-06',300.00);

INSERT INTO items(order_id, sku, product_name, qty, unit_price) VALUES
 (1,'A-1','Widget A',2,30.00),
 (1,'B-1','Widget B',1,60.00),
 (2,'A-1','Widget A',5,30.00),
 (3,'C-1','Widget C',10,7.50),
 (4,'B-1','Widget B',4,75.00);
