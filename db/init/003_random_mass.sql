INSERT INTO customers (id, name, country)
SELECT g, 'Customer ' || g, 
       CASE (random() * 5)::int
           WHEN 0 THEN 'US'
           WHEN 1 THEN 'UK'
           WHEN 2 THEN 'IL'
           WHEN 3 THEN 'FR'
           WHEN 4 THEN 'JP'
       END
FROM generate_series(1, 1000) g;

INSERT INTO orders (customer_id, order_date, total_amount)
SELECT floor(random()*1000 + 1)::int,     -- 1 עד 1000
       NOW() - (random()*365)::int * interval '1 day',
       (random()*1000)::numeric
FROM generate_series(1, 500000);