question,sql,notes
"What was the total revenue last month?",
"SELECT SUM(o.total_amount) AS revenue
 FROM orders o
 WHERE o.order_date >= date_trunc('month', CURRENT_DATE - interval '1 month')
   AND o.order_date < date_trunc('month', CURRENT_DATE);",
"Aggregation by month"

"List top 3 customers by revenue",
"SELECT c.name, SUM(i.qty * i.unit_price) AS revenue
 FROM customers c
 JOIN orders o ON c.id=o.customer_id
 JOIN items i ON o.id=i.order_id
 GROUP BY c.name
 ORDER BY revenue DESC
 LIMIT 3;",
"Join + aggregation"
