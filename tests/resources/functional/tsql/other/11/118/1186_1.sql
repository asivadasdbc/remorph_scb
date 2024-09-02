--Query type: DDL
SELECT CASE WHEN SUM(CASE WHEN order_total > 1000 THEN 1 ELSE 0 END) > 0 THEN 'High' ELSE 'Low' END AS order_category, SUM(order_total) AS total_revenue, AVG(order_total) AS avg_order_value, MAX(order_total) AS max_order_value, MIN(order_total) AS min_order_value, COUNT(DISTINCT order_id) AS num_orders, COUNT(CASE WHEN order_total > 1000 THEN order_id END) AS num_high_value_orders, COUNT(CASE WHEN order_total <= 1000 THEN order_id END) AS num_low_value_orders FROM ( VALUES (1, 100.0, '2020-01-01'), (2, 200.0, '2020-01-02'), (3, 300.0, '2020-01-03') ) AS orders(order_id, order_total, order_date) WHERE order_date >= '2020-01-01' AND order_date < '2021-01-01