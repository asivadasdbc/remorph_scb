--Query type: DDL
CREATE PARTITION FUNCTION [myOrderRangePF1] (int) AS RANGE RIGHT FOR VALUES (100, 200, 300, 400, 500, 600, 700, 800, 900, 1000);