--Query type: DDL
CREATE PARTITION FUNCTION myRangePF2 (int) AS RANGE LEFT FOR VALUES (10, 100);