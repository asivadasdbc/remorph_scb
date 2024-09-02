--Query type: DML
WITH CustomerCTE AS (
    SELECT 'John' AS NAME, 1 AS NATIONKEY
    UNION ALL
    SELECT 'Jane', 2
)
SELECT *
INTO #Customer
FROM CustomerCTE;

CREATE STATISTICS CustomerStats
ON #Customer (NAME, NATIONKEY)
WITH SAMPLE 75 PERCENT;

UPDATE STATISTICS #Customer(CustomerStats)
WITH SAMPLE 75 PERCENT;

SELECT *
FROM #Customer;

-- REMORPH CLEANUP: DROP TABLE #Customer;