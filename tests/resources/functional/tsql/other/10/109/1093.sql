--Query type: DDL
CREATE PROCEDURE dbo.Test3
AS
CREATE TABLE #t (
    x INT PRIMARY KEY
);
INSERT INTO #t
VALUES (2);
WITH temp_result AS (
    SELECT x
    FROM #t
)
SELECT Test2Col = x
FROM temp_result;
-- REMORPH CLEANUP: DROP TABLE #t;
-- REMORPH CLEANUP: DROP PROCEDURE dbo.Test3;