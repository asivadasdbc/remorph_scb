--Query type: DML
CREATE TABLE #Table1 (ColA INT, ColB INT);
CREATE TABLE #Table2 (ColA INT, ColB INT);
INSERT INTO #Table1 (ColA, ColB)
VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO #Table2 (ColA, ColB)
VALUES (1, 100), (2, 200), (3, 300);
UPDATE T2
SET T2.ColB = T2.ColB + T1.ColB
FROM #Table2 T2
INNER JOIN #Table1 T1
ON T2.ColA = T1.ColA;
SELECT *
FROM #Table2;
-- REMORPH CLEANUP: DROP TABLE #Table1;
-- REMORPH CLEANUP: DROP TABLE #Table2;