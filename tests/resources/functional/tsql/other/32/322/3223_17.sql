--Query type: DML
CREATE TABLE t6 (a INT);
INSERT INTO t6 (a)
VALUES (NULL), (NULL);
SELECT * FROM t6;
-- REMORPH CLEANUP: DROP TABLE t6;