--Query type: DQL
WITH seq AS ( SELECT * FROM dbo.Generate_Sequences_UDTF(10.0) ) SELECT seq.* FROM seq INNER JOIN (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS temp_table(id, name) ON seq.id = temp_table.id;