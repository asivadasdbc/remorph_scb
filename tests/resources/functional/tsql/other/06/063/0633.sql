--Query type: DML
DECLARE @sql nvarchar(max) = 'DECLARE @profit decimal(38, 2) = 0.0; BEGIN LET @cost decimal(38, 2) = 100.0; LET @revenue decimal(38, 2) = 110.0; SET @profit = @revenue - @cost; RETURN @profit; END; SELECT @profit AS result FROM (VALUES (@profit)) AS temp_result(profit);'; EXECUTE sp_executesql @sql;