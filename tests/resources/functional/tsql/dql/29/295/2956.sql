--Query type: DQL
WITH temp_result AS ( SELECT 'primary' AS role_desc, 'started' AS state_desc UNION ALL SELECT 'mirror' AS role_desc, 'started' AS state_desc ) SELECT role_desc, state_desc FROM temp_result