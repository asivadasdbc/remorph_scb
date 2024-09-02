--Query type: DML
EXEC sp_create_plan_guide @name = N'Guide6', @stmt = N'WITH EmployeeCTE AS (SELECT 1 AS EmployeeID, 3 AS ManagerID, 1 AS ContactID, ''Manager'' AS Title UNION ALL SELECT 2, 3, 2, ''Employee'' UNION ALL SELECT 3, NULL, 3, ''CEO''), ContactCTE AS (SELECT 1 AS ContactID, ''Smith'' AS LastName, ''John'' AS FirstName UNION ALL SELECT 2, ''Johnson'', ''Jane'' UNION ALL SELECT 3, ''Williams'', ''Bob'') SELECT e.ManagerID, c.LastName, c.FirstName, e.Title FROM EmployeeCTE e JOIN ContactCTE c ON e.ContactID = c.ContactID WHERE e.ManagerID = 3 OPTION (TABLE HINT (e, INDEX( IX_Employee_ManagerID)), TABLE HINT (c, FORCESEEK))', @type = N'SQL', @module_or_batch = NULL, @params = NULL, @hints = N'OPTION (TABLE HINT (e, INDEX( IX_Employee_ManagerID)), TABLE HINT (c, FORCESEEK))';