--Query type: DQL
SELECT c.custkey AS [customer_key], o.orderkey, o.clerk FROM (VALUES (1, 'John'), (2, 'Jane')) AS c (custkey, custname) INNER JOIN (VALUES (1, 1, 'John'), (2, 2, 'Jane')) AS o (orderkey, custkey, clerk) ON c.custkey = o.custkey;