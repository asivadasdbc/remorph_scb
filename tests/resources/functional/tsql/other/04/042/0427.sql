--Query type: DDL
WITH Lineitem AS ( SELECT l_orderkey = 1, l_partkey = 1, l_suppkey = 1, l_linenumber = 1, l_quantity = 1, l_extendedprice = 1.0, l_discount = 0.0, l_tax = 0.0, l_returnflag = 'R', l_linestatus = 'O', l_shipdate = '1996-03-13', l_commitdate = '1996-02-12', l_receiptdate = '1996-03-22', l_shipinstruct = 'TAKE BACK RETURN', l_shipmode = 'TRUCK', l_comment = 'O' UNION ALL SELECT l_orderkey = 2, l_partkey = 2, l_suppkey = 2, l_linenumber = 2, l_quantity = 2, l_extendedprice = 2.0, l_discount = 0.0, l_tax = 0.0, l_returnflag = 'R', l_linestatus = 'O', l_shipdate = '1996-03-13', l_commitdate = '1996-02-12', l_receiptdate = '1996-03-22', l_shipinstruct = 'TAKE BACK RETURN', l_shipmode = 'TRUCK', l_comment = 'O' ) SELECT * FROM Lineitem;