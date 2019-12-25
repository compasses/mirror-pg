CREATE TABLE decimal64table (d decimal64(5,2), id INT);

INSERT INTO decimal64table (id,d) VALUES(1,-123.45);
INSERT INTO decimal64table (id,d) VALUES(2,-123);
INSERT INTO decimal64table (id,d) VALUES(3,-12.34);
INSERT INTO decimal64table (id,d) VALUES(4,-1.34);
INSERT INTO decimal64table (id,d) VALUES(5, 0.12);
INSERT INTO decimal64table (id,d) VALUES(6, 1.23);
INSERT INTO decimal64table (id,d) VALUES(7, 12.34);
INSERT INTO decimal64table (id,d) VALUES(8, 123.45);
INSERT INTO decimal64table (id,d) VALUES(9, 123.456);
INSERT INTO decimal64table (id,d) VALUES(10, 123.456);

-- Should fail
CREATE UNIQUE INDEX decimal64table_d_idx ON decimal64table (d);

DELETE FROM decimal64table WHERE id = 9;

CREATE UNIQUE INDEX decimal64table_d_idx ON decimal64table (d);

SET enable_seqscan = off;

EXPLAIN (COSTS OFF) SELECT * FROM decimal64table ORDER BY d;

SELECT * FROM decimal64table ORDER BY d;

EXPLAIN (COSTS OFF) SELECT * FROM decimal64table WHERE d = '12.34'::decimal64;

SELECT * FROM decimal64table WHERE d = '12.34'::decimal64;

SELECT * FROM decimal64table WHERE d = '-12.34'::decimal64;

SELECT * FROM decimal64table WHERE d = '123.45'::decimal64;


DROP INDEX decimal64table_d_idx;

DROP TABLE decimal64table;

CREATE TABLE decimal64table (d decimal64(16,3));
insert into decimal64table values(1845694983.246);
insert into decimal64table values(1845694983.246);
insert into decimal64table values(1845694983.246);
insert into decimal64table values(1845694983.246);
insert into decimal64table values(1845694983.246);
insert into decimal64table values(18456949830.246);
insert into decimal64table values(184569498003.246);
insert into decimal64table values(18456949800003.246);
insert into decimal64table values(1845694000000983.246);
insert into decimal64table values(18456949999999983.246);

select avg(d), sum(d) from decimal64table;

DROP TABLE decimal64table;

SET enable_seqscan = on;

-- True comparisons

SELECT '123'::decimal64 < '123.01'::decimal64;

SELECT '123'::decimal64 <= '123.01'::decimal64;

SELECT '123'::decimal64 > '122.99'::decimal64;

SELECT '123'::decimal64 >= '122.99'::decimal64;

SELECT '123.00'::decimal64 = '123'::decimal64;

-- Compare to int4

SELECT '123'::INT < '123.01'::decimal64;

SELECT '123'::INT <= '123.01'::decimal64;

SELECT '123'::INT > '122.99'::decimal64;

SELECT '123'::INT >= '122.99'::decimal64;

SELECT '123'::INT = '123.00'::decimal64;

-- Compare to int4 reversed

SELECT '123.01'::decimal64 > '123'::INT;

SELECT  '123.01'::decimal64 >= '123'::INT;

SELECT '122.99'::decimal64 < '123'::INT;

SELECT '122.99'::decimal64 <= '123'::INT;

SELECT '123.00'::decimal64 = '123'::INT;

-- Compare to int2

SELECT '123'::SMALLINT < '123.01'::decimal64;

SELECT '123'::SMALLINT <= '123.01'::decimal64;

SELECT '123'::SMALLINT > '122.99'::decimal64;

SELECT '123'::SMALLINT >= '122.99'::decimal64;

SELECT '123'::SMALLINT = '123.00'::decimal64;

-- Compare to int4 reversed

SELECT '123.01'::decimal64 > '123'::SMALLINT;

SELECT  '123.01'::decimal64 >= '123'::SMALLINT;

SELECT '122.99'::decimal64 < '123'::SMALLINT;

SELECT '122.99'::decimal64 <= '123'::SMALLINT;

SELECT '123.00'::decimal64 = '123'::SMALLINT;

-- False comparisons
SELECT '123'::decimal64 >= '123.01'::decimal64;

SELECT '123'::decimal64 > '123.01'::decimal64;

SELECT '123'::decimal64 <= '122.99'::decimal64;

SELECT '123'::decimal64 < '122.99'::decimal64;

SELECT '123.00'::decimal64 <> '123'::decimal64;

-- Compare to int4

SELECT '123'::INT >= '123.01'::decimal64;

SELECT '123'::INT > '123.01'::decimal64;

SELECT '123'::INT <= '122.99'::decimal64;

SELECT '123'::INT < '122.99'::decimal64;

SELECT '123'::INT <> '123.00'::decimal64;

-- Compare to int4 reversed

SELECT '123.01'::decimal64 <= '123'::INT;

SELECT  '123.01'::decimal64 < '123'::INT;

SELECT '122.99'::decimal64 >= '123'::INT;

SELECT '122.99'::decimal64 > '123'::INT;

SELECT '123.00'::decimal64 <> '123'::INT;

-- Compare to int2

SELECT '123'::SMALLINT >= '123.01'::decimal64;

SELECT '123'::SMALLINT > '123.01'::decimal64;

SELECT '123'::SMALLINT <= '122.99'::decimal64;

SELECT '123'::SMALLINT < '122.99'::decimal64;

SELECT '123'::SMALLINT <> '123.00'::decimal64;

-- Compare to int4 reversed

SELECT '123.01'::decimal64 <= '123'::SMALLINT;

SELECT  '123.01'::decimal64 < '123'::SMALLINT;

SELECT '122.99'::decimal64 >= '123'::SMALLINT;

SELECT '122.99'::decimal64 > '123'::SMALLINT;

SELECT '123.00'::decimal64 <> '123'::SMALLINT;

-- Compare to int8 reversed

SELECT '123.01'::decimal64 <= '123'::bigint;

SELECT  '123.01'::decimal64 < '123'::bigint;

SELECT '122.99'::decimal64 >= '123'::bigint;

SELECT '122.99'::decimal64 > '123'::bigint;

SELECT '123.00'::decimal64 <> '123'::bigint;



SELECT CAST('2147483647'::decimal64 AS INT);

-- Ensure overflow is detected
SELECT CAST('2147483648'::decimal64 AS INT);

SELECT CAST('-2147483648'::decimal64 AS INT);

-- Ensure underflow is detected
SELECT CAST('-2147483649'::decimal64 AS INT);

SELECT CAST('32767'::decimal64 AS SMALLINT);

-- Ensure overflow is detected
SELECT CAST('32768'::decimal64 AS SMALLINT);

SELECT CAST('-32768'::decimal64 AS SMALLINT);

-- Ensure underflow is detected
SELECT CAST('-32769'::decimal64 AS SMALLINT);

SELECT CAST('1234321.23'::decimal64 AS FLOAT);

SELECT CAST('1234321.23'::decimal64 AS DOUBLE PRECISION);

SELECT CAST('9223372036854733.23'::decimal64 AS bigint);



-- Ensure the expected extreme values can be represented
SELECT '-92233720368547758.08'::decimal64 as minvalue,'92233720368547758.07'::decimal64 as maxvalue;

SELECT '-92233720368547758.09'::decimal64;

SELECT '92233720368547758.08'::decimal64;

-- Ensure casts from numeric to decimal64 work
SELECT '92233720368547758.07'::numeric::decimal64;

-- The literal below must be quoted as the parser seems to read the literal as
-- a positive number first and then us the - unary operator to make it negaive.
-- This would overflow without the quotes as this number cannot be represented
-- in a positive decimal64.
SELECT '-92233720368547758.08'::numeric::decimal64;

-- Ensure casts from numeric to fixed decimal detect overflow
SELECT '92233720368547758.08'::numeric::decimal64;

SELECT '-92233720368547758.09'::numeric::decimal64;

SELECT '-92233720368547758.08'::decimal64 - '0.01'::decimal64;

SELECT '92233720368547758.07'::decimal64 + '0.01'::decimal64;

-- Should not overflow
SELECT '46116860184273879.03'::decimal64 * '2.00'::decimal64;

-- Ensure this overflows
SELECT '46116860184273879.04'::decimal64 * '2.00'::decimal64;

-- Should not overflow
SELECT '46116860184273879.03'::decimal64 / '0.50'::decimal64;

-- Ensure this overflows
SELECT '46116860184273879.04'::decimal64 / '0.50'::decimal64;

-- Ensure limits of int2 can be represented
SELECT '32767'::decimal64::INT2,'-32768'::decimal64::INT2;

-- Ensure overflow of int2 is detected
SELECT '32768'::decimal64::INT2;

-- Ensure underflow of int2 is detected
SELECT '-32769'::decimal64::INT2;

-- Ensure limits of int4 can be represented
SELECT '2147483647'::decimal64::INT4,'-2147483648'::decimal64::INT4;

-- Ensure overflow of int4 is detected
SELECT '2147483648'::decimal64::INT4;

-- Ensure underflow of int4 is detected
SELECT '-2147483649'::decimal64::INT4;

-- Ensure overflow is detected
SELECT SUM(a) FROM (VALUES('92233720368547758.07'::decimal64),('0.01'::decimal64)) a(a);

-- Ensure underflow is detected
SELECT SUM(a) FROM (VALUES('-92233720368547758.08'::decimal64),('-0.01'::decimal64)) a(a);

-- Test typmods
SELECT 12345.33::decimal64(3,2); -- Fail

SELECT 12345.33::decimal64(5,2); -- Fail

-- scale of 2 should be enforced.
SELECT 12345.44::decimal64(7,0);

-- should work.
SELECT 12345.33::decimal64(7,2);

-- error, precision limit should be 16
SELECT 12345.33::decimal64(18,2);

CREATE TABLE decimal64table2 (d decimal64(3,2));
INSERT INTO decimal64table2 VALUES(12.34); -- Fail
INSERT INTO decimal64table2 VALUES(1.23); -- Pass
DROP TABLE decimal64table2;


CREATE TABLE decimal_test(a decimal64 NOT NULL);

INSERT INTO decimal_test VALUES ('92233720368547758.07');
INSERT INTO decimal_test VALUES ('0.01');
INSERT INTO decimal_test VALUES ('-92233720368547758.08');
INSERT INTO decimal_test VALUES ('-0.01');

SELECT SUM(a) FROM decimal_test WHERE a > 0;

SELECT SUM(a) FROM decimal_test WHERE a < 0;

TRUNCATE TABLE decimal_test;

INSERT INTO decimal_test VALUES('11.11'),('22.22'),('33.33');

SELECT SUM(a) FROM decimal_test;

SELECT MAX(a) FROM decimal_test;

SELECT MIN(a) FROM decimal_test;

SELECT AVG(a) FROM decimal_test;

DROP TABLE decimal_test;

