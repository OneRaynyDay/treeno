SELECT 1;
SELECT 1 + 2;
SELECT 1 * 2;
SELECT 1 % 2 IS NULL;
SELECT (1 + 2) * (3 / 4) - 10;
SELECT NULL IS NOT NULL;
SELECT 2 BETWEEN 1 AND 3;
SELECT 'a' IN ('a','b');
SELECT 'abc' LIKE '%ab%';
SELECT ARRAY[1,2];
SELECT DECIMAL '3.52';
SELECT CAST(3 AS DECIMAL(30,2));
SELECT CAST('2021-01-01 07:00:01.000110001' AS TIMESTAMP(9));
SELECT CAST(ARRAY[1,2] AS ARRAY(DECIMAL(10,1)));
SELECT "a"
  FROM "t";
SELECT "a","b"
  FROM "t";
SELECT "a" + "b"
  FROM "t";
SELECT "a" + "b"
  FROM "t"
 WHERE "a" > 5;
SELECT "a" + "b" IS NULL
  FROM "t";
SELECT "a" "foo","b" "bar"
  FROM "t";
SELECT "a","b"
  FROM "t"
 ORDER BY "a" DESC NULLS FIRST,"b";
SELECT "a","b"
  FROM "t"
 GROUP BY DISTINCT GROUPING SETS (("a","b"),"a",()),ROLLUP ("a"),CUBE ("a","b","c");
SELECT "a","b"
  FROM (SELECT "a","b","c"
          FROM "t"
         WHERE "c" > 5
               AND "b" = 2
         ORDER BY "a")
 LIMIT 3;
SELECT SUM("a") OVER (
       PARTITION BY "date"
           ORDER BY "timestamp"
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW),
       "x","y","z"
  FROM "t";
SELECT "a"
  FROM (SELECT "a","b"
          FROM (SELECT "a","b","c"
                  FROM "t"));
