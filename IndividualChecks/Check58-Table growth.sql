/*
Check58 - Table growth
Description: 

----------------------

Estimated Benefit:
N/A
Estimated Effort:
N/A

Recommendation:

Quick recommendation:

Detailed recommendation:
----------------------
*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck58') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck58
  
IF OBJECT_ID('tempdb.dbo.#tmpnum') IS NULL
BEGIN
  ;WITH L0
   AS
   (
     SELECT 1 AS C
     UNION ALL
     SELECT 1 AS O
   ), -- 2 rows
        L1
   AS
   (
     SELECT 1 AS C
     FROM L0 AS A
     CROSS JOIN L0 AS B
   ), -- 4 rows
        L2
   AS
   (
     SELECT 1 AS C
     FROM L1 AS A
     CROSS JOIN L1 AS B
   ), -- 16 rows
        L3
   AS
   (
     SELECT 1 AS C
     FROM L2 AS A
     CROSS JOIN L2 AS B
   ), -- 256 rows
        L4
   AS
   (
     SELECT 1 AS C
     FROM L3 AS A
     CROSS JOIN L3 AS B
   ), -- 65,536 rows
        L5
   AS
   (
     SELECT 1 AS C
     FROM L4 AS A
     CROSS JOIN L4 AS B
   ), -- 4,294,967,296 rows
        Nums
   AS
   (
     SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS N
     FROM L5
   )
  SELECT TOP (219146)
         N AS Num
  INTO #tmpnum
  FROM Nums;
END;

IF OBJECT_ID('tempdb.dbo.#tmpseq') IS NOT NULL
  DROP TABLE #tmpseq;
WITH CTE_1
AS
(
  SELECT tmp_histogram.rowid,
         CASE
           WHEN MIN(CONVERT(VARCHAR(30), tmp_histogram.range_hi_key, 112)) <= '19500101' THEN
           (SELECT MIN(CONVERT(VARCHAR(30), t1.range_hi_key, 112))
            FROM tempdb.dbo.tmp_histogram AS t1
            WHERE t1.rowid = tmp_histogram.rowid
                  AND CONVERT(VARCHAR(30), t1.range_hi_key, 112) >= '19500101')
           ELSE MIN(CONVERT(VARCHAR(30), tmp_histogram.range_hi_key, 112))
         END AS MinDt,
         CASE
           WHEN MAX(CONVERT(VARCHAR(30), tmp_histogram.range_hi_key, 112)) >= '20500101' THEN
           (SELECT MAX(CONVERT(VARCHAR(30), t1.range_hi_key, 112))
            FROM tempdb.dbo.tmp_histogram AS t1
            WHERE t1.rowid = tmp_histogram.rowid
                  AND CONVERT(VARCHAR(30), t1.range_hi_key, 112) <= '20500101')
           ELSE MAX(CONVERT(VARCHAR(30), tmp_histogram.range_hi_key, 112))
         END AS MaxDt,
         DATEDIFF(
           DAY,
           CASE
             WHEN MIN(CONVERT(VARCHAR(30), tmp_histogram.range_hi_key, 112)) <= '19500101' THEN
             (SELECT MIN(CONVERT(VARCHAR(30), t1.range_hi_key, 112))
              FROM tempdb.dbo.tmp_histogram AS t1
              WHERE t1.rowid = tmp_histogram.rowid
                    AND CONVERT(VARCHAR(30), t1.range_hi_key, 112) >= '19500101')
             ELSE MIN(CONVERT(VARCHAR(30), tmp_histogram.range_hi_key, 112))
           END,
           CASE
             WHEN MAX(CONVERT(VARCHAR(30), tmp_histogram.range_hi_key, 112)) >= '20500101' THEN
             (SELECT MAX(CONVERT(VARCHAR(30), t1.range_hi_key, 112))
              FROM tempdb.dbo.tmp_histogram AS t1
              WHERE t1.rowid = tmp_histogram.rowid
                    AND CONVERT(VARCHAR(30), t1.range_hi_key, 112) <= '20500101')
             ELSE MAX(CONVERT(VARCHAR(30), tmp_histogram.range_hi_key, 112))
           END) AS Col1
  FROM tempdb.dbo.tmp_histogram
  CROSS APPLY ((SELECT TOP 1
                       *
                FROM tempdb.dbo.tmp_stats
                WHERE tmp_stats.rowid = tmp_histogram.rowid
                      AND tmp_stats.key_column_data_type LIKE 'DATETIME%'
                      AND tmp_stats.current_number_of_rows > 100 /* Ignoring "small" tables */
                ORDER BY tmp_histogram.stepnumber DESC /*Using the stat with biggest number of steps*/)) AS t1
  WHERE tmp_histogram.stepnumber > 3
  GROUP BY tmp_histogram.rowid
),
     CTE_2
AS
(
  SELECT rowid,
         CONVERT(VARCHAR(30), DATEADD(DAY, #tmpnum.Num - 1, MinDt), 112) AS AllDts
  FROM CTE_1
  INNER JOIN #tmpnum
  ON #tmpnum.Num <= CTE_1.Col1 + 1
)
SELECT *
INTO #tmpseq
FROM CTE_2
OPTION (MAXDOP 4);

CREATE UNIQUE CLUSTERED INDEX ix1 ON #tmpseq (rowid, AllDts);

IF OBJECT_ID('tempdb.dbo.#tmp_cte1') IS NOT NULL
  DROP TABLE #tmp_cte1;
WITH CTE_1
AS
(
  SELECT ISNULL(a.rowid, b.rowid) AS rowid,
         CONVERT(DATE, AllDts) AS range_hi_key,
         ISNULL(a.eq_rows, b.avg_range_rows) AS eq_rows
  FROM #tmpseq
  LEFT OUTER JOIN (SELECT *
                   FROM tempdb.dbo.tmp_histogram
                   WHERE EXISTS (SELECT *
                                 FROM tempdb.dbo.tmp_stats
                                 WHERE tmp_stats.rowid = tmp_histogram.rowid
                                       AND tmp_stats.key_column_data_type LIKE 'DATETIME%')) AS a
  ON a.rowid = #tmpseq.rowid
     AND CONVERT(DATE, a.range_hi_key) = CONVERT(DATE, #tmpseq.AllDts)
  OUTER APPLY (SELECT TOP 1
                      *
               FROM tempdb.dbo.tmp_histogram AS b1
               WHERE 1 = 1
                     AND CONVERT(DATE, b1.range_hi_key) > CONVERT(DATE, #tmpseq.AllDts)
                     AND #tmpseq.rowid = b1.rowid
               ORDER BY b1.stepnumber ASC) AS b
)
SELECT *
INTO #tmp_cte1
FROM CTE_1
OPTION (MAXDOP 4);

CREATE CLUSTERED INDEX ix1 ON #tmp_cte1 (rowid, range_hi_key);

SELECT #tmp_cte1.rowid,
       b.database_name,
       b.schema_name,
       b.table_name,
       b.stats_name,
       b.key_column_name,
       b.key_column_data_type,
	   b.current_number_of_rows AS current_number_of_rows_bigint,
       CONVERT(VARCHAR(30), CONVERT(MONEY, b.current_number_of_rows), 1) AS current_number_of_rows,
       b.last_updated AS last_updated_datetime,
       CONVERT(
         VARCHAR(30),
         CONVERT(MONEY,
         (SELECT SUM(eq_rows) AS estimated_rowcount
          FROM #tmp_cte1 AS b
          WHERE CONVERT(DATE, range_hi_key) >= CONVERT(VARCHAR, DATEPART(YEAR, GETDATE())) + '0101'
                AND #tmp_cte1.rowid = b.rowid)),
         1) AS rows_current_year,
       CONVERT(
         VARCHAR(30),
         CONVERT(
           MONEY,
         (SELECT SUM(eq_rows) AS estimated_rowcount
          FROM #tmp_cte1 AS b
          WHERE CONVERT(DATE, range_hi_key) BETWEEN CONVERT(VARCHAR, DATEPART(YEAR, DATEADD(YEAR, -1, GETDATE())))
                                                    + '0101' AND CONVERT(
                                                                   VARCHAR,
                                                                   DATEPART(YEAR, DATEADD(YEAR, -1, GETDATE())))
                                                                 + '1231'
                AND #tmp_cte1.rowid = b.rowid)),
         1) AS rows_last_year,
       CONVERT(
         VARCHAR(30),
         CONVERT(MONEY,
                 CONVERT(NUMERIC(38, 2),
                 (SELECT AVG(estimated_rowcount)
                  FROM (SELECT SUM(eq_rows) AS estimated_rowcount
                        FROM #tmp_cte1 AS b
                        WHERE #tmp_cte1.rowid = b.rowid
                        GROUP BY CONVERT(VARCHAR(4), range_hi_key, 112)) AS t ))),
         1) AS avg_rows_per_year,
       CONVERT(VARCHAR(30),
               CONVERT(MONEY,
               (SELECT SUM(eq_rows) AS estimated_rowcount
                FROM #tmp_cte1 AS b
                WHERE CONVERT(DATE, range_hi_key) >= CONVERT(VARCHAR, GETDATE() - 365)
                      AND #tmp_cte1.rowid = b.rowid)),
               1) AS rows_last_12_months,
       CONVERT(VARCHAR(30),
               CONVERT(MONEY,
               (SELECT SUM(eq_rows) AS estimated_rowcount
                FROM #tmp_cte1 AS b
                WHERE CONVERT(DATE, range_hi_key) >= CONVERT(VARCHAR, GETDATE() - 1825)
                      AND #tmp_cte1.rowid = b.rowid)),
               1) AS rows_last_5_years,
       CONVERT(VARCHAR(30),
               CONVERT(MONEY,
               (SELECT SUM(eq_rows) AS estimated_rowcount
                FROM #tmp_cte1 AS b
                WHERE CONVERT(DATE, range_hi_key) >= CONVERT(VARCHAR, GETDATE() - 3650)
                      AND #tmp_cte1.rowid = b.rowid)),
               1) AS rows_last_10_years,
       CONVERT(
         VARCHAR(30),
         CONVERT(
           MONEY,
           CONVERT(
             NUMERIC(38, 2),
           (SELECT AVG(estimated_rowcount)
            FROM (SELECT SUM(eq_rows) AS estimated_rowcount
                  FROM #tmp_cte1 AS b
                  WHERE #tmp_cte1.rowid = b.rowid
                        AND CONVERT(DATE, range_hi_key) >= CONVERT(VARCHAR, DATEPART(YEAR, GETDATE())) + '0101'
                  GROUP BY CONVERT(VARCHAR(30), range_hi_key, 112)) AS t ))),
         1) AS avg_rows_per_day_current_year,
       CONVERT(VARCHAR(30), CONVERT(MONEY, MIN(Tab1.avg_rows_per_month_current_year)), 1) AS avg_rows_per_month_current_year,
       CONVERT(
         VARCHAR(30),
         CONVERT(
           MONEY,
           CONVERT(
             NUMERIC(38, 2),
           (SELECT AVG(eq_rows) AS estimated_rowcount
            FROM #tmp_cte1 AS b
            WHERE CONVERT(DATE, range_hi_key) BETWEEN CONVERT(VARCHAR, DATEPART(YEAR, DATEADD(YEAR, -1, GETDATE())))
                                                      + '0101' AND CONVERT(
                                                                     VARCHAR,
                                                                     DATEPART(YEAR, DATEADD(YEAR, -1, GETDATE())))
                                                                   + '1231'
                  AND #tmp_cte1.rowid = b.rowid))),
         1) AS avg_rows_per_day_last_year,
       CONVERT(
         VARCHAR(30),
         CONVERT(
           MONEY,
           CONVERT(
             NUMERIC(38, 2),
           (SELECT AVG(estimated_rowcount)
            FROM (SELECT SUM(eq_rows) AS estimated_rowcount
                  FROM #tmp_cte1 AS b
                  WHERE #tmp_cte1.rowid = b.rowid
                        AND CONVERT(DATE, range_hi_key) BETWEEN CONVERT(
                                                                  VARCHAR, DATEPART(YEAR, DATEADD(YEAR, -1, GETDATE())))
                                                                + '0101' AND CONVERT(
                                                                               VARCHAR,
                                                                               DATEPART(
                                                                                 YEAR, DATEADD(YEAR, -1, GETDATE())))
                                                                             + '1231'
                  GROUP BY CONVERT(VARCHAR(6), range_hi_key, 112)) AS t ))),
         1) AS avg_rows_per_month_last_year,
       CONVERT(VARCHAR(30), CONVERT(MONEY, b.current_number_of_rows + (MIN(Tab1.avg_rows_per_month_current_year) * 12)), 1) AS estimated_rows_in_1_years,
       CONVERT(VARCHAR(30), CONVERT(MONEY, b.current_number_of_rows + (MIN(Tab1.avg_rows_per_month_current_year) * (12*5))), 1) AS estimated_rows_in_5_years,
       CONVERT(VARCHAR(30), CONVERT(MONEY, b.current_number_of_rows + (MIN(Tab1.avg_rows_per_month_current_year) * (12*10))), 1) AS estimated_rows_in_10_years,
       CONVERT(XML,
       (SELECT CONVERT(VARCHAR(4), range_hi_key, 112) AS dt_year,
               SUM(eq_rows) AS estimated_rowcount
        FROM #tmp_cte1 AS b
        WHERE #tmp_cte1.rowid = b.rowid
        GROUP BY CONVERT(VARCHAR(4), range_hi_key, 112)
       FOR XML RAW)) AS rows_per_yyyy,
       CONVERT(XML,
       (SELECT CONVERT(VARCHAR(6), range_hi_key, 112) AS dt_year,
               SUM(eq_rows) AS estimated_rowcount
        FROM #tmp_cte1 AS b
        WHERE #tmp_cte1.rowid = b.rowid
        GROUP BY CONVERT(VARCHAR(6), range_hi_key, 112)
       FOR XML RAW)) AS rows_per_yyyymm,
       CONVERT(XML,
       (SELECT range_hi_key AS dt_year,
               SUM(eq_rows) AS estimated_rowcount
        FROM #tmp_cte1 AS b
        WHERE #tmp_cte1.rowid = b.rowid
        GROUP BY range_hi_key
       FOR XML RAW)) AS rows_per_yyyymmdd
INTO tempdb.dbo.tmpStatisticCheck58
FROM #tmp_cte1
INNER JOIN tempdb.dbo.tmp_stats AS b
ON b.rowid = #tmp_cte1.rowid
CROSS APPLY(SELECT 
           CONVERT(
             NUMERIC(38, 2),
           (SELECT AVG(estimated_rowcount)
            FROM (SELECT SUM(eq_rows) AS estimated_rowcount
                  FROM #tmp_cte1 AS b
                  WHERE #tmp_cte1.rowid = b.rowid
                        AND CONVERT(DATE, range_hi_key) >= CONVERT(VARCHAR, DATEPART(YEAR, GETDATE())) + '0101'
                  GROUP BY CONVERT(VARCHAR(6), range_hi_key, 112)) AS t ))) AS Tab1(avg_rows_per_month_current_year)
GROUP BY #tmp_cte1.rowid,
         b.current_number_of_rows,
         b.database_name,
         b.schema_name,
         b.table_name,
         b.stats_name,
         b.key_column_name,
         b.key_column_data_type,
         b.last_updated
ORDER BY b.current_number_of_rows DESC
OPTION (MAXDOP 4);

SELECT * FROM tempdb.dbo.tmpStatisticCheck58
ORDER BY current_number_of_rows_bigint DESC