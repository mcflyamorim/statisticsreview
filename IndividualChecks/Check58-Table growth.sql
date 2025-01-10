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

IF OBJECT_ID('dbo.tmpStatisticCheck58') IS NOT NULL
  DROP TABLE dbo.tmpStatisticCheck58
  
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
  SELECT tmpStatisticCheck_histogram.rowid,
         CASE
           WHEN MIN(CONVERT(VARCHAR(30), tmpStatisticCheck_histogram.range_hi_key, 112)) <= '19500101' THEN
           (SELECT MIN(CONVERT(VARCHAR(30), t1.range_hi_key, 112))
            FROM dbo.tmpStatisticCheck_histogram AS t1
            WHERE t1.rowid = tmpStatisticCheck_histogram.rowid
                  AND CONVERT(VARCHAR(30), t1.range_hi_key, 112) >= '19500101')
           ELSE MIN(CONVERT(VARCHAR(30), tmpStatisticCheck_histogram.range_hi_key, 112))
         END AS MinDt,
         CASE
           WHEN MAX(CONVERT(VARCHAR(30), tmpStatisticCheck_histogram.range_hi_key, 112)) >= '20500101' THEN
           (SELECT MAX(CONVERT(VARCHAR(30), t1.range_hi_key, 112))
            FROM dbo.tmpStatisticCheck_histogram AS t1
            WHERE t1.rowid = tmpStatisticCheck_histogram.rowid
                  AND CONVERT(VARCHAR(30), t1.range_hi_key, 112) <= '20500101')
           ELSE MAX(CONVERT(VARCHAR(30), tmpStatisticCheck_histogram.range_hi_key, 112))
         END AS MaxDt,
         DATEDIFF(
           DAY,
           CASE
             WHEN MIN(CONVERT(VARCHAR(30), tmpStatisticCheck_histogram.range_hi_key, 112)) <= '19500101' THEN
             (SELECT MIN(CONVERT(VARCHAR(30), t1.range_hi_key, 112))
              FROM dbo.tmpStatisticCheck_histogram AS t1
              WHERE t1.rowid = tmpStatisticCheck_histogram.rowid
                    AND CONVERT(VARCHAR(30), t1.range_hi_key, 112) >= '19500101')
             ELSE MIN(CONVERT(VARCHAR(30), tmpStatisticCheck_histogram.range_hi_key, 112))
           END,
           CASE
             WHEN MAX(CONVERT(VARCHAR(30), tmpStatisticCheck_histogram.range_hi_key, 112)) >= '20500101' THEN
             (SELECT MAX(CONVERT(VARCHAR(30), t1.range_hi_key, 112))
              FROM dbo.tmpStatisticCheck_histogram AS t1
              WHERE t1.rowid = tmpStatisticCheck_histogram.rowid
                    AND CONVERT(VARCHAR(30), t1.range_hi_key, 112) <= '20500101')
             ELSE MAX(CONVERT(VARCHAR(30), tmpStatisticCheck_histogram.range_hi_key, 112))
           END) AS Col1
  FROM dbo.tmpStatisticCheck_histogram
  CROSS APPLY ((SELECT TOP 1
                       *
                FROM dbo.tmpStatisticCheck_stats
                WHERE tmpStatisticCheck_stats.rowid = tmpStatisticCheck_histogram.rowid
                      AND tmpStatisticCheck_stats.key_column_data_type LIKE 'DATETIME%'
                      AND tmpStatisticCheck_stats.current_number_of_rows > 100 /* Ignoring "small" tables */
                ORDER BY tmpStatisticCheck_histogram.stepnumber DESC /*Using the stat with biggest number of steps*/)) AS t1
  WHERE tmpStatisticCheck_histogram.stepnumber > 3
  GROUP BY tmpStatisticCheck_histogram.rowid
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

IF OBJECT_ID('tempdb.dbo.#tmp_stats_with_datetime') IS NOT NULL
  DROP TABLE #tmp_stats_with_datetime;

SELECT rowid, CONVERT(DATE, range_hi_key) AS range_hi_key, eq_rows, avg_range_rows, stepnumber
INTO #tmp_stats_with_datetime
FROM dbo.tmpStatisticCheck_histogram
WHERE EXISTS (SELECT *
              FROM dbo.tmpStatisticCheck_stats
              WHERE tmpStatisticCheck_stats.rowid = tmpStatisticCheck_histogram.rowid
                    AND (tmpStatisticCheck_stats.key_column_data_type LIKE 'DATETIME%' OR tmpStatisticCheck_stats.key_column_data_type = 'DATE'))
      
CREATE CLUSTERED INDEX ix1 ON #tmp_stats_with_datetime (rowid, range_hi_key);

IF OBJECT_ID('tempdb.dbo.#tmp_cte1') IS NOT NULL
  DROP TABLE #tmp_cte1;
WITH CTE_1
AS
(
  SELECT ISNULL(a.rowid, b.rowid) AS rowid,
         CONVERT(DATE, AllDts) AS range_hi_key,
         ISNULL(a.eq_rows, b.avg_range_rows) AS eq_rows
  FROM #tmpseq
  LEFT OUTER JOIN #tmp_stats_with_datetime AS a
  ON a.rowid = #tmpseq.rowid
     AND CONVERT(DATE, a.range_hi_key) = CONVERT(DATE, #tmpseq.AllDts)
  OUTER APPLY (SELECT TOP 1
                      *
               FROM #tmp_stats_with_datetime AS b1
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
       b.rows_sampled AS rows_sampled_bigint,
       CONVERT(VARCHAR(30), CONVERT(MONEY, b.rows_sampled), 1) AS rows_sampled,
       Tab2.table_data_size_in_mb,
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
         (SELECT MAX(DATEPART(YEAR, CONVERT(DATE, range_hi_key)))
          FROM #tmp_cte1 AS b
          WHERE #tmp_cte1.rowid = b.rowid)) AS last_year_with_data,
       CONVERT(DATE, t_Min.MinDate) AS min_datetime,
       CONVERT(DATE, t_Max.MaxDate) AS max_datetime,
       DATEDIFF(YEAR, CONVERT(DATE, t_Min.MinDate), CONVERT(DATE, t_Max.MaxDate)) + 1 AS number_of_years_on_range,
       t_Distinct_Years.number_of_distinct_years,
       CONVERT(
         VARCHAR(30),
         CONVERT(
           MONEY,
         (SELECT SUM(eq_rows) AS estimated_rowcount
          FROM #tmp_cte1 AS b
          CROSS APPLY (SELECT CONVERT(VARCHAR, DATEPART(YEAR, MAX(c.range_hi_key))) FROM #tmp_cte1 AS c WHERE c.rowid = b.rowid) AS t1(last_year_with_data)
          WHERE CONVERT(DATE, range_hi_key) BETWEEN CONVERT(VARCHAR, t1.last_year_with_data) + '0101' AND CONVERT(VARCHAR,t1.last_year_with_data) + '1231'
                AND #tmp_cte1.rowid = b.rowid)),
         1) AS rows_last_year_with_data,
       CONVERT(VARCHAR(30), CONVERT(MONEY, MIN(Tab1.avg_rows_per_month_last_year_with_data)), 1) AS avg_rows_per_month_last_year_with_data,
       CASE WHEN MIN(Tab1.avg_rows_per_month_last_year_with_data) > 0 THEN CONVERT(NUMERIC(38, 2), Tab2.table_data_size_in_mb / MIN(Tab1.avg_rows_per_month_last_year_with_data)) END AS estimated_size_per_month_in_mb,
       CONVERT(
         VARCHAR(30),
         CONVERT(
           MONEY,
           CONVERT(
             NUMERIC(38, 2),
           (SELECT AVG(eq_rows) AS estimated_rowcount
            FROM #tmp_cte1 AS b
            CROSS APPLY (SELECT CONVERT(VARCHAR, DATEPART(YEAR, MAX(c.range_hi_key))) FROM #tmp_cte1 AS c WHERE c.rowid = b.rowid) AS t1(last_year_with_data)
            WHERE CONVERT(DATE, range_hi_key) BETWEEN CONVERT(VARCHAR, t1.last_year_with_data) + '0101' AND CONVERT(VARCHAR,t1.last_year_with_data) + '1231'
            AND #tmp_cte1.rowid = b.rowid))),
         1) AS avg_rows_per_day_last_year_with_data,
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
       CONVERT(VARCHAR(30), CONVERT(MONEY, b.current_number_of_rows + (MIN(Tab1.avg_rows_per_month_last_year_with_data) * 12)), 1) AS estimated_rows_in_1_year,
       Tab2.table_data_size_in_mb + (CASE WHEN MIN(Tab1.avg_rows_per_month_last_year_with_data) > 0 THEN CONVERT(NUMERIC(38, 2), Tab2.table_data_size_in_mb / MIN(Tab1.avg_rows_per_month_last_year_with_data)) * 12 END) AS estimated_size_in_mb_in_1_year,
       CONVERT(VARCHAR(30), CONVERT(MONEY, b.current_number_of_rows + (MIN(Tab1.avg_rows_per_month_last_year_with_data) * (12*5))), 1) AS estimated_rows_in_5_years,
       Tab2.table_data_size_in_mb + (CASE WHEN MIN(Tab1.avg_rows_per_month_last_year_with_data) > 0 THEN CONVERT(NUMERIC(38, 2), Tab2.table_data_size_in_mb / MIN(Tab1.avg_rows_per_month_last_year_with_data)) * (12*5) END) AS estimated_size_in_mb_in_5_years,
       CONVERT(VARCHAR(30), CONVERT(MONEY, b.current_number_of_rows + (MIN(Tab1.avg_rows_per_month_last_year_with_data) * (12*10))), 1) AS estimated_rows_in_10_years,
       Tab2.table_data_size_in_mb + (CASE WHEN MIN(Tab1.avg_rows_per_month_last_year_with_data) > 0 THEN CONVERT(NUMERIC(38, 2), Tab2.table_data_size_in_mb / MIN(Tab1.avg_rows_per_month_last_year_with_data)) * (12*10) END) AS estimated_size_in_mb_in_10_years,
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
INTO dbo.tmpStatisticCheck58
FROM #tmp_cte1
INNER JOIN dbo.tmpStatisticCheck_stats AS b
ON b.rowid = #tmp_cte1.rowid
CROSS APPLY(SELECT 
           CONVERT(
             NUMERIC(38, 2),
           (SELECT AVG(estimated_rowcount)
            FROM (SELECT SUM(eq_rows) AS estimated_rowcount
                  FROM #tmp_cte1 AS b
                  WHERE #tmp_cte1.rowid = b.rowid
                  AND CONVERT(DATE, range_hi_key) >= (SELECT CONVERT(VARCHAR, DATEPART(YEAR, MAX(c.range_hi_key))) + '0101' FROM #tmp_cte1 AS c WHERE c.rowid = b.rowid)
                  GROUP BY CONVERT(VARCHAR(6), range_hi_key, 112)) AS t ))) AS Tab1(avg_rows_per_month_last_year_with_data)
CROSS APPLY(SELECT CONVERT(NUMERIC(25, 2), (b.number_of_in_row_data_pages_on_table * 8) / 1024.) + CONVERT(NUMERIC(25, 2), (b.number_of_lob_data_pages_on_table * 8) / 1024.)) AS Tab2(table_data_size_in_mb)
CROSS APPLY (SELECT TOP 1 
                    CONVERT(DATETIME, range_hi_key) AS MinDate,
                    eq_rows AS EstimatedNumberOfRowsMinValue
               FROM #tmp_stats_with_datetime
              WHERE #tmp_stats_with_datetime.rowid = #tmp_cte1.rowid
                AND #tmp_stats_with_datetime.range_hi_key IS NOT NULL
              ORDER BY CONVERT(DATETIME, range_hi_key) ASC) AS t_Min
CROSS APPLY (SELECT TOP 1 
                    CONVERT(DATETIME, range_hi_key) AS MaxDate, 
                    eq_rows AS EstimatedNumberOfRowsMaxValue
               FROM #tmp_stats_with_datetime
              WHERE #tmp_stats_with_datetime.rowid = #tmp_cte1.rowid
                AND #tmp_stats_with_datetime.range_hi_key IS NOT NULL
              ORDER BY CONVERT(DATETIME, range_hi_key) DESC) AS t_Max
CROSS APPLY (SELECT COUNT(DISTINCT DATEPART(YEAR, range_hi_key)) AS number_of_distinct_years
               FROM #tmp_stats_with_datetime
              WHERE #tmp_stats_with_datetime.rowid = #tmp_cte1.rowid
                AND #tmp_stats_with_datetime.range_hi_key BETWEEN t_Min.MinDate AND t_Max.MaxDate
                AND #tmp_stats_with_datetime.range_hi_key IS NOT NULL) AS t_Distinct_Years
GROUP BY #tmp_cte1.rowid,
         b.current_number_of_rows,
         b.database_name,
         b.schema_name,
         b.table_name,
         b.stats_name,
         b.key_column_name,
         b.key_column_data_type,
         b.last_updated,
         b.rows_sampled,
         Tab2.table_data_size_in_mb,
         CONVERT(DATE, t_Min.MinDate),
         CONVERT(DATE, t_Max.MaxDate),
         t_Distinct_Years.number_of_distinct_years
ORDER BY b.current_number_of_rows DESC
OPTION (MAXDOP 4);

SELECT * FROM dbo.tmpStatisticCheck58
ORDER BY current_number_of_rows_bigint DESC