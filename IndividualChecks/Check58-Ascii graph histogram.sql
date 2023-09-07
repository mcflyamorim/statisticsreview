/*
Check58 - Ascii graph histogram
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
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck58') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck58

;WITH CTE_1
AS
(
SELECT *,
       CASE 
         WHEN MAX(stepnumber) OVER(PARTITION BY rowid) % 2 = 0 
          AND stepnumber = MAX(stepnumber) OVER(PARTITION BY rowid) / 2
         THEN eq_rows
         WHEN MAX(stepnumber) OVER(PARTITION BY rowid) % 2 <> 0
          AND stepnumber BETWEEN FLOOR(CONVERT(NUMERIC(25, 2), MAX(stepnumber) OVER(PARTITION BY rowid) / 2.0)) AND FLOOR(CONVERT(NUMERIC(25, 2), MAX(stepnumber) OVER(PARTITION BY rowid) / 2.0)) + 1 THEN eq_rows
         ELSE NULL
       END AS step_middle_midpoint,
       COUNT(*) OVER(PARTITION BY eq_rows) AS cnt,
       MAX(CASE WHEN rn <= max_steps * 0.10 THEN eq_rows END) OVER(PARTITION BY rowid) AS percentile_10,
       MAX(CASE WHEN rn <= max_steps * 0.25 THEN eq_rows END) OVER(PARTITION BY rowid) AS percentile_25,
       MAX(CASE WHEN rn <= max_steps * 0.50 THEN eq_rows END) OVER(PARTITION BY rowid) AS percentile_50,
       MAX(CASE WHEN rn <= max_steps * 0.75 THEN eq_rows END) OVER(PARTITION BY rowid) AS percentile_75,
       MAX(CASE WHEN rn <= max_steps * 0.90 THEN eq_rows END) OVER(PARTITION BY rowid) AS percentile_90,
       MAX(CASE WHEN rn <= max_steps * 0.99 THEN eq_rows END) OVER(PARTITION BY rowid) AS percentile_99
FROM (SELECT TOP (50000) /* Limiting resultset */
       tmp_stats.rowid,
       tmp_stats.database_name,
       tmp_stats.schema_name,
       tmp_stats.table_name,
       tmp_stats.stats_name,
       tmp_stats.histogram_graph,
       tmp_stats.table_index_base_type,
       tmp_stats.key_column_name,
       tmp_stats.key_column_data_type,
       tmp_stats.stat_all_columns,
       tmp_stats.statistic_type,
       tmp_density_vector.all_density,
       tmp_exec_history.leading_column_type,
       tmp_stats.current_number_of_rows AS current_number_of_rows_table,
       tmp_stats.current_number_of_modified_rows_since_last_update,
       tmp_stats.auto_update_threshold_type,
       tmp_stats.auto_update_threshold,
       CONVERT(DECIMAL(25, 2), (tmp_stats.current_number_of_modified_rows_since_last_update / (tmp_stats.auto_update_threshold * 1.0)) * 100.0) AS percent_of_threshold,
       tmp_stats.rows_sampled AS number_of_rows_sampled_on_last_update,
       tmp_stats.number_of_rows_at_time_stat_was_updated,
       tmp_stats.statistic_percent_sampled,
       DATEDIFF(HOUR, tmp_stats.last_updated, GETDATE()) AS hours_since_last_update,
       CONVERT(VARCHAR(4), DATEDIFF(mi,tmp_stats.last_updated,GETDATE()) / 60 / 24) + 'd ' + CONVERT(VARCHAR(4), DATEDIFF(mi,tmp_stats.last_updated,GETDATE()) / 60 % 24) + 'hr '
       + CONVERT(VARCHAR(4), DATEDIFF(mi,tmp_stats.last_updated,GETDATE()) % 60) + 'min' AS time_since_last_update,
       tmp_stats.last_updated AS last_updated_datetime,
       tmp_histogram.stepnumber,
       tmp_histogram.range_hi_key,
       tmp_histogram.range_rows,
       tmp_histogram.eq_rows,
       tmp_histogram.distinct_range_rows,
       tmp_histogram.avg_range_rows,
             ROW_NUMBER() OVER (PARTITION BY tmp_histogram.rowid ORDER BY tmp_histogram.eq_rows) as rn,
             MAX(tmp_histogram.stepnumber) OVER(PARTITION BY tmp_histogram.rowid) as max_steps
      FROM tempdb.dbo.tmp_stats
     INNER JOIN tempdb.dbo.tmp_histogram
        ON tmp_histogram.rowid = tmp_stats.rowid
     INNER JOIN tempdb.dbo.tmp_density_vector
        ON tmp_density_vector.rowid = tmp_stats.rowid
       AND tmp_density_vector.density_number = 1
     INNER JOIN tempdb.dbo.tmp_exec_history
        ON tmp_exec_history.rowid = tmp_stats.rowid
       AND tmp_exec_history.history_number = 1
     WHERE 1=1
       AND tmp_stats.is_unique = 0
       AND tmp_stats.current_number_of_rows >= 1000 /*ignoring small tables*/
       AND (SELECT COUNT(DISTINCT a.eq_rows) AS DistinctCount
              FROM tempdb.dbo.tmp_histogram AS a
             WHERE a.rowid = tmp_stats.rowid) > 1 /*only stats with more than 1 distinct eq_rows*/
      ORDER BY (ISNULL(tmp_stats.user_seeks,0) + ISNULL(tmp_stats.range_scan_count, 0)) DESC, tmp_stats.current_number_of_rows DESC) AS Tab1
),
CTE_2
AS
(
SELECT 
       CTE_1.rowid,
       CTE_1.database_name,
       CTE_1.schema_name,
       CTE_1.table_name,
       CTE_1.stats_name,
       CTE_1.histogram_graph,
       CTE_1.table_index_base_type,
       CTE_1.key_column_name,
       CTE_1.key_column_data_type,
       CTE_1.stat_all_columns,
       CTE_1.statistic_type,
       CTE_1.all_density,
       CTE_1.leading_column_type,
       CTE_1.current_number_of_rows_table,
       CTE_1.number_of_rows_at_time_stat_was_updated,
       CTE_1.current_number_of_modified_rows_since_last_update,
       CTE_1.auto_update_threshold_type,
       CTE_1.auto_update_threshold,
       CTE_1.percent_of_threshold,
       CTE_1.number_of_rows_sampled_on_last_update,
       CTE_1.statistic_percent_sampled,
       CTE_1.hours_since_last_update,
       CTE_1.time_since_last_update,
       CTE_1.last_updated_datetime,
       CTE_1.stepnumber,
       CTE_1.range_hi_key,
       CTE_1.eq_rows,
       MIN(eq_rows) OVER(PARTITION BY rowid) AS [min],
       MAX(eq_rows) OVER(PARTITION BY rowid) AS [max],
       SUM(eq_rows) OVER(PARTITION BY rowid) AS [sum],
       CONVERT(NUMERIC(25, 2), AVG(eq_rows) OVER(PARTITION BY rowid)) AS mean_avg,
       CONVERT(NUMERIC(25, 2), AVG(step_middle_midpoint) OVER(PARTITION BY rowid)) AS median,
       (SELECT TOP 1 a.eq_rows FROM CTE_1 AS a WHERE CTE_1.rowid = a.rowid ORDER BY a.cnt DESC) AS mode,
       percentile_10,
       percentile_25,
       percentile_50,
       percentile_75,
       percentile_90,
       percentile_99,
       CONVERT(NUMERIC(25, 2), (eq_rows / CASE WHEN SUM(eq_rows) OVER(PARTITION BY rowid) = 0 THEN 1 ELSE SUM(eq_rows) OVER(PARTITION BY rowid) END) * 100) AS eq_rows_percent_from_total,
       CASE
           WHEN eq_rows + AVG(eq_rows) OVER(PARTITION BY rowid) > 0 THEN
               CONVERT(
                          NUMERIC(18, 2),
                          (((eq_rows - CONVERT(NUMERIC(25, 2), AVG(eq_rows) OVER(PARTITION BY rowid))) / ((eq_rows + CONVERT(NUMERIC(25, 2), AVG(eq_rows) OVER(PARTITION BY rowid))) / 2.)) * 100)
                      )
           ELSE
               0
       END AS percent_diff_from_avg,
       CASE
           WHEN eq_rows > 0 THEN
               CONVERT(
                          NUMERIC(18, 2),
                          (((eq_rows - CONVERT(NUMERIC(25, 2), AVG(eq_rows) OVER(PARTITION BY rowid))) / (CONVERT(NUMERIC(18, 2), eq_rows))) * 100)
                      )
           ELSE
               0
       END AS percent_change_from_avg,
       CASE
           WHEN eq_rows + CONVERT(NUMERIC(25, 2), AVG(step_middle_midpoint) OVER(PARTITION BY rowid)) > 0 THEN
               CONVERT(
                          NUMERIC(18, 2),
                          (((eq_rows - CONVERT(NUMERIC(25, 2), CONVERT(NUMERIC(25, 2), AVG(step_middle_midpoint) OVER(PARTITION BY rowid)))) / ((eq_rows + CONVERT(NUMERIC(25, 2), AVG(step_middle_midpoint) OVER(PARTITION BY rowid))) / 2.)) * 100)
                      )
           ELSE
               0
       END AS percent_diff_from_median,
       CASE
           WHEN eq_rows > 0 THEN
               CONVERT(
                          NUMERIC(18, 2),
                          (((eq_rows - CONVERT(NUMERIC(25, 2), AVG(step_middle_midpoint) OVER(PARTITION BY rowid))) / (CONVERT(NUMERIC(18, 2), eq_rows))) * 100)
                      )
           ELSE
               0
       END AS percent_change_from_median
FROM CTE_1
)
SELECT REPLICATE('|', CEILING(eq_rows_percent_from_total)) AS g_histogram,
       CTE_2.database_name,
       CTE_2.schema_name,
       CTE_2.table_name,
       CTE_2.stats_name,
       CTE_2.key_column_name,
       CTE_2.statistic_type,
       CTE_2.current_number_of_rows_table,
       CTE_2.number_of_rows_at_time_stat_was_updated,
       CTE_2.number_of_rows_sampled_on_last_update,
       CTE_2.time_since_last_update,
       CTE_2.stepnumber,
       CASE CTE_2.stepnumber WHEN 1 THEN CTE_2.histogram_graph ELSE NULL END AS histogram_ascii_graph,
       CTE_2.range_hi_key,
       CTE_2.eq_rows,
       CTE_2.eq_rows_percent_from_total,
       CONVERT(NUMERIC(25,2), CTE_2.all_density * CTE_2.number_of_rows_at_time_stat_was_updated) AS estimated_number_of_rows_per_value_based_on_density,
       CTE_2.mean_avg, CTE_2.percent_diff_from_avg, CTE_2.percent_change_from_avg,
       CTE_2.median, CTE_2.percent_diff_from_median, CTE_2.percent_change_from_median,
       CTE_2.min, CTE_2.max
FROM CTE_2
ORDER BY rowid, stepnumber

/*
-- Script to test check
USE Northwind
GO
IF OBJECT_ID('OrdersBig') IS NOT NULL
  DROP TABLE OrdersBig
GO
SELECT TOP 1500000
       IDENTITY(Int, 1,1) AS OrderID,
       ABS(CheckSUM(NEWID()) / 10000000) AS CustomerID,
       CONVERT(Date, GETDATE() - (CheckSUM(NEWID()) / 1000000)) AS OrderDate,
       ISNULL(ABS(CONVERT(Numeric(18,2), (CheckSUM(NEWID()) / 1000000.5))),0) AS Value
  INTO OrdersBig
  FROM Orders A
 CROSS JOIN Orders B CROSS JOIN Orders C CROSS JOIN Orders D
GO
INSERT INTO OrdersBig WITH(TABLOCK)
SELECT TOP 500000
       ABS(CheckSUM(NEWID()) / 10000000) AS CustomerID,
       '20220101' AS OrderDate,
       ISNULL(ABS(CONVERT(Numeric(18,2), (CheckSUM(NEWID()) / 1000000.5))),0) AS Value
  FROM Orders A
 CROSS JOIN Orders B CROSS JOIN Orders C CROSS JOIN Orders D
GO
INSERT INTO OrdersBig WITH(TABLOCK)
SELECT TOP 4000000
       99999 AS CustomerID,
       '20220101' AS OrderDate,
       ISNULL(ABS(CONVERT(Numeric(18,2), (CheckSUM(NEWID()) / 1000000.5))),0) AS Value
  FROM Orders A
 CROSS JOIN Orders B CROSS JOIN Orders C CROSS JOIN Orders D
GO
ALTER TABLE OrdersBig ADD CONSTRAINT xpk_OrdersBig PRIMARY KEY(OrderID)
GO
CREATE INDEX ixCustomerID ON OrdersBig(CustomerID)
CREATE INDEX ixOrderDate ON OrdersBig(OrderDate)
GO
UPDATE STATISTICS OrdersBig WITH SAMPLE
GO

DBCC SHOW_STATISTICS(OrdersBig, ixOrderDate)
GO

SELECT COUNT(*) FROM OrdersBig
WHERE OrderDate = '20250101'
AND 1 = (SELECT 1)
GO
SELECT COUNT(*) FROM OrdersBig
WHERE CustomerID <= 1
AND 1 = (SELECT 1)
GO 10

*/