/*
Check55 - Ascii graph histogram
Description: 
Check 55 
----------------------

Estimated Benefit:
Medium
Estimated Effort:
High
Recommendation:
Quick recommendation:
Review reported statistics, comments and recommendations.

Detailed recommendation:
----------------------

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'RedGateMonitorFabianoAmorim', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck55') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck55


-- TODO: Adjust it to show data for the of the most 100 used histograms on equality seeks and range scans...

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
FROM (SELECT 
       tmp_stats.rowid,
       tmp_stats.database_name,
       tmp_stats.schema_name,
       tmp_stats.table_name,
       tmp_stats.stats_name,
       tmp_stats.table_index_base_type,
       tmp_stats.key_column_name,
       tmp_stats.key_column_data_type,
       tmp_stats.stat_all_columns,
       tmp_stats.statistic_type,
       tmp_exec_history.leading_column_type,
       tmp_stats.current_number_of_rows AS current_number_of_rows_table,
       tmp_stats.current_number_of_modified_rows_since_last_update,
       tmp_stats.auto_update_threshold_type,
       tmp_stats.auto_update_threshold,
       CONVERT(DECIMAL(25, 2), (tmp_stats.current_number_of_modified_rows_since_last_update / (tmp_stats.auto_update_threshold * 1.0)) * 100.0) AS percent_of_threshold,
       tmp_stats.rows_sampled AS number_of_rows_sampled_on_last_update,
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
     INNER JOIN tempdb.dbo.tmp_exec_history
        ON tmp_exec_history.rowid = tmp_stats.rowid
       AND tmp_exec_history.history_number = 1
     WHERE 1=1
       AND tmp_stats.is_unique = 0
       AND tmp_stats.current_number_of_rows >= 1000 /*ignoring small tables*/
       AND EXISTS(SELECT 1 
                    FROM tempdb.dbo.tmp_stat_header 
                   WHERE tmp_stat_header.rowid = tmp_stats.rowid 
                     AND tmp_stat_header.steps > 1) /*only stats with more than 1 step*/
       --AND tmp_stats.rowid = 30
      ) AS Tab1
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
       CTE_1.table_index_base_type,
       CTE_1.key_column_name,
       CTE_1.key_column_data_type,
       CTE_1.stat_all_columns,
       CTE_1.statistic_type,
       CTE_1.leading_column_type,
       CTE_1.current_number_of_rows_table,
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
       END AS percent_change_from_avg
FROM CTE_1
)
SELECT TOP (10000) /* Limiting resultset */
       REPLICATE('|', CEILING(eq_rows_percent_from_total)) AS g_histogram,
       stats_name, stepnumber, CTE_2.range_hi_key, CTE_2.eq_rows, CTE_2.min, CTE_2.max, CTE_2.eq_rows_percent_from_total, CTE_2.percent_diff_from_avg, CTE_2.percent_change_from_avg
FROM CTE_2
ORDER BY rowid, stepnumber
GO


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