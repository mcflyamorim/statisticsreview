/*
Check5 - Unused statistics
Description:
Check 5 - Are there any unused statistics?
Check unused statistics.
If number of modifications is greater than the auto update threshold, then I'm considering there is a very high chance that the statistic is not being used (considering auto update stats is on DB).
Estimated Benefit:
Very High
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Remove unused statistics.
Detailed recommendation:
- If the number of changes is higher than the threshold but the statistic is not updated, that means this statistic is not used since last update time (or Auto Update Statistics option is set to OFF). If it was, the auto update would have triggered and updated it.
- Check how many days has been since last update stats and current date to see for how long this statistic considered as "not used". You may want to consider to drop those.
Note 1: Hypothetical indexes will show up as unused as they usually do not get updated by maintenance plans.
Note 2: If you see %_dta_% garbage, please drop those indexes and stats.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck5') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck5

IF OBJECT_ID('tempdb.dbo.#tmpCheck5') IS NOT NULL
  DROP TABLE #tmpCheck5

SELECT a.database_name,
       CASE a.is_auto_update_stats_on WHEN 1 THEN 'Yes' ELSE 'No' END AS is_auto_update_stats_on,
       a.table_name,
       a.stats_name,
       a.key_column_name,
       a.statistic_type,
       a.plan_cache_reference_count,
       a.last_updated AS last_updated_datetime,
       CONVERT(VARCHAR(4), DATEDIFF(mi,a.last_updated,GETDATE()) / 60 / 24) + 'd ' + CONVERT(VARCHAR(4), DATEDIFF(mi,a.last_updated,GETDATE()) / 60 % 24) + 'hr '
       + CONVERT(VARCHAR(4), DATEDIFF(mi,a.last_updated,GETDATE()) % 60) + 'min' AS time_since_last_update,
       TabIndexUsage.last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used,
       CONVERT(VARCHAR(4), DATEDIFF(mi,TabIndexUsage.last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used,GETDATE()) / 60 / 24) + 'd ' + CONVERT(VARCHAR(4), DATEDIFF(mi,TabIndexUsage.last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used,GETDATE()) / 60 % 24) + 'hr '
       + CONVERT(VARCHAR(4), DATEDIFF(mi,TabIndexUsage.last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used,GETDATE()) % 60) + 'min' AS time_since_last_index_or_a_table_if_obj_is_not_a_index_statistic_usage,
       a.current_number_of_rows,
       a.number_of_rows_at_time_stat_was_updated,
       a.current_number_of_modified_rows_since_last_update,
       a.auto_update_threshold,
       a.auto_update_threshold_type,
       CONVERT(DECIMAL(25, 2), (a.current_number_of_modified_rows_since_last_update / (a.auto_update_threshold * 1.0)) * 100.0) AS percent_of_threshold,
       dbcc_command
INTO #tmpCheck5
FROM tempdb.dbo.tmp_stats a
OUTER APPLY (SELECT MAX(Dt) FROM (VALUES(a.last_user_seek), 
                                        (a.last_user_scan),
                                        (a.last_user_lookup)
                               ) AS t(Dt)) AS TabIndexUsage(last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used)
WHERE CONVERT(DECIMAL(25, 2), (a.current_number_of_modified_rows_since_last_update / (a.auto_update_threshold * 1.0)) * 100.0) >= 100 /*Only rows with threshold already hit*/
AND a.is_auto_create_stats_on = 1 /*Considering only DBs with auto update stats on*/
AND a.no_recompute = 0 /*Considering only stats that are not set to no recompute*/
AND a.stats_id <> 1 /*Ignoring clustered keys has they can still be used in lookups and don't trigger update stats*/
--AND DATEDIFF(HOUR, a.last_updated, GETDATE()) >= 48 /*Only considering statistics that were not updated in past 2 days*/

SELECT 'Check 5 - Are there any unused statistics?' AS [info], 
       * 
INTO tempdb.dbo.tmpStatisticCheck5
FROM #tmpCheck5

SELECT * FROM tempdb.dbo.tmpStatisticCheck5
ORDER BY current_number_of_rows DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name

/*
Script to test the check:

USE Northwind
GO
IF OBJECT_ID('OrdersBig') IS NOT NULL
  DROP TABLE OrdersBig
GO
SELECT TOP 2000000
       IDENTITY(Int, 1,1) AS OrderID,
       ABS(CheckSUM(NEWID()) / 10000000) AS CustomerID,
       CONVERT(Date, GETDATE() - (CheckSUM(NEWID()) / 1000000)) AS OrderDate,
       ISNULL(ABS(CONVERT(Numeric(18,2), (CheckSUM(NEWID()) / 1000000.5))),0) AS Value
  INTO OrdersBig
  FROM master.dbo.spt_values A
 CROSS JOIN master.dbo.spt_values B
 CROSS JOIN master.dbo.spt_values C
 CROSS JOIN master.dbo.spt_values D
GO
ALTER TABLE OrdersBig ADD CONSTRAINT xpk_OrdersBig PRIMARY KEY(OrderID)
GO

-- auto_update_threshold = 44721

-- Auto create a stat on OrderDate
SELECT COUNT(*) FROM OrdersBig
WHERE OrderDate <= GETDATE()
AND 1 = (SELECT 1)
GO

-- Modify 50k rows (111.80 percent of auto update threshold)
UPDATE TOP (50000) OrdersBig SET OrderDate = OrderDate
GO

*/