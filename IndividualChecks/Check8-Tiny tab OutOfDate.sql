/*
Check8 - Tiny table with out-of-date statistic
Description:
Check 8 - Is there any tiny (less than or equal to 500 rows) table with out-of-date statistics?
Check if there are small tables (less than or equal to 500 rows) with poor statistics. 
Small tables will only trigger auto-update stats if modification counter is >= 501, depending on the environment this may take a while or never happen. SQL Server ignores very small tables (normal tables not temp tables) for automatic statistics. Unfortunately, this might happen quite often in relational data warehouse solutions which use star schemas. 
The effect of joining a few-hundred million rows fact table with some small dimensions the wrong way might be dramatic - in a negative sense.
This problem is much easier to avoid with huge tables, but if you add 1 row to a 1-row table you double the data.
https://learn.microsoft.com/en-us/archive/blogs/mssqlisv/sql-optimizations-manual-update-statistics-on-small-tables-may-provide-a-big-impact 
Estimated Benefit:
Medium
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Make sure you're updating stats for small tables.
Detailed recommendation:
- To avoid issues, make sure you're updating stats for those small tables.
- To avoid outdated or obsolete statistics on those tiny tables (in terms of number of rows), make sure you're manually updating it, it will not take too much time and may help query optimizer.
- You can use column query_plan_associated_with_last_usage to investigate query plan.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('dbo.tmpStatisticCheck8') IS NOT NULL
  DROP TABLE dbo.tmpStatisticCheck8

SELECT 'Check 8 - Is there any tiny (less than or equal to 500 rows) table with out-of-date statistics?' AS [info],
       a.database_name,
       a.table_name,
       a.stats_name,
       a.statistic_type,
       a.key_column_name,
       a.plan_cache_reference_count,
       a.last_updated AS last_updated_datetime,
       CASE 
           WHEN DATEDIFF(dd,a.last_updated, GETDATE()) >= 7 THEN 
                'Warning - It has been more than 7 days [' + CONVERT(VARCHAR(4), DATEDIFF(mi,a.last_updated,GETDATE()) / 60 / 24) + 'd ' + CONVERT(VARCHAR(4), DATEDIFF(mi,a.last_updated,GETDATE()) / 60 % 24) + 'hr '
                + CONVERT(VARCHAR(4), DATEDIFF(mi,a.last_updated,GETDATE()) % 60) + 'min' 
                + '] since last update statistic.'
           ELSE 'OK'
         END AS statistic_updated_comment,
       TabIndexUsage.last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used,
       a.current_number_of_rows, 
       a.number_of_rows_at_time_stat_was_updated,
       a.current_number_of_modified_rows_since_last_update,
       a.auto_update_threshold,
       a.auto_update_threshold_type,
       dbcc_command
INTO dbo.tmpStatisticCheck8
FROM dbo.tmpStatisticCheck_stats AS a
OUTER APPLY (SELECT MAX(Dt) FROM (VALUES(a.last_user_seek), 
                                        (a.last_user_scan),
                                        (a.last_user_lookup)
                               ) AS t(Dt)) AS TabIndexUsage(last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used)
 WHERE a.number_of_rows_at_time_stat_was_updated <= 500
   AND a.current_number_of_modified_rows_since_last_update >= 1

SELECT * FROM dbo.tmpStatisticCheck8
ORDER BY current_number_of_rows DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name