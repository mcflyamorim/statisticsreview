/*
Check16 - Filtered out-of-date statistics
Description:
Check 16 - Are filtered stats are out of date?
For filtered indexes the threshold invalidation algorithm is tied solely to the column and not just to the filtered set. So, if your table has 10,000 rows it takes 2,500 modifications in that column to update statistics. 
If your filtered index only has 1,000 rows, then you could theoretically modify this specific filtered set 2.5 times before it would be updated. There is a limitation of the automatic update logic is that it only tracks changes to columns in the statistics, but not changes to columns in the predicate. If there are many changes to the columns used in predicates of filtered statistics, consider using manual updates to keep up with the changes. In other words, if you have a stat "create statistics Stats1 on Customers (ContactName) where Active = 1", if you update Active column from 1 to 0 for 100% of table, it will not trigger auto update stats, as column ContactName doesn't have any modification.
Note: For the leading columns, the modification counter is adjusted by the selectivity of the filter before these conditions are tested. For example, for filtered statistics with predicate selecting 50% of the rows, the modification counter is multiplied by 0.5.
Estimated Benefit:
High
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Review outdated filtered statistics and update them more often.
Detailed recommendation:
- Create a job to update these filtered stats manually and more frequently.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck16') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck16

SELECT 
  'Check 16 - Are filtered stats are out of date ?' AS [info],
  database_name,  
  table_name, 
  stats_name, 
  key_column_name, 
  statistic_type,
  filter_definition,
  plan_cache_reference_count,
  last_updated AS last_updated_datetime,
  DATEDIFF(hh,last_updated,GETDATE()) AS hours_since_last_update,
  CASE 
    WHEN DATEDIFF(hh,last_updated, GETDATE()) > 24 THEN 
         'It has been more than 24 hours [' + CONVERT(VarChar(4), DATEDIFF(mi,last_updated,GETDATE()) / 60 / 24) + 'd ' + CONVERT(VarChar(4), DATEDIFF(mi,last_updated,GETDATE()) / 60 % 24) + 'hr '
         + CONVERT(VarChar(4), DATEDIFF(mi,last_updated,GETDATE()) % 60) + 'min' 
         + '] since last update statistic.'
    ELSE 'OK'
  END AS [comment],
  current_number_of_rows, 
  number_of_rows_at_time_stat_was_updated,
  unfiltered_rows AS number_of_rows_on_table_at_time_statistics_was_updated_ignoring_filter,
  current_number_of_modified_rows_since_last_update,
  a.auto_update_threshold,
  a.auto_update_threshold_type,
  CONVERT(DECIMAL(25, 2), (a.current_number_of_modified_rows_since_last_update / (a.auto_update_threshold * 1.0)) * 100.0) AS percent_of_threshold,
  dbcc_command
INTO tempdb.dbo.tmpStatisticCheck16
FROM tempdb.dbo.tmp_stats AS a
WHERE has_filter = 1

SELECT * FROM tempdb.dbo.tmpStatisticCheck16
ORDER BY current_number_of_rows DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name
