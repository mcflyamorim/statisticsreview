/*
Check30 - Out-of-date statistics
Description:
Check 30 - Check if there are outdated (latest update older than 24 hours) statistics
Statistics become out-of-date after modifications from insert, update, delete, or merge operations change the data distribution in the table or indexed view.
Estimated Benefit:
Very High
Estimated Effort:
High
Recommendation:
Quick recommendation:
Update out of date statistics.
Detailed recommendation:
In some cases, you can improve the query plan and therefore improve query performance by updating statistics more frequently.
The frequency with which you should update statistics depends greatly on how much data modification you have and depends on your application. You may require some experimentation to determine when to do it to ensures that queries compile with up-to-date statistics. Another important aspect you should consider is to look at the execution plans and check if you estimated number of rows differ from your actual rows. You can use the following xEvents to help you to identify those cases:
* inaccurate_cardinality_estimate (I would start by tracking this one)
* large_cardinality_misestimate
* query_optimizer_cardinality_guess
* query_optimizer_estimate_cardinality
* large_cardinality_misestimate

However, keep in mind that update statistic will case queries to recompile, therefore, there is a performance tradeoff between improving query plans and the time/cost it takes to recompile queries. The specific tradeoffs depend on your application. Usually, the cost overhead of using out-of-date statistic is higher than the recompile.
A good starting point for the frequency of fullscan update is that if the table under consideration has a high update rate, run fullscan statistics update nightly. If the table has a low update rate, run fullscan statistics update weekly.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck30') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck30

SELECT 
  'Check 30 - Check if there are outdated (latest update older than 24 hours) statistics' AS [info],
  database_name,
  table_name, 
  stats_name, 
  key_column_name, 
  statistic_type,
  a.plan_cache_reference_count,
  a.last_updated AS last_updated_datetime,
  DATEDIFF(hh, a.last_updated, GETDATE()) AS hours_since_last_update,
  CASE
    WHEN DATEDIFF(hh, a.last_updated, GETDATE()) > 24 THEN 
         'It has been more than 24 hours [' + CONVERT(VARCHAR(4), DATEDIFF(mi,a.last_updated,GETDATE()) / 60 / 24) + 'd ' + CONVERT(VARCHAR(4), DATEDIFF(mi,a.last_updated,GETDATE()) / 60 % 24) + 'hr '
         + CONVERT(VARCHAR(4), DATEDIFF(mi,a.last_updated,GETDATE()) % 60) + 'min' 
         + '] since last update statistic.'
    ELSE 'OK'
  END AS comment_1,
  CASE 
    WHEN ISNULL(current_number_of_modified_rows_since_last_update,0) > 0 
    THEN 'Warning - Statistics last updated ' + CASE 
                                                  WHEN a.last_updated IS NULL THEN N'[NEVER]'
					                                             ELSE CONVERT(VARCHAR(200), a.last_updated, 21) + 
						                                             ' and have had ' + CONVERT(NVARCHAR(100), ISNULL(current_number_of_modified_rows_since_last_update,0)) +
						                                             ' modifications since last update, which is ' +
						                                             CONVERT(NVARCHAR(100), CAST((ISNULL(current_number_of_modified_rows_since_last_update,0) / (ISNULL(number_of_rows_at_time_stat_was_updated, 1) * 1.00)) * 100.0 AS DECIMAL(18, 2))) + 
						                                             '% of the table.'
                                                END 
    ELSE 'OK'
  END AS comment_2,
  TabIndexUsage.last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used,
  current_number_of_rows,
  number_of_rows_at_time_stat_was_updated,
  current_number_of_modified_rows_since_last_update,
  CAST((ISNULL(current_number_of_modified_rows_since_last_update,0) / (ISNULL(number_of_rows_at_time_stat_was_updated, 1) * 1.00)) * 100.0 AS DECIMAL(18, 2)) AS percent_modifications,
  a.user_seeks + a.user_scans + a.user_lookups AS number_of_reads_on_index_table_since_last_restart,
  a.user_updates AS number_of_modifications_on_index_table_since_last_restart,
  a.range_scan_count AS number_of_range_scans_since_last_restart_rebuild,
  a.page_latch_wait_count AS number_of_page_latch_since_last_restart_rebuild,
  a.page_io_latch_wait_count AS number_of_page_i_o_latch_since_last_restart_rebuild,
  a.auto_update_threshold,
  a.auto_update_threshold_type,
  CONVERT(DECIMAL(25, 2), (a.current_number_of_modified_rows_since_last_update / (a.auto_update_threshold * 1.0)) * 100.0) AS percent_of_threshold,
  dbcc_command
INTO tempdb.dbo.tmpStatisticCheck30
FROM tempdb.dbo.tmp_stats AS a
OUTER APPLY (SELECT MAX(Dt) FROM (VALUES(a.last_user_seek), 
                                        (a.last_user_scan),
                                        (a.last_user_lookup)
                               ) AS t(Dt)) AS TabIndexUsage(last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used)

SELECT * FROM tempdb.dbo.tmpStatisticCheck30
ORDER BY current_number_of_rows DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name
