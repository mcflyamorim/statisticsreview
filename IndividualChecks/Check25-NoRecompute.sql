/*
Check25 - Statistics set to NoRecompute
Description:
Check 25 - Check if statistic is set to NoRecompute
NoRecompute disable the automatic statistics update option, AUTO_STATISTICS_UPDATE, for statistics_name. If this option is specified, the query optimizer will complete any in-progress statistics updates for statistics_name and disable future updates. Using this option can produce suboptimal query plans. We recommend using this option sparingly, and then only by a qualified system administrator.
Estimated Benefit:
High
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Only use NoRecompute option when there is a demonstrated/documented need.
Detailed recommendation:
- Make sure there is a good and documented reason to use NoRecompute. It may be ok for LOB columns as they usually take a lot of time to update/create.
- To re-enable statistics updates, remove the statistics with DROP STATISTICS and then run CREATE STATISTICS without the NORECOMPUTE option.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck25') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck25

SELECT 'Check 25 - Check if statistic is set to NoRecompute' AS [info],
       a.database_name,
       a.table_name,
       a.stats_name,
       a.key_column_name,
       a.last_updated AS last_updated_datetime,
       a.plan_cache_reference_count,
       a.current_number_of_rows,
       a.rows_sampled AS number_of_rows_sampled_on_last_update_create_statistic,
       a.current_number_of_modified_rows_since_last_update,
       a.no_recompute,
       CASE 
         WHEN no_recompute = 1 THEN 'Warning - Statistic is set to no recompute, this can be helpful if key is not uniformly distributed and you have your own update statistic maintenance plan, but harmful if you expect automatic statistics updates.'
         ELSE 'OK'
       END AS no_recompute_comment,
       dbcc_command
INTO tempdb.dbo.tmpStatisticCheck25
FROM tempdb.dbo.tmp_stats a
WHERE a.current_number_of_rows > 0 /* Ignoring empty tables */
AND a.no_recompute = 1

SELECT * FROM tempdb.dbo.tmpStatisticCheck25
ORDER BY no_recompute DESC,
         current_number_of_rows DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name