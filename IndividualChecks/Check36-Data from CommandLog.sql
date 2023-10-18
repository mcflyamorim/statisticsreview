/*
Check36 - Data from CommandLog
Description:
Check 36 - Check if Commandlog exists on master and return update stats command and duration.
You may want to adjust your maintenance plan to deal with statistics taking too much time to run in a separate window.
Also, it would be good to know whether the statistic is really being used as you don't want to spend time updating it if it is not helping your queries. You can use TF8666 and check plan cache for the stats name to see if you can find any usage from cache. But, keep in mind that plan cache may be under pressure or bloated with ad-hoc plans causing plans to be removed very quick, so you may don't capture the plan from cache.
We might think that a properly architected database system making extensive use of stored procedures should not have unusually large plan cache. But many real-world systems are not well-architected, either in having too much dynamic SQL, or in the case of Entity Frameworks, a bloated parametrized SQL plan cache.
Another option is to track it using auto_stats extended event filtering by the stat name to see if this is loaded.
Estimated Benefit:
Medium
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Review reported statistics and recommendations.
Detailed recommendation:
- Look for statistics taking a lot of time to update and make sure it is really being used. You may find, "bpk", "test", "backup" tables
- Check columns number_of_reads_on_index_table_since_last_restart, number_of_modifications_on_index_table_since_last_restart, number_of_range_scans_since_last_restart_rebuild, number_of_page_latch_since_last_restart_rebuild and number_of_page_i_o_latch_since_last_restart_rebuild as they can give you more info about table usage. If table is not really used, you may want to remove those statistics or remove them from maintenance plan.
- A more drastic approach would just to go ahead and drop statistics taking more time to update, or maybe, drop them all and rely on auto-create stats to re-create really needed stats. This may sound like a radical idea at first, but think about it. What do you have to gain or lose? Dropping all auto stats will place some temporary stress on the system. As queries come in, the query optimizer will begin recreating those statistics that we just dropped.  Every query that a query requires a statistic to be created, it will wait. Soon, typically in a matter of minutes for highly utilized systems, most of the missing statistics will be already back in place and the temporary stress will be over. But now, only the ones that are really needed by the current workload will be re-created and all the redundant ones just came off the expensive maintenance tab. If you are worried about the impact of the initial statistics creation, you can perform the cleanup at off-peak hours and ‘warm-up’ the database by capturing and replaying a trace of the most common read only queries. This will create many of the required statistics objects without impacting your ‘real’ workload.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck36') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck36

IF OBJECT_ID('master.dbo.CommandLog') IS NOT NULL
BEGIN
  DECLARE @SQL VarChar(MAX)
  SET @SQL = 'use [master]; 
              SELECT ''Check 36 - Check if Commandlog exists on master and return updatestats command and duration'' AS [info],
                     CommandLog.ID AS id,
                     CommandLog.[DatabaseName] AS database_name,
                     CommandLog.[SchemaName] AS schema_name,
                     CommandLog.[ObjectName] AS object_bame,
                     CommandLog.[StatisticsName] AS stats_name,
                     a.key_column_name,
                     a.current_number_of_rows,
                     a.plan_cache_reference_count,
                     a.statistic_type,
                     a.is_lob,
                     CommandLog.[PartitionNumber] AS partition_number,
                     DATEDIFF(ms, CommandLog.StartTime, CommandLog.EndTime) AS duration_ms,
                     CONVERT(NUMERIC(25, 3), DATEDIFF(ms, CommandLog.StartTime, CommandLog.EndTime) / 1000.) AS duration_seconds,
                     CONVERT(NUMERIC(25, 3), DATEDIFF(ms, CommandLog.StartTime, CommandLog.EndTime) / 1000. / 60) AS duration_minutes,
                     CASE 
                       WHEN PATINDEX(''%FULLSCAN'', CommandLog.Command) > 0 THEN 1
                       ELSE 0
                     END AS is_full_scan,
                     user_seeks + user_scans + user_lookups AS number_of_reads_on_index_table_since_last_restart,
                     user_updates AS number_of_modifications_on_index_table_since_last_restart,
                     range_scan_count AS number_of_range_scans_since_last_restart_rebuild,
                     page_latch_wait_count AS number_of_page_latch_since_last_restart_rebuild,
                     page_io_latch_wait_count AS number_of_page_i_o_latch_since_last_restart_rebuild,
                     CommandLog.Command AS command,
                     CommandLog.CommandType AS command_type,
                     CommandLog.StartTime AS start_datetime,
                     CommandLog.EndTime AS end_datetime,
                     CommandLog.ErrorNumber AS error_number,
                     CommandLog.ErrorMessage AS error_message,
                     CommandLog.ExtendedInfo AS extended_info
               INTO tempdb.dbo.tmpStatisticCheck36
               FROM CommandLog
               LEFT OUTER JOIN tempdb.dbo.tmp_stats AS a
               ON a.database_name = QUOTENAME(CommandLog.[DatabaseName])
               AND a.schema_name = QUOTENAME(CommandLog.[SchemaName])
               AND a.table_name = QUOTENAME(CommandLog.[ObjectName])
               AND a.stats_name = QUOTENAME(CommandLog.[StatisticsName])
               WHERE CommandLog.StartTime >= GetDate() - 8
               AND CommandLog.Command LIKE ''%UPDATE STA%''
               ORDER BY DATEDIFF(ms, CommandLog.StartTime, CommandLog.EndTime) DESC'

  /*SELECT @SQL*/
  EXEC (@SQL)
  SELECT * FROM tempdb.dbo.tmpStatisticCheck36
END
ELSE
BEGIN
  SELECT TOP 0 
         'Check 36 - Check if Commandlog exists on master and return updatestats command and duration' AS [info]
  INTO tempdb.dbo.tmpStatisticCheck36

  SELECT * FROM tempdb.dbo.tmpStatisticCheck36
END

