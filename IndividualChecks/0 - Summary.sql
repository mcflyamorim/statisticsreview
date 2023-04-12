DECLARE @sqlcmd NVARCHAR(MAX),
        @params NVARCHAR(600),
        @sqlmajorver INT;
DECLARE @UpTime VARCHAR(12),@StartDate DATETIME

SELECT @sqlmajorver = CONVERT(INT, (@@microsoftversion / 0x1000000) & 0xff);

IF @sqlmajorver < 10
BEGIN
    SET @sqlcmd
        = N'SELECT @UpTimeOUT = DATEDIFF(mi, login_time, GETDATE()), @StartDateOUT = login_time FROM master..sysprocesses (NOLOCK) WHERE spid = 1';
END;
ELSE
BEGIN
    SET @sqlcmd
        = N'SELECT @UpTimeOUT = DATEDIFF(mi,sqlserver_start_time,GETDATE()), @StartDateOUT = sqlserver_start_time FROM sys.dm_os_sys_info (NOLOCK)';
END;

SET @params = N'@UpTimeOUT VARCHAR(12) OUTPUT, @StartDateOUT DATETIME OUTPUT';

EXECUTE sp_executesql @sqlcmd,
                      @params,
                      @UpTimeOUT = @UpTime OUTPUT,
                      @StartDateOUT = @StartDate OUTPUT;

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheckSummary') IS NOT NULL
    DROP TABLE tempdb.dbo.tmpStatisticCheckSummary;
WITH CTE_1
AS (

   SELECT CONVERT(VARCHAR(8000), 'SQL Server instance startup time: ' + CONVERT(VARCHAR(30), @StartDate, 20)) AS [info],
          CONVERT(VARCHAR(200), CONVERT(VARCHAR(4), @UpTime / 60 / 24) + 'd ' + CONVERT(VARCHAR(4), @UpTime / 60 % 24) + 'hr ' + CONVERT(VARCHAR(4), @UpTime % 60) + 'min') AS [result],
          'NA' AS prioritycol,
          'NA' AS more_info,
          '' AS quick_fix
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Total number of databases: ') AS [info],
          '- ' + CONVERT(VARCHAR(200), COUNT(DISTINCT database_id)) + ' -' AS [result],
          'NA' AS prioritycol,
          'NA' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmp_stats
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Total number of tables: ') AS [info],
          '- ' + CONVERT(VARCHAR(200),COUNT(DISTINCT database_name + schema_name + table_name)) + ' -' AS [result],
          'NA' AS prioritycol,
          'NA' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmp_stats
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Total number of stats: ') AS [info],
          '- ' + CONVERT(VARCHAR(200), COUNT(*)) + ' -' AS [result],
          'NA' AS prioritycol,
          'NA' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmp_stats
   UNION ALL

   --Number of out-of-date stats: <N>
   SELECT CONVERT(VARCHAR(8000), 'Number of out-of-date (more than 24 hours since last update) stats: ') AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check30' AS more_info,
          'Create an upd stats maintenance plan, or simple run sp_updatestats' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck30
   WHERE hours_since_last_update >= 24
   UNION ALL

   --Percent of stats considered out-of-date (more than 24 hours since last update):: <N>
   SELECT CONVERT(VARCHAR(8000), 'Percent of stats considered out-of-date (more than 24 hours since last update): ') AS [info],
          CONVERT(VARCHAR(200), CONVERT(NUMERIC(18, 0), (COUNT(*) / t1.cnt) * 100)) AS [result],
          'High' AS prioritycol,
          'Check30' AS more_info,
          '' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck30
   OUTER APPLY (SELECT CONVERT(NUMERIC(18, 2), COUNT(*)) AS cnt
                FROM tempdb.dbo.tmpStatisticCheck30) AS t1
   WHERE hours_since_last_update >= 24
   GROUP BY t1.cnt
   UNION ALL

   --Number of tiny table (less than or equal to 500 rows) with out-of-date stats: <N>
   SELECT 'Number of tiny table (less than or equal to 500 rows) with out-of-date stats: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check8' AS more_info,
          'Run upd stats on tables' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck8
   UNION ALL

   --Number of stats with a sampling rate lower than 5% of table: <N>
   SELECT 'Number of stats with a sampling rate lower than 5% of table: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check23' AS more_info,
          'Increase upd stats sampling' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck23
   WHERE [statistic_percent_sampled] < 5
   UNION ALL

   --Number of tables that will have an auto update/create sampling rate lower than 5% of table: 
   SELECT 'Number of tables that will have an auto update/create sampling rate lower than 5% of table: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check42' AS more_info,
          'Make sure you''re updating the stat with an user defined sample' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck42
   WHERE [auto_update_create_percent_sample] < 5
   UNION ALL

   --Number of estimated events of stats being updated from fullscan to sample(due to an auto-update):
   SELECT 'Number of estimated events of stats being updated from fullscan to sample(due to an auto-update): ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check34' AS more_info,
          'Enable NoRecompute and upd stats manually' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck34
   UNION ALL

   --Number of statistics with modifications greater than 1000 or 1% of table:
   SELECT 'Number of statistics with modifications greater than 1000 or 1% of table: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check30' AS more_info,
          'Update statistic more often' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck30
   CROSS APPLY (SELECT CONVERT(DECIMAL(25, 2), (current_number_of_modified_rows_since_last_update / (CASE current_number_of_rows WHEN 0 THEN 1 ELSE current_number_of_rows END * 1.0)) * 100.0) AS [Percent of modifications]) AS Tab1([Percent of modifications])
   WHERE current_number_of_modified_rows_since_last_update >= 1000
   OR Tab1.[Percent of modifications] > 1
   UNION ALL

   --Number of stats with only 1 update since creation: <N>
   SELECT 'Number of stats with only 1 update since creation: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check1' AS more_info,
          '' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck1
   WHERE number_of_statistic_data_available_for_this_object = 1
   UNION ALL

   --Number of duplicated stats:
   SELECT 'Number of duplicated stats: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check28' AS more_info,
          'Drop all duplicated stats' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck28
   WHERE auto_created_stats_duplicated_comment <> 'OK'
   UNION ALL

   --Number of unused stats: <N>
   SELECT 'Number of unused stats: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check5' AS more_info,
          'Drop all unused stats' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck5
   UNION ALL

   --Number of missing multi-column stats: 
   SELECT 'Number of missing multi-column stats: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check40' AS more_info,
          'Create missing stats' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck40
   WHERE comment <> 'OK'
   UNION ALL

   --Number of multi-column stats auto created on SQL 2000: 
   SELECT 'Number of multi-column stats auto created on SQL 2000: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check41' AS more_info,
          'Recreate stats and remove cluster key cols' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck41
   UNION ALL

   --Number of missing column stats events from default trace: 
   SELECT 'Number of missing column stats events from default trace: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check40' AS more_info,
          'Review queries' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck40
   UNION ALL

   --Number of hypothetical stats:
   SELECT 'Number of hypothetical stats: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check35' AS more_info,
          'Drop all hypothetical stats' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck35
   UNION ALL

   --Number of stats with empty histograms: 
   SELECT 'Number of stats with empty histograms: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check31' AS more_info,
          'Update stat with fullscan or sample' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck31
   UNION ALL

   --Number of tables with more stats than columns: 
   SELECT 'Number of tables with more stats than columns: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check27' AS more_info,
          'Review tables and remove duplicated stats' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck27
   WHERE 1=1
   --AND number_of_statistics_in_this_table > 5 /* Only considering tables with more than 5 columns */
   AND number_of_statistics_comment <> 'OK'
   UNION ALL

   --Number of stats on LOBs (Large Objects) or BLOBs (Binary Large Objects): 
   SELECT 'Number of stats on LOBs (Large Objects) or BLOBs (Binary Large Objects): ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check26' AS more_info,
          'Review stats and if necessary enable NoRecompute' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck26
   UNION ALL

   --Number of stats set to NoRecompute:
   SELECT 'Number of stats set to NoRecompute: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check25' AS more_info,
          'Review stats to confirm this is expected and make sure you have a job updating it' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck25
   WHERE no_recompute_comment <> 'OK'
   UNION ALL

   --Number of stats using "persist sample percent": 
   SELECT 'Number of stats using "persist sample percent": ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check33' AS more_info,
          'Review stats to confirm this is expected' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck33
   WHERE persisted_sample_percent > 0
   UNION ALL

   --Number of stats updated with fullscan and not using "persist sample percent": 
   SELECT 'Number of stats updated with fullscan and not using "persist sample percent": ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check33' AS more_info,
          '' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck33
   WHERE [comment] LIKE 'Warning - Last update used FULLSCAN without has_persisted_sample%'
   UNION ALL

   --Number of stats updated with fullscan and set to use "persist sample percent": 
   SELECT 'Number of stats updated with fullscan and set to use "persist sample percent": ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check33' AS more_info,
          'Review stats to confirm this is expected' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck33
   WHERE [comment] LIKE 'Warning - Statistic is set to use persisted sample and last sample was 100%'
   UNION ALL

   --Number of stats using static auto-update threshold: <N>
   SELECT 'Number of stats using static auto-update threshold: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check7' AS more_info,
          '' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck7
   WHERE auto_update_threshold_type = 'Static'
   UNION ALL

   --Number of stats on partitioned tables not using incremental stats: 
   SELECT 'Number of stats on partitioned tables not using incremental stats: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check20' AS more_info,
          'Enable incremental stats' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck20
   WHERE comment LIKE 'Warning - Table is partitioned but statistic is not set to incremental%'
   UNION ALL

   --Number of partitioned tables that will have a sample rate in a index rebuild: 
   SELECT 'Number of partitioned tables that will have a sample rate in a index rebuild: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check37' AS more_info,
          '' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck37
   WHERE comment LIKE 'Warning - Alter index with rebuild on partitioned tables will use a default sampling rate%'
   UNION ALL

   --Number of ascending/descending stats: <N>
   SELECT 'Number of ascending/descending stats: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check10' AS more_info,
          'Review queries using statistic table' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck10
   UNION ALL

   --Number of filtered stats: <N>
   SELECT 'Number of filtered stats: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check14' AS more_info,
          '' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck14
   WHERE filter_definition IS NOT NULL
   UNION ALL

   --Number of good candidates for a filtered stats: <N>
   SELECT 'Number of good candidates for a filtered stats: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check14' AS more_info,
          'Create filtered statistic' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck14
   WHERE [comment_1] <> 'OK'
         OR [comment_2] <> 'OK'
   UNION ALL

   --Number of filtered stats considered out-of-date:
   SELECT 'Number of filtered stats considered out-of-date: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check16' AS more_info,
          'Update stat more often' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck16
   WHERE comment <> 'OK'
   UNION ALL

   --Number of stats with "skewed" histograms causing bad cardinality estimation: 
   SELECT 'Number of stats with "skewed" histograms causing bad cardinality estimation: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check15' AS more_info,
          'Update stat using a bigger percent sample' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck15
   WHERE [comment_1] <> 'OK'
   UNION ALL

   --Number of stats with a histogram step with a RANGE_ROWS greater than 50% of rows in the histogram: 
   SELECT 'Number of stats with a histogram step with a RANGE_ROWS greater than 50% of rows in the histogram: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check15' AS more_info,
          'Review queries using the statistic' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck15
   WHERE [range_rows_percent_from_total] >= 50
   UNION ALL

   --Number of stats with a histogram step with a EQ_ROWS greater than 50% of rows in the histogram: 
   SELECT 'Number of stats with a histogram step with a EQ_ROWS greater than 50% of rows in the histogram: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check15' AS more_info,
          'Review queries using the statistic' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck15
   WHERE [eq_rows_percent_from_total] >= 50
   UNION ALL

   --Number of stats with a histogram step indicating that a NULLs represents more than 10% of all rows in the histogram: 
   SELECT 'Number of stats with a histogram step indicating that a NULLs represents more than 10% of all rows in the histogram: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check15' AS more_info,
          '' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck15
   WHERE [comment_4] <> 'OK'
   UNION ALL

   --Number of stats with an unecessary(with no modifications) update: <N>
   SELECT 'Number of stats with an unecessary(with no modifications) update: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check2' AS more_info,
          'Review maintenance plan' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck2
   WHERE [comment_1] <> 'OK'
   UNION ALL

   --Number of stats with an update with interval of less than 15 minutes: <N>
   SELECT 'Number of stats with an event of update with interval of less than 15 minutes: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check2' AS more_info,
          '' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck2
   WHERE [comment_2] <> 'OK'
   UNION ALL

   --Number of stats with an event of update with interval greater than 25 hours: <N>
   SELECT 'Number of stats with an event of update with interval greater than 25 hours: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check2' AS more_info,
          'Review maintenance plan' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck2
   WHERE [comment_3] <> 'OK'
   UNION ALL

   --Number of stats with an estimated auto-update frequency of less than 1 hour: <N>
   SELECT 'Number of stats with an estimated auto-update frequency of less than 1 hour: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check2' AS more_info,
          '' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck2
   WHERE [comment_4] <> 'OK'
   UNION ALL

   --Number of stats with modifications greater than the average of all objs: <N>
   SELECT 'Number of stats with modifications greater than the average of all objs: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check6' AS more_info,
          '' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck6
   WHERE comment <> 'OK'
   UNION ALL

   --Number of stats with an auto-update about to be triggered: <N>
   SELECT 'Number of stats with an auto-update about to be triggered: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check7' AS more_info,
          '' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck7
   WHERE [comment_1] LIKE 'Warning - Auto update stats will be executed%'
   UNION ALL

   --Number of stats with modifications greater than auto-update threshold: <N>
   SELECT 'Number of stats with modifications greater than auto-update threshold: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check7' AS more_info,
          '' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck7
   WHERE [comment_1] LIKE 'Warning - Auto update stats will be executed on next execution of query using this statistic'
         OR [comment_1] LIKE 'Warning - AutoUpdateStats on DB is OFF, but statistic already hit the threshold to trigger auto update stats%'
   UNION ALL

   --Number of tables with more than 10mi rows: 
   SELECT 'Number of tables with more than 10mi rows: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(DISTINCT database_name + schema_name + table_name)) AS [result],
          'High' AS prioritycol,
          'Check29' AS more_info,
          'Create a maintenance plan to manage update stat on those tables' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck29
   WHERE [number_of_rows_comment] <> 'OK'
   UNION ALL

   --Number of long query plan compilation/optimization time due to an auto update/create stats: <N>
   SELECT 'Number of long query plan compilation/optimization time due to an auto update/create stats: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check3' AS more_info,
          'Review queries' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck3
   UNION ALL

   --Number of statistics with wrong metadata order: <N>
   SELECT 'Number of statistics with wrong metadata order: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check46' AS more_info,
          'Make sure you are not relying on sys.stats_column order' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck46
   UNION ALL

   --Number of tables with clustered ColumnStore indexes: <N>
   SELECT 'Number of tables with clustered ColumnStore indexes that may report wrong number of modifications: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(DISTINCT table_name)) AS [result],
          'Low' AS prioritycol,
          'Check48' AS more_info,
          'You may need to update some table stats using NoRecompute' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck48
   UNION ALL

   --Number of ColumnStore stats that won't be migrated in a DBCC CLONEDATABASE: <N>
   SELECT 'Number of ColumnStore stats that won''t be migrated in a DBCC CLONEDATABASE: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check49' AS more_info,
          'Use tiger''s team github script to close those stats' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck49
   UNION ALL

   --Number of sort spilled to tempdb due to an auto update/create stats: <N>
   SELECT 'Number of sort spilled to tempdb due to an auto update/create stats: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check4' AS more_info,
          'Avoid update stats for those tables on working hours' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck4
   WHERE [comment_1] LIKE 'Sort Warning was%'
   UNION ALL

   --Number of DBs with auto-update-stats disabled: 
   SELECT 'Number of DBs with auto-update-stats disabled: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check18' AS more_info,
          'Consider to enable auto update stats' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck18
   WHERE is_auto_update_stats_on = 0
   UNION ALL

   --Number of DBs with auto-create-stats disabled: 
   SELECT 'Number of DBs with auto-create-stats disabled: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check18' AS more_info,
          'Consider to enable auto create stats' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck18
   WHERE is_auto_create_stats_on = 0
   UNION ALL

   --Number of DBs using auto-create-stats-async: 
   SELECT 'Number of DBs using auto-create-stats-async: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check18' AS more_info,
          'Keep close eye on background jobs(background_job_error XE) and queue' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck18
   WHERE is_auto_update_stats_async_on = 1
   UNION ALL

   --Number of DBs not using auto-create-stats-async: 
   SELECT 'Number of DBs not using auto-create-stats-async: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check18' AS more_info,
          'Consider to enable auto create stats async' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck18
   WHERE is_auto_update_stats_async_on = 0
   UNION ALL

   --Number of DBs using auto-create-stats-async, but with auto-update stats disabled: 
   SELECT 'Number of DBs using auto-create-stats-async, but with auto-update stats disabled: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check18' AS more_info,
          'Enable auto update stats' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck18
   WHERE auto_update_stats_async_comment_3 <> 'OK'
   UNION ALL

   --Number of DBs using date correlation optimization: 
   SELECT 'Number of DBs using date correlation optimization: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check18' AS more_info,
          'Check if is realy being used and if not, disable it' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck18
   WHERE date_correlation_optimization_comment <> 'OK'
   UNION ALL

   --Number of DBs with partitioned tables, but not set to use incremental stats:
   SELECT 'Number of DBs with partitioned tables, but not set to use incremental stats: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check19' AS more_info,
          'Enable incremental stats' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck19
   WHERE auto_create_stats_incremental_comment LIKE 'Warning%'
   UNION ALL

   --Number of multi-column stats with a bad leading column:
   SELECT 'Number of multi-column stats with a bad leading column: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check50' AS more_info,
          'Recreate stat to use most selectivy column first' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck50

   UNION ALL

   --Number of indexed views referenced in a module without noexpand query hint: 
   SELECT 'Number of indexed views referenced in a module without noexpand query hint: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check17' AS more_info,
          'Consider noexpand to allow QO indexed view stat usage' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck17
   UNION ALL

   --Estimated number statistic loads per minute for remote (linked server, remote queries) requests: <N>
   SELECT 'Estimated number statistic loads per minute for remote(linked server, remote queries) requests: ' AS [info],
          CONVERT(VARCHAR(200), [executions_per_minute]),
          'Low' AS prioritycol,
          'Check45' AS more_info,
          'Check out for long compilations, mutex waits and net traffic' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck45
   UNION ALL

   --Number of auto-update stats async pending on background job queue: 
   SELECT 'Number of auto-update stats async pending on background job queue: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check21' AS more_info,
          'Make sure it is low' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck21
   UNION ALL

   --Number of modules manually running "update statistics": 
   SELECT 'Number of modules manually running "update statistics": ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check22' AS more_info,
          'Make sure this is really necessary' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck22
   UNION ALL

   --Number of plans with more than 50 loaded stats: 
   SELECT 'Number of plans with more than 50 loaded stats: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check52' AS more_info,
          'Review plans and check if compilation time is too high' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck52
   WHERE number_of_referenced_stats >= 50
   UNION ALL

   --Number of plans with loaded statistics with high modification count greater than 1k: 
   SELECT 'Number of plans with loaded statistics with high modification count greater than 1k: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check53' AS more_info,
          'Review plans and check if update stats is running for objs' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck53
   WHERE sum_modification_count_for_all_used_stats >= 1000
   UNION ALL

   --Number of tables with a anti-matter column: 
   SELECT 'Number of tables with a anti-matter column: ' AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check38' AS more_info,
          'Rebuild index' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck38
   UNION ALL

   --Found data from CommandLog table: Yes/No
   SELECT 'Found data from CommandLog table: ' AS [info],
          ISNULL(colyes, '0'),
          'Medium' AS prioritycol,
          'Check36' AS more_info,
          'If you''re using Ola''s script set LogToTable to Y' AS quick_fix
   FROM
   (
       VALUES
           (
               (
                   SELECT TOP 1
                          '1'
                   FROM tempdb.dbo.tmpStatisticCheck36
               )
           )
   ) AS t (colyes)
   UNION ALL

   --Adjust MAXDOP on "update statistics" command should be considered: Yes/No
   SELECT 'Adjust MAXDOP on "update statistics" command should be considered: ' AS [info],
          ISNULL(colyes, '0'),
          'Medium' AS prioritycol,
          'Check24' AS more_info,
          'Set MAXDOP on update stat command' AS quick_fix
   FROM
   (
       VALUES
           (
               (
                   SELECT TOP 1
                          '1'
                   FROM tempdb.dbo.tmpStatisticCheck24
                   WHERE comment LIKE 'Warning%'
               )
           )
   ) AS t (colyes)
   UNION ALL

   --Found a maintenance plan job? Yes/No
   SELECT 'Found a maintenance plan job? ' AS [info],
          ISNULL(colyes, '1'),
          'High' AS prioritycol,
          'Check43' AS more_info,
          'Make sure you''ve a maintenance plan' AS quick_fix
   FROM
   (
       VALUES
           (
               (
                   SELECT TOP 1
                          '0'
                   FROM tempdb.dbo.tmpStatisticCheck43
                   WHERE comment = 'OK'
               )
           )
   ) AS t (colyes)
   UNION ALL

   --Is there an extended event tracking Auto_Stats event? Yes/No
   SELECT 'Is there an extended event tracking Auto_Stats event? ' AS [info],
          ISNULL(colyes, '1'),
          'Low' AS prioritycol,
          'Check44' AS more_info,
          'Create a XE to track auto_stats' AS quick_fix
   FROM
   (
       VALUES
           (
               (
                   SELECT TOP 1
                          '0'
                   FROM tempdb.dbo.tmpStatisticCheck44
                   WHERE comment = 'OK'
               )
           )
   ) AS t (colyes)
   UNION ALL

   --Is it sp_GetStatisticInfo executed a few hours after the maintenance window? Yes/No
   SELECT 'Is it sp_GetStatisticInfo executed a few hours after the maintenance window? ' AS [info],
          ISNULL(colyes, '0'),
          'Low' AS prioritycol,
          'Check51' AS more_info,
          'Re-run sp sp_GetStatisticInfo in a diff time win' AS quick_fix
   FROM
   (
       VALUES
           (
               (
                   SELECT TOP 1
                          CASE 
                            WHEN DATEDIFF(HOUR, max_end_datetime, crdate) <= 4 THEN '1'
                          END
                   FROM tempdb.dbo.sysobjects
                   CROSS JOIN tempdb.dbo.tmpStatisticCheck51
                   WHERE name = 'tmpStatisticCheck53'
               )
           )
   ) AS t (colyes)
   UNION ALL

   --Trace flag 2371 usage should be considered: Yes/No
   SELECT 'Trace flag 2371 usage should be considered: ' AS [info],
          CASE
              WHEN EXISTS(SELECT DISTINCT comment FROM tempdb.dbo.tmpStatisticCheck9 WHERE comment <> 'OK') THEN
                  '0'
              ELSE
                  '1'
          END,
          'Medium' AS prioritycol,
          'Check9' AS more_info,
          '' AS quick_fix
   UNION ALL

   --Trace flag 4139 usage should be considered: Yes/No
   SELECT 'Trace flag 4139 usage should be considered: ' AS [info],
          ISNULL(colyes, '0'),
          'Medium' AS prioritycol,
          'Check11' AS more_info,
          '' AS quick_fix
   FROM
   (
       VALUES
           (
               (
                   SELECT TOP 1
                          '1'
                   FROM tempdb.dbo.tmpStatisticCheck11
                   WHERE comment LIKE '%Consider enabling TF4139%'
               )
           )
   ) AS t (colyes)
   UNION ALL

   --Trace flags 2389 and 2390 usage should be considered: Yes/No
   SELECT 'Trace flags 2389 and 2390 usage should be considered: ' AS [info],
          ISNULL(colyes, '0'),
          'Medium' AS prioritycol,
          'Check12' AS more_info,
          '' AS quick_fix
   FROM
   (
       VALUES
           (
               (
                   SELECT TOP 1
                          '1'
                   FROM tempdb.dbo.tmpStatisticCheck12
                   WHERE comment LIKE '%TF 2389 and 2390%'
               )
           )
   ) AS t (colyes)
   UNION ALL

   --Trace flag 4199 usage should be considered: Yes/No
   SELECT 'Trace flag 4199 usage should be considered: ' AS [info],
          CASE
              WHEN comment = 'OK' THEN
                  '0'
              ELSE
                  '1'
          END,
          'Medium' AS prioritycol,
          'Check13' AS more_info,
          'Enable TF 4199 or db scope config' AS quick_fix
   FROM tempdb.dbo.tmpStatisticCheck13
   UNION ALL

   --Trace flag 7471 usage should be considered: Yes/No
   SELECT 'Trace flag 7471 usage should be considered: ' AS [info],
          ISNULL(colyes, '0'),
          'Medium' AS prioritycol,
          'Check32' AS more_info,
          '' AS quick_fix
   FROM
   (
       VALUES
           (
               (
                   SELECT TOP 1
                          '1'
                   FROM tempdb.dbo.tmpStatisticCheck32
                   WHERE comment LIKE 'Warning%'
               )
           )
   ) AS t (colyes) )
SELECT *
INTO tempdb.dbo.tmpStatisticCheckSummary
FROM CTE_1;

SELECT *
FROM tempdb.dbo.tmpStatisticCheckSummary;
