/* 
Check 1 - Do we have statistics with useful history?

< ---------------- Description ----------------- >
By useful I mean statistics with information about at least more than 1 update. 
If I have only 1 update, then, most of counters/checks won't be available, like, 
number of inserted rows since previous update and etc.
Every statistic saves information up to last 4 update stats, those are accessible via TF2388 and DBCC SHOW_STATISTICS.
This query will return all stats and number of statistics sample available, 
this also returns number of rows in the table because if number of rows is small, 
you may (I'm not saying you shouldn't, it depends) don't care about this object.

< -------------- What to look for and recommendations -------------- >
- Ideally result for [Number of statistic data available for this object] should be 4, 
but, the stats history info is reset in a index rebuild, so, it may be ok to have
1 info as index may be recently rebuild.

- Rows with [Number of statistic data available for this object] less than 4, 
check Statistic_Updated column for more Information about last time statistic was updated. 
If it is too old, this may indicate stat is not being used, or worse, it is being used but is out-of-date.

- Rows with [Number of statistic data available for this object] equal to 1, 
may indicate recently auto-created statistics. Remember, auto-created stats will use default sample option, 
depending on the data distribution, you may want to update it with a higher sample to get a better histogram.

Note: If statistic is updated recently (check [HoursSinceLastUpdate] column), it maybe not a issue to have only 1 or 2 stat sample 
as it may be a newly created stat that didn't got 4 updates yet.
*/

/*
  Fabiano Amorim
  http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
*/ 

USE [master];

SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck1') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck1

SELECT 'Check 1 - Do we have statistics with useful history?' AS [info],
       a.database_name,
       a.schema_name,
       a.table_name,
       a.stats_name,
       a.key_column_name,
       a.current_number_of_rows AS current_number_of_rows_table,
       a.last_updated AS last_updated_datetime,
       DATEDIFF(HOUR, a.last_updated, GETDATE()) AS hours_since_last_update,
       (SELECT COUNT(*) FROM tempdb.dbo.tmp_exec_history b 
         WHERE b.rowid = a.rowid
       ) AS number_of_statistic_data_available_for_this_object,
       CASE 
         WHEN ((SELECT COUNT(*) FROM tempdb.dbo.tmp_exec_history b 
                WHERE b.rowid = a.rowid)) < 4
         THEN 'Warning - This statistic had less than 4 updates since it was created. This will limit the results of other checks and may indicate update stats for this obj. is not running'
         ELSE 'OK'
       END AS comment_1,
       a.dbcc_command
INTO tempdb.dbo.tmpStatisticCheck1
FROM tempdb.dbo.tmp_stats a
WHERE a.current_number_of_rows > 0 /* Ignoring empty tables */
GROUP BY a.rowid,
         a.database_name,
         a.schema_name,
         a.table_name,
         a.stats_name,
         a.key_column_name,
         a.current_number_of_rows,
         a.last_updated,
         a.dbcc_command

SELECT * FROM tempdb.dbo.tmpStatisticCheck1
ORDER BY number_of_statistic_data_available_for_this_object ASC, 
         current_number_of_rows_table DESC, 
         database_name,
         schema_name,
         table_name,
         key_column_name,
         stats_name
