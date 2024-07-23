/*
Check18 - Database settings
Description:
Check 18 - Database settings
This check is reviewing DB level statistics settings.
Estimated Benefit:
High
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Review best practices and recommendations for DB setting.
Detailed recommendation:
- AUTO_CREATE_STATISTICS and AUTO_UPDATE_STATISTICS: For a large majority of SQL Server installations, it is a best practice to use auto create and auto update statistics database-wide. Auto create and auto update statistics are on by default. If you observe bad plans and suspect that missing or out of date statistics are at fault, verify that auto create and auto update statistics are on.
- AUTO_UPDATE_STATISTICS_ASYNC: When the setting is off and a statistics update is initiated due to out-of-date statistics in the execution plan, the query must wait until the statistics update is complete before compiling and then returning the result set.  When the setting is on, the query does not need to wait as the statistics update are handled by a background process. The query will not get the benefit of the statistics update, however future queries will.
- - Consider using asynchronous statistics to achieve more predictable query response times for the following scenarios:
- - - - Your application frequently executes the same query, similar queries, or similar cached query plans. 
- - - - Your query response times might be more predictable with asynchronous statistics updates than with synchronous statistics updates because the query optimizer can execute incoming queries without waiting for up-to-date statistics. 
- - - - Your application has experienced client request time outs caused by one or more queries waiting for updated statistics. In some cases, waiting for synchronous statistics could cause applications with aggressive time outs to fail.
Note 1: Auto update stats async is SYNC when the current transaction holds a schema modification lock on the table referenced for statistics. I'll repeat, auto update will ALWAYS be synchronous in this case.

Note 2: Auto update stats async has a limit of 100 requests in the queue.

Note 3: Be careful with auto update stats async and statis using persisted sample, as the background task may take A LOT of time to run with a big persisted sample and cause statistics to be outdated.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('dbo.tmpStatisticCheck18') IS NOT NULL
  DROP TABLE dbo.tmpStatisticCheck18

SELECT 'Check 18 - Database seetings' AS [info], 
       database_name,
       is_auto_create_stats_on,
       CASE 
         WHEN is_auto_create_stats_on = 0 
         THEN 'Warning - Database ' + database_name + ' has auto-create-stats disabled. SQL Server uses statistics to build better execution plans, and without the ability to automatically create more, performance may suffer.'
         ELSE 'OK'
       END auto_create_stats_comment,
       is_auto_update_stats_on,
       CASE 
         WHEN is_auto_update_stats_on = 0 
         THEN 'Warning - Database ' + database_name + ' has auto-update-stats disabled. SQL Server uses statistics to build better execution plans, and without the ability to automatically update them, performance may suffer.'
         ELSE 'OK'
       END auto_update_stats_comment,
       is_auto_update_stats_async_on,
       CASE 
         WHEN is_auto_update_stats_async_on = 0
         THEN 'Information - Database ' + database_name + ' does not have auto-update-stats-async enabled. Consider to enable it to update statistics in the background and avoid high query plan creation time.'
         ELSE 'OK'
       END auto_update_stats_async_comment_1,
       CASE 
         WHEN is_auto_update_stats_async_on = 1 
         THEN 'Information - Database ' + database_name + ' has auto-update-stats-async enabled. When SQL Server gets a query for a table with out-of-date statistics, it will run the query with the stats it has - while updating stats to make later queries better. The initial run of the query may suffer, though.'
         ELSE 'OK'
       END auto_update_stats_async_comment_2,
       CASE 
         WHEN is_auto_update_stats_on = 0 AND is_auto_update_stats_async_on = 1
         THEN 'Warning - Database ' + database_name + ' have Auto_Update_Statistics_Asynchronously ENABLED while Auto_Update_Statistics is DISABLED. If asynch auto statistics update is intended, also enable Auto_Update_Statistics.'
         ELSE 'OK'
       END auto_update_stats_async_comment_3,
       is_date_correlation_on,
       CASE 
         WHEN is_date_correlation_on = 1
         THEN 'Warning - Database ' + database_name + ' has date correlation enabled. This is not a default setting, and it has some performance overhead. Very unlikely it is really being useful, check if indexed views it uses are there but not really being used. If there is date correlation, you may get better performance by beating developers to make them to specify implied date boundaries.'
         ELSE 'OK'
       END date_correlation_optimization_comment
INTO dbo.tmpStatisticCheck18
FROM (SELECT DISTINCT 
             database_name, 
             is_auto_update_stats_on, 
             is_auto_update_stats_async_on, 
             is_auto_create_stats_on, 
             is_auto_create_stats_incremental_on, 
             is_date_correlation_on 
      FROM dbo.tmpStatisticCheck_stats) AS a

SELECT * FROM dbo.tmpStatisticCheck18