/*
Check 34 - Check if there was an event of an auto update stat using a sample smaller than the last sample used

< ---------------- Description ----------------- >
A DBA can choose to manually update statistics by using a fixed sampling rate that can better represent the distribution of data.
However, a subsequent Automatic Update Statistics operation will reset back to the default sampling rate, and possibly introduce 
degradation of query plan efficiency.
  
In this check I'm returning all stats that have a diff in the number of steps, make sure you review all of those 
(not only the ones with a warning) to confirm you identified all the cases.
I understand that having more or less steps in a statistic object is not always  synonym of better key value coverage and 
estimations, but, that's a good indication we can use as a starting point to identify those full to sample issue.

Ideally, this check should be executed after the update stat maintenance, the longer the diff after the maintenance the
better the chances we capture histogram diff due to an auto update stat.
For instance, if the maintenance plan runs at 12AM, it would be nice to run this at  5PM to see if there was any auto 
update that caused histogram change during the day.

< -------------- What to look for and recommendations -------------- >
- If number of steps in the current statistic is different than the last update, then, check if the current statistic created 
is worse than the last one. 
To compare it, you can use DBCC SHOW_STATISTICS to see the existing histogram and open a new session, run an update statistic 
with fullscan and DBCC SHOW_STATISTICS again, then, compare the histograms and see if they're different. 

- PERSIST_SAMPLE_PERCENT command can be used to avoid this issue.
Starting with SQL Server 2016 (13.x) SP1 CU4 and 2017 CU1, you can use the PERSIST_SAMPLE_PERCENT option of CREATE STATISTICS 
or UPDATE STATISTICS, to set and retain a specific sampling percentage for subsequent statistic updates that do not 
explicitly specify a sampling percentage.

- Another option is to add a job to manually update the stats more frequently, or to recreate the 
stats with NO_RECOMPUTE and make sure you have your own job taking care of it.

- You may don't want to take any action now, but it may be a good idea to create a job to be notified if a auto-update 
run for an important table.

- Another option is to re-create the statistic using NoRecompute clause. That would avoid the
statistic to be updated with sample. But, make sure you've a maintenance plan taking care of those statistics.
*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck34') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck34

SELECT 'Check 34 - Check if there was an event of an auto update stat using a sample smaller than the last sample used' AS [info],
       a.database_name,
       a.table_name,
       a.stats_name,
       a.key_column_name,
       a.statistic_type,
       a.current_number_of_rows,
       a.current_number_of_modified_rows_since_last_update,
       Tab_StatSample1.number_of_modifications_on_key_column_since_previous_update,
       a.auto_update_threshold,
       a.auto_update_threshold_type,
       CONVERT(DECIMAL(25, 2), (a.current_number_of_modified_rows_since_last_update / (a.auto_update_threshold * 1.0)) * 100.0) AS percent_of_threshold,
       a.number_of_rows_at_time_stat_was_updated AS [Number of rows on table at time statistic was updated 1 - most recent],
       Tab_StatSample2.number_of_rows_at_time_stat_was_updated AS  [Number of rows on table at time statistic was updated - 2 - previous update],
       a.rows_sampled AS number_of_rows_sampled_on_last_update_create_statistic,
       a.statistic_percent_sampled,
       a.steps AS number_of_steps_on_histogram_1_most_recent,
       Tab_StatSample2.steps AS number_of_steps_on_histogram_2_previous_update,
       a.last_updated AS update_stat_1_most_recent_datetime,
       Tab_StatSample2.last_updated AS update_stat_2_previous_update_datetime,
       steps_diff_pct,
       CASE
         WHEN 
          (steps_diff_pct < 90) 
          /*Only considering stats where number of steps diff is at least 90%*/
          AND (Tab_StatSample1.number_of_modifications_on_key_column_since_previous_update < 1000000) 
          /*Checking if number of modifications is lower than 1mi, because, if number of modifications
            is greater than 1mi, it may be the reason of why number of steps changed.
            If number of modifications is low and steps is diff, then it is very likely it changed because
            of an update with a lower sample*/
         THEN 'Warning - Number of steps on last update stats is greater than the last update stats. This may indicate that stat was updated with a lower sample.'
         ELSE 'OK'
       END AS [comment],
       dbcc_command
INTO tempdb.dbo.tmpStatisticCheck34
FROM tempdb.dbo.tmp_stats AS a
CROSS APPLY (SELECT CASE 
                      WHEN b.inserts_since_last_update IS NULL AND b.deletes_since_last_update IS NULL
                      THEN NULL
                      ELSE (ABS(ISNULL(b.inserts_since_last_update,0)) + ABS(ISNULL(b.deletes_since_last_update,0)))
                    END AS number_of_modifications_on_key_column_since_previous_update
                FROM tempdb.dbo.tmp_exec_history b 
               WHERE b.rowid = a.rowid
                 AND b.history_number = 1
                ) AS Tab_StatSample1
CROSS APPLY (SELECT b.table_cardinality AS number_of_rows_at_time_stat_was_updated,
                    b.updated as last_updated,
                    b.steps
                FROM tempdb.dbo.tmp_exec_history b 
               WHERE b.rowid = a.rowid
                 AND b.history_number = 2 /* Previous update stat sample */
                ) AS Tab_StatSample2
CROSS APPLY (SELECT CAST((a.steps / (Tab_StatSample2.steps * 1.00)) * 100.0 AS DECIMAL(25, 2))) AS t(steps_diff_pct)
WHERE a.statistic_percent_sampled <> 100 /*Only considering stats not using FULLSCAN*/
AND Tab_StatSample2.steps <> 1 /*Ignoring histograms with only 1 step*/
AND a.steps <> Tab_StatSample2.steps /*Only cases where number of steps is diff*/

SELECT * FROM tempdb.dbo.tmpStatisticCheck34
ORDER BY steps_diff_pct ASC, 
         current_number_of_rows DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name


/*
-- Script to show issue

USE Northwind
GO
-- 6 secs to run
IF OBJECT_ID('OrdersBig') IS NOT NULL
  DROP TABLE OrdersBig
GO
SELECT TOP 1000000
       IDENTITY(Int, 1,1) AS OrderID,
       ABS(CheckSUM(NEWID()) / 10000) AS CustomerID,
       CONVERT(Date, GETDATE() - (CheckSUM(NEWID()) / 1000000)) AS OrderDate,
       ISNULL(ABS(CONVERT(Numeric(18,2), (CheckSUM(NEWID()) / 1000000.5))),0) AS Value
  INTO OrdersBig
  FROM Orders A
 CROSS JOIN Orders B CROSS JOIN Orders C CROSS JOIN Orders D
GO
ALTER TABLE OrdersBig ADD CONSTRAINT xpk_OrdersBig PRIMARY KEY(OrderID)
GO

-- This will trigger auto-create stats on column Value
SELECT * 
FROM OrdersBig
WHERE Value <= 1
AND 1 = (SELECT 1)
GO

sp_helpstats OrdersBig
GO

-- Rows Sampled = 316540
DBCC SHOW_STATISTICS (OrdersBig, _WA_Sys_00000004_32C16125)
GO

UPDATE STATISTICS OrdersBig WITH FULLSCAN
GO

-- Rows Sampled = 1000000
DBCC SHOW_STATISTICS (OrdersBig, _WA_Sys_00000004_32C16125)
GO


UPDATE TOP(700000) OrdersBig SET Value = Value
GO


-- This will trigger auto-update stats on column Value
SELECT * 
FROM OrdersBig
WHERE Value <= 1
AND 1 = (SELECT 1)
GO


-- Back to sampled stats
-- Rows Sampled = 316540
DBCC SHOW_STATISTICS (OrdersBig, _WA_Sys_00000004_32C16125)
GO
*/