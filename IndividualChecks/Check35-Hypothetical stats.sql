/*
Check35 - Hypothetical statistics
Description:
Check 35 - Check if there are hypothetical statistics created by DTA.
Hypothetical statistics are created by the Database Tuning Assistant (DTA) during its tests. If a DTA session was interrupted, these objects may not be deleted. 
Note: DTA can recommend to create multi-column statistics, so, it maybe ok to have statistics with "_dta_stat%" name, as they may be those suggested by DTA.
Note: "All statistics, views, partition functions, and partition schemes that Database Engine Tuning Advisor creates are real objects and cannot be distinguished from objects that existed prior to tuning." https://learn.microsoft.com/en-us/previous-versions/sql/sql-server-2005/ms190172(v=sql.90)?redirectedfrom=MSDN 
Estimated Benefit:
Medium
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Remove hypothetical statistics.
Detailed recommendation:
- It is recommended to drop these objects as soon as possible.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('dbo.tmpStatisticCheck35') IS NOT NULL
  DROP TABLE dbo.tmpStatisticCheck35

SELECT 'Check 35 - Check if there are hypothetical statistics created by DTA.' AS [info],
       a.database_name,
       a.table_name,
       a.stats_name,
       a.key_column_name,
       a.stat_all_columns,
       a.statistic_type,
       a.steps,
       a.current_number_of_rows,
       a.last_updated AS last_updated_datetime,
       t.[comment],
       'USE ' + a.database_name + '; BEGIN TRY SET LOCK_TIMEOUT 5; DROP STATISTICS '+ a.schema_name +'.'+ a.table_name +'.' + a.stats_name + '; END TRY BEGIN CATCH PRINT ''Error on ' + a.stats_name + '''; PRINT ERROR_MESSAGE() END CATCH;' AS drop_stat_command,
       dbcc_command
INTO dbo.tmpStatisticCheck35
FROM dbo.tmpStatisticCheck_stats AS a
CROSS APPLY (SELECT CASE
                      WHEN (a.stats_name LIKE '%_dta_stat%') AND (a.steps IS NOT NULL)
                      THEN 'Warning - It looks like this statistic was created/recommended by DTA. I would question its efficiency, I''d say this is probably causing more damage then helping. If I was you, I would probably drop it, but, you may want to test the queries to confirm it will not have a negative impact.'
                      /* 
                         If the name starts with _dta_stat but the number of steps is greater than 0, then it is a real stat, 
                         in that case, it may be useful and I don't want to recommend you to drop it, although I honestly think you should.
                      */
                      WHEN (a.stats_name LIKE '%_dta_stat%') AND (a.steps IS NULL)
                      THEN 'Warning - It looks like this is an hypothetical statistic. Hypothetical objects are created by the Database Tuning Assistant (DTA) during its tests. If a DTA session was interrupted, these indexes may not be deleted. It is recommended to drop these objects as soon as possible.'
                      ELSE 'OK'
                    END) AS t([comment])
WHERE t.comment <> 'OK'
AND statistic_type <> 'Index_Statistic'

SELECT * FROM dbo.tmpStatisticCheck35
ORDER BY [comment], 
         current_number_of_rows DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name