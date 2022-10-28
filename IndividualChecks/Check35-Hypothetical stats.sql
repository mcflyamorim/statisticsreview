/*
Check 35 - Check if there are hypothetical statistics created by DTA.

< ---------------- Description ----------------- >
Hypothetical indexes are created by the Database Tuning Assistant (DTA) during its tests. 
If a DTA session was interrupted, these indexes may not be deleted. 

< -------------- What to look for and recommendations -------------- >
- It is recommended to drop these objects as soon as possible.
*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck35') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck35

SELECT 'Check 35 - Check if there are hypothetical statistics created by DTA.' AS [info],
       a.database_name,
       a.table_name,
       a.stats_name,
       a.key_column_name,
       a.statistic_type,
       a.current_number_of_rows,
       a.last_updated AS last_updated_datetime,
       t.[comment],
       CASE 
         WHEN (a.stats_name LIKE '%_dta_stat%')
         THEN 'USE ' + a.database_name + '; BEGIN TRY SET LOCK_TIMEOUT 5; DROP STATISTICS '+ a.schema_name +'.'+ a.table_name +'.' + a.stats_name + '; END TRY BEGIN CATCH PRINT ''Error on ' + a.stats_name + '''; PRINT ERROR_MESSAGE() END CATCH;'
         ELSE NULL
       END AS drop_stat_command,
       dbcc_command
INTO tempdb.dbo.tmpStatisticCheck35
FROM tempdb.dbo.tmp_stats AS a
CROSS APPLY (SELECT CASE
                      WHEN (a.stats_name LIKE '%_dta_stat%')
                      THEN 'Warning - It looks like this is an hypothetical statistic. Hypothetical objects are created by the Database Tuning Assistant (DTA) during its tests. If a DTA session was interrupted, these indexes may not be deleted. It is recommended to drop these objects as soon as possible.'
                      ELSE 'OK'
                    END) AS t([comment])
WHERE t.comment <> 'OK'
AND statistic_type <> 'Index_Statistic'

SELECT * FROM tempdb.dbo.tmpStatisticCheck35
ORDER BY [comment], 
         current_number_of_rows DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name