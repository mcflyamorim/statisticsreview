/*
Check 41 - Check if there are auto created multi-column statistics

< ---------------- Description ----------------- >
Auto create statistics only creates single-column statistics, never multi-column statistics.
If auto created multi-column stats exists, it is very likely is being there since 
SQL2000 (when SQL used to do it).

< -------------- What to look for and recommendations -------------- >
- If those stats are taking too much time to update, it may be a good ideia to 
evaluate whether they are really needed.

- Capture auto_stats extended event for a few days and use the captured data to identify 
whether the stat is being loaded or not, if not then drop it.
*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck41') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck41

SELECT 'Check 41 - Check if there are auto created multi-column statistics' AS [info],
       a.database_name,
       a.table_name,
       a.stats_name,
       a.statistic_type,
       a.key_column_name,
       a.stat_all_columns,
       a.last_updated AS last_updated_datetime,
       a.current_number_of_rows,
       a.rows_sampled AS number_of_rows_sampled_on_last_update_create_statistic,
       a.number_of_statistics_in_this_table,
       'Warning - It looks like this auto created multi-column stats is being there since SQL2000 (when SQL used to do it). If those stats are taking too much time to update, it may be a good ideia to evaluate whether they are really needed.' AS comment_1,
       'USE ' + a.database_name + '; BEGIN TRY SET LOCK_TIMEOUT 5; DROP STATISTICS '+ a.schema_name +'.'+ a.table_name +'.' + a.stats_name + '; END TRY BEGIN CATCH PRINT ''Error on ' + a.stats_name + '''; PRINT ERROR_MESSAGE() END CATCH;' AS drop_stat_command
INTO tempdb.dbo.tmpStatisticCheck41
FROM tempdb.dbo.tmp_stats a
WHERE a.current_number_of_rows > 0 /* Ignoring empty tables */
AND a.statistic_type = 'Auto_Created'
AND a.stat_all_columns COLLATE Latin1_General_BIN2 LIKE '%,%'

SELECT * FROM tempdb.dbo.tmpStatisticCheck41
ORDER BY current_number_of_rows DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name