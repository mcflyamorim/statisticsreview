/*
Check28 - Duplicated statistics
Description:
Check 28 - Check if there are duplicated statistics
Statistic is considered duplicated has it already has another Index_Statistic on the same key (or keys as long as leading is the same) column(s) and filter_definition.
It is a best practice to have one statistic on each column or combination of columns. 
Duplicate statistics will increase query plan creation time as query optimizer will spend more time deciding which statistics to use.
If you have multiple statistics, QO will pick the one with a biggest sample, if they all have the same sampled number of rows, it will pick the most recent. 
Remove duplicated statistics, will also help to speed up the time to run a maintenance plan. (assuming the duplicated stat is being updated)
Estimated Benefit:
High
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Remove all duplicated statistics.
Detailed recommendation:
- Consider to drop the duplicated statistic. The only case I see this would be a problem is that when the auto-created has a better histogram/stat than the index stats. But, as long as you're updating the index stats, we should be good to drop the auto-created one.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck28') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck28

SELECT 'Check 28 - Check if there are duplicated statistics' AS [info],
       a.database_name,
       a.table_name,
       a.stats_name,
       a.key_column_name,
       a.stat_all_columns,
       a.statistic_type,
       a.filter_definition,
       a.last_updated AS last_updated_datetime,
       a.current_number_of_rows,
       a.rows_sampled AS number_of_rows_sampled_on_last_update_create_statistic,
       CASE 
         WHEN (SELECT COUNT(*) 
                 FROM tempdb.dbo.tmp_stats AS b
                WHERE b.database_id = a.database_id
                  AND b.object_id = a.object_id
                  AND b.stat_all_columns LIKE a.stat_all_columns + '%'
                  AND b.statistic_type = 'Index_Statistic'
                  AND ISNULL(b.filter_definition,'') = ISNULL(a.filter_definition,'')
                  AND a.statistic_type IN ('Auto_Created', 'User_Created')
                  AND b.stats_name <> a.stats_name) > 0 THEN 
                'Warning - This statistic is duplicated has it already has another Index_Statistic on this key column(s) ([' + a.stat_all_columns + ']). Consider to drop it.'
         ELSE 'OK'
       END AS auto_created_stats_duplicated_comment,
       'USE ' + a.database_name + '; BEGIN TRY SET LOCK_TIMEOUT 5; DROP STATISTICS '+ a.schema_name +'.'+ a.table_name +'.' + a.stats_name + '; END TRY BEGIN CATCH PRINT ''Error on ' + a.stats_name + '''; PRINT ERROR_MESSAGE() END CATCH;' AS drop_stat_command,
       dbcc_command
INTO tempdb.dbo.tmpStatisticCheck28
FROM tempdb.dbo.tmp_stats a
WHERE a.current_number_of_rows > 0 /* Ignoring empty tables */

SELECT * FROM tempdb.dbo.tmpStatisticCheck28
ORDER BY current_number_of_rows DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name