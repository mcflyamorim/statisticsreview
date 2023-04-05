/*
Check27 – Tables with more statistics than columns
Description:
Check 27 - Check if there are tables with more statistics than columns
Check if there are tables with more statistics than columns.
High number of statistics may lead to slow remote queries and longer maintenance plan executions.
Estimated Benefit:
High
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Review reported statistics and remove statistics not used/required.
Detailed recommendation:
- It may be ok to have a lot of statistics in a table, but it is definitely unusual to have more stats than columns in a table.
- Make sure all statistics are really needed and used.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck27') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck27

SELECT 'Check 27 - Check if there are tables with more statistics than columns' AS [info],
       a.database_name,
       a.table_name,
       a.stats_name,
       a.key_column_name,
       a.statistic_type,
       a.last_updated AS last_updated_datetime,
       a.current_number_of_rows,
       a.rows_sampled AS number_of_rows_sampled_on_last_update_create_statistic,
       a.number_of_statistics_in_this_table,
       a.number_of_columns_in_this_table,
       CASE 
         WHEN a.number_of_statistics_in_this_table > 5 /* Only considering tables with more than 5 columns */
             AND (a.number_of_statistics_in_this_table >= a.number_of_columns_in_this_table) THEN 
              'Warning - The number of statistics in this table is greater or equal to number of columns (' 
               + CONVERT(VARCHAR, a.number_of_columns_in_this_table) + 
               ') and it is very unlikely all statistics are usefull. High number of statistic may lead to slow remote queries and longer maintenance plan executions.'
         ELSE 'OK'
       END AS number_of_statistics_comment
INTO tempdb.dbo.tmpStatisticCheck27
FROM tempdb.dbo.tmp_stats a
WHERE a.current_number_of_rows > 0 /* Ignoring empty tables */

SELECT * FROM tempdb.dbo.tmpStatisticCheck27
ORDER BY number_of_statistics_in_this_table DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name