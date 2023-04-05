/* 
Check46 - Bad internal statistic order
Description:
Check 46 - Check if there are statistics with wrong metadata order
BOL says that column "stats_column_id" of DMV sys.stats_column is "1-based ordinal within set of stats columns.". However, for index statistics this is not true as it actually reflects table definition order NOT the index order.
Luckily, this doesn't cause a query optimizer problem as the statistic is created based on the index key column, and not the statistic key column.
But, you may see a wrong order on SSMS user interface as it is reading the info from the sys.stats_column DMV.
Note: https://dba.stackexchange.com/questions/94533/is-sys-stats-columns-incorrect
Estimated Benefit:
Low
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Don’t use sys.stats_column to read statistic column order.
Detailed recommendation:
- Don't rely on SSMS to check a statistic info. 
- If you need to read an index statistic info, don't use sys.stats_column DMV, instead, use the DMV sys.index_columns and sort the results by key_ordinal column. The column key_ordinal in the sys.index_columns table is the order in which the columns are stored in the index. There isn't a key_ordinal column for the sys.stats_columns table. The column stats_column_id just replicates the index_column_id column of the object it references.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck46') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck46

SELECT 'Check 46 - Check if there are statistics with wrong metadata order' AS [info],
       database_name,
       table_name,
       stats_name,
       index_type,
       statistic_type,
       key_column_name,
       stat_all_columns_index_order, 
       stat_all_columns_stat_order,
       last_updated AS last_updated_datetime,
       current_number_of_rows,
       dbcc_command
INTO tempdb.dbo.tmpStatisticCheck46
FROM tempdb.dbo.tmp_stats
WHERE stat_all_columns_index_order <> stat_all_columns_stat_order

SELECT * FROM tempdb.dbo.tmpStatisticCheck46
ORDER BY current_number_of_rows DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name