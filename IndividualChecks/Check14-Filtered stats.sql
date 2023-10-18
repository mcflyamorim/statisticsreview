/*
Check14 - Filtered statistics
Description:
Check 14 - Do I have filtered statistics?
Filtered statistics can improve query performance for queries that select from well-defined subsets of data. Well-designed filtered statistics can improve the query execution plan compared with full-table statistics. Filtered statistics are more accurate than full-table statistics because they cover only the rows in the filtered index.
Estimated Benefit:
Medium
Estimated Effort:
Very High
Recommendation:
Quick recommendation:
Consider to create filtered statistics to improve query performance.
Detailed recommendation:
- Filtered stats may don't play well with ad-hoc queries and constant values, for those cases, it may be necessary to use "WHERE Col = (Select 1)" to be able to avoid auto/forced param.
- Filtered stats may take a lot of time to auto-update, make sure you're not relying on it.
- Filtered stats may not be used due to parameter sniffing. May necessary to add OPTION (RECOMPILE) or use dynamic queries.
- Filtered statistics are not necessarily needed, but you should definitely consider them. Review application queries and validate the benefit of using it. Make sure that the performance gain for queries that a filtered statistic provides outweigh the additional maintenance for adding it to the database.

Note: All columns used in a filtered statistics predicate will have a referencing dependency, therefore, you will not be able to drop, rename, or alter the definition of a table column that is defined in a filtered statistics predicate.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck14') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck14

SELECT 
  'Check 14 - Do I have filtered statistics?' AS [info],
  a.database_name,  
  a.table_name, 
  a.stats_name, 
  a.key_column_name,
  a.key_column_data_type,
  a.statistic_type,
  a.plan_cache_reference_count,
  a.last_updated AS last_updated_datetime,
  a.current_number_of_rows,
  a.rows_sampled,
  a.steps AS number_of_steps_on_histogram,
  t.unique_values_on_key_column,
  a.number_of_rows_at_time_stat_was_updated,
  a.unfiltered_rows AS number_of_rows_on_table_at_time_statistics_was_updated_ignoring_filter,
  a.filter_definition,
  CASE 
    WHEN has_filter = 0 THEN 'Information - Statistic key is based in a column that suggests it may be a good candidate for a filtered statistic. ' + CASE WHEN (t.unique_values_on_key_column <= 20 AND current_number_of_rows >= 1000000) THEN '. Table has <= 20 unique values and >= 1mi rows.' ELSE '' END
    ELSE 'OK'
  END comment_1,
  CASE 
    WHEN steps <= 10 THEN 'Information - Statistic has less than or equal to 10 steps, this suggests it may be a good candidate for a filtered statistic'
    ELSE 'OK'
  END comment_2,
  a.dbcc_command
INTO tempdb.dbo.tmpStatisticCheck14
FROM tempdb.dbo.tmp_stats AS a
INNER JOIN tempdb.dbo.tmp_density_vector AS b
ON b.rowid = a.rowid
AND b.density_number = 1
CROSS APPLY (SELECT CONVERT(BIGINT, 1.0 / CASE b.all_density WHEN 0 THEN 1 ELSE b.all_density END)) AS t(unique_values_on_key_column)
WHERE steps IS NOT NULL
AND (has_filter = 1 
     OR (t.unique_values_on_key_column <= 20 AND a.current_number_of_rows >= 1000000) /*Table has only 20 unique values and more than 1mi rows*/
     OR a.key_column_data_type LIKE 'BIT%'
     OR a.key_column_data_type LIKE 'TINYINT%'
     OR a.key_column_name LIKE 'is%'
					OR a.key_column_name LIKE '%archive%'
					OR a.key_column_name LIKE '%active%'
					OR a.key_column_name LIKE '%flag%'
     OR a.key_column_name LIKE '%status%'
     OR a.key_column_name LIKE '%deleted%'
     OR a.key_column_name LIKE '%D_E_L_E_T_%'
     OR a.steps <= 10)

SELECT * FROM tempdb.dbo.tmpStatisticCheck14
ORDER BY current_number_of_rows DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name