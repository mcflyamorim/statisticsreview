/*
Check 31 - Check if there are empty histograms

< ---------------- Description ----------------- >
Check if there are statistic but no histogram.

This can lead to poor cardinality estimations and weird situations 
as queries that require the empty statistic, will show [Columns With No Statistics] 
warning on execution plans, even with auto create/update statistic enabled.

Statistics that have not been updated since the database was restored or upgraded can 
have an empty histogram.

Note: Columnstore indexes will have an empty histogram, I'm ignoring those.
In Columnstore Indexes a unit of any operation is a segment that already have min/max values. 
QO will use those to decide if one particular segment should be scanned or not.

< -------------- What to look for and recommendations -------------- >
- Run DBCC SHOW_STATISTICS command to confirm stat exist and is empty.

- If a statistic exist with an empty histogram, queries using this table will have poor 
cardinality estimates and show [Columns With No Statistics] warning on execution plans.
Warning is only displayed with legacy CE.

Remove this statistic, or update it with fullscan or sample.
*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck31') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck31

SELECT 'Check 31 - Check if there are empty histograms' AS [info],
       database_name,
       schema_name,
       table_name,
       stats_name,
       table_index_base_type,
       index_type,
       key_column_name,
       statistic_type,
       is_unique,
       no_recompute AS is_no_recompute,
       filter_definition,
       last_updated AS last_updated_datetime,
       current_number_of_rows,
       rows_sampled AS number_of_rows_sampled_on_last_update_create_statistic,
       statistic_percent_sampled,
       steps AS number_of_steps_on_histogram,
       CASE 
         WHEN steps IS NULL
              AND current_number_of_rows > 0
           THEN 'Warning - The statistic exists, but there is no histogram, this will lead to poor cardinality estimates and show [Columns With No Statistics] warning on execution plans. Remove this statistic, or update it with fullscan or sample.'
         ELSE 'OK'
       END comment_1,
       'USE ' + database_name + '; BEGIN TRY SET LOCK_TIMEOUT 5; DROP STATISTICS '+ schema_name +'.'+ table_name +'.' + stats_name + '; END TRY BEGIN CATCH PRINT ''Error on ' + stats_name + '''; PRINT ERROR_MESSAGE() END CATCH;' AS drop_stat_command,
       dbcc_command
INTO tempdb.dbo.tmpStatisticCheck31
FROM tempdb.dbo.tmp_stats
WHERE current_number_of_rows > 0 /* Ignoring empty tables */
  AND ISNULL(steps, 0) = 0
  AND index_type NOT LIKE '%COLUMNSTORE%'

SELECT * FROM tempdb.dbo.tmpStatisticCheck31
ORDER BY number_of_steps_on_histogram ASC, 
         database_name,
         table_name,
         key_column_name,
         stats_name