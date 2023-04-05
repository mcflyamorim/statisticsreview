/*
Check26 - Statistics on LOB columns
Description:
Check 26 - Check statistic key column with large value types.
Check if there are statistics on LOB columns. The LOBs (Large Objects) can be broadly classified as Character Large Objects (CLOBs) or Binary Large Objects (BLOBs).
Tables that are smaller than 8MB (1024 pages) are always fully scanned to update/create statistics. But, SQL only consider in-row data, that is, all data types except LOB data types. Because the number of LOB pages is not directly considered the sample percentage remain a large value and the first and last 100 bytes are retrieved it can trigger a large LOB scan operation.
Estimated Benefit:
Very High
Estimated Effort:
Medium
Recommendation:
Quick recommendation:
Review reported statistics set it to NoRecompute and create a job to manually update them.
Detailed recommendation:
- Statistics creation/update on LOB columns may take a lot of time to run. I'd recommend to re-create them as NoRecompute and create a job to manually update them. The idea is to avoid auto-update on those stats as it may take a while to run.
- Check on Command log (considering it exists) how much time it is taking to update the stat.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck26') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck26

SELECT 'Check 26 - Check statistic key column with large value types.' AS [info],
       a.database_name,
       a.table_name,
       a.stats_name,
       a.key_column_name,
       a.key_column_data_type,
       b.string_index,
       a.last_updated AS last_updated_datetime,
       a.current_number_of_rows,
       number_of_in_row_data_pages_on_table,
       CONVERT(NUMERIC(25, 2), (number_of_in_row_data_pages_on_table * 8) / 1024.) AS in_row_data_size_in_mb,
       number_of_lob_data_pages_on_table,
       CONVERT(NUMERIC(25, 2), (number_of_lob_data_pages_on_table * 8) / 1024.) AS lob_data_size_in_mb,
       a.rows_sampled AS number_of_rows_sampled_on_last_update_create_statistic,
       a.statistic_percent_sampled,
       a.is_lob,
       CASE 
         WHEN a.is_lob = 1 THEN 
              'Warning - Statistic key column with large value types. Statsistic creation/update on LOB columns may take a lot of time to run.'
         ELSE 'OK'
       END AS statistic_on_large_value_type_comment,
       dbcc_command
INTO tempdb.dbo.tmpStatisticCheck26
FROM tempdb.dbo.tmp_stats a
INNER JOIN tempdb.dbo.tmp_stat_header b
ON b.rowid = a.rowid
WHERE a.current_number_of_rows > 0 /* Ignoring empty tables */
  AND a.is_lob = 1

SELECT * FROM tempdb.dbo.tmpStatisticCheck26
ORDER BY number_of_in_row_data_pages_on_table DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name