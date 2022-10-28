/*
Check 37 - Check if table is partitioned and warn that alter index rebuild will update stats with default sampling rate.

< ---------------- Description ----------------- >
SQL Server 2012 changed the auto update sample behavior for partitioned table.

This change was made because SQL started to support large number of partitions (up to 15000) by default.
With partitioned table, ALTER INDEX REBUILD actually first rebuilds index and then do a sample 
scan to update stats in order to reduce memory consumption.

< -------------- What to look for and recommendations -------------- >
- If a table is partitioned, ALTER INDEX REBUILD will only update statistics 
for that index with default sampling rate. 
In other words, it is no longer a FULLSCAN, if you want fullscan, 
you will need to run UPDATE STATISTCS WITH FULLSCAN.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck37') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck37

IF EXISTS(SELECT * FROM tempdb.dbo.tmp_stats AS a WHERE a.is_table_partitioned = 1)
BEGIN
  SELECT 
    'Check 37 - Check if table is partitioned and warn that alter index rebuild will update stats with default sampling rate.' AS [info],
    a.database_name,
    a.table_name,
    a.stats_name,
    a.key_column_name,
    a.statistic_type,
    a.is_table_partitioned,
    a.last_updated AS last_updated_datetime,
    a.current_number_of_rows,
    a.rows_sampled AS number_of_rows_sampled_on_last_update_create_statistic,
    a.statistic_percent_sampled,
    'Warning - Alter index with rebuild on partitioned tables will use a default sampling rate. If possible, make sure you have a update stats with FULLSCAN.' AS [comment]
  INTO tempdb.dbo.tmpStatisticCheck37
  FROM tempdb.dbo.tmp_stats AS a
  WHERE a.is_table_partitioned = 1
  ORDER BY a.current_number_of_rows DESC, 
           database_name,
           table_name,
           key_column_name,
           stats_name
END
ELSE
BEGIN
  SELECT 
    'Check 37 - Check if table is partitioned and warn that alter index rebuild will update stats with default sampling rate.' AS [info],
    'There are no partitioned tables, check is not relevant.' AS [comment]
  INTO tempdb.dbo.tmpStatisticCheck37
END

SELECT * FROM tempdb.dbo.tmpStatisticCheck37