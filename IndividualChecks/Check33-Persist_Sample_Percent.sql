/*
Check33 - Statistics using Persist_Sample_Percent
Description:
Check 33 - Check if PERSIST_SAMPLE_PERCENT is being used.
Check if there are statistics using PERSIST_SAMPLE_PERCENT option of set to retain a specific sampling percentage for subsequent statistic updates that do not explicitly specify a sampling percentage.
Statistics using PERSIST_SAMPLE_PERCENT will retain value specified on PERSIST_SAMPLE_PERCENT.
If an user set it to 100, an auto-update stats may trigger an unexpected update using fullscan, which depending on the table size, can take A LOT of time to run.
Estimated Benefit:
Medium
Estimated Effort:
Medium
Recommendation:
Quick recommendation:
Only use persist_sample_percent option when there is a demonstrated/documented need.
Detailed recommendation:
- Make sure you're using a value that is not too high, or, if is too high, make sure you're aware and ok to pay for the extra cost it will have to update the stat with fullscan. Remember that this will make query plan compilation to take a lot more time to complete.
- There are several cases where this value is reset to default, look at following notes and make sure you're PERSIST_SAMPLE_PERCENT is used as you expect it to be. It may be necessary to create your own job to reset this to use PERSIST_SAMPLE_PERCENT to confirm it is really being used.
Note 1: PERSIST_SAMPLE_PERCENT is only available on SQL Server 2016 SP1 CU4, 2017 CU1, and higher builds, but, column has_persisted_sample sys.stats is only available on SQL2019.

Note 2: Keep in mind that statistics updated using persisted_sample_percent=on will be reset back to default after an index rebuild. So, it is "persisted", but not really persisted.

Note 3: When the index creation or rebuild operation is resumable, statistics are created or updated with the default sampling ratio. If statistics were created or last updated with the PERSIST_SAMPLE_PERCENT clause set to ON, resumable index operations use the persisted sampling ratio to create or update statistics.

Note 4: In SQL Server, when rebuilding an index which previously had statistics updated with PERSIST_SAMPLE_PERCENT, the persisted sample percent is reset back to default. Starting with SQL Server 2016 (13.x) SP2 CU17, SQL Server 2017 (14.x) CU26, and SQL Server 2019 (15.x) CU10, the persisted sample percent is kept even when rebuilding an index.

Note 5: If the table is truncated, all statistics built on the truncated HoBT will revert to using the default sampling percentage.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck33') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck33

DECLARE @sqlmajorver INT, @sqlminorver INT, @sqlbuild INT
SELECT @sqlmajorver = CONVERT(INT, (@@microsoftversion / 0x1000000) & 0xff),
	      @sqlminorver = CONVERT(INT, (@@microsoftversion / 0x10000) & 0xff),
 	     @sqlbuild = CONVERT(INT, @@microsoftversion & 0xffff);

IF (@sqlmajorver >= 13 /*SQL2016*/)
BEGIN
  SELECT 'Check 33 - Check if PERSIST_SAMPLE_PERCENT is being used.' AS [info],
         a.database_name,
         a.table_name,
         a.stats_name,
         a.key_column_name,
         a.statistic_type,
         a.plan_cache_reference_count,
         a.last_updated AS last_updated_datetime,
         a.current_number_of_rows,
         a.rows_sampled AS number_of_rows_sampled_on_last_update_create_statistic,
         a.statistic_percent_sampled,
         b.persisted_sample_percent,
         CASE
           WHEN (a.statistic_percent_sampled = 100)
            AND (a.is_auto_update_stats_on = 1) 
            AND (b.persisted_sample_percent = 0)
           THEN 'Warning - Last update used FULLSCAN without has_persisted_sample. Since auto update stats is ON for this DB, if an auto update stats runs, it will reset stat back to the default sampling rate, and possibly introduce degradation of query plan efficiency.'
           WHEN (a.statistic_percent_sampled = 100)
            AND (a.is_auto_update_stats_on = 1)
            AND (b.persisted_sample_percent > 0)
           THEN 'Warning - Statistic is set to use persisted sample and last sample was 100% (FULLSCAN). An auto-update stats may trigger an unexpected update using fullscan, which can take A LOT of time to run and will increase query plan compilation time.'
           WHEN (b.persisted_sample_percent > 0)
           THEN 'Information - Statistic is set to use persisted sample. Please review the last update percent sample and make sure this is using an expected value. Update stats with large samples may take a lof ot time to run.'
           ELSE 'OK'
         END AS [comment],
         CASE
           WHEN b.persisted_sample_percent = 0
           THEN 'UPDATE STATISTICS ' + a.database_name + '.' + a.schema_name + '.' + a.table_name + ' ' + a.stats_name + 
                ' WITH PERSIST_SAMPLE_PERCENT = ON, /*FULLSCAN*/ /*SAMPLE <n> PERCENT*/;'
           ELSE NULL
         END AS command_to_enable_persisted_sample,
         CASE
           WHEN b.persisted_sample_percent > 0
           THEN 'UPDATE STATISTICS ' + a.database_name + '.' + a.schema_name + '.' + a.table_name + ' ' + a.stats_name + 
                ' WITH SAMPLE 0 PERCENT, PERSIST_SAMPLE_PERCENT = OFF; ' + 
                'UPDATE STATISTICS '  + a.database_name + '.' + a.schema_name + '.' + a.table_name + ' ' + a.stats_name + ' WITH SAMPLE;'
           ELSE NULL
         END AS command_to_disable_persisted_sample,
         dbcc_command
  INTO tempdb.dbo.tmpStatisticCheck33
  FROM tempdb.dbo.tmp_stats AS a
  INNER JOIN tempdb.dbo.tmp_stat_header AS b
  ON b.rowid = a.rowid
  ORDER BY a.current_number_of_rows DESC, 
           a.database_name,
           a.table_name,
           a.key_column_name,
           a.stats_name
END
ELSE
BEGIN
  SELECT 'Check 33 - Check if PERSIST_SAMPLE_PERCENT is being used.' AS [info], 
         'Check is not relevant on this SQL version as PERSIST_SAMPLE_PERCENT is availabe on SQL Server 2016 SP1 CU4, 2017 CU1, and higher builds, but, column has_persisted_sample sys.stats is only available on SQL2019 :-( ...' AS [Auto create stats incremental comment],
         0 AS current_number_of_rows,
         0 AS persisted_sample_percent,
         '' AS [comment]
  INTO tempdb.dbo.tmpStatisticCheck33
END

SELECT * FROM tempdb.dbo.tmpStatisticCheck33
ORDER BY current_number_of_rows DESC