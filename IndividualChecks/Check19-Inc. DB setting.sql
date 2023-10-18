/*
Check19 - Incremental database setting
Description:
Check 19 - Check if incremental setting on DB should be set to ON
The default value is OFF, which means stats are combined for all partitions. When ON, the statistics are created and updated per partition whenever incremental stats are supported. When new partitions are added to a large table, statistics should be updated to include the new partitions. However, the time required to scan the entire table (FULLSCAN or SAMPLE option) might be quite long.  Also, scanning the entire table isn't necessary because only the statistics on the new partitions might be needed. 
If you assume that only data in the most recent partition is changing, then ideally you only update statistics for that partition. You can do this now with incremental statistics, and what happens is that information is then merged back into the main histogram. The histogram for the entire table will update without having to read through the entire table to update statistics, and this can help with performance of your maintenance tasks.
The other valuable point is that the percentage of data changes required to trigger the automatic update of statistics, 20% of rows changed, will be applied now at the partition level.
The query optimizer still just uses the main histogram that represents the entire table. 
Note: This is not a statistic/histogram per partition, QO doesn't use this to get information about each partition. It is used to provide a performance benefit when managing statistics for partitioned tables. If statistics only need to be updated for select partitions, just those can be updated. The new information is then merged into the table-level histogram, providing the optimizer more current information, without the cost of reading the entire table.
Estimated Benefit:
High
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Enable incremental statistics at DB level.
Detailed recommendation:
- If there are partitioned tables, consider to enable it.
- Applies to: SQL Server 2014 (12.x) and higher builds.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck19') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck19

DECLARE @sqlmajorver INT, @sqlminorver int, @sqlbuild int
SELECT @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff)
IF @sqlmajorver >= 13 /*SQL2014*/
BEGIN
  EXEC ('
  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SELECT ''Check 19 - Check if incremental setting on DB should be set to ON'' AS [info], 
         [name] AS database_name,
         is_auto_create_stats_incremental_on,
         CASE 
           WHEN (is_auto_create_stats_incremental_on = 0)
            AND EXISTS(SELECT DISTINCT database_id 
                         FROM tempdb.dbo.tmp_stats 
                        WHERE tmp_stats.is_table_partitioned = 1
                          AND tmp_stats.database_id = databases.database_id)
           THEN ''Warning - Database ['' + [name] + ''] has partitioned tables and auto-incremental-stats is disabled. Consider enabling it to allow SQL created and update stats per partition.''
           WHEN NOT EXISTS(SELECT DISTINCT database_id 
                             FROM tempdb.dbo.tmp_stats 
                            WHERE tmp_stats.is_table_partitioned = 1
                              AND tmp_stats.database_id = databases.database_id)
           THEN ''Information - Database ['' + [name] + ''] does not have partitioned tables, check is not relevant.''
           ELSE ''OK''
         END [auto_create_stats_incremental_comment]
  INTO tempdb.dbo.tmpStatisticCheck19
  FROM sys.databases
  WHERE database_id in (SELECT DISTINCT database_id 
                          FROM tempdb.dbo.tmp_stats)
  ')
END
ELSE
BEGIN
  SELECT 'Check 19 - Check if incremental setting on DB should be set to ON' AS [info], 
         'Check is not relevant on this SQL version as Incremental stats only applies to SQL Server 2014 (12.x) and higher builds.' AS [auto_create_stats_incremental_comment]
  INTO tempdb.dbo.tmpStatisticCheck19
END

SELECT * FROM tempdb.dbo.tmpStatisticCheck19