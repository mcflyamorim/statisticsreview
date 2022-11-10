/* 
Check 6 - How many modifications per minute we've? 

< ---------------- Description ----------------- >
This check returns number of modifications per minute for each table.
This uses the average of modifications per minute based on existing update stats intervals, 
for instance, if statistic was updated on 8AM and 11AM, it calculates how many motifications 
it has between that interval and divides by the number of minutes.
The idea is to identify what are the most modified tables and statistics.

< -------------- What to look for and recommendations -------------- >
- Statistics with high number of modifications may require more attention.

- Consider to move those statistics into an specific maintenance plan/job to update them more often.
*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck6') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck6

SELECT 'Check 6 - How many modifications per minute we have?' AS [info],
       a.database_name,
       a.table_name,
       a.stats_name,
       a.key_column_name,
       (SELECT COUNT(*) 
        FROM tempdb.dbo.tmp_exec_history b 
        WHERE b.rowid = a.rowid) AS number_of_statistic_data_available_for_this_object,
       a.last_updated AS last_updated_datetime,
       a.current_number_of_rows, 
       a.number_of_rows_at_time_stat_was_updated,
       a.unfiltered_rows AS number_of_rows_on_table_at_time_statistics_was_updated_ignoring_filter,
       a.current_number_of_modified_rows_since_last_update,
       TabIndexUsage.last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used,
       CONVERT(VARCHAR(4), DATEDIFF(mi,TabIndexUsage.last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used,GETDATE()) / 60 / 24) + 'd ' + CONVERT(VARCHAR(4), DATEDIFF(mi,TabIndexUsage.last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used,GETDATE()) / 60 % 24) + 'hr '
       + CONVERT(VARCHAR(4), DATEDIFF(mi,TabIndexUsage.last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used,GETDATE()) % 60) + 'min' AS time_since_last_index_or_a_table_if_obj_is_not_a_index_statistic_usage,
       TabModificationsPerMinute.avg_modifications_per_minute_based_on_existing_update_stats_intervals,
       CASE 
         WHEN TabModificationsPerMinute.avg_modifications_per_minute_based_on_existing_update_stats_intervals >= AVG(TabModificationsPerMinute.avg_modifications_per_minute_based_on_existing_update_stats_intervals) OVER() 
         THEN 'Warning - This statistic has a number of modifications greater than the average (' + CONVERT(VARCHAR(20), CONVERT(NUMERIC(25, 2), AVG(TabModificationsPerMinute.avg_modifications_per_minute_based_on_existing_update_stats_intervals) OVER())) + ') of modifications for all objects. This may indicate this is one of TOP N modified objects in the environment.'
         ELSE 'OK'
       END AS [comment],
       CONVERT(NUMERIC(25, 2), a.current_number_of_modified_rows_since_last_update 
       / CASE DATEDIFF(MINUTE, a.last_updated, GETDATE()) WHEN 0 THEN 1 ELSE DATEDIFF(MINUTE, a.last_updated, GETDATE()) END) AS avg_modifications_per_minute_based_on_current_getdate,
      user_seeks + user_scans + user_lookups AS number_of_reads_on_index_table_since_last_restart,
      user_seeks + user_scans + user_lookups / 
      CASE DATEDIFF(mi, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), GETDATE())
        WHEN 0 THEN 1
        ELSE DATEDIFF(mi, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), GETDATE())
      END AS avg_of_reads_per_minute_based_on_index_usage_dmv,
      user_updates AS number_of_modifications_on_index_table_since_last_restart,
      user_updates /
      CASE DATEDIFF(mi, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), GETDATE())
        WHEN 0 THEN 1
        ELSE DATEDIFF(mi, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), GETDATE())
      END AS avg_of_modifications_per_minute_based_on_index_usage_dmv,
      range_scan_count AS number_of_range_scans_since_last_restart_rebuild,
      page_latch_wait_count AS number_of_page_latch_since_last_restart_rebuild,
      page_io_latch_wait_count AS number_of_page_i_o_latch_since_last_restart_rebuild,
      dbcc_command
INTO tempdb.dbo.tmpStatisticCheck6
FROM tempdb.dbo.tmp_stats AS a
OUTER APPLY (SELECT MAX(Dt) FROM (VALUES(a.last_user_seek), 
                                        (a.last_user_scan),
                                        (a.last_user_lookup)
                               ) AS t(Dt)) AS TabIndexUsage(last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used)
OUTER APPLY (SELECT CASE 
                      WHEN b.inserts_since_last_update IS NULL AND b.deletes_since_last_update IS NULL
                      THEN NULL
                      ELSE (ABS(ISNULL(b.inserts_since_last_update,0)) + ABS(ISNULL(b.deletes_since_last_update,0)))
                    END AS number_of_modifications_on_key_column_since_previous_update,
                   b.updated AS last_updated
              FROM tempdb.dbo.tmp_exec_history b
             WHERE b.rowid = a.rowid
               AND b.history_number = 1 /* Previous update stat sample */
              ) AS Tab_StatSample1
OUTER APPLY (SELECT CASE 
                      WHEN b.inserts_since_last_update IS NULL AND b.deletes_since_last_update IS NULL
                      THEN NULL
                      ELSE (ABS(ISNULL(b.inserts_since_last_update,0)) + ABS(ISNULL(b.deletes_since_last_update,0)))
                    END AS number_of_modifications_on_key_column_since_previous_update,
                   b.updated AS last_updated
              FROM tempdb.dbo.tmp_exec_history b
             WHERE b.rowid = a.rowid
               AND b.history_number = 2 /* Previous update stat sample */
              ) AS Tab_StatSample2
OUTER APPLY (SELECT CASE 
                      WHEN b.inserts_since_last_update IS NULL AND b.deletes_since_last_update IS NULL
                      THEN NULL
                      ELSE (ABS(ISNULL(b.inserts_since_last_update,0)) + ABS(ISNULL(b.deletes_since_last_update,0)))
                    END AS number_of_modifications_on_key_column_since_previous_update,
                   b.updated AS last_updated
              FROM tempdb.dbo.tmp_exec_history b
             WHERE b.rowid = a.rowid
               AND b.history_number = 3 /* Previous update stat sample */
              ) AS Tab_StatSample3
OUTER APPLY (SELECT CASE 
                      WHEN b.inserts_since_last_update IS NULL AND b.deletes_since_last_update IS NULL
                      THEN NULL
                      ELSE (ABS(ISNULL(b.inserts_since_last_update,0)) + ABS(ISNULL(b.deletes_since_last_update,0)))
                    END AS number_of_modifications_on_key_column_since_previous_update,
                   b.updated AS last_updated
              FROM tempdb.dbo.tmp_exec_history b
             WHERE b.rowid = a.rowid
               AND b.history_number = 4 /* Previous update stat sample */
              ) AS Tab_StatSample4
CROSS APPLY (SELECT SUM(Col1) FROM (VALUES(DATEDIFF(MINUTE, Tab_StatSample2.last_updated, a.last_updated)), 
                                         (DATEDIFF(MINUTE, Tab_StatSample3.last_updated, Tab_StatSample2.last_updated)), 
                                         (DATEDIFF(MINUTE, Tab_StatSample4.last_updated, Tab_StatSample3.last_updated))
                              ) AS Tab(Col1)) AS Tab_MinBetUpdateStats(tot_minutes_between_update_stats)
CROSS APPLY (SELECT SUM(Col1) FROM (VALUES(Tab_StatSample1.number_of_modifications_on_key_column_since_previous_update), 
                                         (Tab_StatSample2.number_of_modifications_on_key_column_since_previous_update), 
                                         (Tab_StatSample3.number_of_modifications_on_key_column_since_previous_update)
                              ) AS Tab(Col1)) AS Tab_TotModifications(tot_modifications_between_update_stats)
CROSS APPLY (SELECT CONVERT(NUMERIC(25, 2), Tab_TotModifications.tot_modifications_between_update_stats 
                    / CASE 
                        WHEN Tab_MinBetUpdateStats.tot_minutes_between_update_stats = 0 THEN 1 
                        ELSE Tab_MinBetUpdateStats.tot_minutes_between_update_stats 
                      END)) AS TabModificationsPerMinute(avg_modifications_per_minute_based_on_existing_update_stats_intervals)
WHERE a.current_number_of_rows > 100 /* Ignoring "small" tables */

SELECT * FROM tempdb.dbo.tmpStatisticCheck6
ORDER BY avg_modifications_per_minute_based_on_existing_update_stats_intervals DESC,
         current_number_of_rows DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name

/*
Script to test the check:

USE Northwind
GO
IF OBJECT_ID('OrdersBigHeap') IS NOT NULL
  DROP TABLE OrdersBigHeap
GO
SELECT TOP 2000000
       IDENTITY(Int, 1,1) AS OrderID,
       ABS(CheckSUM(NEWID()) / 10000000) AS CustomerID,
       CONVERT(Date, GETDATE() - (CheckSUM(NEWID()) / 1000000)) AS OrderDate,
       ISNULL(ABS(CONVERT(Numeric(18,2), (CheckSUM(NEWID()) / 1000000.5))),0) AS Value
  INTO OrdersBigHeap
  FROM master.dbo.spt_values A
 CROSS JOIN master.dbo.spt_values B
 CROSS JOIN master.dbo.spt_values C
 CROSS JOIN master.dbo.spt_values D
GO

-- Auto create a stat on OrderDate
SELECT COUNT(*) FROM OrdersBigHeap
WHERE OrderDate <= GETDATE()
AND 1 = (SELECT 1)
GO

-- Avg of 1500 modifications per minute... 

-- Modify 1500 rows
UPDATE TOP (1500) OrdersBigHeap SET OrderDate = OrderDate
GO
WAITFOR DELAY '00:01:05'
GO
UPDATE STATISTICS OrdersBigHeap
GO
-- Modify 1500 rows
UPDATE TOP (1500) OrdersBigHeap SET OrderDate = OrderDate
GO
WAITFOR DELAY '00:01:05'
GO
UPDATE STATISTICS OrdersBigHeap
GO
WAITFOR DELAY '00:01:05'
GO
UPDATE TOP (1500) OrdersBigHeap SET OrderDate = OrderDate
GO
UPDATE STATISTICS OrdersBigHeap
GO
*/