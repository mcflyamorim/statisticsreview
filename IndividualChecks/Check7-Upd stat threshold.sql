/* 
Check7 - Update statistics threshold
Description:
Check 7 - What is the update statistic threshold for each statistic?
This check returns the auto update statistic threshold for each statistic.
When the AUTO_UPDATE_STATISTICS is ON, the Query Optimizer determines when statistics might be out-of-date and then updates them when they are used by a query.
The Query Optimizer determines when statistics might be out-of-date by counting the number of row modifications since the last statistics update and comparing the number of row modifications to a threshold. The threshold is based on the table cardinality, which can be defined as the number of rows in the table or indexed view. Up to SQL Server 2014 (12.x), the Database Engine uses a recompilation threshold based on the number of rows in the table or indexed view at the time statistics were evaluated. 
Starting with SQL Server 2016 (13.x) and under the database compatibility level 130, the Database Engine also uses a decreasing, dynamic statistics recompilation threshold that adjusts according to the table cardinality at the time statistics were evaluated. With this change, statistics on large tables will be updated more frequently. 
However, if a database has a compatibility level below 130, then the SQL Server 2014 (12.x) thresholds apply.
Estimated Benefit:
Medium
Estimated Effort:
High
Recommendation:
Quick recommendation:
Review reported statistics and consider to update them in a specific maintenance plan/job to update.
Detailed recommendation:
- Queries using statistics that already hit the threshold will be recompiled on next execution, depending on the size of the table, this may take a while to complete. You may want to avoid those cases to achieve a more predictable query response time. For instance, a query that has an average response time of 50ms may take 300ms because of the auto update stats/recompilation, depending on the scenario, this may not be an acceptable response time.
- For most workloads, a full scan is not required and default sampling is adequate. However, certain workloads that are sensitive to widely varying data distributions may require an increased sample size, or even a full scan.
- Certain workloads that are sensitive to widely varying data distributions may require an increased sample size, or even a full scan. That means a sample update may create poor query plans which may lead to performance problems. It may be a good idea to create an exclusive maintenance plan to update those statistics with a higher sample size.
- Review statistics with estimated frequency less than 60 minutes and consider to create a job to update these stats manually and more frequently.
- Starting with SQL Server 2016 (13.x) SP1 CU4, use the PERSIST_SAMPLE_PERCENT option of CREATE STATISTICS (Transact-SQL) or UPDATE STATISTICS (Transact-SQL), to set and retain a specific sampling percentage for subsequent statistic updates that do not explicitly specify a sampling percentage.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck7') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck7

BEGIN TRY
  SELECT 'Check 7 - What is the updatestat threshold for each statistic?' AS [info],
         a.database_name,
         a.table_name,
         a.stats_name,
         a.key_column_name,
         a.last_updated AS last_updated_datetime,
         a.plan_cache_reference_count,
         TabIndexUsage.last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used,
         a.current_number_of_rows, 
         a.number_of_rows_at_time_stat_was_updated,
         a.unfiltered_rows AS number_of_rows_on_table_at_time_statistics_was_updated_ignoring_filter,
         a.filter_definition,
         a.current_number_of_modified_rows_since_last_update,
         a.auto_update_threshold,
         a.auto_update_threshold_type,
	        CONVERT(DECIMAL(25, 2), (a.current_number_of_modified_rows_since_last_update / (a.auto_update_threshold * 1.0)) * 100.0) AS percent_of_threshold,
         CASE 
           WHEN TabModificationsPerMinute.avg_modifications_per_minute_based_on_existing_update_stats_intervals > 0 THEN
				       DATEADD(MINUTE, TRY_CONVERT(INT, ((a.auto_update_threshold - a.current_number_of_modified_rows_since_last_update) / TabModificationsPerMinute.avg_modifications_per_minute_based_on_existing_update_stats_intervals)), GETDATE())
			        ELSE NULL
		       END AS estimated_datetime_of_next_auto_update_stats,
         TabEstimatedMinsUntilNextUpdateStats.estimated_minutes_until_next_auto_update_stats,
         CASE 
            WHEN a.is_auto_update_stats_on = 1
                  AND CONVERT(DECIMAL(25, 2), (a.current_number_of_modified_rows_since_last_update / (a.auto_update_threshold * 1.0)) * 100.0) > = 100
            THEN 'Warning - Auto update stats will be executed on next execution of query using this statistic'
            WHEN a.is_auto_update_stats_on = 1
                  AND estimated_minutes_until_next_auto_update_stats <= 120
            THEN 'Warning - Auto update stats will be executed in about 2 hours on next execution of query using this statistic'
            WHEN a.is_auto_update_stats_on = 0
                  AND estimated_minutes_until_next_auto_update_stats <= 0
            THEN 'Warning - AutoUpdateStats on DB is OFF, but statistic already hit the threshold to trigger auto update stats. Queries using this statistic are likely to be using outdated stats.' 
            ELSE 'OK'
         END AS comment_1,
         Tab_StatSample1.number_of_modifications_on_key_column_since_previous_update AS update_stat_1_number_of_modifications_on_key_column_since_previous_update,
         Tab_StatSample2.number_of_modifications_on_key_column_since_previous_update AS update_stat_2_number_of_modifications_on_key_column_since_previous_update,
         Tab_StatSample3.number_of_modifications_on_key_column_since_previous_update AS update_stat_3_number_of_modifications_on_key_column_since_previous_update,
         Tab_MinBetUpdateStats.tot_minutes_between_update_stats,
         Tab_TotModifications.tot_modifications_between_update_stats,
         TabModificationsPerMinute.avg_modifications_per_minute_based_on_existing_update_stats_intervals,
         TabModificationsPerMinute2.avg_modifications_per_minute_based_on_current_getdate,
         dbcc_command
  INTO tempdb.dbo.tmpStatisticCheck7
  FROM tempdb.dbo.tmp_stats AS a
  OUTER APPLY (SELECT CASE 
                        WHEN b.inserts_since_last_update IS NULL AND b.deletes_since_last_update IS NULL
                        THEN NULL
                        ELSE (ABS(ISNULL(b.inserts_since_last_update,0)) + ABS(ISNULL(b.deletes_since_last_update,0)))
                      END AS number_of_modifications_on_key_column_since_previous_update,
                     b.updated as last_updated
                FROM tempdb.dbo.tmp_exec_history b
               WHERE b.rowid = a.rowid
                 AND b.history_number = 1 /* Previous update stat sample */
                ) AS Tab_StatSample1
  OUTER APPLY (SELECT CASE 
                        WHEN b.inserts_since_last_update IS NULL AND b.deletes_since_last_update IS NULL
                        THEN NULL
                        ELSE (ABS(ISNULL(b.inserts_since_last_update,0)) + ABS(ISNULL(b.deletes_since_last_update,0)))
                      END AS number_of_modifications_on_key_column_since_previous_update,
                     b.updated as last_updated
                FROM tempdb.dbo.tmp_exec_history b
               WHERE b.rowid = a.rowid
                 AND b.history_number = 2 /* Previous update stat sample */
                ) AS Tab_StatSample2
  OUTER APPLY (SELECT CASE 
                        WHEN b.inserts_since_last_update IS NULL AND b.deletes_since_last_update IS NULL
                        THEN NULL
                        ELSE (ABS(ISNULL(b.inserts_since_last_update,0)) + ABS(ISNULL(b.deletes_since_last_update,0)))
                      END AS number_of_modifications_on_key_column_since_previous_update,
                     b.updated as last_updated
                FROM tempdb.dbo.tmp_exec_history b
               WHERE b.rowid = a.rowid
                 AND b.history_number = 3 /* Previous update stat sample */
                ) AS Tab_StatSample3
  OUTER APPLY (SELECT CASE 
                        WHEN b.inserts_since_last_update IS NULL AND b.deletes_since_last_update IS NULL
                        THEN NULL
                        ELSE (ABS(ISNULL(b.inserts_since_last_update,0)) + ABS(ISNULL(b.deletes_since_last_update,0)))
                      END AS number_of_modifications_on_key_column_since_previous_update,
                     b.updated as last_updated
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
  CROSS APPLY (SELECT CONVERT(NUMERIC(25, 2), a.current_number_of_modified_rows_since_last_update 
                      / 
                      CASE DATEDIFF(minute, a.last_updated, GETDATE()) WHEN 0 THEN 1 ELSE DATEDIFF(minute, a.last_updated, GETDATE()) END)) AS TabModificationsPerMinute2(avg_modifications_per_minute_based_on_current_getdate) 
  CROSS APPLY (SELECT DATEDIFF(MINUTE, GETDATE(), CASE 
                                                    WHEN ISNULL(TabModificationsPerMinute.avg_modifications_per_minute_based_on_existing_update_stats_intervals, TabModificationsPerMinute2.avg_modifications_per_minute_based_on_current_getdate)  > 0 THEN
				                                                DATEADD(MINUTE, TRY_CONVERT(INT,((a.auto_update_threshold - a.current_number_of_modified_rows_since_last_update) / ISNULL(TabModificationsPerMinute.avg_modifications_per_minute_based_on_existing_update_stats_intervals, TabModificationsPerMinute2.avg_modifications_per_minute_based_on_current_getdate))), GETDATE())
			                                                 ELSE NULL
		                                                END)) AS TabEstimatedMinsUntilNextUpdateStats(estimated_minutes_until_next_auto_update_stats)
  OUTER APPLY (SELECT MAX(Dt) FROM (VALUES(a.last_user_seek), 
                                          (a.last_user_scan),
                                          (a.last_user_lookup)
                                 ) AS t(Dt)) AS TabIndexUsage(last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used)
  WHERE a.current_number_of_rows > 0 /* Ignoring empty tables */

  SELECT * FROM tempdb.dbo.tmpStatisticCheck7
  ORDER BY current_number_of_rows DESC, 
           database_name,
           table_name,
           key_column_name,
           stats_name
END TRY
BEGIN CATCH
  IF ERROR_NUMBER() = 517 /*Adding a value to a 'datetime' column caused an overflow.*/
  BEGIN
     IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck7') IS NOT NULL
       DROP TABLE tempdb.dbo.tmpStatisticCheck7

     SELECT 'Check 7 - What is the updatestat threshold for each statistic?' AS [info],
             a.database_name,
             a.table_name,
             a.stats_name,
             a.key_column_name,
             a.last_updated AS last_updated_datetime,
             TabIndexUsage.last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used,
             a.current_number_of_rows, 
             a.number_of_rows_at_time_stat_was_updated,
             a.unfiltered_rows AS number_of_rows_on_table_at_time_statistics_was_updated_ignoring_filter,
             a.filter_definition,
             a.current_number_of_modified_rows_since_last_update,
             a.auto_update_threshold,
             a.auto_update_threshold_type,
	            CONVERT(DECIMAL(25, 2), (a.current_number_of_modified_rows_since_last_update / (a.auto_update_threshold * 1.0)) * 100.0) AS percent_of_threshold,
             0 AS estimated_datetime_of_next_auto_update_stats,
             TabEstimatedMinsUntilNextUpdateStats.estimated_minutes_until_next_auto_update_stats,
             CASE 
                WHEN a.is_auto_update_stats_on = 1
                      AND estimated_minutes_until_next_auto_update_stats <= 0
                THEN 'Warning - Auto update stats will be executed on next execution of query using this statistic'
                WHEN a.is_auto_update_stats_on = 1
                      AND estimated_minutes_until_next_auto_update_stats <= 120
                THEN 'Warning - Auto update stats will be executed in about 2 hours on next execution of query using this statistic'
                WHEN a.is_auto_update_stats_on = 0
                      AND estimated_minutes_until_next_auto_update_stats <= 0
                THEN 'Warning - AutoUpdateStats on DB is OFF, but statistic already hit the threshold to trigger auto update stats. Queries using this statistic are likely to be using outdated stats.' 
                ELSE 'OK'
             END AS comment_1,
             Tab_StatSample1.number_of_modifications_on_key_column_since_previous_update AS update_stat_1_number_of_modifications_on_key_column_since_previous_update,
             Tab_StatSample2.number_of_modifications_on_key_column_since_previous_update AS update_stat_2_number_of_modifications_on_key_column_since_previous_update,
             Tab_StatSample3.number_of_modifications_on_key_column_since_previous_update AS update_stat_3_number_of_modifications_on_key_column_since_previous_update,
             Tab_MinBetUpdateStats.tot_minutes_between_update_stats,
             Tab_TotModifications.tot_modifications_between_update_stats,
             TabModificationsPerMinute.avg_modifications_per_minute_based_on_existing_update_stats_intervals,
             TabModificationsPerMinute2.avg_modifications_per_minute_based_on_current_getdate,
             dbcc_command
      INTO tempdb.dbo.tmpStatisticCheck7
      FROM tempdb.dbo.tmp_stats AS a
      OUTER APPLY (SELECT CASE 
                            WHEN b.inserts_since_last_update IS NULL AND b.deletes_since_last_update IS NULL
                            THEN NULL
                            ELSE (ABS(ISNULL(b.inserts_since_last_update,0)) + ABS(ISNULL(b.deletes_since_last_update,0)))
                          END AS number_of_modifications_on_key_column_since_previous_update,
                         b.updated as last_updated
                    FROM tempdb.dbo.tmp_exec_history b
                   WHERE b.rowid = a.rowid
                     AND b.history_number = 1 /* Previous update stat sample */
                    ) AS Tab_StatSample1
      OUTER APPLY (SELECT CASE 
                            WHEN b.inserts_since_last_update IS NULL AND b.deletes_since_last_update IS NULL
                            THEN NULL
                            ELSE (ABS(ISNULL(b.inserts_since_last_update,0)) + ABS(ISNULL(b.deletes_since_last_update,0)))
                          END AS number_of_modifications_on_key_column_since_previous_update,
                         b.updated as last_updated
                    FROM tempdb.dbo.tmp_exec_history b
                   WHERE b.rowid = a.rowid
                     AND b.history_number = 2 /* Previous update stat sample */
                    ) AS Tab_StatSample2
      OUTER APPLY (SELECT CASE 
                            WHEN b.inserts_since_last_update IS NULL AND b.deletes_since_last_update IS NULL
                            THEN NULL
                            ELSE (ABS(ISNULL(b.inserts_since_last_update,0)) + ABS(ISNULL(b.deletes_since_last_update,0)))
                          END AS number_of_modifications_on_key_column_since_previous_update,
                         b.updated as last_updated
                    FROM tempdb.dbo.tmp_exec_history b
                   WHERE b.rowid = a.rowid
                     AND b.history_number = 3 /* Previous update stat sample */
                    ) AS Tab_StatSample3
      OUTER APPLY (SELECT CASE 
                            WHEN b.inserts_since_last_update IS NULL AND b.deletes_since_last_update IS NULL
                            THEN NULL
                            ELSE (ABS(ISNULL(b.inserts_since_last_update,0)) + ABS(ISNULL(b.deletes_since_last_update,0)))
                          END AS number_of_modifications_on_key_column_since_previous_update,
                         b.updated as last_updated
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
      CROSS APPLY (SELECT 0) AS TabModificationsPerMinute2(avg_modifications_per_minute_based_on_current_getdate) 
      CROSS APPLY (SELECT 0) AS TabEstimatedMinsUntilNextUpdateStats(estimated_minutes_until_next_auto_update_stats)
      OUTER APPLY (SELECT MAX(Dt) FROM (VALUES(a.last_user_seek), 
                                              (a.last_user_scan),
                                              (a.last_user_lookup)
                                     ) AS t(Dt)) AS TabIndexUsage(last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used)
      WHERE a.current_number_of_rows > 0 /* Ignoring empty tables */

      SELECT * FROM tempdb.dbo.tmpStatisticCheck7
      ORDER BY current_number_of_rows DESC, 
               database_name,
               table_name,
               key_column_name,
               stats_name
  END
  ELSE
  BEGIN
    DECLARE @ErrMessage NVARCHAR(MAX)
    SET @ErrMessage = ERROR_MESSAGE()
    RAISERROR ('Error_Message() = %s', 16, -1, @ErrMessage) WITH NOWAIT
  END
END CATCH