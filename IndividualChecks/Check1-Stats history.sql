/* 
Check1 - Statistic history
Description:
Check 1 - Do we have statistics with useful history?
By useful I mean statistics with information about at least more than 1 update. 
If a statistic has only 1 update, then, some of counters/checks won't be available, like, number of inserted rows since previous update and etc.
Every statistic saves information up to last 4 update stats, those are accessible via TF2388 and DBCC SHOW_STATISTICS.
This check will return all stats and number of statistics sample available, this also returns number of rows in the table because if number of rows is small, you may (I'm not saying you shouldn't, it depends) don't care about this object.
Estimated Benefit:
Medium
Estimated Effort:
High
Recommendation:
Quick recommendation:
Review reported statistics that have only 1 history
Detailed recommendation:
- Ideally result for [number_of_statistic_data_available_for_this_object] should be 4, but the stats history info is reset in an index rebuild, so, it may be ok to have 1 info as index may be recently rebuild.
- Rows with [number_of_statistic_data_available_for_this_object] less than 4, check Statistic_Updated column for more Information about last time statistic was updated. If it is too old, this may indicate stat is not being used, or worse, it is being used but is out-of-date.
- Rows with [number_of_statistic_data_available_for_this_object] equal to 1, may indicate recently auto-created statistics. Remember, auto-created stats will use default sample option, depending on the data distribution, you may want to update it with a higher sample to get a better histogram.
- Note: If statistic is updated recently (check [HoursSinceLastUpdate] column), it maybe not an issue to have only 1 or 2 stat sample as it may be a newly created stat that didn't get 4 updates yet.

*/

/*
  Fabiano Amorim
  http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
*/ 

SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; SET ANSI_WARNINGS OFF; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck1') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck1

SELECT 'Check 1 - Do we have statistics with useful history?' AS [info],
       a.database_name,
       a.schema_name,
       a.table_name,
       a.stats_name,
       a.table_index_base_type,
       a.key_column_name,
       a.key_column_data_type,
       a.stat_all_columns,
       a.statistic_type,
       b.leading_column_type,
       a.current_number_of_rows AS current_number_of_rows_table,
       a.plan_cache_reference_count,
       a.current_number_of_modified_rows_since_last_update,
       a.auto_update_threshold_type,
       a.auto_update_threshold,
       CONVERT(DECIMAL(25, 2), (a.current_number_of_modified_rows_since_last_update / (a.auto_update_threshold * 1.0)) * 100.0) AS percent_of_threshold,
       a.rows_sampled AS number_of_rows_sampled_on_last_update,
       a.statistic_percent_sampled,
       DATEDIFF(HOUR, a.last_updated, GETDATE()) AS hours_since_last_update,
       CONVERT(VARCHAR(4), DATEDIFF(mi,a.last_updated,GETDATE()) / 60 / 24) + 'd ' + CONVERT(VARCHAR(4), DATEDIFF(mi,a.last_updated,GETDATE()) / 60 % 24) + 'hr '
       + CONVERT(VARCHAR(4), DATEDIFF(mi,a.last_updated,GETDATE()) % 60) + 'min' AS time_since_last_update,
       a.last_updated AS last_updated_datetime,
       Tab_StatSample2.last_updated AS update_stat_2_datetime,
       Tab_StatSample3.last_updated AS update_stat_3_datetime,
       Tab_StatSample4.last_updated AS update_stat_4_datetime,
       CONVERT(NUMERIC(25, 2), (a.number_of_in_row_data_pages_on_table * 8) / 1024. / 1024.) AS in_row_data_size_in_gb,
       CONVERT(NUMERIC(25, 2), (a.number_of_lob_data_pages_on_table * 8) / 1024. / 1024.) AS lob_data_size_in_gb,
       a.is_lob,
       a.is_unique,
       a.is_incremental,
       a.is_table_partitioned,
       d.string_index,
       a.has_filter,
       a.filter_definition,
       a.number_of_columns_in_this_table,
       a.number_of_statistics_in_this_table,
       a.steps AS number_of_steps_on_histogram,     
       c.all_density AS key_column_density,
       CONVERT(BigInt, 1.0 / CASE c.all_density WHEN 0 THEN 1 ELSE c.all_density END) AS unique_values_on_key_column_based_on_density,
       CONVERT(BigInt, c.all_density * current_number_of_rows) AS estimated_number_of_rows_per_value_based_on_density,
       user_seeks + user_scans + user_lookups AS number_of_reads_on_index_table_since_last_restart,
       user_updates AS number_of_modifications_on_index_table_since_last_restart,
       range_scan_count AS number_of_range_scans_since_last_restart_rebuild,
       singleton_lookup_count AS number_of_singleton_lookups_since_last_restart_rebuild,
       leaf_insert_count AS number_of_inserts_since_last_restart_rebuild,
       leaf_delete_count AS number_of_deletes_since_last_restart_rebuild,
       leaf_update_count AS number_of_updates_since_last_restart_rebuild,
       forwarded_fetch_count AS number_of_forwarded_records_fetch_since_last_restart_rebuild,
       page_latch_wait_count AS number_of_page_latch_since_last_restart_rebuild,
       page_latch_wait_in_ms AS number_of_page_latch_in_ms_since_last_restart_rebuild,
       avg_page_latch_wait_in_ms AS avg_page_latch_in_ms_since_last_restart_rebuild,
       page_latch_wait_time_d_h_m_s AS total_page_latch_wait_time_d_h_m_s_since_last_restart_rebuild,
       page_io_latch_wait_count AS number_of_page_i_o_latch_since_last_restart_rebuild,
       page_io_latch_wait_in_ms AS number_of_page_io_latch_in_ms_since_last_restart_rebuild,
       avg_page_io_latch_wait_in_ms AS avg_page_io_latch_in_ms_since_last_restart_rebuild,
       page_io_latch_wait_time_d_h_m_s AS total_page_io_latch_wait_time_d_h_m_s_since_last_restart_rebuild,
       TabIndexUsage.last_datetime_obj_was_used,
       (SELECT COUNT(*) FROM tempdb.dbo.tmp_exec_history b 
         WHERE b.rowid = a.rowid
       ) AS number_of_statistic_data_available_for_this_object,

       CASE 
         WHEN ((SELECT COUNT(*) FROM tempdb.dbo.tmp_exec_history b 
                WHERE b.rowid = a.rowid)) < 4
         THEN 'Warning - This statistic had less than 4 updates since it was created. This will limit the results of other checks and may indicate update stats for this obj. is not running'
         ELSE 'OK'
       END AS comment_1,
       CASE 
         WHEN (a.table_name LIKE '%bkp%'
               OR 
               a.table_name LIKE '%test%'
               OR 
               a.table_name LIKE '%backup%'
               OR 
               a.table_name LIKE '%temp%') THEN 'Warning - Table name may indicate this table is not really used. Please confirm that update stat on this object is really needed.'
         ELSE 'OK'
       END AS comment_2,
       a.dbcc_command
INTO tempdb.dbo.tmpStatisticCheck1
FROM tempdb.dbo.tmp_stats a
INNER JOIN tempdb.dbo.tmp_exec_history AS b
ON b.rowid = a.rowid
AND b.history_number = 1
INNER JOIN tempdb.dbo.tmp_density_vector AS c
ON c.rowid = a.rowid
AND c.density_number = 1
INNER JOIN tempdb.dbo.tmp_stat_header AS d
ON d.rowid = a.rowid
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
OUTER APPLY (SELECT MAX(Dt) FROM (VALUES(a.last_user_seek), 
                                        (a.last_user_scan),
                                        (a.last_user_lookup)
                               ) AS t(Dt)) AS TabIndexUsage(last_datetime_obj_was_used)
WHERE a.current_number_of_rows > 0 /* Ignoring empty tables */

SELECT * FROM tempdb.dbo.tmpStatisticCheck1
ORDER BY current_number_of_rows_table DESC, 
         database_name,
         schema_name,
         table_name,
         key_column_name,
         stats_name
