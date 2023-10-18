/* 
Check2 - Estimate update statistic frequency
Description:
Check 2 - How often statistics is updated?
This check return data about last 4 statistic updates with Information about number of modifications on statistic key column since previous update, number of minutes between each update stats and the average of minutes it took between each update stats.
The idea is to figure out how much time it takes to update a statistic, in other words, the frequency that the statistic is updated.
Estimated Benefit:
High
Estimated Effort:
Medium
Recommendation:
Quick recommendation:
Review out dated statistics and statistics with high number of updates.
Detailed recommendation:
- Statistics with Information about only 1 sample indicate that statistic is not being updated.
- If more than one sample is found, this script will calculate what is the interval average of time in minutes that statistic took to be updated. If the interval is too short, it may indicate the statistic has a lot of auto update stats or a job running update stats too often. Make sure you have enough modifications to justify the need of an update stats.
- Check if there was an event of statistic update with no modifications since last update. If so, make sure your maintenance script is smart enough to avoid update stats for non-modified stats.
- Check if there was an event of statistic update with interval of less than 15 minutes. Those may be caused by a very high number of modifications triggering auto update or a job with a bad schedule running unnecessary updates.
- Check if there was an event of statistic update with interval greater than 25 hours. If modification count between the update interval is high, that may lead to poor exec plan estimations.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com

SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck2') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck2

;WITH CTE_1
AS
(
SELECT a.database_name,
       a.table_name,
       a.stats_name,
       a.key_column_name,
       a.current_number_of_rows,
       a.last_updated AS update_stat_1_most_recent_datetime, 
       Tab_StatSample2.last_updated AS update_stat_2_datetime,
       Tab_StatSample3.last_updated AS update_stat_3_datetime,
       Tab_StatSample4.last_updated AS update_stat_4_datetime,
       Tab_StatSample1.number_of_modifications_on_key_column_since_previous_update AS update_stat_1_number_of_modifications_on_key_column_since_previous_update,
       Tab_StatSample2.number_of_modifications_on_key_column_since_previous_update AS update_stat_2_number_of_modifications_on_key_column_since_previous_update,
       Tab_StatSample3.number_of_modifications_on_key_column_since_previous_update AS update_stat_3_number_of_modifications_on_key_column_since_previous_update,
       DATEDIFF(MINUTE, Tab_StatSample2.last_updated, a.last_updated) AS minutes_between_update_stats_1_and_2,
       DATEDIFF(MINUTE, Tab_StatSample3.last_updated, Tab_StatSample2.last_updated) AS minutes_between_update_stats_2_and_3,
       DATEDIFF(MINUTE, Tab_StatSample4.last_updated, Tab_StatSample3.last_updated) AS minutes_between_update_stats_3_and_4,
       (SELECT AVG(Col1) FROM (VALUES(DATEDIFF(MINUTE, Tab_StatSample2.last_updated, a.last_updated)), 
                                     (DATEDIFF(MINUTE, Tab_StatSample3.last_updated, Tab_StatSample2.last_updated)), 
                                     (DATEDIFF(MINUTE, Tab_StatSample4.last_updated, Tab_StatSample3.last_updated))
                               ) AS T(Col1)) AS avg_minutes_between_update_stats,
       Tab_TotModifications.tot_modifications_between_update_stats,
       TabModificationsPerMinute.avg_modifications_per_minute_based_on_existing_update_stats_intervals,
       a.auto_update_threshold_type,
       a.auto_update_threshold,
       a.current_number_of_modified_rows_since_last_update,
       a.dbcc_command
FROM tempdb.dbo.tmp_stats a
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
WHERE a.current_number_of_rows > 0 /* Ignoring empty tables */
)
SELECT 'Check 2 - How often statistics is updated?' AS [info],
       CTE_1.database_name,
       CTE_1.table_name,
       CTE_1.stats_name,
       CTE_1.key_column_name,
       CTE_1.current_number_of_rows,
       CTE_1.auto_update_threshold_type,
       CTE_1.auto_update_threshold,
       CTE_1.current_number_of_modified_rows_since_last_update,
	      CONVERT(DECIMAL(25, 2), (CTE_1.current_number_of_modified_rows_since_last_update / (CASE WHEN CTE_1.auto_update_threshold = 0 THEN 1 ELSE CTE_1.auto_update_threshold END * 1.0)) * 100.0) AS percent_of_threshold,
       'Statistic is updated every ' 
       + CONVERT(VARCHAR(4), DATEDIFF(mi, '19000101', (DATEADD(mi, CTE_1.avg_minutes_between_update_stats, '19000101'))) / 60 / 24) + 'd '
       + CONVERT(VARCHAR(4), DATEDIFF(mi, '19000101', (DATEADD(mi, CTE_1.avg_minutes_between_update_stats, '19000101'))) / 60 % 24) + 'hr '
       + CONVERT(VARCHAR(4), DATEDIFF(mi, '19000101', (DATEADD(mi, CTE_1.avg_minutes_between_update_stats, '19000101'))) % 60) + 'min' AS info_0,
       'Considering that statistic has Avg of ' + 
       CONVERT(VARCHAR, avg_modifications_per_minute_based_on_existing_update_stats_intervals) + 
       ' modifications per minute and update stat threshold of ' + 
       CONVERT(VARCHAR, CTE_1.auto_update_threshold) + 
       ', estimated frequency of auto update stats is every ' + CONVERT(VARCHAR, CONVERT(BIGINT, CTE_1.auto_update_threshold / CASE WHEN CTE_1.avg_modifications_per_minute_based_on_existing_update_stats_intervals = 0 THEN 1 ELSE CTE_1.avg_modifications_per_minute_based_on_existing_update_stats_intervals END)) + 
       ' minutes.'
       AS [info_1],
       CTE_1.update_stat_1_most_recent_datetime,
       CTE_1.update_stat_2_datetime,
       CTE_1.update_stat_3_datetime,
       CTE_1.update_stat_4_datetime,
       CTE_1.update_stat_1_number_of_modifications_on_key_column_since_previous_update,
       CTE_1.update_stat_2_number_of_modifications_on_key_column_since_previous_update,
       CTE_1.update_stat_3_number_of_modifications_on_key_column_since_previous_update,
       CTE_1.minutes_between_update_stats_1_and_2,
       CTE_1.minutes_between_update_stats_2_and_3,
       CTE_1.minutes_between_update_stats_3_and_4,
       CTE_1.avg_minutes_between_update_stats,
       CASE 
         WHEN (update_stat_1_number_of_modifications_on_key_column_since_previous_update = 0)
              OR (update_stat_2_number_of_modifications_on_key_column_since_previous_update = 0)
              OR (update_stat_3_number_of_modifications_on_key_column_since_previous_update = 0)
           THEN 'Warning - There was an event of statistic update with no modifications since last update. Make sure your maintenance script is smart enough to avoid update stats for non-modified stats.'
         ELSE 'OK'
       END AS comment_1,
       CASE 
         WHEN (minutes_between_update_stats_1_and_2 <= 15)
              OR (minutes_between_update_stats_2_and_3 = 15)
              OR (minutes_between_update_stats_3_and_4 = 15)
           THEN 'Warning - There was an event of statistic update with interval of less than 15 minutes. Those may be caused by a very high number of modifications triggering auto update or a bad job running unecessary updates.'
         ELSE 'OK'
       END AS comment_2,
       CASE 
         WHEN (minutes_between_update_stats_1_and_2 >= 1500/*25 hours*/)
              OR (minutes_between_update_stats_2_and_3 = 1500/*25 hours*/)
              OR (minutes_between_update_stats_3_and_4 = 1500/*25 hours*/)
           THEN 'Warning - There was an event of statistic update with interval greater than 25 hours. If modification count is high, that will lead to poor exec plan estimations.'
         ELSE 'OK'
       END AS comment_3,
       CASE
         WHEN CTE_1.avg_minutes_between_update_stats <= 60
         THEN 'Warning - Statistic has estimated frequency of auto update stats of less than or equal to 1 hour, consider to create a job to update it.'
         ELSE 'OK'
       END comment_4
INTO tempdb.dbo.tmpStatisticCheck2
FROM CTE_1
--WHERE avg_modifications_per_minute_based_on_existing_update_stats_intervals > 0

SELECT * FROM tempdb.dbo.tmpStatisticCheck2
ORDER BY ISNULL(avg_minutes_between_update_stats,2147483647) ASC, 
         current_number_of_rows DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name