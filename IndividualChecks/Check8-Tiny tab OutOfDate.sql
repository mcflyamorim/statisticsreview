/*
Check8 - Tiny table with out-of-date statistic
Description:
Check 8 - Is there any tiny (less than or equal to 500 rows) table with out-of-date statistics?
Check if there are small tables (less than or equal to 500 rows) with poor statistics. 
Small tables will only trigger auto-update stats if modification counter is >= 501, depending on the environment this may take a while or never happen. SQL Server ignores very small tables (normal tables not temp tables) for automatic statistics. Unfortunately, this might happen quite often in relational data warehouse solutions which use star schemas. 
The effect of joining a few-hundred million rows fact table with some small dimensions the wrong way might be dramatic - in a negative sense.
This problem is much easier to avoid with huge tables, but if you add 1 row to a 1-row table you double the data.
https://learn.microsoft.com/en-us/archive/blogs/mssqlisv/sql-optimizations-manual-update-statistics-on-small-tables-may-provide-a-big-impact 
Estimated Benefit:
Medium
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Make sure you're updating stats for small tables.
Detailed recommendation:
- To avoid issues, make sure you're updating stats for those small tables.
- To avoid outdated or obsolete statistics on those tiny tables (in terms of number of rows), make sure you're manually updating it, it will not take too much time and may help query optimizer.
- You can use column query_plan_associated_with_last_usage to investigate query plan.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck8') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck8

IF OBJECT_ID('tempdb.dbo.#tmpdm_exec_query_stats') IS NOT NULL
  DROP TABLE #tmpdm_exec_query_stats
  
SELECT TOP 100
       plan_handle,
       statement_start_offset,
       statement_end_offset,
       CONVERT(XML, NULL) AS statement_plan, 
       CONVERT(XML, NULL) AS statement_text,
       creation_time,
       last_execution_time,
       execution_count, 
       CONVERT(NUMERIC(25, 4), (total_worker_time / execution_count) / 1000.) AS avg_cpu_time_ms,
       CONVERT(NUMERIC(25, 4), total_worker_time / 1000.) AS total_cpu_time_ms,
       total_logical_reads,
       total_logical_reads / execution_count AS avg_logical_reads,
       total_logical_writes,
       total_logical_writes / execution_count AS avg_logical_writes,
       CONVERT(NUMERIC(25, 2), LOG((total_worker_time * 3) + total_logical_reads + total_logical_writes)) AS query_impact
INTO #tmpdm_exec_query_stats
FROM sys.dm_exec_query_stats
WHERE total_worker_time > 0 /* Only plans with CPU time > 0ms */
ORDER BY query_impact DESC

CREATE CLUSTERED INDEX ix1 ON #tmpdm_exec_query_stats(plan_handle)

DECLARE @number_plans BIGINT,
        @err_msg      NVARCHAR(4000),
        @plan_handle  VARBINARY(64),
        @statement_start_offset BIGINT, 
        @statement_end_offset BIGINT,
        @i            BIGINT

SELECT @number_plans = COUNT(*) 
FROM #tmpdm_exec_query_stats

SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to capture XML query plan for cached plans. Found ' + CONVERT(VARCHAR(200), @number_plans) + ' plans on sys.dm_exec_query_stats.'
RAISERROR (@err_msg, 0, 0) WITH NOWAIT

DBCC TRACEON(8666) WITH NO_INFOMSGS

SET @i = 1
DECLARE c_plans CURSOR FAST_FORWARD FOR
    SELECT plan_handle, statement_start_offset, statement_end_offset 
    FROM #tmpdm_exec_query_stats
OPEN c_plans

FETCH NEXT FROM c_plans
INTO @plan_handle, @statement_start_offset, @statement_end_offset
WHILE @@FETCH_STATUS = 0
BEGIN
  BEGIN TRY
    SET @err_msg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Progress ' + '(' + CONVERT(VARCHAR(200), CONVERT(NUMERIC(25, 2), (CONVERT(NUMERIC(25, 2), @i) / CONVERT(NUMERIC(25, 2), @number_plans)) * 100)) + '%%) - ' 
                   + CONVERT(VARCHAR(200), @i) + ' of ' + CONVERT(VARCHAR(200), @number_plans)
    IF @i % 1000 = 0
      RAISERROR (@err_msg, 0, 1) WITH NOWAIT

    UPDATE #tmpdm_exec_query_stats SET statement_plan = detqp.query_plan
    FROM #tmpdm_exec_query_stats AS qs
    OUTER APPLY sys.dm_exec_text_query_plan(qs.plan_handle,
                                            qs.statement_start_offset,
                                            qs.statement_end_offset) AS detqp
    WHERE qs.plan_handle = @plan_handle
    AND qs.statement_start_offset = @statement_start_offset
    AND qs.statement_end_offset = @statement_end_offset
		END TRY
		BEGIN CATCH
			 --SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to work on plan [' + CONVERT(NVARCHAR(800), @plan_handle, 1) + ']. Skipping this plan.'
    --RAISERROR (@err_msg, 0, 0) WITH NOWAIT
    --SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
    --RAISERROR (@err_msg, 0, 0) WITH NOWAIT
		END CATCH

  SET @i = @i + 1
  FETCH NEXT FROM c_plans
  INTO @plan_handle, @statement_start_offset, @statement_end_offset
END
CLOSE c_plans
DEALLOCATE c_plans

DBCC TRACEOFF(8666) WITH NO_INFOMSGS

SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to capture XML query plan for cached plans.'
RAISERROR (@err_msg, 0, 0) WITH NOWAIT

SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to capture query statement for cached plans. Found ' + CONVERT(VARCHAR(200), @number_plans) + ' plans on sys.dm_exec_query_stats.'
RAISERROR (@err_msg, 0, 0) WITH NOWAIT

SET @i = 1
DECLARE c_plans CURSOR FAST_FORWARD FOR
    SELECT plan_handle, statement_start_offset, statement_end_offset 
    FROM #tmpdm_exec_query_stats
OPEN c_plans

FETCH NEXT FROM c_plans
INTO @plan_handle, @statement_start_offset, @statement_end_offset
WHILE @@FETCH_STATUS = 0
BEGIN
  BEGIN TRY
    SET @err_msg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Progress ' + '(' + CONVERT(VARCHAR(200), CONVERT(NUMERIC(25, 2), (CONVERT(NUMERIC(25, 2), @i) / CONVERT(NUMERIC(25, 2), @number_plans)) * 100)) + '%%) - ' 
                   + CONVERT(VARCHAR(200), @i) + ' of ' + CONVERT(VARCHAR(200), @number_plans)
    IF @i % 1000 = 0
      RAISERROR (@err_msg, 0, 1) WITH NOWAIT

    UPDATE #tmpdm_exec_query_stats SET statement_text = t2.cStatement
    FROM #tmpdm_exec_query_stats AS qs
    OUTER APPLY sys.dm_exec_sql_text(qs.plan_handle) st
    CROSS APPLY (SELECT ISNULL(
                            NULLIF(
                                SUBSTRING(
                                  st.text, 
                                  (qs.statement_start_offset / 2) + 1,
                                  CASE WHEN qs.statement_end_offset < qs.statement_start_offset 
                                   THEN 0
                                  ELSE (qs.statement_end_offset - qs.statement_start_offset) / 2 END + 2
                                ), ''
                            ), st.text
                        )) AS t1(query)
    CROSS APPLY (SELECT CONVERT(XML, ISNULL(CONVERT(XML, '<?query --' +
                                                            REPLACE
					                                                       (
						                                                       REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
						                                                       REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
						                                                       REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
							                                                       CONVERT
							                                                       (
								                                                       VARCHAR(MAX),
								                                                       N'--' + NCHAR(13) + NCHAR(10) + t1.query + NCHAR(13) + NCHAR(10) + N'--' COLLATE Latin1_General_Bin2
							                                                       ),
							                                                       NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
							                                                       NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
							                                                       NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
						                                                       NCHAR(0),
						                                                       N'')
                                                             + '--?>'),
                                                  '<?query --' + NCHAR(13) + NCHAR(10) +
                                                  'Could not render the query due to XML data type limitations.' + NCHAR(13) + NCHAR(10) +
                                                  '--?>'))) AS t2 (cStatement)
    WHERE qs.plan_handle = @plan_handle
    AND qs.statement_start_offset = @statement_start_offset
    AND qs.statement_end_offset = @statement_end_offset
		END TRY
		BEGIN CATCH
			 --SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to work on plan [' + CONVERT(NVARCHAR(800), @plan_handle, 1) + ']. Skipping this plan.'
    --RAISERROR (@err_msg, 0, 0) WITH NOWAIT
    --SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
    --RAISERROR (@err_msg, 0, 0) WITH NOWAIT
		END CATCH

  SET @i = @i + 1
  FETCH NEXT FROM c_plans
  INTO @plan_handle, @statement_start_offset, @statement_end_offset
END
CLOSE c_plans
DEALLOCATE c_plans

SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to capture query statement for cached plans.'
RAISERROR (@err_msg, 0, 0) WITH NOWAIT

CREATE NONCLUSTERED INDEX ix_last_execution_time ON #tmpdm_exec_query_stats(last_execution_time)

SELECT 'Check 8 - Is there any tiny (less than or equal to 500 rows) table with out-of-date statistics?' AS [info],
       a.database_name,
       a.table_name,
       a.stats_name,
       a.statistic_type,
       a.key_column_name,
       a.last_updated AS last_updated_datetime,
       CASE 
           WHEN DATEDIFF(dd,a.last_updated, GETDATE()) >= 7 THEN 
                'Warning - It has been more than 7 days [' + CONVERT(VARCHAR(4), DATEDIFF(mi,a.last_updated,GETDATE()) / 60 / 24) + 'd ' + CONVERT(VARCHAR(4), DATEDIFF(mi,a.last_updated,GETDATE()) / 60 % 24) + 'hr '
                + CONVERT(VARCHAR(4), DATEDIFF(mi,a.last_updated,GETDATE()) % 60) + 'min' 
                + '] since last update statistic.'
           ELSE 'OK'
         END AS statistic_updated_comment,
       TabIndexUsage.last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used,
       CASE 
         WHEN DATEDIFF(HOUR, TabIndexUsage.last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used, GETDATE()) <= 6 /*If last usage was in past 6 hours, try to see if can find associated query plan from cache plan*/
         THEN (
               SELECT TOP 1 statement_plan
               FROM #tmpdm_exec_query_stats qs
               WHERE qs.last_execution_time = TabIndexUsage.last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used
               AND CONVERT(NVARCHAR(MAX), statement_plan) COLLATE Latin1_General_BIN2 LIKE '%' + REPLACE(REPLACE(a.stats_name, '[', ''), ']', '') + '%'
               AND CONVERT(NVARCHAR(MAX), statement_plan) COLLATE Latin1_General_BIN2 LIKE '%' + REPLACE(REPLACE(a.table_name, '[', ''), ']', '') + '%'
         )
         ELSE NULL
       END AS query_plan_associated_with_last_usage,
       a.current_number_of_rows, 
       a.number_of_rows_at_time_stat_was_updated,
       a.current_number_of_modified_rows_since_last_update,
       a.auto_update_threshold,
       a.auto_update_threshold_type,
       dbcc_command
INTO tempdb.dbo.tmpStatisticCheck8
FROM tempdb.dbo.tmp_stats AS a
OUTER APPLY (SELECT MAX(Dt) FROM (VALUES(a.last_user_seek), 
                                        (a.last_user_scan),
                                        (a.last_user_lookup)
                               ) AS t(Dt)) AS TabIndexUsage(last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used)
 WHERE a.number_of_rows_at_time_stat_was_updated <= 500
   AND a.current_number_of_modified_rows_since_last_update >= 1

SELECT * FROM tempdb.dbo.tmpStatisticCheck8
ORDER BY current_number_of_rows DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name