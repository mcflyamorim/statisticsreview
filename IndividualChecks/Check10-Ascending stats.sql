/*
Check10 - Ascending statistics
Description:
Check 10 - Check if there are statistics set as ascending/descending
Statistics on ascending or descending key columns, such as IDENTITY or real-time timestamp columns, might require more frequent statistics updates than the Query Optimizer performs. 
Insert operations append new values to ascending or descending columns. The number of rows added might be too small to trigger a statistics update. If statistics are not up-to-date and queries select from the most recently added rows, the current statistics will not have cardinality estimates for these new values. This can result in inaccurate cardinality estimates and slow query performance.
For example, a query that selects from the most recent sales order dates will have inaccurate cardinality estimates if the statistics are not updated to include cardinality estimates for the most recent sales order dates.
Estimated Benefit:
Medium
Estimated Effort:
High
Recommendation:
Quick recommendation:
Update statistics on ascending columns more often.
Detailed recommendation:
- Check if queries using tables are trying to read latest inserted rows, I mean, check if queries are using predicates beyond the RANGE_HI_KEY value of the existing statistics, if so, make sure you've a script to update the statistic more often to guarantee those queries will have Information about newest records.
- Review column query_plan_associated_with_last_usage that may return a plan associated with the statistic.
- If you can't spend time looking at all queries using those tables, go a-head and create a job to update those statistics more often. Make sure your script is smart enough to only run update if number of modified rows changed. Probably an update with sample is enough, as long as you do a bigger sample in the regular maintenance window update.
Note: On KB3189645 (SQL2014 SP1 CU9(12.00.4474) and SP2 CU2(12.00.5532)) filtered indexes are exempted from quickstats queries because it had a bug with filtered indexes and columnstore, but that ended up fixing another problem that when the quickstats query was issued for filtered index stats it has no filter, which was making a full scan (unless a nonfiltered index with the same first column happens to be around to help).


*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

DECLARE @sqlmajorver INT, @sqlminorver INT, @sqlbuild INT
SELECT @sqlmajorver = CONVERT(INT, (@@microsoftversion / 0x1000000) & 0xff),
	      @sqlminorver = CONVERT(INT, (@@microsoftversion / 0x10000) & 0xff),
 	     @sqlbuild = CONVERT(INT, @@microsoftversion & 0xffff);

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck10') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck10

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

SELECT 'Check 10 - Check if there are statistics set as ascending/descending' AS [info],
       a.database_name,
       a.table_name,
       a.stats_name,
       a.key_column_name,
       a.statistic_type,
       a.last_updated AS last_updated_datetime,
       a.filter_definition,
       CASE 
         WHEN a.filter_definition IS NOT NULL
          AND @sqlmajorver <= 12 /*SQL2014*/
         THEN 'Be careful, a SQL Server lower SQL2014 SP1 CU9(12.00.4474) and SP2 CU2(12.00.5532) has a bug with filtered indexes that may cause quickstats queries to do a fullscan.'
         ELSE 'OK'
       END AS filter_comment,
       a.current_number_of_rows,
       a.current_number_of_modified_rows_since_last_update,
       CONVERT(NUMERIC(25, 2), b.rows_above) AS number_of_rows_inserted_above,
       CONVERT(NUMERIC(25, 2), b.rows_below) AS number_of_rows_inserted_below,
       b.leading_column_type,
       CASE 
           WHEN b.leading_column_type IN ('Ascending',  'Descending') THEN 'Warning - Statistic brand is ascending/descending, make sure you are using new CE or TFs 2389/2390 to get good cardinality estimates for searches on latest data.'
           ELSE 'OK'
         END AS leading_column_type_comment,
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
       dbcc_command
INTO tempdb.dbo.tmpStatisticCheck10
FROM tempdb.dbo.tmp_stats a
INNER JOIN tempdb.dbo.tmp_exec_history b
ON b.rowid = a.rowid
AND b.history_number = 1
AND b.leading_column_type IN ('Ascending', 'Descending') 
OUTER APPLY (SELECT MAX(Dt) FROM (VALUES(a.last_user_seek), 
                                        (a.last_user_scan),
                                        (a.last_user_lookup)
                               ) AS t(Dt)) AS TabIndexUsage(last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used)
WHERE a.current_number_of_rows > 0 /* Ignoring empty tables */

SELECT * FROM tempdb.dbo.tmpStatisticCheck10
ORDER BY current_number_of_rows DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name
