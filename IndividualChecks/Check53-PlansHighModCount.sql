/*
Check 53 - Plans with loaded statistics with high modification count

< ---------------- Description ----------------- >
Check StatisticsInfo from query plan cache.

Searching for plans using statistics with a high modification count.

Note 1: Requires SQL Server 2016 SP2+

Note 2: 
Query impact is a calculated metric which represents the overall impact of 
the query on the server. 
This allows you to identify the queries which need most attention.
The query impact is calculated from multiple metrics. The calculation is:
log((TotalCPUTime × 3) + TotalLogicalReads + TotalLogicalWrites)

< -------------- What to look for and recommendations -------------- >
- If the number of modification count is too big, query plan may be inaccurate due to a
out-of-date statistics. Make sure you've a maintenance plan updating those stats.
*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ANSI_WARNINGS ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck53') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck53

IF OBJECT_ID('tempdb.dbo.#tmpdm_exec_query_stats') IS NOT NULL
  DROP TABLE #tmpdm_exec_query_stats
  
SELECT TOP 100000
       query_hash,
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
WHERE total_worker_time >= 10000 /* Only plans with CPU time >= 10ms */
AND NOT EXISTS(SELECT 1 
               FROM sys.dm_exec_cached_plans
               WHERE dm_exec_cached_plans.plan_handle = dm_exec_query_stats.plan_handle
               AND dm_exec_cached_plans.cacheobjtype = 'Compiled Plan Stub') /*Ignoring AdHoc - Plan Stub*/
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


--select * from #tmpdm_exec_query_stats
--order by query_impact desc

DELETE FROM #tmpdm_exec_query_stats
WHERE statement_plan IS NULL

SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Filtering to work only on plans with //p:OptimizerStatsUsage/p:StatisticsInfo.'
RAISERROR (@err_msg, 0, 0) WITH NOWAIT

IF OBJECT_ID('tempdb.dbo.#query_plan') IS NOT NULL
  DROP TABLE #query_plan

;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT query_hash,
       plan_handle, 
       statement_plan, 
       statement_text,
       creation_time,
       last_execution_time,
       execution_count,
       avg_cpu_time_ms,
       total_cpu_time_ms,
       total_logical_reads,
       avg_logical_reads,
       total_logical_writes,
       avg_logical_writes,
       query_impact
INTO #query_plan
FROM #tmpdm_exec_query_stats qs
WHERE statement_plan.exist('//p:OptimizerStatsUsage/p:StatisticsInfo') = 1

SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting final query to return data.'
RAISERROR (@err_msg, 0, 0) WITH NOWAIT

;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT  TOP 1000
        CONVERT(VARCHAR(800), query_hash, 1) AS query_hash,
        plan_handle = CONVERT(VARCHAR(800), plan_handle, 1),
        statement_type = COALESCE(Batch.x.value('(//p:StmtSimple/@StatementType)[1]', 'VarChar(500)'),
                                 Batch.x.value('(//p:StmtCond/@StatementType)[1]', 'VarChar(500)'),
                                 Batch.x.value('(//p:StmtCursor/@StatementType)[1]', 'VarChar(500)'),
                                 Batch.x.value('(//p:StmtReceive/@StatementType)[1]', 'VarChar(500)'),
                                 Batch.x.value('(//p:StmtUseDb/@StatementType)[1]', 'VarChar(500)')),
        ce_model_version = COALESCE(Batch.x.value('(//p:StmtSimple/@CardinalityEstimationModelVersion)[1]', 'int'),
                                 Batch.x.value('(//p:StmtCond/@CardinalityEstimationModelVersion)[1]', 'int'),
                                 Batch.x.value('(//p:StmtCursor/@CardinalityEstimationModelVersion)[1]', 'int'),
                                 Batch.x.value('(//p:StmtReceive/@CardinalityEstimationModelVersion)[1]', 'int'),
                                 Batch.x.value('(//p:StmtUseDb/@CardinalityEstimationModelVersion)[1]', 'int')),
        statement_optm_early_abort_reason = COALESCE(Batch.x.value('(//p:StmtSimple/@StatementOptmEarlyAbortReason)[1]', 'sysname'),
                                 Batch.x.value('(//p:StmtCond/@StatementOptmEarlyAbortReason)[1]', 'sysname'),
                                 Batch.x.value('(//p:StmtCursor/@StatementOptmEarlyAbortReason)[1]', 'sysname'),
                                 Batch.x.value('(//p:StmtReceive/@StatementOptmEarlyAbortReason)[1]', 'sysname'),
                                 Batch.x.value('(//p:StmtUseDb/@StatementOptmEarlyAbortReason)[1]', 'sysname')),
        number_of_loaded_stats = statement_plan.value('count(//p:OptimizerStatsUsage/p:StatisticsInfo)', 'float'),
        sum_modification_count_for_all_used_stats = statement_plan.value('sum(//p:OptimizerStatsUsage/p:StatisticsInfo/@ModificationCount)', 'float'),
        compile_time = x.value('sum(..//p:QueryPlan/@CompileTime)', 'float'),
        cached_plan_size = x.value('sum(..//p:QueryPlan/@CachedPlanSize)', 'float'),
        creation_time AS creation_datetime,
        last_execution_time AS last_execution_datetime,
        execution_count,
        avg_cpu_time_ms,
        total_cpu_time_ms,
        total_logical_reads,
        avg_logical_reads,
        total_logical_writes,
        avg_logical_writes,
        query_impact,
        statement_text,
        statement_plan
INTO tempdb.dbo.tmpStatisticCheck53
FROM #query_plan qp
OUTER APPLY statement_plan.nodes('//p:Batch') AS Batch(x)
ORDER BY number_of_loaded_stats DESC
OPTION (RECOMPILE);

SELECT 'Check 53 - Plans with loaded statistics with high modification count' AS [info],
       *
FROM tempdb.dbo.tmpStatisticCheck53
ORDER BY sum_modification_count_for_all_used_stats DESC