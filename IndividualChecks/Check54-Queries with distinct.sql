/*
Check54 - Queries with distinct
Description:
Check 54 - Look on cached plans to find queries with several columns on distinct clause
Use query plan cache to search for queries with several columns on distinct clause. SQL will trigger auto create/update stats for each column specified on distinct. This may cause long compilation time and create unnecessary (or not very useful) statistics.
Estimated Benefit:
Medium
Estimated Effort:
High
Recommendation:
Quick recommendation:
Review reported queries and check distinct is really necessary.
Detailed recommendation:
- Review queries and check if distinct is really necessary, sometimes a query re-write with exists/not exists clause can be used to avoid unnecessary distinct operations.
- Check if auto created statistics are really useful.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck54') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck54

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
WHERE 1=1
AND total_worker_time >= 10000 /* Only plans with CPU time >= 10ms */
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

SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Filtering to work only on plans with "Distinct Sort" or "Flow Distinct" logical operations.'
RAISERROR (@err_msg, 0, 0) WITH NOWAIT

IF OBJECT_ID('tempdb.dbo.#query_plan') IS NOT NULL
  DROP TABLE #query_plan

;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT query_hash,
       plan_handle, 
       statement_start_offset,
       statement_end_offset,
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
WHERE statement_plan.exist('//p:RelOp[@LogicalOp="Distinct Sort" or @LogicalOp="Flow Distinct"]') = 1

SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting final query to return data.'
RAISERROR (@err_msg, 0, 0) WITH NOWAIT

;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT  TOP 1000
        CONVERT(VARCHAR(800), query_hash, 1) AS query_hash,
        plan_handle = CONVERT(VARCHAR(800), plan_handle, 1),
        logical_op = operators.value('@LogicalOp','nvarchar(50)'),
        STUFF((SELECT ', ' + ISNULL(c1.n.value('(@Database)[1]', 'sysname') + '.' +
                      c1.n.value('(@Schema)[1]', 'sysname') + '.' +
                      c1.n.value('(@Table)[1]', 'sysname') + '.' +
                      QUOTENAME(c1.n.value('(@Column)[1]', 'sysname')), 
                     c2.n.value('(@Database)[1]', 'sysname') + '.' +
                                           c2.n.value('(@Schema)[1]', 'sysname') + '.' +
                                           c2.n.value('(@Table)[1]', 'sysname') + '.' +
                                           QUOTENAME(c2.n.value('(@Column)[1]', 'sysname')))
               FROM #query_plan qp1
               OUTER APPLY qp1.statement_plan.nodes('declare namespace p = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
                                                    //p:RelOp[@LogicalOp="Distinct Sort"]/p:Sort/p:OrderBy/p:OrderByColumn/p:ColumnReference') AS c1(n)
               OUTER APPLY qp1.statement_plan.nodes('declare namespace p = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
                                                    //p:RelOp[@LogicalOp="Flow Distinct"]/p:Hash/p:HashKeysBuild/p:ColumnReference') AS c2(n)
               WHERE qp1.plan_handle = qp.plan_handle
               AND qp1.statement_start_offset = qp.statement_start_offset
               AND qp1.statement_end_offset = qp.statement_end_offset
               FOR XML PATH('')), 1, 2, '') AS referenced_columns,
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
INTO tempdb.dbo.tmpStatisticCheck54
FROM #query_plan qp
CROSS APPLY statement_plan.nodes('//p:Batch') AS Batch(x)
CROSS APPLY statement_plan.nodes('//p:RelOp[@LogicalOp="Distinct Sort" or @LogicalOp="Flow Distinct"]') rel(operators)
ORDER BY query_impact DESC
OPTION (RECOMPILE);

SELECT 'Check 54 - Plans with several columns on distinct clause' AS [info],
       query_hash,
       plan_handle,
       logical_op,
       referenced_columns,
       LEN(referenced_columns) - LEN(REPLACE(referenced_columns, ',', '')) + 1 AS cnt_referenced_columns,
       statement_type,
       ce_model_version,
       statement_optm_early_abort_reason,
       number_of_loaded_stats,
       sum_modification_count_for_all_used_stats,
       compile_time,
       cached_plan_size,
       creation_datetime,
       last_execution_datetime,
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
FROM tempdb.dbo.tmpStatisticCheck54
ORDER BY query_impact DESC

/*
-- Script to show issue

USE Northwind
GO
IF OBJECT_ID('TabTestStats') IS NOT NULL
  DROP TABLE TabTestStats
GO
CREATE TABLE TabTestStats (ID Int IDENTITY(1,1) PRIMARY KEY,
                   Col1 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())),  5)) ,
                   Col2 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())),  5)) ,
                   Col3 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())),  5)) ,
                   Col4 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())),  5)) ,
                   Col5 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())),  5)) ,
                   Col6 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())),  5)) ,
                   Col7 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())),  5)) ,
                   Col8 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())),  5)) ,
                   Col9 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())),  5)) ,
                   Col10 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col11 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col12 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col13 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col14 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col15 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col16 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col17 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col18 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col19 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col20 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col21 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col22 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col23 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col24 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col25 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col26 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col27 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col28 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col29 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col30 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col31 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col32 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col33 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col34 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col35 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col36 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col37 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col38 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col39 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col40 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col41 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col42 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col43 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col44 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col45 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col46 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col47 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col48 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col49 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col50 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000)) ,
                   ColFoto VarBinary(MAX))
GO

-- 6 seconds to run
INSERT INTO TabTestStats (Col1)
SELECT TOP 10000
       CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000)) AS Col1
  FROM sysobjects a, sysobjects b, sysobjects c, sysobjects d
GO

-- 14 seconds to run
SELECT DISTINCT
       Col1,
       Col2,
       Col3,
       Col4,
       Col5,
       Col6,
       Col7,
       Col8,
       Col9,
       Col10,
       Col11,
       Col12,
       Col13,
       Col14,
       Col15,
       Col16,
       Col17,
       Col18,
       Col19,
       Col20,
       Col21,
       Col22,
       Col23,
       Col24,
       Col25,
       Col26,
       Col27,
       Col28,
       Col29,
       Col30,
       Col31,
       Col32,
       Col33,
       Col34,
       Col35,
       Col36,
       Col37,
       Col38,
       Col39,
       Col40,
       Col41,
       Col42,
       Col43,
       Col44,
       Col45,
       Col46,
       Col47,
       Col48,
       Col49,
       Col50,
       ColFoto
 FROM TabTestStats
WHERE Col50 IS NULL
AND 1 = (SELECT 1)
GO

--EXEC sp_helpstats TabTestStats
--GO

--DROP STATISTICS TabTestStats.[_WA_Sys_00000033_01892CED]
--GO
*/