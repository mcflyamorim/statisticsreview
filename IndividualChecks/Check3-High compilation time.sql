/*
Check3 - High query plan compilation time
Description:
Check 3 - Do I have plans with high compilation time due to an auto update/create stats?
Check if statistic used in a query plan caused a long query plan compilation and optimization time.
If last update timestamp of statistic is close to the query plan creation time, then, it is very likely that the update/create stat caused a higher query plan creation duration.
Estimated Benefit:
High
Estimated Effort:
High
Recommendation:
Quick recommendation:
Review queries with high compilation due to auto update/create statistic.
Detailed recommendation:
- If this is happening, I recommend to enable auto update stats asynchronous option. With asynchronous statistics updates, queries compile with existing statistics even if the existing statistics are out-of-date. The Query Optimizer could choose a suboptimal query plan if statistics are out-of-date when the query compiles. Statistics are typically updated soon thereafter and queries that compile after the stats updates complete will benefit from using the updated statistics. 
- Another option to avoid the high compilation time is to deal with case by case and update the statistic causing the problem using no_recompute, then create a job to update it manually and don't rely on auto update stat. 
- Check if statistic causing high compilation time is new, if so, it may be an auto created stat and not auto updated, avoid those cases are harder as there is no easy way to disable auto update stats for a specific table/column. An option you have is to pre-create the statistic with no_recompute and update it using a job.
- If problem is happening with an important query that you need achieve a more predictable query response time, consider to use OPTION (KEEPFIXED PLAN) query hint.
Note 1: Keep in mind that this check is an attempt to identify those cases based on what we've in the plan cache. Ideally, if you want to identify all those cases you may want to create an extended event to capture sqlserver.auto_stats event with duration > 0 (or maybe 100ms). In my opinion, use the extended event is a safer and a good practice.

Note 2: https://techcommunity.microsoft.com/t5/azure-sql/diagnostic-data-for-synchronous-statistics-update-blocking/ba-p/386280 

Note 3: Ideally, this check should be executed several hours after the maintenance plan, as the idea is to capture long plan compilations due to the auto update/create stats.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck3') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck3

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
       CONVERT(NUMERIC(25, 2), last_elapsed_time / 1000.) AS last_elapsed_time_ms,
       CONVERT(NUMERIC(25, 4), (total_elapsed_time / execution_count) / 1000.) AS avg_elapsed_time_ms,
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

IF NOT EXISTS(SELECT * FROM tempdb.dbo.sysindexes where name = 'ixlast_updated')
BEGIN
  CREATE INDEX ixlast_updated
  ON tempdb.dbo.tmp_stats(last_updated) 
  INCLUDE(stats_name, database_name, table_name, current_number_of_rows)
END

IF OBJECT_ID('tempdb.dbo.#tmp1') IS NOT NULL
  DROP TABLE #tmp1

;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT  CONVERT(VARCHAR(800), query_hash, 1) AS query_hash,
        CONVERT(VARCHAR(800), plan_handle, 1) AS plan_handle, 
        creation_time AS creation_datetime,
        last_execution_time AS last_execution_datetime,
        last_elapsed_time_ms,
        avg_elapsed_time_ms,
        execution_count,
        avg_cpu_time_ms,
        total_cpu_time_ms,
        total_logical_reads,
        avg_logical_reads,
        total_logical_writes,
        avg_logical_writes,
        query_impact,
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
        cached_plan_size = x.value('sum(..//p:QueryPlan/@CachedPlanSize)', 'float'),
        /* 
           If there is only one execution, then, the compilation time can be calculated by
           checking the diff from the creation_time and last_execution_time.
           This is possible because creation_time is the time which the plan started creation
           and last_execution_time is the time which the plan started execution.
           So, for instance, considering the following:
           creation_time = "2022-11-09 07:56:19.123" 
           last_execution_time = "2022-11-09 07:56:26.937"
           This means, the plan started to be created at "2022-11-09 07:56:19.123" 
           and started execution at "2022-11-09 07:56:26.937", in other words, 
           it took 7813ms (DATEDIFF(ms, "2022-11-09 07:56:19.123" , "2022-11-09 07:56:26.937")) 
           to create the plan.
        */
        CASE 
         WHEN execution_count = 1
         THEN DATEDIFF(ms, creation_time, last_execution_time)
         ELSE NULL
        END AS compilation_time_from_dm_exec_query_stats,
        compile_time = x.value('sum(..//p:QueryPlan/@CompileTime)', 'float'),
        compile_cpu = x.value('sum(..//p:QueryPlan/@CompileCPU)', 'float'),
        compile_memory = x.value('sum(..//p:QueryPlan/@CompileMemory)', 'float'),
        exec_plan_creation_start_datetime = CONVERT(VARCHAR, creation_time, 21),
        associated_stats_update_datetime = (SELECT TOP 1 CONVERT(VARCHAR, a.last_updated, 21)
                                            FROM tempdb.dbo.tmp_stats AS a
                                            WHERE a.last_updated >= creation_time
                                            ORDER BY a.last_updated ASC),
        /* Creation time plus the compilation time in milliseconds is the datetime the plan finished to compile */
        exec_plan_creation_end_datetime = CONVERT(VARCHAR, DATEADD(ms, x.value('sum(..//p:QueryPlan/@CompileTime)', 'float'), creation_time), 21),
        associated_stats_name = (SELECT TOP 1 a.stats_name
                                 FROM tempdb.dbo.tmp_stats AS a
                                 WHERE a.last_updated >= creation_time
                                 ORDER BY a.last_updated ASC),
        statistic_associated_with_compile = (SELECT TOP 1
                                                    'Statistic ' + a.stats_name + 
                                                    ' on table ' + a.database_name + '.' + a.table_name + ' ('+ CONVERT(VARCHAR, a.current_number_of_rows) +' rows)' +
                                                    ' was updated about the same time (' + CONVERT(VARCHAR, a.last_updated, 21) + ') that the plan was created, that may be the reason of the high compile time.'
                                             FROM tempdb.dbo.tmp_stats AS a
                                             WHERE a.last_updated >= creation_time
                                             ORDER BY a.last_updated ASC),
        statement_text,
        statement_plan
INTO #tmp1
FROM #tmpdm_exec_query_stats qp
OUTER APPLY statement_plan.nodes('//p:Batch') AS Batch(x)
WHERE x.value('sum(..//p:QueryPlan/@CompileTime)', 'float') >= 200 /* Only plans taking more than 200ms to create */
OPTION (RECOMPILE);

SELECT 'Check 3 - Do I have plans with high compilation time due to an auto update/create stats?' AS [info], 
       * 
INTO tempdb.dbo.tmpStatisticCheck3
FROM #tmp1
WHERE 1=1
/* 
   Adding 50ms on exec_plan_creation_end_datetime because I've seen some cases where there 
   was a small diff between the last update stats datetime and the time it took to create 
   the plan. Maybe due to a rounding issue? Anyway, add 50ms should be enough to fix this.
*/
AND associated_stats_update_datetime <= DATEADD(ms, 50, exec_plan_creation_end_datetime)
AND CONVERT(VarChar(MAX), statement_plan) COLLATE Latin1_General_BIN2 LIKE '%' + REPLACE(REPLACE(associated_stats_name, '[', ''), ']', '') + '%'
/* OR associated_stats_name IS NULL */ -- Uncomment this if you want to see info about all plans, I mean, including plans where an associated stat was not found

SELECT * FROM tempdb.dbo.tmpStatisticCheck3
ORDER BY compile_time DESC

/*
  Script to test the check:

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

-- 5 seconds to run
INSERT INTO TabTestStats (Col1)
SELECT TOP 5000
       CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000)) AS Col1
  FROM sysobjects a, sysobjects b, sysobjects c, sysobjects d
GO

-- 4 seconds to run
SELECT COUNT(*) FROM TabTestStats
WHERE Col50 IS NULL
AND 1 = (SELECT 1)
GO

--EXEC sp_helpstats TabTestStats
--GO

--DROP STATISTICS TabTestStats.[_WA_Sys_00000033_01892CED]
--GO
*/