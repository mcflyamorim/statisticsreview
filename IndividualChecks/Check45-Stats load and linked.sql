/* 
Check45 - Statistics load and linked-server
Description:
Check 45 - Check if there are too many linked server query calls to sp_table_statistics2_rowset
Every time a linked server SQL Server query compiles, it calls sp_table_statistics2_rowset to load SQL Server statistics. Depending on the number of calls and resultset (with statistics info) it may take a while to run it. I've seen cases with SOSHOST_MUTEX waits when there was more than 200 simultaneously calls for sp_table_statistics2_rowset.
In this check, I'm looking at plan cache to identify how many calls per minute we've for sp_table_statistics2_rowset
Estimated Benefit:
Medium
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Identify linked server queries calling sp_table_statistics2_rowset and reduce number of compilations/requests.
Detailed recommendation:
- Try to identify linked servers and source queries calling sp_table_statistics2_rowset and if possible, reduce number of compilations (avoid ad-hoc queries). 
- Reduce number of statistics on table to reduce network traffic between linked servers. I would be concerned with a [Executions per minute] greater than 100.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck45') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck45

SELECT 'Check 45 - Check if there are too many linked server query calls to sp_table_statistics2_rowset' AS [info],
       ISNULL(MAX(dm_exec_procedure_stats.execution_count),0) AS [total_execution_count],
       ISNULL(CONVERT(NUMERIC(25, 2), CONVERT(NUMERIC(25, 2), MAX(dm_exec_procedure_stats.execution_count)) / 
              CASE 
                WHEN DATEDIFF(MINUTE, MIN(dm_exec_query_stats.creation_time), MAX(dm_exec_query_stats.last_execution_time)) = 0 
                THEN 1
                ELSE DATEDIFF(MINUTE, MIN(dm_exec_query_stats.creation_time), MAX(dm_exec_query_stats.last_execution_time))
              END),0) AS executions_per_minute
INTO tempdb.dbo.tmpStatisticCheck45
FROM sys.dm_exec_procedure_stats
INNER JOIN sys.dm_exec_query_stats
ON dm_exec_query_stats.plan_handle = dm_exec_procedure_stats.plan_handle
WHERE OBJECT_NAME(dm_exec_procedure_stats.object_id) = 'sp_table_statistics2_rowset'

SELECT * FROM tempdb.dbo.tmpStatisticCheck45