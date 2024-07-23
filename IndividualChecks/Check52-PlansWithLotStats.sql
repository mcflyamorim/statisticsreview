/*
Check52 - Query plans using a lot of statistics
Description:
Check 52 - Plans with lot of loaded statistics
Check StatisticsInfo from query plan cache.
Note 1: Requires SQL Server 2016 SP2+
Note 2: Query impact is a calculated metric which represents the overall impact of the query on the server. This allows you to identify the queries which need most attention. The query impact is calculated from multiple metrics. The calculation is: log((TotalCPUTime × 3) + TotalLogicalReads + TotalLogicalWrites)
Estimated Benefit:
Medium
Estimated Effort:
High
Recommendation:
Quick recommendation:
Review reported execution plans and statistic usage.
Detailed recommendation:
- There is a high relation between the number of statistics and the "Compilation Time"/"Statement Optimization Early Abort Reason". Check if all stats are really needed as the more stats you have, higher will be the compilation time, which will also increase chances of a time-out plan.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('dbo.tmpStatisticCheck52') IS NOT NULL
  DROP TABLE dbo.tmpStatisticCheck52

;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT  TOP 1000
        *
INTO dbo.tmpStatisticCheck52
FROM dbo.tmpStatsCheckCachePlanData
WHERE number_of_referenced_stats > 0
ORDER BY number_of_referenced_stats DESC
OPTION (RECOMPILE);

SELECT 'Check 52 - Return query plans with lot of loaded stats' AS [info],
       *
FROM dbo.tmpStatisticCheck52
ORDER BY number_of_referenced_stats  DESC