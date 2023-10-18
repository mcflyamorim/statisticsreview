/*
Check53 - Query plans with high modification counter
Description:
Check 53 - Plans with loaded statistics with high modification count
Check StatisticsInfo from query plan cache. Searching for plans using statistics with a high modification count.
Note 1: Requires SQL Server 2016 SP2+
Note 2:  Query impact is a calculated metric which represents the overall impact of the query on the server. This allows you to identify the queries which need most attention. The query impact is calculated from multiple metrics. The calculation is: log((TotalCPUTime × 3) + TotalLogicalReads + TotalLogicalWrites)
Estimated Benefit:
Medium
Estimated Effort:
High
Recommendation:
Quick recommendation:
Review reported execution plans and statistic usage. If necessary, update out of date statistics more often.
Detailed recommendation:
- If the number of modification count is too big, query plan may be inaccurate due to an out-of-date statistic. Make sure you've a maintenance plan updating those stats.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck53') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck53

;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT  TOP 1000
        *
INTO tempdb.dbo.tmpStatisticCheck53
FROM tempdb.dbo.tmpStatsCheckCachePlanData
WHERE number_of_referenced_stats > 0
ORDER BY number_of_referenced_stats DESC
OPTION (RECOMPILE);

SELECT 'Check 53 - Plans with loaded statistics with high modification count' AS [info],
       *
FROM tempdb.dbo.tmpStatisticCheck53
ORDER BY sum_modification_count_for_all_used_stats DESC