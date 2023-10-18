/*
Check 55 - Report detailed information about plan cache

Description:
Report plan cache info

Estimated Benefit:
NA

Estimated Effort:
NA

Recommendation:
Quick recommendation:

Detailed recommendation:
*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck55') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck55

SELECT TOP 10000
       'Check 56 - Report detailed information about plan cache' AS [Info],
       *
INTO tempdb.dbo.tmpStatisticCheck55
FROM tempdb.dbo.tmpStatsCheckCachePlanData

SELECT * FROM tempdb.dbo.tmpStatisticCheck55
ORDER BY query_impact DESC