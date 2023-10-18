/* 
Check51 - Maintenance time window
Description:
Check 51 - Maintenance time window 
Tries to identify what is the maintenance time window based on the number of statistics updated in an hour interval.
Estimated Benefit:
Low
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Review recommendation and if possible, run review a few hours after maintenance window.
Detailed recommendation:
- The bigger the difference between the maintenance window EndTime to the execution of sp_GetStatisticInfo the better the chances of more interesting data the checks will return. For instance, considering an environment with maintenance ending at 4am, if you run sp_GetStatisticInfo at 1am, you'll have more chances to capture auto-updated statistic info, but, if you run it at 8am, you'll miss the whole day worthy data.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck51') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck51

;WITH CTE_1
AS
(
  SELECT CONVERT(VARCHAR(13), last_updated, 120) + ':00:00' AS t,
         COUNT(*) AS Cnt
  FROM tempdb.dbo.tmp_stats
  WHERE last_updated >= DATEADD(month, -1, GETDATE()) /* only considering stats updated within last month */
  GROUP BY CONVERT(VARCHAR(13), last_updated, 120)
  HAVING COUNT(*) > 5
),
CTE_2
AS
(
  SELECT t,
         Cnt,
         ROW_NUMBER() OVER(ORDER BY CONVERT(DATETIME, t)) AS rn,
         AVG(Cnt) OVER() AS cAVG
  FROM CTE_1
),
CTE_3
AS
(
  SELECT *, 
         rn - ROW_NUMBER() OVER(ORDER BY (SELECT 1)) AS r
  FROM CTE_2
  WHERE 1=1
  AND Cnt > cAVG
)
SELECT TOP 1
       'Check 51 - Maintenance time window' AS [info],
       MIN(t) AS start_datetime,
       MAX(t) AS end_datetime,
       DATEDIFF(HOUR, MIN(t), MAX(t)) + 1 AS number_of_hours,
       SUM(Cnt) AS number_of_updated_stats,
       (SELECT MAX(t) FROM CTE_3) AS max_end_datetime
INTO tempdb.dbo.tmpStatisticCheck51
FROM CTE_3
GROUP BY r
ORDER BY SUM(Cnt) - (SELECT AVG(Cnt) FROM CTE_3) DESC

SELECT * FROM tempdb.dbo.tmpStatisticCheck51