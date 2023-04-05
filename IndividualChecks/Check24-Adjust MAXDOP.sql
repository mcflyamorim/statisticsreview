/*
Check24 - Adjust MAXDOP
Description:
Check 24 - Check if it may be good to adjust MAXDOP on UPDATE STATISTIC command
This is checking if MAXDOP at instance is lower than available CPUs and recommend to increate MAXDOP.
Estimated Benefit:
High
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Consider to use MAXDOP on update statistics commands.
Detailed recommendation:
- If table is too big (over a million rows), it may be a good idea to specify MAXDOP to increase number of CPUs available on update stats command. Default is to use whatever is specified on MAXDOP at the instance level.
Note: MAXDOP option is only available on SQL Server 2014 (SP3), 2016 (SP2), 2017 (CU3) and higher builds.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck24') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck24

DECLARE @cpucount INT, @maxdop INT
	SELECT @cpucount = COUNT(cpu_id)
	FROM sys.dm_os_schedulers
	WHERE scheduler_id < 255 AND parent_node_id < 64
 AND status = 'VISIBLE ONLINE'

SELECT @maxdop = CONVERT(INT, value)
FROM sys.configurations
WHERE name = 'max degree of parallelism';

DECLARE @MaxNumberofRows BIGINT
SELECT 'Check 24 - Check if it may be good to adjust MAXDOP on UPDATE STATISTIC command' AS [info],
       database_name, 
       MAX(current_number_of_rows) AS max_number_of_rows_in_a_table,
       COUNT(CASE WHEN current_number_of_rows >= 1000000 /*1mi*/ THEN 1 ELSE NULL END) AS number_of_tables_with_more_than_1mi_rows,
       CASE 
         WHEN MAX(current_number_of_rows) >= 1000000 /*1mi*/
          AND @maxdop < @cpucount
         THEN 'Warning - Database ' + database_name + 
              ' has ' + 
              CONVERT(VarChar, COUNT(CASE WHEN current_number_of_rows >= 1000000 /*1mi*/ THEN 1 ELSE NULL END)) + 
              ' tables with more than 1mi rows. Update stats is currently running with MAXDOP of ' +
              CONVERT(VarChar, @maxdop) + 
              ' and there are ' +
              CONVERT(VarChar, @cpucount) + 
              ' CPUs available.' +  
              ' Consider to increase MAXDOP on UPDATE STATISTICS command to speed up the update at cost of use more CPU.'
         ELSE 'OK'
       END AS [comment]
INTO tempdb.dbo.tmpStatisticCheck24
FROM tempdb.dbo.tmp_stats a
GROUP BY database_name

SELECT * FROM tempdb.dbo.tmpStatisticCheck24