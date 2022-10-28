/*
Check 39 - Missing column stats from default trace

< ---------------- Description ----------------- >
Check if there are statistic but no histogram.

This can lead to poor cardinality estimations and weird situations 
as queries that require the empty statistic, will show [Columns With No Statistics] 
warning on execution plans, even with auto create/update statistic enabled.

< -------------- What to look for and recommendations -------------- >
- Run DBCC SHOW_STATISTICS command to confirm stat exist and is empty.

- If a statistic exist with an empty histogram, queries using this table will have poor 
cardinality estimates and show [Columns With No Statistics] warning on execution plans. 
Remove this statistic, or update it with fullscan or sample.

Note 1: Legacy cardinality estimator doesn't auto-create statistics for queries
with MIN/MAX and will show warning for missing stats on columns used on MIN/MAX, 
check "Check39-Missing column stats.sql" file for an example of this.

Note 2: I prefer xEvents to XML/PlanCache, so, you should consider to create a xEvent 
to capture "sqlserver.missing_column_statistics" event.
*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ANSI_WARNINGS ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck39') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck39

-- Declare variables
DECLARE @filename NVarChar(1000);
DECLARE @bc INT;
DECLARE @ec INT;
DECLARE @bfn VarChar(1000);
DECLARE @efn VarChar(10);

-- Get the name of the current default trace
SELECT @filename = CAST(value AS NVarChar(1000))
FROM::fn_trace_getinfo(DEFAULT)
WHERE traceid = 1
      AND property = 2;

-- rip apart file name into pieces
SET @filename = REVERSE(@filename);
SET @bc = CHARINDEX('.', @filename);
SET @ec = CHARINDEX('_', @filename) + 1;
SET @efn = REVERSE(SUBSTRING(@filename, 1, @bc));
SET @bfn = REVERSE(SUBSTRING(@filename, @ec, LEN(@filename)));

-- set filename without rollover number
SET @filename = @bfn + @efn;

IF @filename <> ''
BEGIN
  -- process all trace files
  SELECT 'Check 39 - Missing column stats from default trace' AS [info],
         ftg.spid,
         te.name,
         DB_NAME(ftg.DatabaseID) AS database_name,
         ftg.TextData AS text_data,
         ftg.StartTime AS start_datetime,
         ftg.ApplicationName AS application_name,
         ftg.Hostname AS host_name,
         ftg.LoginName AS login_name
  INTO tempdb.dbo.tmpStatisticCheck39
  FROM::fn_trace_gettable(@filename, DEFAULT) AS ftg
      INNER JOIN sys.trace_events AS te
          ON ftg.EventClass = te.trace_event_id
  WHERE te.name = 'Missing Column Statistics'
  AND DB_NAME(ftg.DatabaseID) NOT IN ('tempdb', 'master', 'model', 'msdb')
  AND CONVERT(VARCHAR(MAX), ftg.TextData COLLATE Latin1_General_BIN2) NOT IN ('NO STATS:([j].[job_id])')
  AND CONVERT(VARCHAR(MAX), ftg.TextData COLLATE Latin1_General_BIN2) NOT LIKE '%recursion%'

  SELECT * FROM tempdb.dbo.tmpStatisticCheck39
  ORDER BY start_datetime ASC, spid;
END
ELSE
BEGIN
-- process all trace files
  SELECT 'Check 39 - Missing column stats from default trace' AS [info]
  INTO tempdb.dbo.tmpStatisticCheck39

  SELECT * FROM tempdb.dbo.tmpStatisticCheck39
END
/*
IF OBJECT_ID('Tab1') IS NOT NULL
  DROP TABLE Tab1
GO
CREATE TABLE Tab1(ID INT, Col1 VARCHAR(200))
GO
INSERT INTO Tab1(ID, Col1) VALUES(1, 'Fabiano')
INSERT INTO Tab1(ID, Col1) VALUES(2, 'Amorim')
GO
-- This will show missing statistic warning for ID
SELECT MAX(ID) FROM Tab1
OPTION (USE HINT ('FORCE_LEGACY_CARDINALITY_ESTIMATION'), RECOMPILE)
GO
-- This will trigger auto create statistic for ID
SELECT MAX(ID) FROM Tab1
OPTION (USE HINT ('FORCE_DEFAULT_CARDINALITY_ESTIMATION'), RECOMPILE)
GO
*/

/*
-- The following query can be used to identify ColumnsWithNoStatistics
-- from plan cache... But, it may take a lot of time to run in 
-- servers with large plan cache.

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb.dbo.#tmpdm_exec_query_stats_check') IS NOT NULL
  DROP TABLE #tmpdm_exec_query_stats_check

SELECT plan_handle,
       statement_start_offset,
       statement_end_offset,
       creation_time,
       last_execution_time,
       execution_count
INTO #tmpdm_exec_query_stats_check
FROM sys.dm_exec_query_stats
CREATE CLUSTERED INDEX ix1 ON #tmpdm_exec_query_stats_check(plan_handle)

;WITH XMLNAMESPACES ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
SELECT qs.plan_handle, 
       TabPlanXML.qXML AS StatementPlan,
       CONVERT(XML, Tab1.Col1) AS FullCommandText,
       CONVERT(XML, Tab2.Col1) AS StatementText,
       creation_time,
       last_execution_time,
       execution_count
FROM #tmpdm_exec_query_stats_check qs
OUTER APPLY sys.dm_exec_sql_text(qs.plan_handle) st
OUTER APPLY (SELECT CHAR(13)+CHAR(10) + st.text + CHAR(13)+CHAR(10) FOR XML RAW, ELEMENTS) AS Tab1(Col1)
OUTER APPLY (SELECT CHAR(13)+CHAR(10) + 
                    ISNULL(
                        NULLIF(
                            SUBSTRING(
                              st.text, 
                              qs.statement_start_offset / 2, 
                              CASE WHEN qs.statement_end_offset < qs.statement_start_offset 
                               THEN 0
                              ELSE( qs.statement_end_offset - qs.statement_start_offset ) / 2 END + 2
                            ), ''
                        ), st.text
                    ) + CHAR(13)+CHAR(10) + CHAR(13)+CHAR(10) FOR XML RAW, ELEMENTS) AS Tab2(Col1)
OUTER APPLY sys.dm_exec_text_query_plan(qs.plan_handle,
                                        qs.statement_start_offset,
                                        qs.statement_end_offset) AS detqp
OUTER APPLY (SELECT CONVERT(XML, detqp.query_plan) AS qXML) AS TabPlanXML
WHERE TabPlanXML.qXML.exist('//p:ColumnsWithNoStatistics') = 1
ORDER BY execution_count DESC;
GO

*/

