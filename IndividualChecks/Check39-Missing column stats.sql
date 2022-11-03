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

/* 
  If table tempdb.dbo.tmp_default_trace was not created on sp_GetStatisticInfo
  create it now 
*/
IF OBJECT_ID('tempdb.dbo.tmp_default_trace') IS NULL
BEGIN
  /* Declaring variables */
  DECLARE @filename NVARCHAR(1000),
          @bc INT,
          @ec INT,
          @bfn VARCHAR(1000),
          @efn VARCHAR(10);

  /* Get the name of the current default trace */
  SELECT @filename = [path]
  FROM sys.traces 
  WHERE is_default = 1;

  IF @@ROWCOUNT > 0
  BEGIN
    /* Rip apart file name into pieces */
    SET @filename = REVERSE(@filename);
    SET @bc = CHARINDEX('.', @filename);
    SET @ec = CHARINDEX('_', @filename) + 1;
    SET @efn = REVERSE(SUBSTRING(@filename, 1, @bc));
    SET @bfn = REVERSE(SUBSTRING(@filename, @ec, LEN(@filename)));

    -- Set filename without rollover number
    SET @filename = @bfn + @efn;

    /* Process all trace files */
    SELECT ftg.spid AS session_id,
           te.name AS event_name,
           ftg.EventSubClass AS event_subclass,
           ftg.TextData AS text_data,
           ftg.StartTime AS start_time,
           ftg.ApplicationName AS application_name,
           ftg.Hostname AS host_name,
           DB_NAME(ftg.databaseID) AS database_name,
           ftg.LoginName AS login_name
    INTO tempdb.dbo.tmp_default_trace
    FROM::fn_trace_gettable(@filename, DEFAULT) AS ftg
    INNER JOIN sys.trace_events AS te
    ON ftg.EventClass = te.trace_event_id
    WHERE te.name = 'Missing Column Statistics'

    CREATE CLUSTERED INDEX ix1 ON tempdb.dbo.tmp_default_trace(start_time)
  END
  ELSE
  BEGIN
    /* trace doesn't exist, creating an empty table */
    CREATE TABLE tempdb.dbo.tmp_default_trace
    (
      [spid] [int] NULL,
      [name] [nvarchar] (128) NULL,
      [event_subclass] [int] NULL,
      [text_data] [nvarchar] (max),
      [start_time] [datetime] NULL,
      [application_name] [nvarchar] (256) NULL,
      [host_name] [nvarchar] (256) NULL,
      [database_name] [nvarchar] (128) NULL,
      [login_name] [nvarchar] (256) NULL
    )
    CREATE CLUSTERED INDEX ix1 ON tempdb.dbo.tmp_default_trace(start_time)
  END
END


-- process all trace files
SELECT 'Check 39 - Missing column stats from default trace' AS [info],
       session_id,
       event_name,
       database_name,
       text_data,
       start_time AS start_datetime,
       application_name,
       host_name,
       login_name
INTO tempdb.dbo.tmpStatisticCheck39
FROM tempdb.dbo.tmp_default_trace
WHERE event_name = 'Missing Column Statistics'
AND database_name NOT IN ('master', 'model', 'msdb')
AND CONVERT(VARCHAR(MAX), text_data COLLATE Latin1_General_BIN2) NOT IN ('NO STATS:([j].[job_id])')
AND CONVERT(VARCHAR(MAX), text_data COLLATE Latin1_General_BIN2) NOT LIKE '%recursion%'

SELECT * FROM tempdb.dbo.tmpStatisticCheck39
ORDER BY start_datetime ASC, session_id;

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
OPTION (USE HINT ('FORCE_LEGACY_CARDINALITY_ESTIMATION'))
GO
-- This will trigger auto create statistic for ID
SELECT MAX(ID) FROM Tab1
OPTION (USE HINT ('FORCE_DEFAULT_CARDINALITY_ESTIMATION'))
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

