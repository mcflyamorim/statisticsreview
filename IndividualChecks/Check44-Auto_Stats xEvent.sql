/*
Check44 - Auto_Stats xEvent
Description:
Check 44 - Check if auto_stats extended event is being monitored
Consider the following query execution scenario:
You execute a query that triggers an automatic synchronous statistics create or update. While the sync statistics is running, your query waits (is essentially blocked) until the statistic is generated. Query optimizer will wait for statistic operation to complete before it compiles queries, that means, the query compilation and execution does not resume until the sync statistics operation completes. If the statistics update takes a long time (due to a large table and\or busy system), there is no easy way to determine root cause of the high duration. 
On SQL Server 2019 and newer versions you could use wait_on_sync_statistics_refresh, but this will provide a limited Information about what is happening.
If you need to need to have a more predictable query response time (who doesn't?), you'll need to watch out for those high compilation events caused by a sync statistic operation.
The only way to guarantee you'll identify all those cases it to capture sqlserver.auto_stats extended event.
Estimated Benefit:
Low
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Create a trace to monitor sqlserver.auto_stats extended event.
Detailed recommendation:
- If there are no extended events capturing sqlserver.auto_stats event, this is a finding.
- Create an alert to notify a DBA about long running auto create/update events.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck44') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck44

IF OBJECT_ID('tempdb.dbo.#tmp1') IS NOT NULL
  DROP TABLE #tmp1

SELECT QUOTENAME(session.name) AS session_name,
       event.event_id AS [id],
       event.package + '.' + event.name AS [name],
       event.package AS [package_name],
       targ.name AS [target_name],
       event.predicate AS [predicate_expression]
INTO #tmp1
FROM sys.server_event_sessions AS session
    LEFT OUTER JOIN sys.dm_xe_sessions AS running
        ON running.name = session.name,
     sys.server_event_session_events AS event
    INNER JOIN sys.dm_xe_objects AS object2
        ON event.name = object2.name
           AND object2.object_type = 'event'
    INNER JOIN sys.dm_xe_packages AS package2
        ON event.module = package2.module_guid
           AND event.package = package2.name
           AND package2.guid = object2.package_guid
    LEFT OUTER JOIN sys.server_event_session_targets AS targ
        ON targ.event_session_id = event.event_session_id
WHERE (session.event_session_id = event.event_session_id)
AND event.name = 'auto_stats'
AND session.name <> 'telemetry_xevents'

IF EXISTS (SELECT * FROM #tmp1)
BEGIN
		SELECT 'Check 44 - Check if auto_stats extended event is being monitored' AS [info],
         #tmp1.*,
         t3.Col1 AS wait_on_sync_statistics_refresh_time,
         'OK' AS comment
  INTO tempdb.dbo.tmpStatisticCheck44
  FROM #tmp1
  OUTER APPLY (SELECT t2.Col1 + ' of wait_on_sync_statistics_refresh have been recorded.' AS wait_on_sync_statistics_refresh_time
               FROM sys.dm_os_wait_stats 
               CROSS APPLY (SELECT DATEADD(second, (wait_time_ms / 1000) * -1, GETDATE())) AS t(Col1)
               CROSS APPLY (SELECT CONVERT(VARCHAR(4), DATEDIFF(mi,t.Col1, GETDATE()) / 60 / 24) + 'd ' + 
                                   CONVERT(VARCHAR(4), DATEDIFF(mi,t.Col1,GETDATE()) / 60 % 24) + 'hr ' + 
                                   CONVERT(VARCHAR(4), DATEDIFF(mi,t.Col1,GETDATE()) % 60) + 'min') AS t2(Col1)
               WHERE wait_type = 'WAIT_ON_SYNC_STATISTICS_REFRESH') AS t3(Col1)
END
ELSE
BEGIN
	 SELECT 'Check 44 - Check if auto_stats extended event is being monitored' AS [info],
          t3.Col1 AS wait_on_sync_statistics_refresh_time,
         'Warning - Could not find an extended event capturing auto_stats.' AS comment
  INTO tempdb.dbo.tmpStatisticCheck44
  FROM (SELECT 1 AS id) AS tab1
  OUTER APPLY (SELECT t2.Col1 + ' of wait_on_sync_statistics_refresh have been recorded.' AS wait_on_sync_statistics_refresh_time
               FROM sys.dm_os_wait_stats 
               CROSS APPLY (SELECT DATEADD(second, (wait_time_ms / 1000) * -1, GETDATE())) AS t(Col1)
               CROSS APPLY (SELECT CONVERT(VARCHAR(4), DATEDIFF(mi,t.Col1, GETDATE()) / 60 / 24) + 'd ' + 
                                   CONVERT(VARCHAR(4), DATEDIFF(mi,t.Col1,GETDATE()) / 60 % 24) + 'hr ' + 
                                   CONVERT(VARCHAR(4), DATEDIFF(mi,t.Col1,GETDATE()) % 60) + 'min') AS t2(Col1)
               WHERE wait_type = 'WAIT_ON_SYNC_STATISTICS_REFRESH') AS t3(Col1)

END;

SELECT * FROM tempdb.dbo.tmpStatisticCheck44
/*
< -------------- Recommendation ---------------- >

-- Create an extended event to capture sqlserver.auto_stats. The following script can help:

-- If event session already exists, then drop it.
IF EXISTS (SELECT 1 FROM sys.server_event_sessions 
           WHERE name = 'DBA_CaptureStatsInfo')
BEGIN
  DROP EVENT SESSION [DBA_CaptureStatsInfo] ON SERVER;
END
GO
-- Creating the event session
-- Change filename entry if "'C:\Temp\DBA_CaptureStatsInfo.xel'" is not appropriate
-- and please make sure you've at least 50GB available, or reduce/increase max_file_size 
-- property if you want to change it.
-- Also noticed that I'm capturing loaded events as well, if you want to capture ignore those
-- uncomment "--AND [duration]>(0)"
CREATE EVENT SESSION [DBA_CaptureStatsInfo] ON SERVER 
ADD EVENT sqlserver.auto_stats(SET collect_database_name=(1)
    ACTION(sqlserver.session_id,sqlserver.sql_text,sqlserver.tsql_frame)
    WHERE ([package0].[not_equal_uint64]([database_id],(2)) AND [package0].[not_equal_uint64]([database_id],(1))
           --AND [duration]>(0)
           ))
ADD TARGET package0.event_file(SET filename=N'C:\Temp\DBA_CaptureStatsInfo.xel',
                                    max_file_size=(500),
                                    max_rollover_files=(100))
WITH (MAX_MEMORY=8192 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,
      MAX_DISPATCH_LATENCY=30 SECONDS,MAX_EVENT_SIZE=0 KB,
      MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=OFF,STARTUP_STATE=OFF);
GO
-- Starting the event
ALTER EVENT SESSION [DBA_CaptureStatsInfo]
ON SERVER STATE = START;
*/