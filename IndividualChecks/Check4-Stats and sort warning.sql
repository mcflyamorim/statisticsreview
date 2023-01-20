/*
Check 4 - Is there a sort warning on default trace at the same time last update stats happened? 

< ---------------- Description ----------------- >
Check if auto update/create statistic StatMan query is spilling data to disk
In this check I'm looking at the default trace and I'm searching for sort warnings 
that may happened at the same time that a statistic was updated.

< -------------- What to look for and recommendations -------------- >
- Ideally, you don't want that the Sort operation of update stat/StatMan query to spill to tempdb.

- If this is happening, I would confirm that the auto update stat is indeed spilling data to 
disk by re-running the auto update statistic command and looking at actual exec query plan on profiler 
to confirm this is causing the sort warning.

-- On SQL Azure DB, we could use guide plan hints to force a MIN_MEMORY_GRANT to avoid the spill, but, 
on SQL on-premise there are not many alternatives to avoid it.

- My recommendation is to isolate this statistic to run in a maintenance window that you are ok to pay for the spill on tempdb.

- A not very elegant solution would be update the statistic with rowcount/pagecount with a big number before run the update stat
and run DBCC CHECKTABLE or another update statistics with rowcount/pagecount to reset the values after the update stats.

- Another option to try is to reduce MAXDOP or maybe create a dummy column store to enable sort to run on batch mode.

Note 1: If number_of_statistic_data_available_for_this_object is equal to 1, it is very likely this was related to a auto created statistic.
*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck4') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck4

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
    WHERE te.name = 'Sort Warnings'

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

BEGIN TRY
  SELECT 'Check 4 - Is there a sort Warning on default trace at the same time last update stats happened?' AS [info],
         a.database_name,
         a.table_name,
         a.stats_name,
         a.key_column_name,
         a.number_of_rows_at_time_stat_was_updated,
         (SELECT COUNT(*) 
          FROM tempdb.dbo.tmp_exec_history b 
          WHERE b.rowid = a.rowid) AS number_of_statistic_data_available_for_this_object,
         a.last_updated AS last_updated_datetime,
         Tab1.closest_sort_warning AS closest_sort_warning_datetime,
         DATEDIFF(MILLISECOND, Tab1.closest_sort_warning, a.last_updated) AS diff_of_update_stats_to_the_sort_warning_in_ms,
         CASE
           WHEN DATEDIFF(MILLISECOND, Tab1.closest_sort_warning, a.last_updated) BETWEEN 0 AND 10 THEN 'Sort Warning was VERY CLOSE (less than 10ms diff) to the update stats, high probability this was triggered by the update stats'
           WHEN DATEDIFF(MILLISECOND, Tab1.closest_sort_warning, a.last_updated) BETWEEN 11 AND 50 THEN 'Sort Warning was CLOSE (between than 11 and 50ms diff) to the update stats, high probability this was triggered by the update stats'
           WHEN DATEDIFF(MILLISECOND, Tab1.closest_sort_warning, a.last_updated) BETWEEN 51 AND 100 THEN 'Sort Warning was CLOSE (between than 51 and 100ms diff) to the update stats, high probability this was triggered by the update stats'
           WHEN DATEDIFF(MILLISECOND, Tab1.closest_sort_warning, a.last_updated) BETWEEN 101 AND 500 THEN 'Sort Warning was NEAR (between than 101 and 500ms diff) to the update stats, high probability this was triggered by the update stats'
           WHEN a.number_of_rows_at_time_stat_was_updated >= 1000000 AND DATEDIFF(MILLISECOND, Tab1.closest_sort_warning, a.last_updated) BETWEEN 501 AND 20000 THEN 'Sort Warning was not close (between than 501 and 20000ms diff) to the update stats, but, since number of rows on table is greater than 1mi, depending on how much time spill took, update stat may still be related to this Warning'
           ELSE 'Very unlikely this was related to the update stats, but, may be.'
         END comment_1,
         CASE
           WHEN Tab1.cnt > 1 THEN 'Found ' + CONVERT(VARCHAR(30), Tab1.cnt) + ' sort warning events happening at ' + CONVERT(VARCHAR(30), Tab1.closest_sort_warning, 21) + '. This probably means the update stats ran in parallel and there was multiple threads spilling data to tempdb.'
           ELSE NULL
         END comment_2
  INTO tempdb.dbo.tmpStatisticCheck4
  FROM tempdb.dbo.tmp_stats a
  CROSS APPLY (SELECT TOP 1 WITH TIES tmp_default_trace.start_time, COUNT(*) AS cnt 
               FROM tempdb.dbo.tmp_default_trace
               WHERE tmp_default_trace.start_time <= a.last_updated
               AND tmp_default_trace.event_name = 'Sort Warnings'
               GROUP BY tmp_default_trace.start_time
               ORDER BY tmp_default_trace.start_time DESC) AS Tab1(closest_sort_warning, cnt)
  WHERE (a.number_of_rows_at_time_stat_was_updated >= 10000 or a.is_lob = 1) /* Ignoring small tables unless is LOB*/
END TRY
BEGIN CATCH
  IF ERROR_NUMBER() = 535 /*The datediff function resulted in an overflow.*/
  BEGIN
    IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck4') IS NOT NULL
      DROP TABLE tempdb.dbo.tmpStatisticCheck4

    SELECT 'Check 4 - Is there a sort Warning on default trace at the same time last update stats happened?' AS [info],
            a.database_name,
            a.table_name,
            a.stats_name,
            a.key_column_name,
            a.number_of_rows_at_time_stat_was_updated,
            (SELECT COUNT(*) 
             FROM tempdb.dbo.tmp_exec_history b 
             WHERE b.rowid = a.rowid) AS number_of_statistic_data_available_for_this_object,
            a.last_updated AS last_updated_datetime,
            Tab1.closest_sort_warning AS closest_sort_warning_datetime,
            0 AS diff_of_update_stats_to_the_sort_warning_in_ms,
            'Unable to check datediff... check the diff manually' comment_1,
            CASE
              WHEN Tab1.cnt > 1 THEN 'Found ' + CONVERT(VARCHAR(30), Tab1.cnt) + ' sort warning events happening at ' + CONVERT(VARCHAR(30), Tab1.closest_sort_warning, 21) + '. This probably means the update stats ran in parallel and there was multiple threads spilling data to tempdb.'
              ELSE NULL
            END comment_2
      INTO tempdb.dbo.tmpStatisticCheck4
      FROM tempdb.dbo.tmp_stats a
      CROSS APPLY (SELECT TOP 1 WITH TIES tmp_default_trace.start_time, COUNT(*) AS cnt 
                   FROM tempdb.dbo.tmp_default_trace
                   WHERE tmp_default_trace.start_time <= a.last_updated
                   AND tmp_default_trace.event_name = 'Sort Warnings'
                   GROUP BY tmp_default_trace.start_time
                   ORDER BY tmp_default_trace.start_time DESC) AS Tab1(closest_sort_warning, cnt)
      WHERE (a.number_of_rows_at_time_stat_was_updated >= 10000 or a.is_lob = 1) /* Ignoring small tables unless is LOB*/
  END
  ELSE
  BEGIN
    DECLARE @ErrMessage NVARCHAR(MAX)
    SET @ErrMessage = ERROR_MESSAGE()
    RAISERROR ('Error_Message() = %s', 16, -1, @ErrMessage) WITH NOWAIT
  END
END CATCH

SELECT * FROM tempdb.dbo.tmpStatisticCheck4
ORDER BY number_of_rows_at_time_stat_was_updated DESC


/*
-- Script to show issue


USE Northwind
GO
DROP TABLE IF EXISTS TestSortWarning
GO
CREATE TABLE TestSortWarning (RowID INT NOT NULL, Col1 INT NOT NULL, Col2 VARCHAR(100))
GO
-- 1 second to run
DECLARE @RowID INT
SELECT @RowID = MAX(RowID) FROM TestSortWarning
INSERT INTO TestSortWarning WITH(TABLOCK) (RowID, Col1, Col2) 
SELECT TOP 7000000
       ISNULL(@RowID,0) + ROW_NUMBER() OVER(ORDER BY (SELECT 1)) AS RowID, 
       CHECKSUM(NEWID()) AS Col1,
       REPLICATE('X', 100)
  FROM master.dbo.spt_values a
 CROSS JOIN  master.dbo.spt_values b
 CROSS JOIN  master.dbo.spt_values c
 CROSS JOIN  master.dbo.spt_values d
OPTION (MAXDOP 8)
GO

-- Optional: Start profiler an capture actual plan
-- 7-9 seconds to run
SELECT COUNT(*) 
FROM TestSortWarning
WHERE Col2 IS NULL
OPTION (MAXDOP 1)
GO
-- Why is this taking so much time to run?
-- Bonus question
-- How could a MAXDOP 1 query have CXPACKET and CXCONSUMER waits?

-- 0 second to run
SELECT COUNT(*) 
FROM TestSortWarning
WHERE Col2 IS NULL
OPTION (MAXDOP 1)
GO


--sp_helpstats TestSortWarning
--GO
--DROP STATISTICS TestSortWarning.[_WA_Sys_00000003_75D84E76]
--GO


-- While query is running
EXEC sp_whoisactive @get_task_info = 2
GO
SELECT * FROM sys.dm_os_waiting_tasks
WHERE session_id >= 50
GO

--------------------------------------------
-- DON'T FORGET TO SET TEMPDB BACK TO SSD --
--------------------------------------------

-- SET TEMPDB on SSD
USE master
GO
ALTER DATABASE TempDB MODIFY FILE
(NAME = tempdev, FILENAME = 'D:\DBs\tempdb1_sql2019.mdf', SIZE = 100MB, FILEGROWTH = 1MB)
GO
ALTER DATABASE TempDB MODIFY FILE
(NAME = templog, FILENAME = 'D:\DBs\log_tempdb_sql2019.ldf', SIZE = 25MB , FILEGROWTH = 1MB)
GO
EXEC xp_cmdShell 'net stop MSSQL$SQL2019 && net start MSSQL$SQL2019'
GO
SELECT * FROM tempdb.dbo.sysfiles
GO

-- SET TEMPDB on flashdrive
USE master
GO
ALTER DATABASE TempDB MODIFY FILE
(NAME = tempdev, FILENAME = 'E:\DBs\tempdb1_sql2019.mdf', SIZE = 100MB, FILEGROWTH = 1MB)
GO
ALTER DATABASE TempDB MODIFY FILE
(NAME = templog, FILENAME = 'E:\DBs\log_tempdb_sql2019.ldf', SIZE = 25MB , FILEGROWTH = 1MB)
GO
EXEC xp_cmdShell 'net stop MSSQL$SQL2019 && net start MSSQL$SQL2019'
GO
SELECT * FROM tempdb.dbo.sysfiles
GO


*/