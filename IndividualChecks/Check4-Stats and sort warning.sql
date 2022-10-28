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

IF OBJECT_ID('tempdb.dbo.#tmpCheckSortWarning') IS NOT NULL
  DROP TABLE #tmpCheckSortWarning

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

-- process all trace files
SELECT ftg.spid,
       te.name,
       ftg.EventSubClass,
       ftg.StartTime,
       ftg.ApplicationName,
       ftg.Hostname,
       DB_NAME(ftg.databaseID) AS DBName,
       ftg.LoginName
INTO #tmpCheckSortWarning
FROM::fn_trace_gettable(@filename, DEFAULT) AS ftg
    INNER JOIN sys.trace_events AS te
        ON ftg.EventClass = te.trace_event_id
WHERE te.name = 'Sort Warnings'
ORDER BY ftg.StartTime ASC, ftg.spid;

CREATE CLUSTERED INDEX ix1 ON #tmpCheckSortWarning(StartTime)

BEGIN TRY
  SELECT 'Check 4 - Is there a sort Warning on default trace at the same time last update stats happened?' AS [info],
         a.database_name,
         a.table_name,
         a.stats_name,
         a.key_column_name,
         a.number_of_rows_at_time_stat_was_updated,
         a.last_updated AS last_updated_datetime,
         (SELECT COUNT(*) 
          FROM tempdb.dbo.tmp_exec_history b 
          WHERE b.rowid = a.rowid) AS number_of_statistic_data_available_for_this_object,
         Tab1.closest_sort_warning AS closest_sort_warning_datetime,
         DATEDIFF(MILLISECOND, Tab1.closest_sort_warning, a.last_updated) AS diff_of_update_stats_to_the_sort_warning_in_ms,
         CASE 
           WHEN DATEDIFF(MILLISECOND, Tab1.closest_sort_warning, a.last_updated) BETWEEN 0 AND 10 THEN 'Sort Warning was VERY CLOSE (less than 10ms diff) to the update stats, very high chances this was triggered by the update stats'
           WHEN DATEDIFF(MILLISECOND, Tab1.closest_sort_warning, a.last_updated) BETWEEN 11 AND 50 THEN 'Sort Warning was CLOSE (between than 11 and 50ms diff) to the update stats, still very high chances this was triggered by the update stats'
           WHEN DATEDIFF(MILLISECOND, Tab1.closest_sort_warning, a.last_updated) BETWEEN 51 AND 100 THEN 'Sort Warning was CLOSE (between than 51 and 100ms diff) to the update stats, high chances this was triggered by the update stats'
           WHEN DATEDIFF(MILLISECOND, Tab1.closest_sort_warning, a.last_updated) BETWEEN 101 AND 500 THEN 'Sort Warning was NEAR (between than 101 and 500ms diff) to the update stats, high chances this was triggered by the update stats'
           WHEN a.number_of_rows_at_time_stat_was_updated >= 1000000 AND DATEDIFF(MILLISECOND, Tab1.closest_sort_warning, a.last_updated) BETWEEN 501 AND 20000 THEN 'Sort Warning was not close (between than 501 and 20000ms diff) to the update stats, but, since number of rows on table is greater than 1mi, depending on how much time spill took, update stat may still be related to this Warning'
           ELSE 'Very unlikely this was related to the update stats, but, may be.'
         END comment_1
  INTO tempdb.dbo.tmpStatisticCheck4
  FROM tempdb.dbo.tmp_stats a
  CROSS APPLY (SELECT TOP 1 StartTime FROM #tmpCheckSortWarning
                WHERE #tmpCheckSortWarning.StartTime <= a.last_updated
                ORDER BY StartTime DESC) AS Tab1(closest_sort_warning)
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
             a.last_updated AS last_updated_datetime,
             (SELECT COUNT(*) 
              FROM tempdb.dbo.tmp_exec_history b 
              WHERE b.rowid = a.rowid) AS number_of_statistic_data_available_for_this_object,
             Tab1.closest_sort_warning AS closest_sort_warning_datetime,
             0 AS diff_of_update_stats_to_the_sort_warning_in_ms,
             'Unable to check datediff... check the diff manually' comment_1
      INTO tempdb.dbo.tmpStatisticCheck4
      FROM tempdb.dbo.tmp_stats a
      CROSS APPLY (SELECT TOP 1 StartTime FROM #tmpCheckSortWarning
                    WHERE #tmpCheckSortWarning.StartTime <= a.last_updated
                    ORDER BY StartTime DESC) AS Tab1(closest_sort_warning)
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