USE [master];
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'sp_GetStatisticInfo')
	EXEC ('CREATE PROC dbo.sp_GetStatisticInfo AS SELECT 1')
GO

ALTER PROC dbo.sp_GetStatisticInfo
(
  @database_name_filter NVARCHAR(200) = NULL, /* By default I'm collecting information about all DBs */
  @refreshdata BIT = 0 /* 1 to force drop/create of statistics tables, 0 will skip table creation if they already exists */
)
/*
-------------------------------------------------------------------------------
|  _____ _        _   _     _   _           ______           _                |
| /  ___| |      | | (_)   | | (_)          | ___ \         (_)               |
| \ `--.| |_ __ _| |_ _ ___| |_ _  ___ ___  | |_/ /_____   ___  _____      __ |
|  `--. \ __/ _` | __| / __| __| |/ __/ __| |    // _ \ \ / / |/ _ \ \ /\ / / |
| /\__/ / || (_| | |_| \__ \ |_| | (__\__ \ | |\ \  __/\ V /| |  __/\ V  V /  |
| \____/ \__\__,_|\__|_|___/\__|_|\___|___/ \_| \_\___| \_/ |_|\___| \_/\_/   |
|                                           __                                |
|                                  |_     |__ |_ . _  _  _    /\  _  _  _. _  |
|                                  |_)\/  |(_||_)|(_|| )(_)  /--\|||(_)| |||| |
-------------------------------------------------------------------------------

sp_GetStatisticInfo - November 2022 (v1)

Fabiano Amorim
http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com

For help and more information, visit https://github.com/mcflyamorim/StatisticsReview

How to use:
Collect statistic information for all DBs:
  EXEC sp_GetStatisticInfo @database_name_filter = NULL

Collect statistic information for Northwind DB:
  EXEC sp_GetStatisticInfo @database_name_filter = 'Northwind', @refreshdata = 1

Credit: 
Some checks and scripts were used based on 
Brent Ozar sp_blitz scripts, MS Tiger team BP, Glenn Berry's diagnostic queries, Kimberly Tripp queries
and probably a lot of other SQL community folks out there, so, a huge kudos for SQL community.

Important notes and pre-requisites:
 * Found a bug or want to change something? Please feel free to create an issue on https://github.com/mcflyamorim/StatisticsReview
   or, you can also e-mail (really? I didn't know people were still using this.) me at fabianonevesamorim@hotmail.com
 * I'm using unsupported/undocumented TF 2388 to check statistic lead column type.
 * Depending on the number of statistics, the PS script to generate the excel file may use a lot (a few GBs) of memory.
 * You should know about it, but I'm going to say it anyways:
   Before implementing any trace flag in a production environment, carefully review all Microsoft 
   information and recommendations and learn what you can from other reliable sources. 
   Microsoft recommends that you thoroughly test any trace flags that you plan to implement in a 
   production environment before enabling them in that environment. 
   Trace flags can have unpredictable consequences and should be deployed with care.

Known issues and limitations:
 * Not tested and not support on Azure SQL DBs, Amazon RDS and Managed Instances (I’m planning to add support for this in a new release).
 * As for v1, there are no specific checks and validations for Memory-Optimized Tables. (I'm planning to add support for this in a new release).
 * Tables with Clustered ColumnStore index, may fail to report index usage information. (I still have to test this and find a workaround, 
   should be easy to fix, but, did't dit it yet)
 * SQL Server creates and maintains temporary statistics in tempdb for read-only DBs, 
   snapshots or read-only AG replicas. 
   I'm not checking those yet, but, I'm planing to support it in a new release.

Disclaimer:
This code and information are provided "AS IS" without warranty of any kind, either expressed or implied.
Furthermore, the author shall not be liable for any damages you may sustain by using this information, whether direct, 
indirect, special, incidental or consequential, even if it has been advised of the possibility of such damages.
	
License:
Pretty much free to everyone and to do anything you'd like as per MIT License - https://en.wikipedia.org/wiki/MIT_License

With all love and care,
Fabiano Amorim

*/
AS
BEGIN
  SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET LOCK_TIMEOUT 60000; /*if I get blocked for more than 1 minute I'll quit, I don't want to wait or cause other blocks*/

  DECLARE @sqlmajorver       INT,
          @number_of_stats   BIGINT,
          @rowid             INT,
          @database_name     SYSNAME,
          @schema_name       SYSNAME,
          @table_name        SYSNAME,
          @stats_name        NVARCHAR(2000),
          @err_msg           NVARCHAR(4000),
          @sqlcmd            NVARCHAR(MAX) = N'',
          @sqlcmd_db         NVARCHAR(MAX) = N'',
          @sqlcmd_dbcc       NVARCHAR(MAX) = N'',
          @sqlcmd_dbcc_local NVARCHAR(MAX) = N'';

  /* If data already exists, skip the population, unless refresh was asked via @refreshdata */
  IF OBJECT_ID('tempdb.dbo.tmp_stats') IS NOT NULL
  BEGIN
    /* 
       I'm assuming data for all tables exists, but I'm only checking tmp_stats... 
       if you're not sure if this is ok, use @refreshdata = 1 to force the refresh and 
       table population
    */
    IF EXISTS(SELECT 1 FROM tempdb.dbo.tmp_stats) AND (@refreshdata = 0)
    BEGIN
			   SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Table with list of statistics already exists, I''ll reuse it and skip the code to populate the table.'
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      RETURN
    END
    ELSE
    BEGIN
      DROP TABLE tempdb.dbo.tmp_stats
      DROP TABLE tempdb.dbo.tmp_stat_header
      DROP TABLE tempdb.dbo.tmp_density_vector
      DROP TABLE tempdb.dbo.tmp_histogram
      DROP TABLE tempdb.dbo.tmp_stats_stream
      DROP TABLE tempdb.dbo.tmp_exec_history
    END
  END

  /* Clean up tables from a old execution */
  DECLARE @sql_old_table NVARCHAR(MAX)
  DECLARE @tmp_table_name NVARCHAR(MAX)

  IF OBJECT_ID('tempdb.dbo.#tmp_old_exec') IS NOT NULL
    DROP TABLE #tmp_old_exec

  SELECT [name] 
  INTO #tmp_old_exec
  FROM tempdb.sys.tables
  WHERE type = 'U'
  AND name LIKE'tmpStatisticCheck%'

  DECLARE c_old_exec CURSOR READ_ONLY FOR
      SELECT [name] FROM #tmp_old_exec
  OPEN c_old_exec

  FETCH NEXT FROM c_old_exec
  INTO @tmp_table_name
  WHILE @@FETCH_STATUS = 0
  BEGIN
    SET @sql_old_table = 'DROP TABLE tempdb.dbo.[' + @tmp_table_name + '];'; 
    EXEC (@sql_old_table)

    FETCH NEXT FROM c_old_exec
    INTO @tmp_table_name
  END
  CLOSE c_old_exec
  DEALLOCATE c_old_exec

  IF OBJECT_ID('tempdb.dbo.tmp_stats') IS NOT NULL
    DROP TABLE tempdb.dbo.tmp_stats
  IF OBJECT_ID('tempdb.dbo.tmp_stat_header') IS NOT NULL
    DROP TABLE tempdb.dbo.tmp_stat_header
  IF OBJECT_ID('tempdb.dbo.tmp_density_vector') IS NOT NULL
    DROP TABLE tempdb.dbo.tmp_density_vector
  IF OBJECT_ID('tempdb.dbo.tmp_histogram') IS NOT NULL
    DROP TABLE tempdb.dbo.tmp_histogram
  IF OBJECT_ID('tempdb.dbo.tmp_stats_stream') IS NOT NULL
    DROP TABLE tempdb.dbo.tmp_stats_stream
  IF OBJECT_ID('tempdb.dbo.tmp_exec_history') IS NOT NULL
    DROP TABLE tempdb.dbo.tmp_exec_history
  IF OBJECT_ID('tempdb.dbo.tmp_default_trace') IS NOT NULL
    DROP TABLE tempdb.dbo.tmp_default_trace

  /* 
    On "Check4-Stats and sort warning" and on "Check39-Missing column stats" I'm reading 
    data from the default trace. 
    Since the DBCC commands are captured on default trace and I'm about to run
    several of those commands, I'm saving a snapshot of current data of default 
    trace to use it later on Check4 and Check39.
    This is a good idea because after I run several DBCC commands here, I may lose
    some of those events I'm searching for.
  */
  /* Starting code to create a copy of default trace data */
		SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting code to create a copy of default trace data.'
  RAISERROR (@err_msg, 0, 0) WITH NOWAIT

  /* Declaring variables */
  DECLARE @filename NVARCHAR(1000),
          @bc INT,
          @ec INT,
          @bfn VARCHAR(1000),
          @efn VARCHAR(10),
          @sort_warning_rows INT,
          @missing_stats_rows INT;

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
    WHERE te.name IN ('Sort Warnings', 'Missing Column Statistics')

    SELECT @sort_warning_rows = COUNT(*)
    FROM tempdb.dbo.tmp_default_trace
    WHERE event_name = 'Sort Warnings'

    SELECT @missing_stats_rows = COUNT(*)
    FROM tempdb.dbo.tmp_default_trace
    WHERE event_name = 'Missing Column Statistics'

    CREATE CLUSTERED INDEX ix1 ON tempdb.dbo.tmp_default_trace(start_time)

		  SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Found ' + CONVERT(VARCHAR(200), @sort_warning_rows) + ' sort warning events on default trace.'
    RAISERROR (@err_msg, 0, 0) WITH NOWAIT

		  SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Found ' + CONVERT(VARCHAR(200), @missing_stats_rows) + ' missing column statistics events on default trace.'
    RAISERROR (@err_msg, 0, 0) WITH NOWAIT

		  SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished code to create a copy of default trace data.'
    RAISERROR (@err_msg, 0, 0) WITH NOWAIT
  END
  ELSE
  BEGIN
		  SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Default trace is not running.'
    RAISERROR (@err_msg, 0, 0) WITH NOWAIT

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
    
		  SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished code to create a copy of default trace data.'
    RAISERROR (@err_msg, 0, 0) WITH NOWAIT
  END
  /* Finished code to create a copy of default trace data */

  SET @err_msg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to collect cache plan info...'
  RAISERROR(@err_msg, 0, 42) WITH NOWAIT;

  /* Config params: */
  DECLARE @TOP BIGINT = 5000 /* By default, I'm only reading TOP 5k plans */

  IF OBJECT_ID('tempdb.dbo.#tmpdm_exec_query_stats') IS NOT NULL
    DROP TABLE #tmpdm_exec_query_stats
  
  DECLARE @total_elapsed_time BIGINT,
          @total_worker_time BIGINT,
          @total_logical_page_reads BIGINT,
          @total_physical_page_reads BIGINT,
          @total_logical_page_writes BIGINT,
          @total_execution_count BIGINT;

  SELECT  @total_worker_time = SUM(total_worker_time),
          @total_elapsed_time = SUM(total_elapsed_time),
          @total_logical_page_reads = SUM(total_logical_reads),
          @total_physical_page_reads = SUM(total_physical_reads),
          @total_logical_page_writes = SUM(total_logical_writes),
          @total_execution_count = SUM(execution_count)
  FROM sys.dm_exec_query_stats
  WHERE dm_exec_query_stats.total_worker_time > 0 /* Only plans with CPU time > 0ms */
  AND dm_exec_query_stats.query_plan_hash <> 0x0000000000000000
  AND NOT EXISTS(SELECT 1 
                   FROM sys.dm_exec_cached_plans
                   WHERE dm_exec_cached_plans.plan_handle = dm_exec_query_stats.plan_handle
                   AND dm_exec_cached_plans.cacheobjtype = 'Compiled Plan Stub') /*Ignoring AdHoc - Plan Stub*/
  OPTION (RECOMPILE);

  IF OBJECT_ID('tempdb.dbo.#tmpdm_exec_query_stats_indx') IS NOT NULL
    DROP TABLE #tmpdm_exec_query_stats_indx

  SELECT *
  INTO #tmpdm_exec_query_stats_indx 
  FROM sys.dm_exec_query_stats
  WHERE dm_exec_query_stats.total_worker_time > 0 /* Only plans with CPU time > 0ms */
  AND dm_exec_query_stats.query_plan_hash <> 0x0000000000000000
  AND NOT EXISTS(SELECT 1 
                   FROM sys.dm_exec_cached_plans
                   WHERE dm_exec_cached_plans.plan_handle = dm_exec_query_stats.plan_handle
                   AND dm_exec_cached_plans.cacheobjtype = 'Compiled Plan Stub') /*Ignoring AdHoc - Plan Stub*/
  OPTION (RECOMPILE);

  CREATE CLUSTERED INDEX ixquery_hash ON #tmpdm_exec_query_stats_indx(query_hash, last_execution_time)

  SELECT TOP (@TOP)
         CONVERT(INT, NULL) AS database_id,
         CONVERT(INT, NULL) AS object_id,
         CONVERT(sysname, NULL) AS object_name,
         query_hash,
         plan_count,
         plan_generation_num,
         ISNULL(t_dm_exec_query_stats.plan_handle, 0x) AS plan_handle,
         ISNULL(statement_start_offset, 0) AS statement_start_offset,
         ISNULL(statement_end_offset, 0) AS statement_end_offset,
         CONVERT(XML, NULL) AS statement_plan, 
         CONVERT(XML, NULL) AS statement_text,
         creation_time,
         last_execution_time,
         /*
           Query impact is a calculated metric which represents the overall impact of the query on the server. 
           This allows you to identify the queries which need most attention.
           It is calculated FROM a combination of metrics as follows: 
           QueryImpact = log((TotalCPUTime x 3) + TotalLogicalReads + TotalLogicalWrites)
         */
         CONVERT(NUMERIC(25, 2), LOG((total_worker_time * 3) + total_logical_reads + total_logical_writes)) AS query_impact,
         execution_count,
         CASE 
           WHEN @total_execution_count = 0 THEN 0
           ELSE CONVERT(NUMERIC(25, 2), (100. * execution_count) / @total_execution_count) 
         END AS execution_count_percent_over_total,
         execution_count / CASE WHEN DATEDIFF(MINUTE, creation_time, last_execution_time) = 0 THEN 1 ELSE DATEDIFF(MINUTE, creation_time, last_execution_time) END AS execution_count_per_minute,
         CONVERT(BIGINT, NULL) AS execution_count_current,
         CONVERT(BIGINT, NULL) AS execution_count_last_minute,

         CONVERT(NUMERIC(25, 4), (total_elapsed_time) /1000. /1000.) AS total_elapsed_time_sec,
         CASE 
           WHEN @total_elapsed_time = 0 THEN 0
           ELSE CONVERT(NUMERIC(25, 2), (100. * total_elapsed_time) / @total_elapsed_time) 
         END AS elapsed_time_sec_percent_over_total,
         CONVERT(NUMERIC(25, 4), (total_elapsed_time / execution_count) /1000. /1000.) AS avg_elapsed_time_sec,
         CONVERT(NUMERIC(25, 4), min_elapsed_time /1000. /1000.) AS min_elapsed_time_sec,
         CONVERT(NUMERIC(25, 4), max_elapsed_time /1000. /1000.) AS max_elapsed_time_sec,
         CONVERT(NUMERIC(25, 4), last_elapsed_time /1000. /1000.) AS last_elapsed_time_sec,

         CONVERT(NUMERIC(25, 4), (total_worker_time) /1000. /1000.) AS total_cpu_time_sec,
         CASE 
           WHEN @total_worker_time = 0 THEN 0
           ELSE CONVERT(NUMERIC(25, 2), (100. * total_worker_time) / @total_worker_time) 
         END AS cpu_time_sec_percent_over_total,
         CONVERT(NUMERIC(25, 4), (total_worker_time / execution_count) /1000. /1000.) AS avg_cpu_time_sec,
         CONVERT(NUMERIC(25, 4), min_worker_time /1000. /1000.) AS min_cpu_time_sec,
         CONVERT(NUMERIC(25, 4), max_worker_time /1000. /1000.) AS max_cpu_time_sec,
         CONVERT(NUMERIC(25, 4), last_worker_time /1000. /1000.) AS last_cpu_time_sec,

         total_logical_reads AS total_logical_page_reads,
         CASE 
           WHEN @total_logical_page_reads = 0 THEN 0
           ELSE CONVERT(NUMERIC(25, 2), (100. * total_logical_reads) / @total_logical_page_reads) 
         END AS logical_page_reads_percent_over_total,
         CONVERT(BIGINT, (total_logical_reads / execution_count)) AS avg_logical_page_reads,
         min_logical_reads AS min_logical_page_reads,
         max_logical_reads AS max_logical_page_reads,
         last_logical_reads AS last_logical_page_reads,

         CONVERT(NUMERIC(25, 4), total_logical_reads * 8 / 1024. / 1024.) AS total_logical_reads_gb,
         CASE 
           WHEN @total_logical_page_reads = 0 THEN 0
           ELSE CONVERT(NUMERIC(25, 2), (100. * total_logical_reads) / @total_logical_page_reads) 
         END AS logical_reads_gb_percent_over_total,
         CONVERT(NUMERIC(25, 4), CONVERT(BIGINT, (total_logical_reads / execution_count)) * 8 / 1024. / 1024.) AS avg_logical_reads_gb,
         CONVERT(NUMERIC(25, 4), min_logical_reads * 8 / 1024. / 1024.) AS min_logical_reads_gb,
         CONVERT(NUMERIC(25, 4), max_logical_reads * 8 / 1024. / 1024.) AS max_logical_reads_gb,
         CONVERT(NUMERIC(25, 4), last_logical_reads * 8 / 1024. / 1024.) AS last_logical_reads_gb,

         total_physical_reads AS total_physical_page_reads,
         CASE 
           WHEN @total_physical_page_reads = 0 THEN 0
           ELSE CONVERT(NUMERIC(25, 2), (100. * total_physical_reads) / @total_physical_page_reads) 
         END AS physical_page_reads_percent_over_total,
         CONVERT(BIGINT, (total_physical_reads / execution_count)) AS avg_physical_page_reads,
         min_physical_reads AS min_physical_page_reads,
         max_physical_reads AS max_physical_page_reads,
         last_physical_reads AS last_physical_page_reads,

         CONVERT(NUMERIC(25, 4), total_physical_reads * 8 / 1024. / 1024.) AS total_physical_reads_gb,
         CASE 
           WHEN @total_physical_page_reads = 0 THEN 0
           ELSE CONVERT(NUMERIC(25, 2), (100. * total_physical_reads) / @total_physical_page_reads) 
         END AS physical_reads_gb_percent_over_total,
         CONVERT(NUMERIC(25, 4), CONVERT(BIGINT, (total_physical_reads / execution_count)) * 8 / 1024. / 1024.) AS avg_physical_reads_gb,
         CONVERT(NUMERIC(25, 4), min_physical_reads * 8 / 1024. / 1024.) AS min_physical_reads_gb,
         CONVERT(NUMERIC(25, 4), max_physical_reads * 8 / 1024. / 1024.) AS max_physical_reads_gb,
         CONVERT(NUMERIC(25, 4), last_physical_reads * 8 / 1024. / 1024.) AS last_physical_reads_gb,

         total_logical_writes AS total_logical_page_writes,
         CASE 
           WHEN @total_logical_page_writes = 0 THEN 0
           ELSE CONVERT(NUMERIC(25, 2), (100. * total_logical_writes) / @total_logical_page_writes) 
         END AS logical_page_writes_percent_over_total,
         CONVERT(BIGINT, (total_logical_writes / execution_count)) AS avglogical_page_writes,
         min_logical_writes AS min_logical_page_writes,
         max_logical_writes AS max_logical_page_writes,
         last_logical_writes AS last_logical_page_writes,

         CONVERT(NUMERIC(25, 4), total_logical_writes * 8 / 1024. / 1024.) AS total_logical_writes_gb,
         CASE 
           WHEN @total_logical_page_writes = 0 THEN 0
           ELSE CONVERT(NUMERIC(25, 2), (100. * total_logical_writes) / @total_logical_page_writes)
         END AS logical_writes_gb_percent_over_total,
         CONVERT(NUMERIC(25, 4), CONVERT(BIGINT, (total_physical_reads / execution_count)) * 8 / 1024. / 1024.) AS avg_logical_writes_gb,
         CONVERT(NUMERIC(25, 4), min_logical_writes * 8 / 1024. / 1024.) AS min_logical_writes_gb,
         CONVERT(NUMERIC(25, 4), max_logical_writes * 8 / 1024. / 1024.) AS max_logical_writes_gb,
         CONVERT(NUMERIC(25, 4), last_logical_writes * 8 / 1024. / 1024.) AS last_logical_writes_gb,

         total_rows AS total_returned_rows,
         CONVERT(BIGINT, (total_rows / execution_count)) AS avg_returned_rows,
         min_rows AS min_returned_rows,
         max_rows AS max_returned_rows,
         last_rows AS last_returned_rows,
         CONVERT(NUMERIC(25, 4), dm_exec_cached_plans.size_in_bytes / 1024. / 1024.) AS cached_plan_size_mb
  INTO #tmpdm_exec_query_stats
  FROM (SELECT query_hash,
               COUNT(DISTINCT query_plan_hash)          AS plan_count,
               MAX(t_last_value.plan_handle)            AS plan_handle,
               MAX(t_last_value.statement_start_offset) AS statement_start_offset,
               MAX(t_last_value.statement_end_offset)   AS statement_end_offset,
               MAX(t_last_value.plan_generation_num)    AS plan_generation_num,
               MAX(t_last_value.creation_time)          AS creation_time,
               MAX(t_last_value.last_execution_time)    AS last_execution_time,
               SUM(execution_count)                     AS execution_count,
               SUM(total_worker_time)                   AS total_worker_time,
               MAX(t_last_value.last_worker_time)       AS last_worker_time,
               MIN(min_worker_time)                     AS min_worker_time,
               MAX(max_worker_time)                     AS max_worker_time,
               SUM(total_physical_reads)                AS total_physical_reads,
               MAX(t_last_value.last_physical_reads)    AS last_physical_reads,
               MIN(min_physical_reads)                  AS min_physical_reads,
               MAX(max_physical_reads)                  AS max_physical_reads,
               SUM(total_logical_writes)                AS total_logical_writes,
               MAX(t_last_value.last_logical_writes)    AS last_logical_writes,
               MIN(min_logical_writes)                  AS min_logical_writes,
               MAX(max_logical_writes)                  AS max_logical_writes,
               SUM(total_logical_reads)                 AS total_logical_reads,
               MAX(t_last_value.last_logical_reads)     AS last_logical_reads,
               MIN(min_logical_reads)                   AS min_logical_reads,
               MAX(max_logical_reads)                   AS max_logical_reads,
               SUM(total_elapsed_time)                  AS total_elapsed_time,
               MAX(t_last_value.last_elapsed_time)      AS last_elapsed_time,
               MIN(min_elapsed_time)                    AS min_elapsed_time,
               MAX(max_elapsed_time)                    AS max_elapsed_time,
               SUM(total_rows)                          AS total_rows,
               MAX(t_last_value.last_rows)              AS last_rows,
               MIN(min_rows)                            AS min_rows,
               MAX(max_rows)                            AS max_rows
        FROM #tmpdm_exec_query_stats_indx
        CROSS APPLY (SELECT TOP 1 plan_handle,
                                  statement_start_offset, 
                                  statement_end_offset,
                                  plan_generation_num,
                                  creation_time,
                                  last_execution_time, 
                                  last_worker_time, 
                                  last_physical_reads, 
                                  last_logical_writes, 
                                  last_logical_reads, 
                                  last_elapsed_time, 
                                  last_rows
                     FROM #tmpdm_exec_query_stats_indx AS b
                     WHERE b.query_hash = #tmpdm_exec_query_stats_indx.query_hash
                     ORDER BY last_execution_time DESC) AS t_last_value
        GROUP BY query_hash) AS t_dm_exec_query_stats
  INNER JOIN sys.dm_exec_cached_plans
  ON dm_exec_cached_plans.plan_handle = t_dm_exec_query_stats.plan_handle
  ORDER BY query_impact DESC
  OPTION (RECOMPILE);

  ALTER TABLE #tmpdm_exec_query_stats ADD CONSTRAINT pk_sp_getstatsinfo_tmpdm_exec_query_stats
  PRIMARY KEY (plan_handle, statement_start_offset, statement_end_offset)

  DECLARE @number_plans BIGINT,
          @query_hash   VARBINARY(64),
          @plan_handle  VARBINARY(64),
          @statement_start_offset BIGINT, 
          @statement_end_offset BIGINT,
          @i            BIGINT

  SELECT @number_plans = COUNT(*) 
  FROM #tmpdm_exec_query_stats

  SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to capture XML plan and statement text for cached plans. Found ' + CONVERT(VARCHAR(200), @number_plans) + ' plans on sys.dm_exec_query_stats.'
  RAISERROR (@err_msg, 0, 0) WITH NOWAIT

  SET @i = 1
  DECLARE c_plans CURSOR FORWARD_ONLY READ_ONLY FOR
      SELECT query_hash, plan_handle, statement_start_offset, statement_end_offset 
      FROM #tmpdm_exec_query_stats
  OPEN c_plans

  FETCH NEXT FROM c_plans
  INTO @query_hash, @plan_handle, @statement_start_offset, @statement_end_offset
  WHILE @@FETCH_STATUS = 0
  BEGIN
    BEGIN TRY
      SET @err_msg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Progress ' + '(' + CONVERT(VARCHAR(200), CONVERT(NUMERIC(25, 2), (CONVERT(NUMERIC(25, 2), @i) / CONVERT(NUMERIC(25, 2), @number_plans)) * 100)) + '%%) - ' 
                     + CONVERT(VARCHAR(200), @i) + ' of ' + CONVERT(VARCHAR(200), @number_plans)
      IF @i % 100 = 0
        RAISERROR (@err_msg, 0, 1) WITH NOWAIT

      ;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
      UPDATE #tmpdm_exec_query_stats SET database_id = detqp.dbid,
                                         object_id = detqp.objectid,
                                         object_name = OBJECT_NAME(detqp.objectid, detqp.dbid),
                                         statement_plan = CASE 
                                                            WHEN detqp.encrypted = 1 THEN '<?query ---- Plan is encrypted. ----?>'
                                                            /* If conversion of query_plan text to XML is not possible, return plan has a text.
                                                               One of most common reasons it may not able to convert the text to XML is due to the 
                                                               "XML datatype instance has too many levels of nested nodes. Maximum allowed depth is 128 levels." limitation.*/
                                                            WHEN detqp.query_plan IS NOT NULL AND t0.query_plan IS NULL THEN '<?query ---- ' + NCHAR(13) + NCHAR(10) + detqp.query_plan + NCHAR(13) + NCHAR(10) + ' ----?>'
                                                            ELSE t0.query_plan
                                                          END,
                                         statement_text = CASE detqp.encrypted WHEN 1 THEN '<?query ---- Stmt is encrypted. ----?>' ELSE t2.cStatement END
      FROM #tmpdm_exec_query_stats AS qs
      CROSS APPLY sys.dm_exec_text_query_plan(qs.plan_handle,
                                              qs.statement_start_offset,
                                              qs.statement_end_offset) AS detqp
    OUTER APPLY (SELECT TRY_CONVERT(XML, STUFF(detqp.query_plan,
                                               CHARINDEX(N'<BatchSequence>', detqp.query_plan),
                                               0,
                                               (
                                                   SELECT ISNULL(stat.c_data,'<?dm_exec_query_stats ---- QueryStats not found. ----?>') + 
                                                          ISNULL(attrs.c_data, '<?dm_exec_plan_attributes ---- PlanAttributes not found. ----?>')
                                                   FROM (
                                                           SELECT t_last_value.*
                                                           FROM #tmpdm_exec_query_stats_indx AS a
                                                           CROSS APPLY (SELECT TOP 1 b.*
                                                                        FROM #tmpdm_exec_query_stats_indx AS b
                                                                        WHERE b.query_hash = a.query_hash
                                                                        ORDER BY b.last_execution_time DESC) AS t_last_value
                                                           WHERE a.plan_handle = @plan_handle
                                                           AND a.statement_start_offset = @statement_start_offset
                                                           AND a.statement_end_offset = @statement_end_offset
                                                           FOR XML RAW('Stats'), ROOT('dm_exec_query_stats'), BINARY BASE64
                                                       ) AS stat(c_data)
                                                   OUTER APPLY (
                                                           SELECT pvt.*
                                                           FROM (
                                                               SELECT epa.attribute, epa.value
                                                               FROM sys.dm_exec_plan_attributes(@plan_handle) AS epa) AS ecpa   
                                                           PIVOT (MAX(ecpa.value) FOR ecpa.attribute IN ("set_options","objectid","dbid","dbid_execute","user_id","language_id","date_format","date_first","compat_level","status","required_cursor_options","acceptable_cursor_options","merge_action_type","is_replication_specific","optional_spid","optional_clr_trigger_dbid","optional_clr_trigger_objid","parent_plan_handle","inuse_exec_context","free_exec_context","hits_exec_context","misses_exec_context","removed_exec_context","inuse_cursors","free_cursors","hits_cursors","misses_cursors","removed_cursors","sql_handle")) AS pvt
                                                           FOR XML RAW('Attr'), ROOT('dm_exec_plan_attributes'), BINARY BASE64
                                                       ) AS attrs(c_data)
                                                   )
                                               ))) AS t0 (query_plan)
      OUTER APPLY t0.query_plan.nodes('//p:Batch') AS Batch(x)
      OUTER APPLY (SELECT COALESCE(Batch.x.value('(//p:StmtSimple/@StatementText)[1]', 'VarChar(MAX)'),
                                   Batch.x.value('(//p:StmtCond/@StatementText)[1]', 'VarChar(MAX)'),
                                   Batch.x.value('(//p:StmtCursor/@StatementText)[1]', 'VarChar(MAX)'),
                                   Batch.x.value('(//p:StmtReceive/@StatementText)[1]', 'VarChar(MAX)'),
                                   Batch.x.value('(//p:StmtUseDb/@StatementText)[1]', 'VarChar(MAX)')) AS query) AS t1
      OUTER APPLY (SELECT CONVERT(XML, ISNULL(CONVERT(XML, '<?query --' +
                                                              REPLACE
					                                                         (
						                                                         REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
						                                                         REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
						                                                         REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
							                                                         CONVERT
							                                                         (
								                                                         VARCHAR(MAX),
								                                                         N'--' + NCHAR(13) + NCHAR(10) + t1.query + NCHAR(13) + NCHAR(10) + NCHAR(13) + NCHAR(10) + '/* Note: Query text was retrieved from showplan XML, and may be truncated. */' + NCHAR(13) + NCHAR(10) + N'--' COLLATE Latin1_General_Bin2
							                                                         ),
							                                                         NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
							                                                         NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
							                                                         NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
						                                                         NCHAR(0),
						                                                         N'')
                                                               + '--?>'),
                                                    '<?query --' + NCHAR(13) + NCHAR(10) +
                                                    'Statement not found.' + NCHAR(13) + NCHAR(10) +
                                                    '--?>'))) AS t2 (cStatement)
      WHERE qs.plan_handle = @plan_handle
      AND qs.statement_start_offset = @statement_start_offset
      AND qs.statement_end_offset = @statement_end_offset

      /* If wasn't able to extract text from the query plan, try to get it from the very slow sys.dm_exec_sql_text DMF */
      IF EXISTS(SELECT 1 FROM #tmpdm_exec_query_stats AS qs
                 WHERE qs.plan_handle = @plan_handle
                 AND qs.statement_start_offset = @statement_start_offset
                 AND qs.statement_end_offset = @statement_end_offset
                 AND CONVERT(VARCHAR(MAX), qs.statement_text) LIKE '%Statement not found.%')
      BEGIN
        UPDATE #tmpdm_exec_query_stats SET database_id = st.dbid,
                                           object_id = st.objectid,
                                           object_name = OBJECT_NAME(st.objectid, st.dbid),
                                           statement_text = CASE st.encrypted WHEN 1 THEN '<?query ---- Stmt is encrypted. ----?>' ELSE t2.cStatement END
        FROM #tmpdm_exec_query_stats AS qs
        OUTER APPLY sys.dm_exec_sql_text(qs.plan_handle) st
        CROSS APPLY (SELECT ISNULL(
                                NULLIF(
                                    SUBSTRING(
                                      st.text, 
                                      (qs.statement_start_offset / 2) + 1,
                                      CASE WHEN qs.statement_end_offset < qs.statement_start_offset 
                                       THEN 0
                                      ELSE (qs.statement_end_offset - qs.statement_start_offset) / 2 END + 2
                                    ), ''
                                ), st.text
                            )) AS t1(Query)
        CROSS APPLY (SELECT TRY_CONVERT(XML, ISNULL(TRY_CONVERT(XML, 
                                                                '<?query --' +
                                                                REPLACE
					                                                           (
						                                                           REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
						                                                           REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
						                                                           REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
							                                                           CONVERT
							                                                           (
								                                                           VARCHAR(MAX),
								                                                           N'--' + NCHAR(13) + NCHAR(10) + t1.query + NCHAR(13) + NCHAR(10) + N'--' COLLATE Latin1_General_Bin2
							                                                           ),
							                                                           NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
							                                                           NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
							                                                           NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
						                                                           NCHAR(0),
						                                                           N'')
                                                                 + '--?>'),
                                                      '<?query --' + NCHAR(13) + NCHAR(10) +
                                                      'Could not render the query due to XML data type limitations.' + NCHAR(13) + NCHAR(10) +
                                                      '--?>'))) AS t2 (cStatement)
        WHERE qs.plan_handle = @plan_handle
        AND qs.statement_start_offset = @statement_start_offset
        AND qs.statement_end_offset = @statement_end_offset
      END
		  END TRY
		  BEGIN CATCH
			   --SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to work on plan [' + CONVERT(NVARCHAR(800), @plan_handle, 1) + ']. Skipping this plan.'
      --RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      --SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
      --RAISERROR (@err_msg, 0, 0) WITH NOWAIT
		  END CATCH

    SET @i = @i + 1
    FETCH NEXT FROM c_plans
    INTO @query_hash, @plan_handle, @statement_start_offset, @statement_end_offset
  END
  CLOSE c_plans
  DEALLOCATE c_plans

  SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to capture XML query plan and statement text for cached plans.'
  RAISERROR (@err_msg, 0, 0) WITH NOWAIT

  --SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to remove plans bigger than 2MB.'
  --RAISERROR (@err_msg, 0, 0) WITH NOWAIT

  --DECLARE @removed_plans INT = 0
  --DELETE FROM #tmpdm_exec_query_stats 
  --WHERE DATALENGTH(statement_plan) / 1024. > 2048 /*Ignoring big plans to avoid delay and issues when exporting it to Excel*/

  --SET @removed_plans = @@ROWCOUNT

  --SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to remove plans bigger than 2MB, removed ' + CONVERT(VARCHAR, ISNULL(@removed_plans, 0)) + ' plans.'
  --RAISERROR (@err_msg, 0, 0) WITH NOWAIT

  SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to collect data about last minute execution count.'
  RAISERROR (@err_msg, 0, 0) WITH NOWAIT

  /* Update execution_count_current with current number of executions */
  UPDATE #tmpdm_exec_query_stats SET execution_count_current = dm_exec_query_stats.execution_count
  FROM #tmpdm_exec_query_stats AS qs
  INNER JOIN sys.dm_exec_query_stats
  ON qs.plan_handle = dm_exec_query_stats.plan_handle
  AND qs.statement_start_offset = dm_exec_query_stats.statement_start_offset
  AND qs.statement_end_offset = dm_exec_query_stats.statement_end_offset

  /* Wait for 1 minute */
  WAITFOR DELAY '00:01:00.000'

  /* Update execution_count_last_minute with number of executions on last minute */
  UPDATE #tmpdm_exec_query_stats SET execution_count_last_minute = dm_exec_query_stats.execution_count - qs.execution_count_current
  FROM #tmpdm_exec_query_stats AS qs
  INNER JOIN sys.dm_exec_query_stats
  ON qs.plan_handle = dm_exec_query_stats.plan_handle
  AND qs.statement_start_offset = dm_exec_query_stats.statement_start_offset
  AND qs.statement_end_offset = dm_exec_query_stats.statement_end_offset

  SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to update data about last minute execution count.'
  RAISERROR (@err_msg, 0, 0) WITH NOWAIT

  SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to create XML indexes on #tmpdm_exec_query_stats.'
  RAISERROR (@err_msg, 0, 0) WITH NOWAIT

  CREATE PRIMARY XML INDEX ix1 ON #tmpdm_exec_query_stats(statement_plan)
  CREATE XML INDEX ix2 ON #tmpdm_exec_query_stats(statement_plan)
  USING XML INDEX ix1 FOR PROPERTY

  SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to create XML indexes on #tmpdm_exec_query_stats.'
  RAISERROR (@err_msg, 0, 0) WITH NOWAIT

  SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to run final query and parse query plan XML and populate tmpStatsCheckCachePlanData'
  RAISERROR (@err_msg, 0, 0) WITH NOWAIT

  IF OBJECT_ID('tempdb.dbo.tmpStatsCheckCachePlanData') IS NOT NULL
    DROP TABLE tempdb.dbo.tmpStatsCheckCachePlanData

  CREATE TABLE tempdb.dbo.tmpStatsCheckCachePlanData
  (
    [database_name] [sys].[sysname] NULL,
    [object_name] [sys].[sysname] NULL,
    [query_hash] [varchar] (800) NULL,
    [plan_handle] [varchar] (800) NULL,
    [query_impact] [numeric] (25, 2) NULL,
    [number_of_referenced_indexes] [bigint] NOT NULL,
    [index_list] [xml] NULL,
    [number_of_referenced_stats] [bigint] NOT NULL,
    [stats_list] [xml] NULL,
    [sum_modification_count_for_all_used_stats] [float] NULL,
    [statement_text] [xml] NULL,
    [statement_plan] [xml] NULL,
    [execution_count] [bigint] NULL,
    [execution_count_percent_over_total] [numeric] (25, 2) NULL,
    [execution_count_per_minute] [bigint] NULL,
    [execution_count_current] [bigint] NULL,
    [execution_count_last_minute] [bigint] NULL,
    [compilation_time_from_dm_exec_query_stats] [int] NULL,
    [exec_plan_creation_start_datetime] [varchar] (30) NULL,
    [last_execution_datetime] [datetime] NULL,
    [cached_plan_size_mb] [numeric] (25, 4) NULL,
    [statement_cached_plan_size_mb] [numeric] (25, 4) NULL,
    [cached_plan_size_status] [varchar] (50) NOT NULL,
    [statement_type] [varchar] (500) NULL,
    [ce_model_version] [int] NULL,
    [statement_optm_early_abort_reason] [sys].[sysname] NULL,
    [query_plan_cost] [float] NULL,
    [cost_threshold_for_parallelism] [int] NULL,
    [is_parallel] [bit] NOT NULL,
    [has_serial_ordered_backward_scan] [bit] NULL,
    [compile_time_sec] [numeric] (25, 4) NULL,
    [compile_cpu_sec] [numeric] (25, 4) NULL,
    [compile_memory_mb] [numeric] (25, 4) NULL,
    [serial_desired_memory_mb] [numeric] (25, 4) NULL,
    [serial_required_memory_mb] [numeric] (25, 4) NULL,
    [missing_index_count] [int] NULL,
    [warning_count] [int] NULL,
    [has_implicit_conversion_warning] [bit] NOT NULL,
    [has_no_join_predicate_warning] [bit] NULL,
    [operator_max_estimated_rows] [float] NULL,
    [has_nested_loop_join] [bit] NULL,
    [has_merge_join] [bit] NULL,
    [has_hash_join] [bit] NULL,
    [has_many_to_many_merge_join] [bit] NULL,
    [has_join_residual_predicate] [bit] NULL,
    [has_index_seek_residual_predicate] [bit] NULL,
    [has_key_or_rid_lookup] [bit] NULL,
    [has_spilling_operators] [bit] NULL,
    [has_remote_operators] [bit] NULL,
    [has_spool_operators] [bit] NULL,
    [has_index_spool_operators] [bit] NULL,
    [has_table_scan_on_heap] [bit] NULL,
    [has_table_valued_functions] [bit] NULL,
    [has_user_defined_function] [bit] NULL,
    [has_partitioned_tables] [bit] NULL,
    [has_min_max_agg] [bit] NOT NULL,
    [is_prefetch_enabled] [bit] NULL,
    [has_parameter_sniffing_problem] [int] NULL,
    [is_parameterized] [bit] NULL,
    [is_using_table_variable] [bit] NULL,
    [total_elapsed_time_sec] [numeric] (25, 4) NULL,
    [elapsed_time_sec_percent_over_total] [numeric] (25, 2) NULL,
    [avg_elapsed_time_sec] [numeric] (25, 4) NULL,
    [min_elapsed_time_sec] [numeric] (25, 4) NULL,
    [max_elapsed_time_sec] [numeric] (25, 4) NULL,
    [last_elapsed_time_sec] [numeric] (25, 4) NULL,
    [total_cpu_time_sec] [numeric] (25, 4) NULL,
    [cpu_time_sec_percent_over_total] [numeric] (25, 2) NULL,
    [avg_cpu_time_sec] [numeric] (25, 4) NULL,
    [min_cpu_time_sec] [numeric] (25, 4) NULL,
    [max_cpu_time_sec] [numeric] (25, 4) NULL,
    [last_cpu_time_sec] [numeric] (25, 4) NULL,
    [total_logical_page_reads] [bigint] NULL,
    [logical_page_reads_percent_over_total] [numeric] (25, 2) NULL,
    [avg_logical_page_reads] [bigint] NULL,
    [min_logical_page_reads] [bigint] NULL,
    [max_logical_page_reads] [bigint] NULL,
    [last_logical_page_reads] [bigint] NULL,
    [total_logical_reads_gb] [numeric] (25, 4) NULL,
    [logical_reads_gb_percent_over_total] [numeric] (25, 2) NULL,
    [avg_logical_reads_gb] [numeric] (25, 4) NULL,
    [min_logical_reads_gb] [numeric] (25, 4) NULL,
    [max_logical_reads_gb] [numeric] (25, 4) NULL,
    [last_logical_reads_gb] [numeric] (25, 4) NULL,
    [total_physical_page_reads] [bigint] NULL,
    [physical_page_reads_percent_over_total] [numeric] (25, 2) NULL,
    [avg_physical_page_reads] [bigint] NULL,
    [min_physical_page_reads] [bigint] NULL,
    [max_physical_page_reads] [bigint] NULL,
    [last_physical_page_reads] [bigint] NULL,
    [total_physical_reads_gb] [numeric] (25, 4) NULL,
    [physical_reads_gb_percent_over_total] [numeric] (25, 2) NULL,
    [avg_physical_reads_gb] [numeric] (25, 4) NULL,
    [min_physical_reads_gb] [numeric] (25, 4) NULL,
    [max_physical_reads_gb] [numeric] (25, 4) NULL,
    [last_physical_reads_gb] [numeric] (25, 4) NULL,
    [total_logical_page_writes] [bigint] NULL,
    [logical_page_writes_percent_over_total] [numeric] (25, 2) NULL,
    [avglogical_page_writes] [bigint] NULL,
    [min_logical_page_writes] [bigint] NULL,
    [max_logical_page_writes] [bigint] NULL,
    [last_logical_page_writes] [bigint] NULL,
    [total_logical_writes_gb] [numeric] (25, 4) NULL,
    [logical_writes_gb_percent_over_total] [numeric] (25, 2) NULL,
    [avg_logical_writes_gb] [numeric] (25, 4) NULL,
    [min_logical_writes_gb] [numeric] (25, 4) NULL,
    [max_logical_writes_gb] [numeric] (25, 4) NULL,
    [last_logical_writes_gb] [numeric] (25, 4) NULL,
    [total_returned_rows] [bigint] NULL,
    [avg_returned_rows] [bigint] NULL,
    [min_returned_rows] [bigint] NULL,
    [max_returned_rows] [bigint] NULL,
    [last_returned_rows] [bigint] NULL
  )

  DECLARE @ctp INT;
  SELECT  @ctp = CAST(value AS INT)
  FROM    sys.configurations
  WHERE   name = 'cost threshold for parallelism'
  OPTION (RECOMPILE);

  /* Setting "parameter sniffing variance percent" to 30% */
  DECLARE @parameter_sniffing_warning_pct TINYINT = 30;
  /* Setting min number of rows to be considered on PSP to 100*/
  DECLARE @parameter_sniffing_rows_threshold TINYINT = 100;

  SET @i = 1
  DECLARE c_plans CURSOR FORWARD_ONLY READ_ONLY FOR
      SELECT query_hash, plan_handle, statement_start_offset, statement_end_offset 
      FROM #tmpdm_exec_query_stats
  OPEN c_plans

  FETCH NEXT FROM c_plans
  INTO @query_hash, @plan_handle, @statement_start_offset, @statement_end_offset
  WHILE @@FETCH_STATUS = 0
  BEGIN
    BEGIN TRY
      SET @err_msg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Progress ' + '(' + CONVERT(VARCHAR(200), CONVERT(NUMERIC(25, 2), (CONVERT(NUMERIC(25, 2), @i) / CONVERT(NUMERIC(25, 2), @number_plans)) * 100)) + '%%) - ' 
                     + CONVERT(VARCHAR(200), @i) + ' of ' + CONVERT(VARCHAR(200), @number_plans)
      IF @i % 100 = 0
        RAISERROR (@err_msg, 0, 1) WITH NOWAIT

      ;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
      INSERT INTO tempdb.dbo.tmpStatsCheckCachePlanData WITH(TABLOCK)
      SELECT  CASE database_id 
                WHEN 32767 THEN 'ResourceDB' 
                ELSE DB_NAME(database_id)
              END AS database_name,
              object_name,
              CONVERT(VARCHAR(800), query_hash, 1) AS query_hash,
              CONVERT(VARCHAR(800), plan_handle, 1) AS plan_handle,
              query_impact,
              ISNULL(LEN(t_index_list.index_list) - LEN(REPLACE(t_index_list.index_list, ',', '')) + 1, 0) AS number_of_referenced_indexes,
              CONVERT(XML, ISNULL(t_index_list.index_list,'')) AS index_list,
              ISNULL(LEN(t_stats_list.stats_list) - LEN(REPLACE(t_stats_list.stats_list, ',', '')) + 1, 0) AS number_of_referenced_stats,
              CONVERT(XML, ISNULL(t_stats_list.stats_list,'')) AS stats_list,
              Batch.x.value('sum(//p:OptimizerStatsUsage/p:StatisticsInfo/@ModificationCount)', 'float') AS sum_modification_count_for_all_used_stats,
              statement_text,
              statement_plan,
              execution_count,
              execution_count_percent_over_total,
              execution_count_per_minute,
              execution_count_current,
              execution_count_last_minute,
              /* 
                 If there is only one execution, then, the compilation time can be calculated by
                 checking the diff from the creation_time and last_execution_time.
                 This is possible because creation_time is the time which the plan started creation
                 and last_execution_time is the time which the plan started execution.
                 So, for instance, considering the following:
                 creation_time = "2022-11-09 07:56:19.123" 
                 last_execution_time = "2022-11-09 07:56:26.937"
                 This means, the plan started to be created at "2022-11-09 07:56:19.123" 
                 and started execution at "2022-11-09 07:56:26.937", in other words, 
                 it took 7813ms (DATEDIFF(ms, "2022-11-09 07:56:19.123" , "2022-11-09 07:56:26.937")) 
                 to create the plan.
              */
              CASE 
               WHEN execution_count = 1
               THEN DATEDIFF(ms, creation_time, last_execution_time)
               ELSE NULL
              END AS compilation_time_from_dm_exec_query_stats,
              CONVERT(VARCHAR, creation_time, 21) AS exec_plan_creation_start_datetime,
              last_execution_time AS last_execution_datetime,
              cached_plan_size_mb,
              CONVERT(NUMERIC(25, 4), x.value('sum(..//p:QueryPlan/@CachedPlanSize)', 'float') / 1024.) AS statement_cached_plan_size_mb,
              CASE 
                WHEN cached_plan_size_mb >= 20 THEN 'Forget about it, don''t even try to see (over 20MB)'
                WHEN cached_plan_size_mb >= 15 THEN 'Planetarium plan (over 15MB)'
                WHEN cached_plan_size_mb >= 10 THEN 'Colossal plan (over 10MB)'
                WHEN cached_plan_size_mb >= 5 THEN 'Huge plan (over 5MB)'
                WHEN cached_plan_size_mb >= 2 THEN 'Big plan (over 2MB)'
                ELSE 'Normal plan (less than 2MB)'
              END AS cached_plan_size_status,
              COALESCE(Batch.x.value('(//p:StmtSimple/@StatementType)[1]', 'VarChar(500)'),
                       Batch.x.value('(//p:StmtCond/@StatementType)[1]', 'VarChar(500)'),
                       Batch.x.value('(//p:StmtCursor/@StatementType)[1]', 'VarChar(500)'),
                       Batch.x.value('(//p:StmtReceive/@StatementType)[1]', 'VarChar(500)'),
                       Batch.x.value('(//p:StmtUseDb/@StatementType)[1]', 'VarChar(500)')) AS statement_type,
              COALESCE(Batch.x.value('(//p:StmtSimple/@CardinalityEstimationModelVersion)[1]', 'int'),
                       Batch.x.value('(//p:StmtCond/@CardinalityEstimationModelVersion)[1]', 'int'),
                       Batch.x.value('(//p:StmtCursor/@CardinalityEstimationModelVersion)[1]', 'int'),
                       Batch.x.value('(//p:StmtReceive/@CardinalityEstimationModelVersion)[1]', 'int'),
                       Batch.x.value('(//p:StmtUseDb/@CardinalityEstimationModelVersion)[1]', 'int')) AS ce_model_version,
              COALESCE(Batch.x.value('(//p:StmtSimple/@StatementOptmEarlyAbortReason)[1]', 'sysname'),
                       Batch.x.value('(//p:StmtCond/@StatementOptmEarlyAbortReason)[1]', 'sysname'),
                       Batch.x.value('(//p:StmtCursor/@StatementOptmEarlyAbortReason)[1]', 'sysname'),
                       Batch.x.value('(//p:StmtReceive/@StatementOptmEarlyAbortReason)[1]', 'sysname'),
                       Batch.x.value('(//p:StmtUseDb/@StatementOptmEarlyAbortReason)[1]', 'sysname')) AS statement_optm_early_abort_reason,
              COALESCE(Batch.x.value('(//p:StmtSimple/@StatementSubTreeCost)[1]', 'float'),
                       Batch.x.value('(//p:StmtCond/@StatementSubTreeCost)[1]', 'float'),
                       Batch.x.value('(//p:StmtCursor/@StatementSubTreeCost)[1]', 'float'),
                       Batch.x.value('(//p:StmtReceive/@StatementSubTreeCost)[1]', 'float'),
                       Batch.x.value('(//p:StmtUseDb/@StatementSubTreeCost)[1]', 'float')) AS query_plan_cost,
              @ctp AS cost_threshold_for_parallelism,
              CASE WHEN Batch.x.value('max(//p:RelOp/@Parallel)', 'float') > 0 THEN 1 ELSE 0 END AS is_parallel,
              Batch.x.exist('(//p:IndexScan[@ScanDirection="BACKWARD" and @Ordered="1"])') AS has_serial_ordered_backward_scan,
              CONVERT(NUMERIC(25, 4), x.value('sum(..//p:QueryPlan/@CompileTime)', 'float') /1000. /1000.) AS compile_time_sec,
              CONVERT(NUMERIC(25, 4), x.value('sum(..//p:QueryPlan/@CompileCPU)', 'float') /1000. /1000.) AS compile_cpu_sec,
              CONVERT(NUMERIC(25, 4), x.value('sum(..//p:QueryPlan/@CompileMemory)', 'float') / 1024.) AS compile_memory_mb,
              CONVERT(NUMERIC(25, 4), Batch.x.value('sum(//p:MemoryGrantInfo/@SerialDesiredMemory)', 'float') / 1024.) AS serial_desired_memory_mb,
              CONVERT(NUMERIC(25, 4), Batch.x.value('sum(//p:MemoryGrantInfo/@SerialRequiredMemory)', 'float') / 1024.) AS serial_required_memory_mb,
              Batch.x.value('count(//p:MissingIndexGroup)', 'int') AS missing_index_count,
              Batch.x.value('count(//p:QueryPlan/p:Warnings/*)', 'int') AS warning_count,
              CASE WHEN Batch.x.exist('(//p:QueryPlan/p:Warnings/p:PlanAffectingConvert/@Expression[contains(., "CONVERT_IMPLICIT")])') = 1 THEN 1 ELSE 0 END AS has_implicit_conversion_warning,
              Batch.x.exist('//p:RelOp/p:Warnings[(@NoJoinPredicate[.="1"])]') AS has_no_join_predicate_warning,
              Batch.x.value('max(//p:RelOp/@EstimateRows)', 'float') AS operator_max_estimated_rows,
              Batch.x.exist('(//p:RelOp[contains(@PhysicalOp, "Nested Loops")])') AS has_nested_loop_join,
              Batch.x.exist('(//p:RelOp[contains(@PhysicalOp, "Merge Join")])') AS has_merge_join,
              Batch.x.exist('(//p:RelOp[contains(@PhysicalOp, "Hash Match")])') AS has_hash_join,
              Batch.x.exist('(//p:Merge/@ManyToMany[.="1"])') AS has_many_to_many_merge_join,
              Batch.x.exist('(//p:RelOp/p:Hash/p:ProbeResidual or //p:RelOp/p:Merge/p:Residual)') AS has_join_residual_predicate,
              Batch.x.exist('(//p:IndexScan/p:Predicate)') AS has_index_seek_residual_predicate,
              Batch.x.exist('(//p:RelOp[contains(@PhysicalOp, " Lookup")])') AS has_key_or_rid_lookup,
              Batch.x.exist('(//p:RelOp[contains(@PhysicalOp, "Sort") or contains(@PhysicalOp, "Hash Match")])') AS has_spilling_operators,
              Batch.x.exist('(//p:RelOp[contains(@PhysicalOp, "Remote")])') AS has_remote_operators,
              Batch.x.exist('(//p:RelOp[contains(@PhysicalOp, "Spool")])') AS has_spool_operators,
              Batch.x.exist('(//p:RelOp[contains(@PhysicalOp, "Index Spool")])') AS has_index_spool_operators,
              Batch.x.exist('(//p:RelOp[contains(@PhysicalOp, "Table Scan")])') AS has_table_scan_on_heap,
              Batch.x.exist('(//p:RelOp[contains(@PhysicalOp, "Table-valued function")])') AS has_table_valued_functions,
              Batch.x.exist('(//p:UserDefinedFunction)') AS has_user_defined_function,
              Batch.x.exist('(//p:RelOp/@Partitioned[.="1"])') AS has_partitioned_tables,
              CASE 
                WHEN Batch.x.exist('(//p:Aggregate[@AggType="MIN" or @AggType="MAX"])') = 1 THEN 1
                WHEN Batch.x.exist('(//p:TopSort[@Rows="1"])') = 1 THEN 1
                ELSE 0
              END AS has_min_max_agg,

              Batch.x.exist('(//p:NestedLoops[@WithUnorderedPrefetch])') AS is_prefetch_enabled,
        
              /* Return true if it find a large percent of variance on number of returned rows */
              CASE
                WHEN (min_returned_rows + max_returned_rows) / 2 >= @parameter_sniffing_rows_threshold 
                  AND min_returned_rows < ((1.0 - (@parameter_sniffing_warning_pct / 100.0)) * avg_returned_rows) THEN 1
                WHEN (min_returned_rows + max_returned_rows) / 2 >= @parameter_sniffing_rows_threshold 
                  AND max_returned_rows > ((1.0 + (@parameter_sniffing_warning_pct / 100.0)) * avg_returned_rows) THEN 1
                ELSE 0
              END AS has_parameter_sniffing_problem,

		            CASE 
                WHEN Batch.x.exist('(//p:ParameterList)') = 1 THEN 1
                ELSE 0
              END AS is_parameterized,
              CASE WHEN t_index_list.index_list LIKE '%@%' THEN 1 ELSE 0 END is_using_table_variable,
              total_elapsed_time_sec,
              elapsed_time_sec_percent_over_total,
              avg_elapsed_time_sec,
              min_elapsed_time_sec,
              max_elapsed_time_sec,
              last_elapsed_time_sec,
              total_cpu_time_sec,
              cpu_time_sec_percent_over_total,
              avg_cpu_time_sec,
              min_cpu_time_sec,
              max_cpu_time_sec,
              last_cpu_time_sec,
              total_logical_page_reads,
              logical_page_reads_percent_over_total,
              avg_logical_page_reads,
              min_logical_page_reads,
              max_logical_page_reads,
              last_logical_page_reads,
              total_logical_reads_gb,
              logical_reads_gb_percent_over_total,
              avg_logical_reads_gb,
              min_logical_reads_gb,
              max_logical_reads_gb,
              last_logical_reads_gb,
              total_physical_page_reads,
              physical_page_reads_percent_over_total,
              avg_physical_page_reads,
              min_physical_page_reads,
              max_physical_page_reads,
              last_physical_page_reads,
              total_physical_reads_gb,
              physical_reads_gb_percent_over_total,
              avg_physical_reads_gb,
              min_physical_reads_gb,
              max_physical_reads_gb,
              last_physical_reads_gb,
              total_logical_page_writes,
              logical_page_writes_percent_over_total,
              avglogical_page_writes,
              min_logical_page_writes,
              max_logical_page_writes,
              last_logical_page_writes,
              total_logical_writes_gb,
              logical_writes_gb_percent_over_total,
              avg_logical_writes_gb,
              min_logical_writes_gb,
              max_logical_writes_gb,
              last_logical_writes_gb,
              total_returned_rows,
              avg_returned_rows,
              min_returned_rows,
              max_returned_rows,
              last_returned_rows
      FROM #tmpdm_exec_query_stats AS qp
      OUTER APPLY statement_plan.nodes('//p:Batch') AS Batch(x)
      OUTER APPLY 
        --Get a comma-delimited list of indexes
        (SELECT index_list = STUFF((SELECT DISTINCT ', ' + '(' +
                                           ISNULL(t_index_nodes.col_index.value('(@Database)[1]','sysname') + '.','') + 
                                           ISNULL(t_index_nodes.col_index.value('(@Schema)[1]','sysname') + '.', '') +
                                           t_index_nodes.col_index.value('(@Table)[1]','sysname')  +
                                           ISNULL('.' + t_index_nodes.col_index.value('(@Index)[1]','sysname'),'') + ')'
                                    FROM Batch.x.nodes('//p:Object') t_index_nodes(col_index)
                                    FOR XML PATH(''))
                                  , 1, 2,'')
        ) t_index_list
      OUTER APPLY 
        --Get a comma-delimited list of stats
        (SELECT stats_list = STUFF((SELECT DISTINCT ', ' + '(' +
                                          t_stats_nodes.col_stats.value('(@Database)[1]','sysname') + '.' +
                                          t_stats_nodes.col_stats.value('(@Schema)[1]','sysname') + '.' +
                                          t_stats_nodes.col_stats.value('(@Table)[1]','sysname')  +
                                           ISNULL('.' + t_stats_nodes.col_stats.value('(@Statistics)[1]','sysname'),'') + ')'
                                    FROM Batch.x.nodes('//p:OptimizerStatsUsage/p:StatisticsInfo') t_stats_nodes(col_stats)
                                    FOR XML PATH(''))
                                  , 1, 2,'')
        ) t_stats_list
      WHERE qp.plan_handle = @plan_handle
      AND qp.statement_start_offset = @statement_start_offset
      AND qp.statement_end_offset = @statement_end_offset
		  END TRY
		  BEGIN CATCH
			   SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to work on plan [' + CONVERT(NVARCHAR(800), @plan_handle, 1) + ']. Skipping this plan.'
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
		  END CATCH

    SET @i = @i + 1
    FETCH NEXT FROM c_plans
    INTO @query_hash, @plan_handle, @statement_start_offset, @statement_end_offset
  END
  CLOSE c_plans
  DEALLOCATE c_plans

  SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to run final query and parse query plan XML and populate tmpStatsCheckCachePlanData'
  RAISERROR (@err_msg, 0, 0) WITH NOWAIT

  SET @err_msg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to collect cache plan info...'
  RAISERROR(@err_msg, 0, 42) WITH NOWAIT;

  /*
    Creating temporary table to store stats info
  */
  IF OBJECT_ID('tempdb.dbo.#tmp_stats') IS NOT NULL
    DROP TABLE #tmp_stats;

  CREATE TABLE #tmp_stats
  (
    [rowid] [int] IDENTITY(1, 1) NOT NULL PRIMARY KEY,
    [database_name] SYSNAME,
    [schema_name] SYSNAME,
    [table_name] SYSNAME,
    [stats_name] NVARCHAR(4000),
    [database_id] [int] NOT NULL,
    [object_id] [int] NOT NULL,
    [stats_id] [int] NOT NULL,
    [current_number_of_rows] [bigint] NULL,
    [last_updated] DATETIME NULL,
    [number_of_rows_at_time_stat_was_updated] [bigint] NULL,
    [rows_sampled] [bigint] NULL,
    [steps] [smallint] NULL,
    [unfiltered_rows] [bigint] NULL,
    [current_number_of_modified_rows_since_last_update] [bigint] NULL,
    [auto_created] [bit] NULL,
    [user_created] [bit] NULL,
    [no_recompute] [bit] NULL,
    [has_filter] [bit] NULL,
    [filter_definition] [nvarchar] (max) NULL,
    [statistic_type] [nvarchar] (max) NULL,
    [number_of_columns_in_this_table] [int] NULL,
    [number_of_statistics_in_this_table] [int] NULL,
    [is_table_partitioned] [bit] NULL,
    [table_index_base_type] [nvarchar] (max) NULL,
    [index_type] [NVARCHAR] (max) NULL,
    [number_of_in_row_data_pages_on_table] [bigint] NULL,
    [number_of_lob_data_pages_on_table] [bigint] NULL,
    [key_column_name] NVARCHAR(max),
    [key_column_data_type] NVARCHAR(max),
    [stat_all_columns] NVARCHAR(max),
    [stat_all_columns_index_order] NVARCHAR(max),
    [stat_all_columns_stat_order] NVARCHAR(max),
    [is_lob] BIT,
    [is_unique] BIT,
    [is_temporary] [bit] NULL,
    [is_incremental] [bit] NULL,
    [has_persisted_sample] [bit] NULL,
    [auto_drop] [bit] NULL
  )

  IF OBJECT_ID('tempdb.dbo.#tmp_stat_header') IS NOT NULL
    DROP TABLE #tmp_stat_header;

  CREATE TABLE #tmp_stat_header
  (
    [rowid] INT NULL,
    [database_name] SYSNAME NULL,
    [schema_name] SYSNAME NULL,
    [table_name] SYSNAME NULL,
    [stats_name] NVARCHAR(4000) NULL,
    [name] SYSNAME,
    [updated] DATETIME,
    [rows] BIGINT,
    [rows_sampled] BIGINT,
    [steps] SMALLINT,
    [density] REAL,
    [average_key_length] INT,
    [string_index] VARCHAR(10),
    [filter_expression] VARCHAR(8000),
    [unfiltered_rows] BIGINT,
    [persisted_sample_percent] FLOAT
  );
  CREATE CLUSTERED INDEX ixrowid ON #tmp_stat_header(rowid)

  IF OBJECT_ID('tempdb.dbo.#tmp_density_vector') IS NOT NULL
    DROP TABLE #tmp_density_vector;

  CREATE TABLE #tmp_density_vector
  (
    [rowid] INT NULL,
    [database_name] SYSNAME NULL,
    [schema_name] SYSNAME NULL,
    [table_name] SYSNAME NULL,
    [stats_name] NVARCHAR(4000) NULL,
    [density_number] SMALLINT NULL,
    [all_density] FLOAT,
    [average_length] FLOAT,
    [columns] NVARCHAR(2000)
  );
  CREATE CLUSTERED INDEX ixrowid ON #tmp_density_vector(rowid)

  IF OBJECT_ID('tempdb.dbo.#tmp_histogram') IS NOT NULL
    DROP TABLE #tmp_histogram;

  CREATE TABLE #tmp_histogram
  (
    [rowid] INT NULL,
    [database_name] SYSNAME NULL,
    [schema_name] SYSNAME NULL,
    [table_name] SYSNAME NULL,
    [stats_name] NVARCHAR(4000) NULL,
    [stepnumber] SMALLINT,
    [range_hi_key] SQL_VARIANT NULL,
    [range_rows] DECIMAL(28, 2),
    [eq_rows] DECIMAL(28, 2),
    [distinct_range_rows] BIGINT,
    [avg_range_rows] DECIMAL(28, 4)
  );
  CREATE CLUSTERED INDEX ixrowid ON #tmp_histogram(rowid, stepnumber)

  IF OBJECT_ID('tempdb.dbo.#tmp_stats_stream') IS NOT NULL
    DROP TABLE #tmp_stats_stream;

  CREATE TABLE #tmp_stats_stream
  (
    [rowid] INT NULL,
    [database_name] SYSNAME NULL,
    [schema_name] SYSNAME NULL,
    [table_name] SYSNAME NULL,
    [stats_name] NVARCHAR(4000) NULL,
    [stats_stream] VARBINARY(MAX),
    [rows] BIGINT,
    [data_pages] BIGINT
  );
  CREATE CLUSTERED INDEX ixrowid ON #tmp_stats_stream(rowid)

  IF OBJECT_ID('tempdb.dbo.#tmp_exec_history') IS NOT NULL
    DROP TABLE #tmp_exec_history;

  CREATE TABLE #tmp_exec_history
  (
    [rowid] INT NULL,
    [history_number] SMALLINT NULL,
    [database_name] SYSNAME NULL,
    [schema_name] SYSNAME NULL,
    [table_name] SYSNAME NULL,
    [stats_name] NVARCHAR(4000) NULL,
    [updated] DATETIME,
    [table_cardinality] BIGINT,
    [snapshot_ctr] BIGINT,
    [steps] BIGINT,
    [density] FLOAT,
    [rows_above] FLOAT,
    [rows_below] FLOAT,
    [squared_variance_error] FLOAT,
    [inserts_since_last_update] FLOAT,
    [deletes_since_last_update] FLOAT,
    [leading_column_type] NVARCHAR(200)
  )
  CREATE CLUSTERED INDEX ixrowid ON #tmp_exec_history(rowid)

		SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Determining SQL Server version.'
  RAISERROR (@err_msg, 0, 0) WITH NOWAIT

  SET @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff)

  /*
    Creating list of DBs we'll collect the information
  */
		SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Creating list of databases to work on.'
  RAISERROR (@err_msg, 0, 0) WITH NOWAIT

  IF OBJECT_ID('tempdb.dbo.#tmp_db') IS NOT NULL
    DROP TABLE #tmp_db

  CREATE TABLE #tmp_db ([database_name] sysname)

  /* If this is SQL2012+, check AG status */
  IF (@sqlmajorver >= 11 /*SQL2012*/)
  BEGIN    
    BEGIN TRY
      INSERT INTO #tmp_db
      SELECT d1.[name] 
      FROM sys.databases d1
      LEFT JOIN sys.dm_hadr_availability_replica_states hars
      ON d1.replica_id = hars.replica_id
      LEFT JOIN sys.availability_replicas ar
      ON d1.replica_id = ar.replica_id
      WHERE /* Filtering by the specified DB */
      (d1.name = @database_name_filter OR ISNULL(@database_name_filter, '') = '')
      /* I'm not interested to read DBs that are not online :-) */
      AND d1.state_desc = 'ONLINE'
      /* I'm not sure if info about read_only DBs would be useful, I'm ignoring it until someone convince me otherwise. */
      AND d1.is_read_only = 0 
      /* Not interested to read data about Microsoft stuff, those DBs are already tuned by Microsoft experts, so, no need to tune it, right? ;P */
      AND d1.name not in ('tempdb', 'master', 'model', 'msdb') AND d1.is_distributor = 0
      /* If DB is part of AG, check only DBs that allow connections */
      AND (  
           (hars.role_desc = 'PRIMARY' OR hars.role_desc IS NULL)
           OR 
           (hars.role_desc = 'SECONDARY' AND ar.secondary_role_allow_connections_desc IN ('READ_ONLY','ALL'))
          )
		  END TRY
		  BEGIN CATCH
			   SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to create list of databases.'
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
		  END CATCH
  END
  /* SQL2008R2 doesn't have AG, so, ignoring the AG DMVs */
  ELSE IF (@sqlmajorver <= 10 /*SQL2008R2*/)
  BEGIN    
    BEGIN TRY
      INSERT INTO #tmp_db
      SELECT d1.[name] 
      FROM sys.databases d1
      WHERE /* Filtering by the specified DB */
      (d1.name = @database_name_filter OR ISNULL(@database_name_filter, '') = '')
      /* I'm not interested to read DBs that are not online :-) */
      AND d1.state_desc = 'ONLINE'
      /* I'm not sure if info about read_only DBs would be useful, I'm ignoring it until someone convince me otherwise. */
      AND d1.is_read_only = 0 
      /* Not interested to read data about Microsoft stuff, those DBs are already tuned by Microsoft experts, so, no need to tune it, right? ;P */
      AND d1.name not in ('tempdb', 'master', 'model', 'msdb') AND d1.is_distributor = 0
		  END TRY
		  BEGIN CATCH
			   SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to create list of databases.'
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
		  END CATCH
  END

		SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Creating dynamic SQL to read sys.stats based on SQL Server version.'
  RAISERROR (@err_msg, 0, 0) WITH NOWAIT

  DECLARE @sqlpart1 NVARCHAR(MAX), @sqlpart2 NVARCHAR(MAX) 
  SET @sqlpart1 = N'
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    SET LOCK_TIMEOUT 1000; /*1 second*/

    /* Creating a copy of sys.partitions and sys.allocation_units because unindexed access to it can be very slow */
    IF OBJECT_ID(''tempdb.dbo.#tmp_sys_partitions'') IS NOT NULL
        DROP TABLE #tmp_sys_partitions;
    SELECT * INTO #tmp_sys_partitions FROM sys.partitions
    CREATE CLUSTERED INDEX ix1 ON #tmp_sys_partitions (object_id, index_id, partition_number)
    IF OBJECT_ID(''tempdb.dbo.#tmp_sys_allocation_units'') IS NOT NULL
        DROP TABLE #tmp_sys_allocation_units;

    SELECT * INTO #tmp_sys_allocation_units FROM sys.allocation_units
    CREATE CLUSTERED INDEX ix1 ON #tmp_sys_allocation_units (container_id)

    SELECT QUOTENAME(DB_NAME()) AS database_name,
           QUOTENAME(OBJECT_SCHEMA_NAME(a.object_id)) schema_name,
           QUOTENAME(OBJECT_NAME(a.object_id)) AS table_name,
           QUOTENAME(a.name) AS stats_name,
           DB_ID() as database_id,
           a.object_id,
           a.stats_id,
           (
                 SELECT SUM(p.rows)
                 FROM #tmp_sys_partitions p
                 WHERE a.object_id = p.object_id
                       AND index_id <= 1
           ) AS current_number_of_rows,
           sp.last_updated,
           sp.rows AS number_of_rows_at_time_stat_was_updated,
           sp.rows_sampled,
           sp.steps,
           sp.unfiltered_rows,
           ISNULL(sp.modification_counter,0) AS current_number_of_modified_rows_since_last_update,
           a.auto_created,
           a.user_created,
           a.no_recompute,
           a.has_filter,
           a.filter_definition,
           CASE 
             WHEN a.auto_created = 0 AND a.user_created = 0 THEN ''Index_Statistic''
		           WHEN a.auto_created = 0 AND a.user_created = 1 THEN ''User_Created''
		           WHEN a.auto_created = 1 AND a.user_created = 0 THEN ''Auto_Created''
		           ELSE NULL
           END AS statistic_type,
           (SELECT COUNT(*) 
            FROM sys.columns AS cntColumns 
            WHERE cntColumns.object_id = a.object_id) AS number_of_columns_in_this_table,
           (SELECT COUNT(*) 
            FROM sys.stats AS cntStats 
            WHERE cntStats.object_id = a.object_id) AS number_of_statistics_in_this_table,
           CASE 
             WHEN EXISTS(SELECT *
                         FROM #tmp_sys_partitions p
                         WHERE p.partition_number > 1
                         AND p.object_id = a.object_id
                         AND p.index_id IN (0, 1)) THEN 1
             ELSE 0
           END AS is_table_partitioned,
           (SELECT type_desc
            FROM sys.indexes i
            WHERE i.object_id = a.object_id
            AND i.index_id IN (0, 1)) AS table_index_base_type,
           ISNULL((SELECT type_desc
            FROM sys.indexes i
            WHERE i.object_id = a.object_id
            AND i.index_id = a.stats_id),'''') AS index_type,
           (SELECT TOP 1 sysindexes.dpages 
            FROM sysindexes 
            WHERE sysindexes.id = a.object_id 
            AND sysindexes.indid <= 1) AS number_of_in_row_data_pages_on_table,
           ISNULL((
                   SELECT SUM(u.total_pages) 
                   FROM #tmp_sys_allocation_units AS u 
                   JOIN #tmp_sys_partitions AS p 
                   ON u.container_id = p.hobt_id
                   WHERE u.type_desc = ''LOB_DATA''
                   AND p.object_id = a.object_id
                 ),0) AS number_of_lob_data_pages_on_table,
           ISNULL(tab_index_key_column.indexkeycolumnname, tab_stats_key_column.statkeycolumnname) AS key_column_name,
           ISNULL(tab_index_key_column.keycolumndatatype, tab_stats_key_column.keycolumndatatype) AS key_column_data_type,
           STUFF(ISNULL(tab_index_all_columns.indexallcolumns, tab_stat_all_columns.statallcolumns), 1, 1, '''') AS stat_all_columns,
           STUFF(tab_index_all_columns.indexallcolumns, 1, 1, '''') AS stat_all_columns_index_order,
           STUFF(tab_stat_all_columns.statallcolumns, 1, 1, '''') AS stat_all_columns_stat_order,
           ISNULL(tab_index_key_column.islob, tab_stats_key_column.islob) AS is_lob,
           INDEXPROPERTY(a.object_id, a.name,''IsUnique'') AS is_unique
	         ' + CASE WHEN @sqlmajorver >= 11 /*SQL2012*/ THEN ',is_temporary' ELSE ', NULL' END + '
	         ' + CASE WHEN @sqlmajorver >= 12 /*SQL2014*/ THEN ',is_incremental' ELSE ', NULL' END + '
	         ' + CASE WHEN @sqlmajorver >= 15 /*SQL2019*/ THEN ',has_persisted_sample' ELSE ', NULL' END + '
          ' + CASE WHEN @sqlmajorver >= 16 /*SQL2022*/ THEN ',auto_drop' ELSE ', NULL' END + '
    FROM sys.stats AS a
        INNER JOIN sys.objects AS b
            ON a.object_id = b.object_id
        CROSS APPLY sys.dm_db_stats_properties(a.object_id, a.stats_id) AS sp
        OUTER APPLY (SELECT all_columns.Name AS indexkeycolumnname, 
                             CASE 
                               WHEN COLUMNPROPERTY(all_columns.object_id, all_columns.name, ''Precision'') = -1 THEN 1
                               WHEN COLUMNPROPERTY(all_columns.object_id, all_columns.name, ''Precision'') = 2147483647 THEN 1
                               ELSE 0
                             END AS islob,
                             UPPER(TYPE_NAME(types.system_type_id)) + '' (precision = '' + 
                             CONVERT(VARCHAR(20), COLUMNPROPERTY(all_columns.object_id, all_columns.name, ''Precision'')) + 
                             '', scale = '' +
                             ISNULL(CONVERT(VARCHAR(20), COLUMNPROPERTY(all_columns.object_id, all_columns.name, ''Scale'')), ''0'') + 
                             '')'' AS keycolumndatatype
                     FROM sys.index_columns
                     INNER JOIN sys.all_columns
                     ON all_columns.object_id = index_columns.object_id
                     AND all_columns.column_id = index_columns.column_id
                     INNER JOIN sys.types
                     ON types.user_type_id = all_columns.user_type_id
                     WHERE a.object_id = index_columns.object_id
                     AND a.stats_id = index_columns.index_id
                     AND index_columns.key_ordinal = 1
                     AND index_columns.is_included_column = 0) AS tab_index_key_column
  '

  SET @sqlpart2 = N'
           CROSS APPLY (SELECT all_columns.Name AS statkeycolumnname, 
                               CASE 
                                 WHEN COLUMNPROPERTY(all_columns.object_id, all_columns.name, ''Precision'') = -1 THEN 1
                                 WHEN COLUMNPROPERTY(all_columns.object_id, all_columns.name, ''Precision'') = 2147483647 THEN 1
                                 ELSE 0
                               END AS islob,
                               UPPER(TYPE_NAME(types.system_type_id)) + '' (precision = '' + 
                               CONVERT(VARCHAR(20), COLUMNPROPERTY(all_columns.object_id, all_columns.name, ''Precision'')) + 
                               '', scale = '' +
                               ISNULL(CONVERT(VARCHAR(20), COLUMNPROPERTY(all_columns.object_id, all_columns.name, ''Scale'')), ''0'') + 
                               '')'' AS keycolumndatatype
                        FROM sys.stats_columns
                        INNER JOIN sys.all_columns
                        ON all_columns.object_id = stats_columns.object_id
                        AND all_columns.column_id = stats_columns.column_id
                        INNER JOIN sys.types
                        ON types.user_type_id = all_columns.user_type_id
                        WHERE a.object_id = stats_columns.object_id
                        AND a.stats_id = stats_columns.stats_id
                        AND stats_columns.stats_column_id = 1) AS tab_stats_key_column
           OUTER APPLY (SELECT '','' + Name 
                        FROM sys.index_columns
                        INNER JOIN sys.all_columns
                        ON all_columns.object_id = index_columns.object_id
                        AND all_columns.column_id = index_columns.column_id
                        WHERE a.object_id = index_columns.object_id
                        AND a.stats_id = index_columns.index_id
                        AND index_columns.is_included_column = 0
                        ORDER BY index_columns.key_ordinal
                        FOR XML PATH('''')) AS tab_index_all_columns (indexallcolumns)
            CROSS APPLY (SELECT '','' + Name 
                         FROM sys.stats_columns
                         INNER JOIN sys.all_columns
                         ON all_columns.object_id = stats_columns.object_id
                         AND all_columns.column_id = stats_columns.column_id
                         WHERE a.object_id = stats_columns.object_id
                         AND a.stats_id = stats_columns.stats_id
                         ORDER BY stats_columns.stats_column_id
                         FOR XML PATH('''')) as tab_stat_all_columns (statallcolumns)
        WHERE b.type = ''U''
  '

  SET @sqlcmd = @sqlpart1 + @sqlpart2
  DECLARE c_databases CURSOR READ_ONLY FOR
      SELECT [database_name] FROM #tmp_db
  OPEN c_databases

  FETCH NEXT FROM c_databases
  INTO @database_name
  WHILE @@FETCH_STATUS = 0
  BEGIN
    SET @err_msg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to collect list of statistics on DB - [' + @database_name + ']'
    RAISERROR (@err_msg, 0, 1) WITH NOWAIT

    SET @sqlcmd_db = 'use [' + @database_name + '];' + @sqlcmd

    BEGIN TRY
      INSERT INTO #tmp_stats
      (
          database_name,
          schema_name,
          table_name,
          stats_name,
          database_id,
          object_id,
          stats_id,
          current_number_of_rows,
          last_updated,
          number_of_rows_at_time_stat_was_updated,
          rows_sampled,
          steps,
          unfiltered_rows,
          current_number_of_modified_rows_since_last_update,
          auto_created,
          user_created,
          no_recompute,
          has_filter,
          filter_definition,
          statistic_type,
          number_of_columns_in_this_table,
          number_of_statistics_in_this_table,
          is_table_partitioned,
          table_index_base_type,
          index_type,
          number_of_in_row_data_pages_on_table,
          number_of_lob_data_pages_on_table,
          key_column_name,
          key_column_data_type,
          stat_all_columns,
          stat_all_columns_index_order,
          stat_all_columns_stat_order,
          is_lob,
          is_unique,
          is_temporary,
          is_incremental,
          has_persisted_sample,
          auto_drop
      )
      EXECUTE sp_executesql @sqlcmd_db;
      SET @number_of_stats = @@ROWCOUNT

			   SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Found ' + CONVERT(VARCHAR(200), @number_of_stats)  + ' statistics on DB - [' + @database_name + '].'
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
		  END TRY
		  BEGIN CATCH
			   SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to collect list of statistics on DB - [' + @database_name + ']. Skipping this DB.'
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
		  END CATCH

    SET @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to collect list of statistics on DB - [' + @database_name + ']'
    RAISERROR (@err_msg, 0, 1) WITH NOWAIT

    FETCH NEXT FROM c_databases
    INTO @database_name
  END
  CLOSE c_databases
  DEALLOCATE c_databases

  /*
    Starting code to get information about statistic header, density, histogram and stats_stream info
  */
  SET @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting loop through statistics list to collect header, density, histogram and stats_stream info'
  RAISERROR (@err_msg, 0, 1) WITH NOWAIT

  SELECT @number_of_stats = COUNT(*) 
  FROM #tmp_stats

  DECLARE c_stats CURSOR READ_ONLY FOR
      SELECT [rowid], 
             [database_name],
             [schema_name],
             [table_name],
             [stats_name],
             'DBCC SHOW_STATISTICS (''' + [database_name] + '.' + [schema_name] + '.' + REPLACE([table_name], '''', '''''') + ''',' + [stats_name] + ')' AS sqlcmd_dbcc
      FROM #tmp_stats
  OPEN c_stats

  FETCH NEXT FROM c_stats
  INTO @rowid, @database_name, @schema_name, @table_name, @stats_name, @sqlcmd_dbcc
  WHILE @@FETCH_STATUS = 0
  BEGIN
    SET @err_msg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Progress ' + '(' + CONVERT(VARCHAR(200), CONVERT(NUMERIC(25, 2), (CONVERT(NUMERIC(25, 2), @rowid) / CONVERT(NUMERIC(25, 2), @number_of_stats)) * 100)) + '%%) - ' 
           + CONVERT(VARCHAR(200), @rowid) + ' of ' + CONVERT(VARCHAR(200), @number_of_stats)
    IF @rowid % 1000 = 0
      RAISERROR (@err_msg, 0, 1) WITH NOWAIT

    /* Code to read stat_header */
    SET @sqlcmd_dbcc_local = @sqlcmd_dbcc + ' WITH STAT_HEADER, NO_INFOMSGS;'
    BEGIN TRY
      /* persisted_sample_percent was added on SQL2016 */
      IF (@sqlmajorver >= 13 /*SQL2016*/)
      BEGIN
        INSERT INTO #tmp_stat_header
        (
            name,
            updated,
            rows,
            [rows_sampled],
            steps,
            density,
            [average_key_length],
            [string_index],
            [filter_expression],
            [unfiltered_rows],
            [persisted_sample_percent]
        )
        EXECUTE sp_executesql @sqlcmd_dbcc_local;
      END
      ELSE
      BEGIN
        INSERT INTO #tmp_stat_header
        (
            name,
            updated,
            rows,
            [rows_sampled],
            steps,
            density,
            [average_key_length],
            [string_index],
            [filter_expression],
            [unfiltered_rows]
        )
        EXECUTE sp_executesql @sqlcmd_dbcc_local;
      END
		  END TRY
		  BEGIN CATCH
			   SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to run command to read stat_header. Skipping this statistic.'
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Command: ' + @sqlcmd_dbcc_local
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
		  END CATCH
    BEGIN TRY
      UPDATE #tmp_stat_header SET [rowid]         = @rowid,
                                  [database_name] = @database_name,
                                  [schema_name]   = @schema_name,
                                  [table_name]    = @table_name,
                                  [stats_name]    = @stats_name
      WHERE [rowid] IS NULL
		  END TRY
		  BEGIN CATCH
			   SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to update stat_header on temporary table. Skipping this statistic.'
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Command: ' + @sqlcmd_dbcc_local
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
		  END CATCH

    /* Code to read density_vector */
    SET @sqlcmd_dbcc_local = @sqlcmd_dbcc + ' WITH DENSITY_VECTOR, NO_INFOMSGS;'
    BEGIN TRY
      INSERT INTO #tmp_density_vector
      (
          [all_density],
          [average_length],
          [columns]
      )
      EXECUTE sp_executesql @sqlcmd_dbcc_local;
		  END TRY
		  BEGIN CATCH
			   SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to run command to read density_vector. Skipping this statistic.'
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Command: ' + @sqlcmd_dbcc_local
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
		  END CATCH
    BEGIN TRY
      ;WITH CTE_1
      AS
      (
        SELECT density_number, ROW_NUMBER() OVER(ORDER BY (SELECT 0)) AS rn 
        FROM #tmp_density_vector
        WHERE [rowid] IS NULL
      )
      UPDATE CTE_1 SET CTE_1.density_number = CTE_1.rn

      UPDATE #tmp_density_vector SET [rowid]         = @rowid,
                                     [database_name] = @database_name,
                                     [schema_name]   = @schema_name,
                                     [table_name]    = @table_name,
                                     [stats_name]    = @stats_name
      WHERE [rowid] IS NULL
		  END TRY
		  BEGIN CATCH
			   SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to update density_vector on temporary table. Skipping this statistic.'
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Command: ' + @sqlcmd_dbcc_local
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
		  END CATCH

    /* Code to read histogram */
    SET @sqlcmd_dbcc_local = @sqlcmd_dbcc + ' WITH HISTOGRAM, NO_INFOMSGS;'
    BEGIN TRY
      INSERT INTO #tmp_histogram
      (
          range_hi_key,
          range_rows,
          eq_rows,
          distinct_range_rows,
          avg_range_rows
      )
      EXECUTE sp_executesql @sqlcmd_dbcc_local;
		  END TRY
		  BEGIN CATCH
      -- If error is "Operand type clash: timestamp is incompatible with sql_variant"
      -- then, insert into a table with a VARBINARY first, then insert into #Stat_Histogram
      IF ERROR_NUMBER() = 206
      BEGIN
        IF OBJECT_ID('tempdb.dbo.#tmp_histogram_timestamp') IS NOT NULL
          DROP TABLE #tmp_histogram_timestamp

        CREATE TABLE #tmp_histogram_timestamp
        (
          [range_hi_key] VARBINARY(250) NULL,
          [range_rows] DECIMAL(28, 2),
          [eq_rows] DECIMAL(28, 2),
          [distinct_range_rows] BIGINT,
          [avg_range_rows] DECIMAL(28, 4)
        );

        INSERT INTO #tmp_histogram_timestamp
        (
            range_hi_key,
            range_rows,
            eq_rows,
            distinct_range_rows,
            avg_range_rows
        )
        EXECUTE sp_executesql @sqlcmd_dbcc_local;

        INSERT INTO #tmp_histogram
        (
            range_hi_key,
            range_rows,
            eq_rows,
            distinct_range_rows,
            avg_range_rows
        )
        SELECT * FROM #tmp_histogram_timestamp
      END
      ELSE
      BEGIN
			     SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to run command to read histogram. Skipping this statistic.'
        RAISERROR (@err_msg, 0, 0) WITH NOWAIT
        SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Command: ' + @sqlcmd_dbcc_local
        RAISERROR (@err_msg, 0, 0) WITH NOWAIT
        SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
        RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      END
		  END CATCH
    BEGIN TRY
      ;WITH CTE_1
      AS
      (
        SELECT stepnumber, ROW_NUMBER() OVER(ORDER BY (SELECT 0)) AS rn 
        FROM #tmp_histogram
        WHERE [rowid] IS NULL
      )
      UPDATE CTE_1 SET CTE_1.stepnumber = CTE_1.rn

      UPDATE #tmp_histogram SET [rowid]         = @rowid,
                                [database_name] = @database_name,
                                [schema_name]   = @schema_name,
                                [table_name]    = @table_name,
                                [stats_name]    = @stats_name
      WHERE [rowid] IS NULL
		  END TRY
		  BEGIN CATCH
			   SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to update histogram on temporary table. Skipping this statistic.'
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Command: ' + @sqlcmd_dbcc_local
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
		  END CATCH

    /* Code to read stats_stream */
    SET @sqlcmd_dbcc_local = @sqlcmd_dbcc + ' WITH STATS_STREAM, NO_INFOMSGS;'
    BEGIN TRY
      INSERT INTO #tmp_stats_stream
      (
          [stats_stream],
          [rows],
          [data_pages]
      )
      EXECUTE sp_executesql @sqlcmd_dbcc_local;
		  END TRY
		  BEGIN CATCH
			   SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to run command to read stats_stream. Skipping this statistic.'
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Command: ' + @sqlcmd_dbcc_local
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
		  END CATCH
    BEGIN TRY
      UPDATE #tmp_stats_stream SET [rowid]         = @rowid,
                                   [database_name] = @database_name,
                                   [schema_name]   = @schema_name,
                                   [table_name]    = @table_name,
                                   [stats_name]    = @stats_name
      WHERE [rowid] IS NULL
		  END TRY
		  BEGIN CATCH
			   SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to update stats_stream on temporary table. Skipping this statistic.'
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Command: ' + @sqlcmd_dbcc_local
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
		  END CATCH

    FETCH NEXT FROM c_stats
    INTO @rowid, @database_name, @schema_name, @table_name, @stats_name, @sqlcmd_dbcc
  END
  CLOSE c_stats
  DEALLOCATE c_stats

  SET @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished loop through statistics list to collect header, density, histogram and stats_stream info'
  RAISERROR (@err_msg, 0, 1) WITH NOWAIT

  /*
    Starting code to get information about statistic execution history info using TF2388
  */
  SET @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting loop through statistics list to collect execution history info using TF2388'
  RAISERROR (@err_msg, 0, 1) WITH NOWAIT

  /* Enabling TF2388 at session level to capture result of DBCC SHOW_STATISTICS with stats history info*/
  DBCC TRACEON(2388) WITH NO_INFOMSGS;

  DECLARE c_stats CURSOR READ_ONLY FOR
      SELECT [rowid], 
             [database_name],
             [schema_name],
             [table_name],
             [stats_name],
             'DBCC SHOW_STATISTICS (''' + [database_name] + '.' + [schema_name] + '.' + REPLACE([table_name], '''', '''''') + ''',' + [stats_name] + ')' AS sqlcmd_dbcc
      FROM #tmp_stats
  OPEN c_stats

  FETCH NEXT FROM c_stats
  INTO @rowid, @database_name, @schema_name, @table_name, @stats_name, @sqlcmd_dbcc
  WHILE @@FETCH_STATUS = 0
  BEGIN
    SET @err_msg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Progress ' + '(' + CONVERT(VARCHAR(200), CONVERT(NUMERIC(25, 2), (CONVERT(NUMERIC(25, 2), @rowid) / CONVERT(NUMERIC(25, 2), @number_of_stats)) * 100)) + '%%) - ' 
           + CONVERT(VARCHAR(200), @rowid) + ' of ' + CONVERT(VARCHAR(200), @number_of_stats)
    IF @rowid % 1000 = 0
      RAISERROR (@err_msg, 0, 1) WITH NOWAIT

    /* Code to read execution history */
    SET @sqlcmd_dbcc_local = @sqlcmd_dbcc + ' WITH NO_INFOMSGS;'
    BEGIN TRY
      INSERT INTO #tmp_exec_history
      (
          updated,
          table_cardinality,
          snapshot_ctr,
          steps,
          density,
          rows_above,
          rows_below,
          squared_variance_error,
          inserts_since_last_update,
          deletes_since_last_update,
          leading_column_type
      )
      EXECUTE sp_executesql @sqlcmd_dbcc_local;
		  END TRY
		  BEGIN CATCH
			   SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to run command to read execution history. Skipping this statistic.'
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Command: ' + @sqlcmd_dbcc_local
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
		  END CATCH
    BEGIN TRY
      ;WITH CTE_1
      AS
      (
        SELECT history_number, ROW_NUMBER() OVER(ORDER BY (SELECT 0)) AS rn 
        FROM #tmp_exec_history
        WHERE [rowid] IS NULL
      )
      UPDATE CTE_1 SET CTE_1.history_number = CTE_1.rn

      UPDATE #tmp_exec_history SET [rowid]         = @rowid,
                                   [database_name] = @database_name,
                                   [schema_name]   = @schema_name,
                                   [table_name]    = @table_name,
                                   [stats_name]    = @stats_name
      WHERE [rowid] IS NULL
		  END TRY
		  BEGIN CATCH
			   SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to update exec_history on temporary table. Skipping this statistic.'
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
      SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
      RAISERROR (@err_msg, 0, 0) WITH NOWAIT
		  END CATCH

    FETCH NEXT FROM c_stats
    INTO @rowid, @database_name, @schema_name, @table_name, @stats_name, @sqlcmd_dbcc
  END
  CLOSE c_stats
  DEALLOCATE c_stats

  /* Disable TF2388 */
  DBCC TRACEOFF(2388) WITH NO_INFOMSGS;

  SET @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished loop through statistics list to collect execution history info using TF2388'
  RAISERROR (@err_msg, 0, 1) WITH NOWAIT

  /* Start to get index usage data */
  SET @err_msg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to get index usage information'
  RAISERROR (@err_msg, 10, 1) WITH NOWAIT

  ALTER TABLE #tmp_stats ADD user_seeks               BIGINT
  ALTER TABLE #tmp_stats ADD user_scans               BIGINT
  ALTER TABLE #tmp_stats ADD user_lookups             BIGINT
  ALTER TABLE #tmp_stats ADD user_updates             BIGINT
  ALTER TABLE #tmp_stats ADD last_user_seek           DATETIME
  ALTER TABLE #tmp_stats ADD last_user_scan           DATETIME
  ALTER TABLE #tmp_stats ADD last_user_lookup         DATETIME
  ALTER TABLE #tmp_stats ADD range_scan_count         BIGINT
  ALTER TABLE #tmp_stats ADD singleton_lookup_count   BIGINT
  ALTER TABLE #tmp_stats ADD leaf_insert_count BIGINT
  ALTER TABLE #tmp_stats ADD leaf_delete_count BIGINT
  ALTER TABLE #tmp_stats ADD leaf_update_count BIGINT
  ALTER TABLE #tmp_stats ADD forwarded_fetch_count BIGINT

  ALTER TABLE #tmp_stats ADD page_latch_wait_count    BIGINT
  ALTER TABLE #tmp_stats ADD page_latch_wait_in_ms BIGINT
  ALTER TABLE #tmp_stats ADD avg_page_latch_wait_in_ms NUMERIC(25, 2)
  ALTER TABLE #tmp_stats ADD page_latch_wait_time_d_h_m_s VARCHAR(200)

  ALTER TABLE #tmp_stats ADD page_io_latch_wait_count BIGINT
  ALTER TABLE #tmp_stats ADD page_io_latch_wait_in_ms BIGINT
  ALTER TABLE #tmp_stats ADD avg_page_io_latch_wait_in_ms NUMERIC(25, 2)
  ALTER TABLE #tmp_stats ADD page_io_latch_wait_time_d_h_m_s VARCHAR(200)

  IF OBJECT_ID('tempdb.dbo.#tmp_dm_db_index_usage_stats') IS NOT NULL
    DROP TABLE #tmp_dm_db_index_usage_stats
  BEGIN TRY
    /* Creating a copy of sys.dm_db_index_usage_stats because this is too slow to access without an index */
    SELECT database_id, object_id, index_id, user_seeks, user_scans, user_lookups, user_updates, last_user_seek, last_user_scan, last_user_lookup
      INTO #tmp_dm_db_index_usage_stats 
      FROM sys.dm_db_index_usage_stats AS ius WITH(NOLOCK)
  END TRY
  BEGIN CATCH
    SET @err_msg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Error while trying to read data from sys.dm_db_index_usage_stats. You may see limited results because of it.'
    RAISERROR (@err_msg, 0,0) WITH NOWAIT
  END CATCH

  CREATE CLUSTERED INDEX ix1 ON #tmp_dm_db_index_usage_stats (database_id, object_id, index_id)

  IF OBJECT_ID('tempdb.dbo.#tmp_dm_db_index_operational_stats') IS NOT NULL
    DROP TABLE #tmp_dm_db_index_operational_stats

  BEGIN TRY
    /* Creating a copy of sys.dm_db_index_operational_stats because this is too slow to access without an index */
    /* Aggregating the results, to have total for all partitions */
    SELECT t.database_id,
           object_id, 
           index_id, 
           SUM(range_scan_count) AS range_scan_count,
           SUM(singleton_lookup_count) AS singleton_lookup_count,
           SUM(page_latch_wait_count) AS page_latch_wait_count,
           SUM(page_io_latch_wait_count) AS page_io_latch_wait_count,
           SUM(leaf_insert_count) AS leaf_insert_count,
           SUM(leaf_delete_count) AS leaf_delete_count,
           SUM(leaf_update_count) AS leaf_update_count,
           SUM(forwarded_fetch_count) AS forwarded_fetch_count,
           SUM(page_latch_wait_in_ms) AS page_latch_wait_in_ms,
           CONVERT(NUMERIC(25, 2),
           CASE 
             WHEN SUM(page_latch_wait_count) > 0 THEN SUM(page_latch_wait_in_ms) / (1. * SUM(page_latch_wait_count))
             ELSE 0 
           END) AS avg_page_latch_wait_in_ms,
           CONVERT(VARCHAR(200), (SUM(page_latch_wait_in_ms) / 1000) / 86400) + ':' + CONVERT(VARCHAR(20), DATEADD(s, (SUM(page_latch_wait_in_ms) / 1000), 0), 108) AS page_latch_wait_time_d_h_m_s,
           SUM(page_io_latch_wait_in_ms) AS page_io_latch_wait_in_ms,
           CONVERT(NUMERIC(25, 2), 
           CASE 
             WHEN SUM(page_io_latch_wait_count) > 0 THEN SUM(page_io_latch_wait_in_ms) / (1. * SUM(page_io_latch_wait_count))
             ELSE 0 
           END) AS avg_page_io_latch_wait_in_ms,
           CONVERT(VARCHAR(200), (SUM(page_io_latch_wait_in_ms) / 1000) / 86400) + ':' + CONVERT(VARCHAR(20), DATEADD(s, (SUM(page_io_latch_wait_in_ms) / 1000), 0), 108) AS page_io_latch_wait_time_d_h_m_s
      INTO #tmp_dm_db_index_operational_stats
      FROM (SELECT DISTINCT database_id FROM #tmp_stats) AS t
     CROSS APPLY sys.dm_db_index_operational_stats (t.database_id, NULL, NULL, NULL) AS ios
     GROUP BY t.database_id, 
           object_id, 
           index_id
     OPTION (MAXDOP 1)
  END TRY
  BEGIN CATCH
    SET @err_msg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Error while trying to read data from sys.dm_db_index_operational_stats. You may see limited results because of it.'
    RAISERROR (@err_msg, 0,0) WITH NOWAIT
  END CATCH

  CREATE CLUSTERED INDEX ix1 ON #tmp_dm_db_index_operational_stats (database_id, object_id, index_id)

  /* 
    I know I'm using a fixed 1 (cluster index id), which means data for clustered columnstore may be missed 
    this is something I need to test and think on how to fix... for now, data for b-tree clustered index should 
    be good enough.
  */
  UPDATE #tmp_stats 
  SET user_seeks                      = ius.user_seeks,
      user_scans                      = ius.user_scans,
      user_lookups                    = ius.user_lookups,
      user_updates                    = ius.user_updates,
      last_user_seek                  = ius.last_user_seek,
      last_user_scan                  = ius.last_user_scan,
      last_user_lookup                = ius.last_user_lookup,
      range_scan_count                = ios.range_scan_count,
      singleton_lookup_count          = ios.singleton_lookup_count,
      page_latch_wait_count           = ios.page_latch_wait_count,
      page_io_latch_wait_count        = ios.page_io_latch_wait_count,
      leaf_insert_count               = ios.leaf_insert_count,
      leaf_delete_count               = ios.leaf_delete_count,
      leaf_update_count               = ios.leaf_update_count,
      forwarded_fetch_count           = ios.forwarded_fetch_count,
      page_latch_wait_in_ms           = ios.page_latch_wait_in_ms,
      avg_page_latch_wait_in_ms       = ios.avg_page_latch_wait_in_ms,
      page_latch_wait_time_d_h_m_s    = ios.page_latch_wait_time_d_h_m_s,
      page_io_latch_wait_in_ms        = ios.page_io_latch_wait_in_ms,
      avg_page_io_latch_wait_in_ms    = ios.avg_page_io_latch_wait_in_ms,
      page_io_latch_wait_time_d_h_m_s = ios.page_io_latch_wait_time_d_h_m_s
  FROM #tmp_stats
  LEFT OUTER JOIN #tmp_dm_db_index_usage_stats AS ius WITH (NOLOCK)
  ON ius.database_id = #tmp_stats.database_id
  AND ius.object_id = #tmp_stats.object_id
  AND (ius.index_id <= CASE WHEN #tmp_stats.[statistic_type] = 'Index_Statistic' THEN #tmp_stats.stats_id ELSE 1 END)
  LEFT OUTER JOIN #tmp_dm_db_index_operational_stats AS ios WITH (NOLOCK)
  ON ios.database_id = #tmp_stats.database_id
  AND ios.object_id = #tmp_stats.object_id
  AND (ios.index_id <= CASE WHEN #tmp_stats.[statistic_type] = 'Index_Statistic' THEN #tmp_stats.stats_id ELSE 1 END)

  /* Finished to get index usage data */
  SET @err_msg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to get index usage information.'
  RAISERROR (@err_msg, 10, 1) WITH NOWAIT

  /* Updating is_auto_update_stats_on, is_auto_update_stats_async_on, is_auto_create_stats_on is_auto_create_stats_incremental_on and is_date_correlation_on columns */
  SET @err_msg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Updating is_auto_update_stats_on, is_auto_update_stats_async_on, is_auto_create_stats_on, is_auto_create_stats_incremental_on and is_date_correlation_on columns.'
  RAISERROR (@err_msg, 10, 1) WITH NOWAIT

  ALTER TABLE #tmp_stats ADD is_auto_update_stats_on BIT
  ALTER TABLE #tmp_stats ADD is_auto_update_stats_async_on BIT
  ALTER TABLE #tmp_stats ADD is_auto_create_stats_on BIT
  ALTER TABLE #tmp_stats ADD is_auto_create_stats_incremental_on BIT
  ALTER TABLE #tmp_stats ADD is_date_correlation_on BIT

  UPDATE #tmp_stats SET is_auto_update_stats_on = databases.is_auto_update_stats_on,
                        is_auto_update_stats_async_on = databases.is_auto_update_stats_async_on,
                        is_auto_create_stats_on = databases.is_auto_create_stats_on,
                        is_auto_create_stats_incremental_on = databases.is_auto_create_stats_incremental_on,
                        is_date_correlation_on = databases.is_date_correlation_on
  FROM #tmp_stats
  INNER JOIN sys.databases
  ON databases.database_id = #tmp_stats.database_id

  /* Updating statistic_percent_sampled column */
  SET @err_msg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Updating statistic_percent_sampled column.'
  RAISERROR (@err_msg, 10, 1) WITH NOWAIT

  ALTER TABLE #tmp_stats ADD statistic_percent_sampled DECIMAL(25, 2)
  UPDATE #tmp_stats SET statistic_percent_sampled = CONVERT(DECIMAL(25, 2), (rows_sampled / (number_of_rows_at_time_stat_was_updated * 1.00)) * 100.0)

  /* Updating plan_cache_reference_count column */
  SET @err_msg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Updating plan_cache_reference_count column.'
  RAISERROR (@err_msg, 10, 1) WITH NOWAIT

  ALTER TABLE #tmp_stats ADD plan_cache_reference_count INT NULL
  UPDATE #tmp_stats
  SET plan_cache_reference_count = (SELECT COUNT(DISTINCT query_hash) 
                                      FROM tempdb.dbo.tmpStatsCheckCachePlanData
                                     WHERE CONVERT(NVARCHAR(MAX), tmpStatsCheckCachePlanData.stats_list) COLLATE Latin1_General_BIN2 LIKE '%' + REPLACE(REPLACE(Tab1.Col1,'[','!['),']','!]') + '%' ESCAPE '!')
  FROM #tmp_stats
  CROSS APPLY (SELECT '(' + (database_name) + '.' + 
                            (schema_name) + '.' + 
                            (table_name) + 
                            ISNULL('.' + (stats_name),'') + ')') AS Tab1(Col1)

  /* Updating dbcc_command column */
  SET @err_msg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Updating dbcc_command column.'
  RAISERROR (@err_msg, 10, 1) WITH NOWAIT

  ALTER TABLE #tmp_stats ADD dbcc_command NVARCHAR(MAX)
  UPDATE #tmp_stats SET dbcc_command = N'DBCC SHOW_STATISTICS (' + '''' + database_name + '.' + schema_name + '.' + table_name + '''' + ',' + stats_name + ')'

  /* Updating auto_update_threshold_type and auto_update_threshold columns */
  SET @err_msg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Updating auto_update_threshold_type and auto_update_threshold columns.'
  RAISERROR (@err_msg, 10, 1) WITH NOWAIT

  ALTER TABLE #tmp_stats ADD auto_update_threshold_type VARCHAR(50)
  ALTER TABLE #tmp_stats ADD auto_update_threshold BIGINT
  UPDATE #tmp_stats SET auto_update_threshold_type = tab1.auto_update_threshold_type,
                        auto_update_threshold = tab1.auto_update_threshold
  FROM #tmp_stats AS a
  CROSS APPLY (SELECT CASE 
                         WHEN (SELECT compatibility_level 
                                 FROM sys.databases 
                                WHERE QUOTENAME(name) = a.database_name) >= 130 
                              AND 
                              COALESCE(a.unfiltered_rows, 0) >= 25001
                           THEN 'Dynamic'
                         ELSE 'Static'
                       END,
                       CASE 
                         WHEN (SELECT compatibility_level 
                                 FROM sys.databases 
                                WHERE QUOTENAME(name) = a.database_name) >= 130
                              AND 
                              COALESCE(a.unfiltered_rows, 0) >= 25001
                           THEN CONVERT(BIGINT, SQRT(1000 * COALESCE(a.unfiltered_rows, 0)))
                         ELSE (CASE
				                             WHEN COALESCE(a.unfiltered_rows, 0) IS NULL THEN 0
				                             WHEN COALESCE(a.unfiltered_rows, 0) <= 500 THEN 501
				                             ELSE 500 + CONVERT(BIGINT, COALESCE(a.unfiltered_rows, 0) * 0.2)
			                            END)
                       END) AS tab1(auto_update_threshold_type, auto_update_threshold)

  /* Finished to get index usage data */
  SET @err_msg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Almost done, creating the final tables with data on tempdb DB.'
  RAISERROR (@err_msg, 10, 1) WITH NOWAIT


  /* Creating tables with collected data */
  SELECT * INTO tempdb.dbo.tmp_stats          FROM #tmp_stats
  SELECT * INTO tempdb.dbo.tmp_stat_header    FROM #tmp_stat_header
  SELECT * INTO tempdb.dbo.tmp_density_vector FROM #tmp_density_vector
  SELECT * INTO tempdb.dbo.tmp_histogram      FROM #tmp_histogram
  SELECT * INTO tempdb.dbo.tmp_stats_stream   FROM #tmp_stats_stream
  SELECT * INTO tempdb.dbo.tmp_exec_history   FROM #tmp_exec_history

  CREATE CLUSTERED INDEX ix1 ON tempdb.dbo.tmp_stats(database_id, object_id, stats_id)
  CREATE INDEX ix2 ON tempdb.dbo.tmp_stats (rowid)
  CREATE INDEX ix3 ON tempdb.dbo.tmp_stats (table_name)
  CREATE INDEX ix4 ON tempdb.dbo.tmp_stats (stats_name)

  CREATE CLUSTERED INDEX ix1 ON tempdb.dbo.tmp_stat_header(rowid)
  CREATE INDEX ix2 ON tempdb.dbo.tmp_stat_header (table_name)
  CREATE INDEX ix3 ON tempdb.dbo.tmp_stat_header (stats_name)

  CREATE CLUSTERED INDEX ix1 ON tempdb.dbo.tmp_density_vector(rowid)
  CREATE INDEX ix2 ON tempdb.dbo.tmp_density_vector (table_name)
  CREATE INDEX ix3 ON tempdb.dbo.tmp_density_vector (stats_name)

  CREATE CLUSTERED INDEX ix1 ON tempdb.dbo.tmp_histogram(rowid)
  CREATE INDEX ix2 ON tempdb.dbo.tmp_histogram (table_name)
  CREATE INDEX ix3 ON tempdb.dbo.tmp_histogram (stats_name)

  CREATE CLUSTERED INDEX ix1 ON tempdb.dbo.tmp_stats_stream(rowid)
  CREATE INDEX ix2 ON tempdb.dbo.tmp_stats_stream (table_name)
  CREATE INDEX ix3 ON tempdb.dbo.tmp_stats_stream (stats_name)

  CREATE CLUSTERED INDEX ix1 ON tempdb.dbo.tmp_exec_history(rowid)
  CREATE INDEX ix2 ON tempdb.dbo.tmp_exec_history (table_name)
  CREATE INDEX ix3 ON tempdb.dbo.tmp_exec_history (stats_name)

  SET @err_msg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Done, statistics information saved on tempdb, tables tmp_stats, tmp_stat_header, tmp_density_vector, tmp_histogram, tmp_stats_stream and tmp_exec_history.'
  RAISERROR (@err_msg, 10, 1) WITH NOWAIT
END
GO