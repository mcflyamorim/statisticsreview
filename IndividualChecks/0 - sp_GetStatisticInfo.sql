USE [master];
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'sp_GetStatisticInfo')
	EXEC ('CREATE PROC dbo.sp_GetStatisticInfo AS SELECT 1')
GO

/*


*/

ALTER PROC dbo.sp_GetStatisticInfo
(
  @database_name_filter NVARCHAR(200) = NULL, /* By default I'm collecting information about all DBs */
  @refreshdata  BIT = 0 /* 1 to force drop/create of statistics tables, 0 will skip table creation if they already exists */
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
  EXEC sp_GetStatisticInfo @DatabaseName = NULL

Collect statistic information for Northwind DB:
  EXEC sp_GetStatisticInfo @DatabaseName = 'Northwind'

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
  SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; SET ANSI_WARNINGS OFF;
  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET LOCK_TIMEOUT 60000; /*if I get blocked for more than 1 minute I'll quit, I don't want to wait or cause other blocks*/

  DECLARE @sqlmajorver       INT,
          @number_of_stats   BIGINT,
          @rowid             INT,
          @database_name     SYSNAME,
          @schema_name       SYSNAME,
          @table_name        SYSNAME,
          @stats_name        SYSNAME,
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
    [stats_name] SYSNAME,
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
    [statistic_type] [nvarchar] (200) NULL,
    [number_of_columns_in_this_table] [int] NULL,
    [number_of_statistics_in_this_table] [int] NULL,
    [is_table_partitioned] [bit] NULL,
    [table_index_base_type] [nvarchar] (200) NULL,
    [index_type] [NVARCHAR] (200) NULL,
    [number_of_in_row_data_pages_on_table] [bigint] NULL,
    [number_of_lob_data_pages_on_table] [bigint] NULL,
    [key_column_name] NVARCHAR(800),
    [key_column_data_type] NVARCHAR(800),
    [stat_all_columns] NVARCHAR(4000),
    [stat_all_columns_index_order] NVARCHAR(4000),
    [stat_all_columns_stat_order] NVARCHAR(4000),
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
    [stats_name] SYSNAME NULL,
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
    [stats_name] SYSNAME NULL,
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
    [stats_name] SYSNAME NULL,
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
    [stats_name] SYSNAME NULL,
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
    [stats_name] SYSNAME NULL,
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
  ALTER TABLE #tmp_stats ADD page_latch_wait_count    BIGINT
  ALTER TABLE #tmp_stats ADD page_io_latch_wait_count BIGINT

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
    SELECT ios.database_id, 
           ios.object_id, 
           ios.index_id, 
           SUM(ios.range_scan_count) AS range_scan_count,
           SUM(ios.singleton_lookup_count) AS singleton_lookup_count,
           SUM(ios.page_latch_wait_count) AS page_latch_wait_count,
           SUM(ios.page_io_latch_wait_count) AS page_io_latch_wait_count
      INTO #tmp_dm_db_index_operational_stats
      FROM (SELECT DISTINCT database_id FROM #tmp_stats) AS t
     CROSS APPLY sys.dm_db_index_operational_stats (t.database_id, NULL, NULL, NULL) AS ios
     GROUP BY ios.database_id, 
           ios.object_id, 
           ios.index_id
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
  SET user_seeks               = ius.user_seeks,
      user_scans               = ius.user_scans,
      user_lookups             = ius.user_lookups,
      user_updates             = ius.user_updates,
      last_user_seek           = ius.last_user_seek,
      last_user_scan           = ius.last_user_scan,
      last_user_lookup         = ius.last_user_lookup,
      range_scan_count         = ios.range_scan_count,
      singleton_lookup_count   = ios.singleton_lookup_count,
      page_latch_wait_count    = ios.page_latch_wait_count,
      page_io_latch_wait_count = ios.page_io_latch_wait_count
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
  UPDATE #tmp_stats SET statistic_percent_sampled = CONVERT(DECIMAL(25, 2), (rows_sampled / (current_number_of_rows * 1.00)) * 100.0)

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

EXEC [sys].[sp_MS_marksystemobject] 'sp_GetStatisticInfo';