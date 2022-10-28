/*
Check 38 - Check if there are "anti-matter" columns, as this may cause issues with update stats.

< ---------------- Description ----------------- >
During the build phase, rows in the new "in-build" index may be in an intermediate state called antimatter. 
This mechanism allows concurrent DELETE statements to leave a trace for the index builder transaction 
to avoid inserting deleted rows. At the end of the index build operation 
all antimatter rows should be cleared. If an error occurs and antimatter rows remain in the index.
Rebuilding the index will remove the antimatter rows and resolve the error.

< -------------- What to look for and recommendations -------------- >
- I had weird (stats blob was not saved) issues when table had "anti-matter" columns.
I didn't had time to create a repro, I'll do it later. For now I'd say to 
go ahead and rebuild the index to avoid issues.
*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck38') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck38

IF OBJECT_ID('tempdb.dbo.#db') IS NOT NULL
  DROP TABLE #db

IF OBJECT_ID('tempdb.dbo.#tmp_AntiMatterColumns') IS NOT NULL
  DROP TABLE #tmp_AntiMatterColumns

CREATE TABLE #tmp_AntiMatterColumns (database_name VARCHAR(400),
                                     table_name VARCHAR(400),
                                     index_name VARCHAR(400),
                                     number_of_rows BIGINT,
                                     command VARCHAR(MAX))

SELECT d1.[name] INTO #db
FROM sys.databases d1
WHERE d1.state_desc = 'ONLINE' AND is_read_only = 0
AND d1.database_id IN (SELECT DISTINCT database_id FROM tempdb.dbo.tmp_stats)

DECLARE @SQL VARCHAR(MAX)
DECLARE @database_name sysname
DECLARE @ErrMsg VARCHAR(8000)

DECLARE c_databases CURSOR READ_ONLY FOR
    SELECT [name] FROM #db
OPEN c_databases

FETCH NEXT FROM c_databases
INTO @database_name
WHILE @@FETCH_STATUS = 0
BEGIN
  SET @ErrMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Checking anti-matter columns on DB - [' + @database_name + ']'
  RAISERROR (@ErrMsg, 10, 1) WITH NOWAIT

  SET @SQL = 'use [' + @database_name + ']; 
              SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
              SELECT DISTINCT 
                     DB_NAME() AS database_name, 
                     OBJECT_NAME(p.object_id) AS table_name,
                     i.name AS index_name,
                     t.number_of_rows,
                     ''ALTER INDEX '' + QUOTENAME(i.name) + 
                     + '' ON '' + QUOTENAME(DB_NAME()) + ''.'' + 
                                QUOTENAME(OBJECT_SCHEMA_NAME(p.object_id)) + ''.'' +
                                QUOTENAME(OBJECT_NAME(p.object_id)) + 
                     '' REBUILD WITH('' + 
                     CASE 
                       WHEN CONVERT(VarChar(200), SERVERPROPERTY(''Edition'')) LIKE ''Developer%''
                         OR CONVERT(VarChar(200), SERVERPROPERTY(''Edition'')) LIKE ''Enterprise%'' THEN ''ONLINE=ON)''
                       ELSE ''ONLINE=OFF)''
                     END AS [command]
              FROM sys.system_internals_partitions p
              INNER JOIN sys.system_internals_partition_columns pc
	              ON p.partition_id = pc.partition_id
              LEFT OUTER JOIN sys.indexes i
              ON p.object_id = i.object_id
              AND p.index_id = i.index_id
              OUTER APPLY (SELECT partitions.rows
                           FROM sys.partitions
                           WHERE p.object_id = partitions.object_id
                           AND partitions.index_id <= 1
                           AND partitions.partition_number <= 1) AS t (number_of_rows)
              WHERE pc.is_anti_matter = 1'

  /*SELECT @SQL*/
  INSERT INTO #tmp_AntiMatterColumns
  EXEC (@SQL)
  
  FETCH NEXT FROM c_databases
  into @database_name
END
CLOSE c_databases
DEALLOCATE c_databases

SELECT 'Check 38 - Check if there are "anti-matter" columns, as this may cause issues with update stats.' AS [info],
       *
INTO tempdb.dbo.tmpStatisticCheck38
FROM #tmp_AntiMatterColumns

SELECT * FROM tempdb.dbo.tmpStatisticCheck38
ORDER BY number_of_rows DESC