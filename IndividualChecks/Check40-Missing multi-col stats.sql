/*
Check40 - Missing multi column statistics
Description:
Check 40 - Multi column statistics missing on foreign keys.
Creating manual statistics on both columns together (a, b) could allow the Database Engine to make a better estimate for the query, because the statistics also contain the average number of distinct values for the combination of columns a and b.
Estimated Benefit:
Medium
Estimated Effort:
Medium
Recommendation:
Quick recommendation:
Consider to create a statistic or index on the multi-column foreign.
Detailed recommendation:
- Check if the multi-column foreign key should have an index or a statistic.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck40') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck40

DECLARE @sqlcmd NVARCHAR(MAX);
DECLARE @ErrorMessage NVARCHAR(4000);

IF EXISTS
(
    SELECT [object_id]
    FROM tempdb.sys.objects (NOLOCK)
    WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblFK')
)
    DROP TABLE #tblFK;
IF NOT EXISTS
(
    SELECT [object_id]
    FROM tempdb.sys.objects (NOLOCK)
    WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblFK')
)
    CREATE TABLE #tblFK
    (
        [database_name] sysname,
        [constraint_name] NVARCHAR(200),
        [parent_schema_name] NVARCHAR(100),
        [parent_table_name] NVARCHAR(200),
        parent_columns NVARCHAR(4000),
        [referenced_schema] NVARCHAR(100),
        [referenced_table_name] NVARCHAR(200),
        referenced_columns NVARCHAR(4000),
        CONSTRAINT PK_FK_check40
            PRIMARY KEY CLUSTERED (
                                      database_name,
                                      [constraint_name],
                                      [parent_schema_name]
                                  )
    );

IF OBJECT_ID('tempdb.dbo.#db') IS NOT NULL
    DROP TABLE #db;

SELECT d1.[name]
INTO #db
FROM sys.databases d1
WHERE d1.state_desc = 'ONLINE'
      AND is_read_only = 0
      AND d1.database_id IN
          (
              SELECT DISTINCT
                     database_id
              FROM tempdb.dbo.tmp_stats
          );

DECLARE @database_name sysname;
DECLARE @ErrMsg VARCHAR(8000);

DECLARE c_databases CURSOR READ_ONLY FOR SELECT [name] FROM #db;
OPEN c_databases;

FETCH NEXT FROM c_databases
INTO @database_name;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @ErrMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Checking FKs stats on DB - [' + @database_name + ']';
    RAISERROR(@ErrMsg, 10, 1) WITH NOWAIT;

    SET @sqlcmd
        = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                  USE ' + QUOTENAME(@database_name)
          + N'
                  ;WITH cteFK AS (
                  SELECT t.name AS [parent_schema_name], OBJECT_NAME(FKC.parent_object_id) [parent_table_name], OBJECT_NAME(constraint_object_id) AS [constraint_name],
	                  t2.name AS [referenced_schema], OBJECT_NAME(referenced_object_id) AS [referenced_table_name],
	                  tParentCols.ParentCols AS [parent_columns],
	                  SUBSTRING((SELECT '','' + RTRIM(COL_NAME(k.referenced_object_id,referenced_column_id)) AS [data()]
		                  FROM sys.foreign_key_columns k (NOLOCK)
		                  INNER JOIN sys.foreign_keys (NOLOCK) ON k.constraint_object_id = [object_id] AND k.constraint_object_id = FKC.constraint_object_id
		                  ORDER BY constraint_column_id FOR XML PATH('''')), 2, 8000) AS [referenced_columns]
                  FROM sys.foreign_key_columns FKC (NOLOCK)
                  INNER JOIN sys.objects o (NOLOCK) ON FKC.parent_object_id = o.[object_id]
                  INNER JOIN sys.tables mst (NOLOCK) ON mst.[object_id] = o.[object_id]
                  INNER JOIN sys.schemas t (NOLOCK) ON t.[schema_id] = mst.[schema_id]
                  INNER JOIN sys.objects so (NOLOCK) ON FKC.referenced_object_id = so.[object_id]
                  INNER JOIN sys.tables AS mst2 (NOLOCK) ON mst2.[object_id] = so.[object_id]
                  INNER JOIN sys.schemas AS t2 (NOLOCK) ON t2.[schema_id] = mst2.[schema_id]
                  CROSS APPLY (SELECT SUBSTRING((SELECT '','' + RTRIM(COL_NAME(k.parent_object_id,parent_column_id)) AS [data()] FROM sys.foreign_key_columns k (NOLOCK) INNER JOIN sys.foreign_keys (NOLOCK) ON k.constraint_object_id = [object_id] AND k.constraint_object_id = FKC.constraint_object_id ORDER BY constraint_column_id FOR XML PATH('''')), 2, 8000)) AS tParentCols(ParentCols)
                  WHERE o.type = ''U'' AND so.type = ''U'' AND tParentCols.ParentCols LIKE ''%,%''
                  GROUP BY o.[schema_id],so.[schema_id],FKC.parent_object_id,constraint_object_id,referenced_object_id,t.name,t2.name, tParentCols.ParentCols
                  ),
                  cteStatsCols AS (
                  SELECT t.name AS schema_name, OBJECT_NAME(mst.[object_id]) AS objectName,
                  SUBSTRING(( SELECT '','' + RTRIM(ac.name) FROM sys.tables AS st
	                  INNER JOIN sys.stats AS mi ON st.[object_id] = mi.[object_id]
	                  INNER JOIN sys.stats_columns AS ic ON mi.[object_id] = ic.[object_id] AND mi.[stats_id] = ic.[stats_id] 
	                  INNER JOIN sys.all_columns AS ac ON st.[object_id] = ac.[object_id] AND ic.[column_id] = ac.[column_id]
	                  WHERE i.[object_id] = mi.[object_id] AND i.stats_id = mi.stats_id
	                  ORDER BY ac.column_id FOR XML PATH('''')), 2, 8000) AS KeyCols
                  FROM sys.stats AS i
                  INNER JOIN sys.tables AS mst ON mst.[object_id] = i.[object_id]
                  INNER JOIN sys.schemas AS t ON t.[schema_id] = mst.[schema_id] WHERE mst.is_ms_shipped = 0)
                  SELECT DB_NAME() AS Database_Name, fk.constraint_name AS constraintName,
	                  fk.parent_schema_name AS schema_name, fk.parent_table_name AS table_name,
	                  REPLACE(fk.parent_columns,'' ,'','','') AS parentColumns, fk.referenced_schema AS referencedschema_name,
	                  fk.referenced_table_name AS referencedtable_name, REPLACE(fk.referenced_columns,'' ,'','','') AS referencedColumns
                  FROM cteFK fk
                  WHERE NOT EXISTS (SELECT 1 FROM cteStatsCols ict WHERE fk.parent_schema_name = ict.schema_name AND fk.parent_table_name = ict.objectName AND REPLACE(fk.parent_columns,'' ,'','','') = ict.KeyCols);';
    BEGIN TRY
        --PRINT @sqlcmd;
        INSERT INTO #tblFK
        EXECUTE sp_executesql @sqlcmd;
    END TRY
    BEGIN CATCH
        SELECT ERROR_NUMBER() AS ErrorNumber,
               ERROR_MESSAGE() AS ErrorMessage;
        SELECT @ErrorMessage
            = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - '
              + N'Check 40 - Multi column statistics missing on foreign keys - Error raised in TRY block in database ' + @database_name
              + N'. ' + ERROR_MESSAGE();
        RAISERROR(@ErrorMessage, 16, 1);
    END CATCH;

    FETCH NEXT FROM c_databases
    INTO @database_name;
END;
CLOSE c_databases;
DEALLOCATE c_databases;

IF
(
    SELECT COUNT(*)FROM #tblFK
) > 0
BEGIN
    SELECT 'Check 40 - Multi column statistics missing on foreign keys' AS [info],
           'Warning - Multi column foreign key constraints is not supported by a statistic. It is recommended to revise these.' AS [comment],
           QUOTENAME(FK.[database_name]) AS [database_name],
           QUOTENAME(constraint_name) AS [constraint_name],
           QUOTENAME(FK.parent_schema_name) AS [schema_name],
           QUOTENAME(FK.parent_table_name) AS [table_name],
           QUOTENAME(FK.parent_columns) AS parent_columns,
           QUOTENAME(FK.referenced_schema) AS referenced_schema_name,
           QUOTENAME(FK.referenced_table_name) AS referenced_table_name,
           QUOTENAME(FK.referenced_columns) AS referenced_columns,
           'CREATE STATISTICS [Stats_'
           + REPLACE(constraint_name, ' ', '_') + ']' + ' ON ' + QUOTENAME(FK.[database_name]) + '.' + QUOTENAME(parent_schema_name) + '.'
           + QUOTENAME(parent_table_name) + ' ([' + REPLACE(REPLACE(parent_columns, ',', '],['), ']]', ']') + ']);' AS create_stat_command
    INTO tempdb.dbo.tmpStatisticCheck40
    FROM #tblFK FK
    ORDER BY [database_name],
             parent_schema_name,
             parent_table_name,
             referenced_schema,
             referenced_table_name;
END;
ELSE
BEGIN
    SELECT 'Check 40 - Multi column statistics missing on foreign keys' AS [info],
           'OK' AS [comment]
    INTO tempdb.dbo.tmpStatisticCheck40;
END;

SELECT * FROM tempdb.dbo.tmpStatisticCheck40