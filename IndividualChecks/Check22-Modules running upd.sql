/* 
Check22 - Modules running update statistic
Description:
Check 22 - Modules manually running UPDATE STATISTICS
It is not usual to run update statistics in a user module (proc, function and etc.).
Estimated Benefit:
Medium
Estimated Effort:
Medium
Recommendation:
Quick recommendation:
Avoid run update statistics in user defined modules.
Detailed recommendation:
- It is not usual to run update statistics in a user module (proc, function and etc.). If you find any, review those to make sure that's it is ok and really needed.
Note: A common case where you may want to do it, is with temporary tables to avoid issues with temporary table cache and statistics.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck22') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck22

IF OBJECT_ID('tempdb.dbo.#db') IS NOT NULL
  DROP TABLE #db

IF OBJECT_ID('tempdb.dbo.#tmp_Objs2') IS NOT NULL
  DROP TABLE #tmp_Objs2

CREATE TABLE #tmp_Objs2 (database_name          NVARCHAR(800),
                         schema_name            NVARCHAR(800),
                         object_name            NVARCHAR(800),
                         type_of_object         NVARCHAR(800),
                         object_code_definition XML)

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
  SET @ErrMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Checking objs on DB - [' + @database_name + ']'
  RAISERROR (@ErrMsg, 10, 1) WITH NOWAIT


  SET @SQL = 'use [' + @database_name + ']; 
              SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
              SELECT QUOTENAME(DB_NAME()) AS database_name, 
                     QUOTENAME(ss.name) AS [schema_name], 
                     QUOTENAME(so.name) AS [object_name], 
                     so.type_desc AS type_of_object,
                     CONVERT(XML, Tab1.Col1) AS object_code_definition
              FROM sys.sql_modules AS sm
              INNER JOIN sys.objects so 
              ON sm.[object_id] = so.[object_id]
              INNER JOIN sys.schemas ss 
              ON so.[schema_id] = ss.[schema_id]
              CROSS APPLY (SELECT CHAR(13)+CHAR(10) + sm.[definition] + CHAR(13)+CHAR(10) FOR XML RAW, ELEMENTS) AS Tab1(Col1)
              WHERE OBJECTPROPERTY(sm.[object_id],''IsMSShipped'') = 0
              AND PATINDEX(''%UPDATE STATISTICS%'', LOWER(sm.[definition]) COLLATE DATABASE_DEFAULT) > 0
              OPTION (FORCE ORDER, MAXDOP 1)'

  /*SELECT @SQL*/
  INSERT INTO #tmp_Objs2
  EXEC (@SQL)
  
  FETCH NEXT FROM c_databases
  into @database_name
END
CLOSE c_databases
DEALLOCATE c_databases

SELECT 'Check 22 - Modules manually running UPDATE STATISTICS' AS [info],
       [database_name],
       [schema_name],
       [object_name],
       type_of_object,
       object_code_definition
INTO tempdb.dbo.tmpStatisticCheck22
FROM #tmp_Objs2

SELECT * FROM tempdb.dbo.tmpStatisticCheck22