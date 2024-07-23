/*
Check17 - NoExpand statistics on views
Description:
Check 17 - Statistics on views are only created and used when using noexpand.
SQL Server can create statistics automatically to assist with cardinality estimation and cost-based decision-making during query optimization. 
This feature works with indexed views as well as base tables, but, for indexed views, it only create/uses the statistic if the view is explicitly named in the query and the NOEXPAND hint is specified.
Estimated Benefit:
High
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Use noexpand hint to enable auto create and usage of statistics on indexes views.
Detailed recommendation:
- If you have query using an indexed view, it may be worthy to review all queries using it and make sure you're using NOEXPAND to enable auto create and usage of statistics to have more accurate estimates.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('dbo.tmpStatisticCheck17') IS NOT NULL
  DROP TABLE dbo.tmpStatisticCheck17

IF OBJECT_ID('tempdb.dbo.#db') IS NOT NULL
  DROP TABLE #db

IF OBJECT_ID('tempdb.dbo.#tmp_IndexedViews1') IS NOT NULL
  DROP TABLE #tmp_IndexedViews1

CREATE TABLE #tmp_IndexedViews1 (database_name NVarChar(800),
                                 schema_name   NVarChar(800),
                                 view_name     NVarChar(2000),
                                 IndexName    NVarChar(800))

IF OBJECT_ID('tempdb.dbo.#tmp_IndexedViews2') IS NOT NULL
  DROP TABLE #tmp_IndexedViews2

CREATE TABLE #tmp_IndexedViews2 (database_name                 NVARCHAR(800),
                                 schema_name                   NVARCHAR(800),
                                 object_name                   NVARCHAR(800),
                                 type_of_object                NVARCHAR(800),
                                 does_it_has_noexpand_keyword  VARCHAR(3),
                                 view_name                     NVARCHAR(2000),
                                 index_name                    NVARCHAR(800),
                                 object_code_definition        XML)

SELECT d1.[name] into #db
FROM sys.databases d1
where d1.state_desc = 'ONLINE' and is_read_only = 0
and d1.database_id in (SELECT DISTINCT database_id FROM dbo.tmpStatisticCheck_stats)

DECLARE @SQL VarChar(MAX)
declare @database_name sysname
DECLARE @ErrMsg VarChar(8000)

DECLARE c_databases CURSOR read_only FOR
    SELECT [name] FROM #db
OPEN c_databases

FETCH NEXT FROM c_databases
into @database_name
WHILE @@FETCH_STATUS = 0
BEGIN
  SET @SQL = 'use [' + @database_name + ']; 
              SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
              select 
                    QUOTENAME(DB_NAME()) AS database_name, 
                    QUOTENAME(OBJECT_SCHEMA_NAME(si.object_id)) AS schema_name,
                    QUOTENAME(OBJECT_NAME(si.object_id)) AS [view_name],
                    QUOTENAME(si.name) AS index_name
                from sys.indexes AS si
                inner join sys.views AS sv
                    ON si.object_id = sv.object_id
                OPTION (MAXDOP 1)'

  /*SELECT @SQL*/
  INSERT INTO #tmp_IndexedViews1
  EXEC (@SQL)

  SET @SQL = 'use [' + @database_name + ']; 
              SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
              ;WITH CTE_1
              AS
              (
                select 
                    QUOTENAME(DB_NAME()) + ''.'' + 
                    QUOTENAME(OBJECT_SCHEMA_NAME(si.object_id)) + ''.'' + 
                    QUOTENAME(OBJECT_NAME(si.object_id)) AS [view_name],
                    OBJECT_NAME(si.object_id) AS tmpview_name,
                    QUOTENAME(si.name) AS index_name
                from sys.indexes AS si
                inner join sys.views AS sv
                    ON si.object_id = sv.object_id
              )
              SELECT QUOTENAME(DB_NAME()) AS database_name, 
                     QUOTENAME(ss.name) AS [schema_name], 
                     QUOTENAME(so.name) AS [object_name], 
                     so.type_desc type_of_object,
                     CASE 
                       WHEN PATINDEX(''%noexpand%'', LOWER(sm.[definition]) COLLATE DATABASE_DEFAULT) > 0 THEN ''Yes''
                       ELSE ''No''
                     END AS does_it_has_noexpand_keyword,
                     t.view_name,
                     t.index_name,
                     CONVERT(XML, Tab1.Col1) AS object_code_definition
              FROM CTE_1 AS t
              INNER JOIN sys.sql_modules sm
              ON PATINDEX(''%'' + t.tmpview_name + ''%'', LOWER(sm.[definition]) COLLATE DATABASE_DEFAULT) > 0
              INNER JOIN sys.objects so 
              ON sm.[object_id] = so.[object_id]
              INNER JOIN sys.schemas ss 
              ON so.[schema_id] = ss.[schema_id]
              CROSS APPLY (SELECT CHAR(13)+CHAR(10) + sm.[definition] + CHAR(13)+CHAR(10) FOR XML RAW, ELEMENTS) AS Tab1(Col1)
              WHERE OBJECTPROPERTY(sm.[object_id],''IsMSShipped'') = 0
              AND OBJECT_NAME(sm.object_id) <> t.tmpview_name
              OPTION (FORCE ORDER, MAXDOP 1)'

  /*SELECT @SQL*/
  INSERT INTO #tmp_IndexedViews2
  EXEC (@SQL)
  
  FETCH NEXT FROM c_databases
  into @database_name
END
CLOSE c_databases
DEALLOCATE c_databases

--SELECT 'Check 17 - Statistics on views are only created and used when using noexpand' AS [info],
--       * 
--FROM #tmp_IndexedViews1


SELECT 'Check 17 - Statistics on views are only created and used when using noexpand' AS [info],
       database_name,
       schema_name,
       object_name,
       type_of_object,
       does_it_has_noexpand_keyword,
       CASE does_it_has_noexpand_keyword
         WHEN 'Yes' THEN 'Code looks good, but, please double check it to confirm that noexpand is really used, it may be in a commented text or something else.'
         ELSE 'Warning - Indexed view is referenced but NOEXPAND is not used, make sure you add it to benefit of statistics'
       END AS [comment],
       view_name,
       object_code_definition
INTO dbo.tmpStatisticCheck17
FROM #tmp_IndexedViews2

SELECT * FROM dbo.tmpStatisticCheck17