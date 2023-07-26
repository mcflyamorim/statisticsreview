/*
Check43 - Search maintenance plan
Description:
Check 43 - Search for an update statistic maintenance plan
Looking at modules and jobs to see if I can find a statistic maintenance plan.
Estimated Benefit:
High
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Make sure you have an update statistic maintenance plan.
Detailed recommendation:
- If the maintenance plan is not identified, double check there is one, and if not, make sure you add it.
- Avoid sp_updatestats, this proc is not very smart, it simple does a "@ind_rowmodctr <> 0", I think you can do better. I would always vote for Ola's maintenance script.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck43') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck43

DECLARE @dbid int, @dbname VARCHAR(1000), @sqlcmd NVARCHAR(4000)
DECLARE @ErrorMessage NVARCHAR(4000)

IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.##tmp1Check43'))
DROP TABLE ##tmp1Check43;
IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.##tmp1Check43'))
CREATE TABLE ##tmp1Check43 ([DBName] VARCHAR(MAX), [Schema] VARCHAR(MAX), [Object] VARCHAR(MAX), [Type] VARCHAR(MAX), [JobName] VARCHAR(MAX), [is_enabled] BIT, [Step] VARCHAR(MAX), CommandFound VARCHAR(MAX));
		
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblKeywords'))
DROP TABLE #tblKeywords;
IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblKeywords'))
CREATE TABLE #tblKeywords (
	KeywordID int IDENTITY(1,1) PRIMARY KEY,
	Keyword VARCHAR(64) -- the keyword itself
	);

IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.indexes (NOLOCK) WHERE name = N'UI_Keywords' AND [object_id] = OBJECT_ID('tempdb.dbo.#tblKeywords'))
CREATE UNIQUE INDEX UI_Keywords ON #tblKeywords(Keyword);

INSERT INTO #tblKeywords (Keyword)
VALUES ('UPDATE STATISTICS'), ('UPDATE STATS'), ('sp_updatestats')

IF EXISTS
(
   SELECT [object_id]
   FROM tempdb.sys.objects (NOLOCK)
   WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpdbs0')
)
   DROP TABLE #tmpdbs0;
IF NOT EXISTS
(
   SELECT [object_id]
   FROM tempdb.sys.objects (NOLOCK)
   WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpdbs0')
)
   CREATE TABLE #tmpdbs0
   (
       id INT IDENTITY(1, 1),
       [dbid] INT,
       [dbname] NVARCHAR(1000),
       is_read_only BIT,
       [state] TINYINT,
       isdone BIT
   );

SET @sqlcmd
   = N'SELECT database_id, name, is_read_only, [state], 0 FROM master.sys.databases (NOLOCK) 
               WHERE name <> ''tempdb'' and state_desc = ''ONLINE'' and is_read_only = 0';
INSERT INTO #tmpdbs0
(
   [dbid],
   [dbname],
   is_read_only,
   [state],
   [isdone]
)
EXEC sp_executesql @sqlcmd;

UPDATE #tmpdbs0
SET isdone = 0;

IF (SELECT COUNT(id) FROM #tmpdbs0 WHERE isdone = 0) > 0
BEGIN
	 WHILE (SELECT COUNT(id) FROM #tmpdbs0 WHERE isdone = 0) > 0
	 BEGIN
		  SELECT TOP 1 @dbname = [dbname], @dbid = [dbid] FROM #tmpdbs0 WHERE isdone = 0

		  SET @sqlcmd = 'USE ' + QUOTENAME(@dbname) + ';
                   SELECT N''' + REPLACE(@dbname, CHAR(39), CHAR(95)) + ''' AS [DBName], ss.name AS [Schema_Name], so.name AS [Object_Name], so.type_desc, tk.Keyword
                   FROM sys.sql_modules sm (NOLOCK)
                   INNER JOIN sys.objects so (NOLOCK) ON sm.[object_id] = so.[object_id]
                   INNER JOIN sys.schemas ss (NOLOCK) ON so.[schema_id] = ss.[schema_id]
                   CROSS JOIN #tblKeywords tk (NOLOCK)
                   WHERE PATINDEX(''%'' + tk.Keyword + ''%'', LOWER(sm.[definition]) COLLATE DATABASE_DEFAULT) > 1
                   AND OBJECTPROPERTY(sm.[object_id],''IsMSShipped'') = 0;'

    BEGIN TRY
	     INSERT INTO ##tmp1Check43 ([DBName], [Schema], [Object], [Type], CommandFound)
	     EXECUTE sp_executesql @sqlcmd
    END TRY
    BEGIN CATCH
	     SELECT @ErrorMessage = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Error raised in TRY block. ' + ERROR_MESSAGE()
	     RAISERROR (@ErrorMessage, 16, 1);
    END CATCH
		
		  UPDATE #tmpdbs0
		  SET isdone = 1
		  WHERE [dbid] = @dbid
	 END
END;

INSERT INTO #tblKeywords (Keyword)
SELECT DISTINCT Object
FROM ##tmp1Check43

SET @sqlcmd = 'USE [msdb];
               SELECT t.[DBName], t.[Schema], t.[Object], t.[Type], sj.[name], sj.[enabled], sjs.step_name, sjs.[command]
               FROM msdb.dbo.sysjobsteps sjs (NOLOCK)
               INNER JOIN msdb.dbo.sysjobs sj (NOLOCK) ON sjs.job_id = sj.job_id
               CROSS JOIN #tblKeywords tk (NOLOCK)
               OUTER APPLY (SELECT TOP 1 * FROM ##tmp1Check43 WHERE ##tmp1Check43.[Object] = tk.Keyword) AS t
               WHERE PATINDEX(''%'' + tk.Keyword + ''%'', LOWER(sjs.[command]) COLLATE DATABASE_DEFAULT) > 0
               AND sjs.[subsystem] IN (''TSQL'',''PowerShell'', ''CMDEXEC'');'

BEGIN TRY
	 INSERT INTO ##tmp1Check43 ([DBName], [Schema], [Object], [Type], JobName, [is_enabled], Step, CommandFound)
	 EXECUTE sp_executesql @sqlcmd
END TRY
BEGIN CATCH
	 SELECT @ErrorMessage = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Error raised in jobs TRY block. ' + ERROR_MESSAGE()
	 RAISERROR (@ErrorMessage, 16, 1);
END CATCH

IF (SELECT COUNT(*) FROM ##tmp1Check43) > 0
BEGIN
		SELECT 'Check 43 - Search for an update statistic maintenance plan' AS [info],
         DBName AS database_name,
         [Schema] AS schema_name,
         Object AS object_name,
         Type AS type,
         JobName AS job_name,
         is_enabled,
         Step AS step,
         CommandFound AS command_found,
         'OK' AS Comment
  INTO tempdb.dbo.tmpStatisticCheck43
  FROM ##tmp1Check43
  WHERE JobName IS NOT NULL
END
ELSE
BEGIN
	 SELECT 'Check 43 - Search for an update statistic maintenance plan' AS [info],
         'Could not find a job or procedure running update statistic, check manually.' AS comment
  INTO tempdb.dbo.tmpStatisticCheck43
END;

SELECT * FROM tempdb.dbo.tmpStatisticCheck43