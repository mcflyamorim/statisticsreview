/*
Check12 - TF2389 and TF2390 histogram
Description:
Check 12 - Trace flag check - TF2389 and TF2390 (Enable automatically generated quick statistics (histogram amendment))
Check TF2389 and TF2390:
TF2389 enable automatically generated quick statistics for ascending keys (histogram amendment).
TF2390 enable automatically generated quick statistics regardless of the leading statistics column status (ascending, descending, unknown or stationary).
Estimated Benefit:
Medium
Estimated Effort:
High
Recommendation:
Quick recommendation:
Consider to enable trace flag 2389 and 2390.
Detailed recommendation:
- Check whether you have statistics with number of inserted rows that are beyond the highest RANGE_HI_KEY value in the histogram, and those statistics are still considered unknown or stationary. If so, queries trying to read those recent rows would be beneficial of automatically generate quick statistics (histogram amendment) regardless of key column status.
- If you have databases with compatibility level < 120 (SQL2014). Enable TF 2389 and 2390 to automatically generate quick statistics (histogram amendment) regardless of key column status.
Note 1: This trace flag does not apply to CE version 120 or above. Use trace flag 4139 instead.

Note 2: On KB3189645 (SQL2014 SP1 CU9(12.00.4474) and SP2 CU2(12.00.5532)) filtered indexes are exempted from quickstats queries because it had a bug with filtered indexes and columnstore, but that ended up fixing another problem that when the quickstats query was issued for filtered index stats it has no filter, which was making a full scan (unless a nonfiltered index with the same first column happens to be around to help).

Warning Note: Customers should always test changes related to trace flags or/and to the compatibility level carefully. You should always test and evaluate those changes before apply it in production.  Use mitigation technologies, such as the Query Store, if there is a plan-choice related performance issue.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('dbo.tmpStatisticCheck12') IS NOT NULL
  DROP TABLE dbo.tmpStatisticCheck12

IF OBJECT_ID('tempdb.dbo.#tmpCheck12') IS NOT NULL
  DROP TABLE #tmpCheck12
SELECT 
  a.database_name, a.table_name, a.stats_name, a.key_column_name, a.current_number_of_rows,
  b.leading_column_type, 
  CONVERT(NUMERIC(25, 2), b.rows_above) AS number_of_rows_inserted_above,
  CONVERT(NUMERIC(25, 2), b.rows_below) AS number_of_rows_inserted_below,
  a.dbcc_command
INTO #tmpCheck12
FROM dbo.tmpStatisticCheck_stats AS a
INNER JOIN dbo.tmpStatisticCheck_exec_history b
ON b.rowid = a.rowid
AND b.history_number = 1
AND b.leading_column_type IN ('Unknown', 'Stationary')
WHERE b.rows_above > 0 
AND b.rows_below = 0
AND current_number_of_rows > 1000 /* Ignoring small tables */

DECLARE @sqlmajorver INT, @sqlminorver INT, @sqlbuild INT
SELECT @sqlmajorver = CONVERT(INT, (@@microsoftversion / 0x1000000) & 0xff),
	      @sqlminorver = CONVERT(INT, (@@microsoftversion / 0x10000) & 0xff),
 	     @sqlbuild = CONVERT(INT, @@microsoftversion & 0xffff);

DECLARE @tracestatus TABLE(TraceFlag nVarChar(40)
                         , Status    tinyint
                         , Global    tinyint
                         , Session   tinyint)

INSERT INTO @tracestatus
EXEC ('DBCC TRACESTATUS WITH NO_INFOMSGS')

IF OBJECT_ID('tempdb.dbo.#dbScopedConfig') IS NOT NULL
  DROP TABLE #dbScopedConfig

CREATE TABLE #dbScopedConfig (database_name VARCHAR(400), [Legacy_Cardinality_Estimation_Status] Bit)

-- If SQL2016, check LEGACY_CARDINALITY_ESTIMATION scoped config
IF @sqlmajorver >= 13 /*SQL2016*/
BEGIN
  IF OBJECT_ID('tempdb.dbo.#db') IS NOT NULL
    DROP TABLE #db

  SELECT d1.[name] INTO #db
  FROM sys.databases d1
  where d1.state_desc = 'ONLINE' and is_read_only = 0
  AND Name NOT IN('master', 'msdb', 'model', 'tempdb', 'distribution')
  and d1.database_id in (SELECT DISTINCT database_id FROM dbo.tmpStatisticCheck_stats)

  DECLARE @SQL NVarChar(MAX)
  declare @database_name sysname
  DECLARE @ErrMsg VarChar(8000)

  DECLARE c_databases CURSOR read_only FOR
      SELECT [name] FROM #db
  OPEN c_databases

  FETCH NEXT FROM c_databases
  into @database_name
  WHILE @@FETCH_STATUS = 0
  BEGIN
    SET @ErrMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Checking Legacy_Cardinality_Estimation_Status on DB - [' + @database_name + ']'
    --RAISERROR (@ErrMsg, 10, 1) WITH NOWAIT

    SET @SQL = 
    'use [' + @database_name + '];
     SELECT DB_NAME() AS database_name, CONVERT(Int, value) AS [Legacy_Cardinality_Estimation_Status] FROM sys.database_scoped_configurations WHERE name=''LEGACY_CARDINALITY_ESTIMATION'';'; 

    INSERT INTO #dbScopedConfig
    EXEC (@SQL)

    FETCH NEXT FROM c_databases
    into @database_name
  END
  CLOSE c_databases
  DEALLOCATE c_databases
END

SELECT 
  'Check 12 - Trace flag check - TF2389 and TF2390 (Enable automatically generated quick statistics (histogram amendment))' AS [info],
  #tmpCheck12.*,
  [compatibility_level],
  CASE 
    WHEN (SELECT COUNT(*)
		        FROM @tracestatus
		        WHERE [Global] = 1 AND TraceFlag IN (2389, 2390)) = 1
				THEN 
      CASE
        WHEN ([compatibility_level] < 120 /*SQL2014*/)
				    THEN 'Warning - Only one TF is enabled, Consider enabling TF 2389 and 2390 to automatically generate quick statistics (histogram amendment) regardless of key column status.'
			     ELSE 'OK'
      END
    WHEN NOT EXISTS (SELECT TraceFlag
		                   FROM @tracestatus
		                   WHERE [Global] = 1 AND TraceFlag IN (2389, 2390))
				THEN 
      CASE
        WHEN ([compatibility_level] < 120 /*SQL2014*/ OR EXISTS(SELECT * FROM #dbScopedConfig WHERE databases.name = #dbScopedConfig.database_name AND Legacy_Cardinality_Estimation_Status = 1))
				    THEN 'Warning - Database is using legacy CE (70). Consider enabling TF 2389 and 2390 to automatically generate quick statistics (histogram amendment) regardless of key column status'
        ELSE 'OK'
			   END
    ELSE 'OK'
  END AS [comment]
INTO dbo.tmpStatisticCheck12
FROM #tmpCheck12
INNER JOIN sys.databases
ON #tmpCheck12.database_name = QUOTENAME(databases.name)
WHERE databases.state_desc = 'ONLINE'
AND databases.is_read_only = 0 
AND databases.name not in ('tempdb', 'master', 'model', 'msdb')

SELECT * FROM dbo.tmpStatisticCheck12
ORDER BY database_name,
         current_number_of_rows DESC, 
         table_name,
         key_column_name,
         stats_name