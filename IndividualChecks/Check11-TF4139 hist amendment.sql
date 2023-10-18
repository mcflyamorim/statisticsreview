/*
Check11 - TF4139 histogram amendment
Description:
Check 11 - Trace flag check - TF4139 (Enable automatically generated quick statistics (histogram amendment) regardless of key column status.)
Check TF4139, TF4139 Enable automatically generated quick statistics (histogram amendment) regardless of key column status. 
If trace flag 4139 is set, regardless of the leading statistics column status (ascending, descending, unknown or stationary), the histogram used to estimate cardinality will be adjusted at query compile time.
When fewer than 90 percent of the inserted rows have values that are beyond the highest RANGE_HI_KEY value in the histogram, the column is considered stationary instead of ascending. Therefore, the ascending key is not detected, and trace flags 4139 (new CE), 2389 and 2390 that are usually used to fix the ascending keys problem do not work. This causes poor cardinality estimation when you use predicates that are beyond the RANGE_HI_KEY value of the existing statistics.
Estimated Benefit:
Medium
Estimated Effort:
High
Recommendation:
Quick recommendation:
Consider to enable trace flag 4139.
Detailed recommendation:
- Check whether you have statistics with number of inserted rows that are beyond the highest RANGE_HI_KEY value in the histogram, and those statistics are still considered unknown or stationary. If so, queries trying to read those recent rows would be beneficial of automatically generate quick statistics (histogram amendment) regardless of key column status.
- If you have databases with cardinality estimation model version >= 120 (new CE). Enable TF4139 to automatically generate quick statistics (histogram amendment) regardless of key column status.
Note 1: This trace flag does not apply to CE version 70 (legacy CE). Use trace flags 2389 and 2390 instead.

Note 2: On KB3189645 (SQL2014 SP1 CU9(12.00.4474) and SP2 CU2(12.00.5532)) filtered indexes are exempted from quickstats queries because it had a bug with filtered indexes and columnstore, but that ended up fixing another problem that when the quickstats query was issued for filtered index stats it has no filter, which was making a full scan (unless a nonfiltered index with the same first column happens to be around to help).

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck11') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck11

IF OBJECT_ID('tempdb.dbo.#tmpCheck11') IS NOT NULL
  DROP TABLE #tmpCheck11

SELECT 
  a.database_name, 
  a.table_name, 
  a.stats_name, 
  a.key_column_name, 
  a.current_number_of_rows,
  a.last_updated AS last_updated_datetime,
  b.leading_column_type, 
  CONVERT(DECIMAL(25, 2), b.rows_above) AS number_of_rows_inserted_above,
  CONVERT(DECIMAL(25, 2), b.rows_below) AS number_of_rows_inserted_below,
  Tab1.percent_of_modifications,
  number_of_modifications_on_key_column_since_previous_update,
  b.inserts_since_last_update AS number_of_inserted_rows_on_key_column_since_previous_update,
  b.deletes_since_last_update AS number_of_deleted_rows_on_key_column_since_previous_update
INTO #tmpCheck11
FROM tempdb.dbo.tmp_stats a
INNER JOIN tempdb.dbo.tmp_exec_history b
ON b.rowid = a.rowid
AND b.history_number = 1
AND b.leading_column_type IN ('Unknown', 'Stationary')
OUTER APPLY (SELECT CASE 
                      WHEN b.inserts_since_last_update IS NULL AND b.deletes_since_last_update IS NULL
                      THEN NULL
                      ELSE (ABS(ISNULL(b.inserts_since_last_update,0)) + ABS(ISNULL(b.deletes_since_last_update,0)))
                    END AS number_of_modifications_on_key_column_since_previous_update,
                   b.updated AS last_updated
              FROM tempdb.dbo.tmp_exec_history b
             WHERE b.rowid = a.rowid
               AND b.history_number = 1 /* Previous update stat sample */
              ) AS Tab_StatSample1
CROSS APPLY (SELECT CONVERT(DECIMAL(25, 2), (b.rows_above / (CASE WHEN Tab_StatSample1.number_of_modifications_on_key_column_since_previous_update = 0 THEN 1 ELSE Tab_StatSample1.number_of_modifications_on_key_column_since_previous_update END * 1.0)) * 100.0)) AS Tab1(percent_of_modifications)
WHERE b.rows_above > 0 
AND b.rows_below = 0
AND a.current_number_of_rows > 1000 /* Ignoring small tables */

DECLARE @sqlmajorver INT, @sqlminorver int, @sqlbuild int
SELECT @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff),
	      @sqlminorver = CONVERT(int, (@@microsoftversion / 0x10000) & 0xff),
 	     @sqlbuild = CONVERT(int, @@microsoftversion & 0xffff);

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
  AND d1.name not in ('tempdb', 'master', 'model', 'msdb')

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
     SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
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
  'Check 11 - Trace flag check - TF4139 (Enable automatically generated quick statistics (histogram amendment) regardless of key column status.)' AS [info],
  #tmpCheck11.*,
  [compatibility_level],
  CASE
    WHEN EXISTS (SELECT TraceFlag
		               FROM @tracestatus
		               WHERE [Global] = 1 AND TraceFlag = 9481 /*TF9481 is used to force legacy CE*/)
				THEN 
      'Warning - TF9481 is enabled and will force legacy (70) CE. TF4139 is not valid, use trace flags 2389 and 2390 instead.'
    WHEN EXISTS (SELECT TraceFlag
		               FROM @tracestatus
		               WHERE [Global] = 1 AND TraceFlag = 4139)
				THEN 
      CASE
        WHEN (@sqlmajorver = 11 /*SQL2012*/ AND @sqlbuild >= 5532 /*CU1 SP2*/)
					        OR (@sqlmajorver = 11 /*SQL2012*/ AND @sqlbuild >= 3431 /*CU10 SP1*/ AND @sqlbuild < 5058 /*SP2*/)
					        OR @sqlmajorver >= 12 /*SQL2014*/
				    THEN 'Information - TF4139 is enabled and will automatically generated quick statistics (histogram amendment) regardless of key column status'
			     ELSE 'Warning - TF4139 only works starting with "CU10 for SQL2012 SP1", "CU1 for SQL2012 SP2" and "CU2 for SQL2014", no need to enabled it in older versions'
			   END
    WHEN (NOT EXISTS (SELECT TraceFlag
		                   FROM @tracestatus
		                   WHERE [Global] = 1 AND TraceFlag = 4139)
          AND NOT EXISTS(SELECT * FROM #dbScopedConfig WHERE databases.name = #dbScopedConfig.database_name AND Legacy_Cardinality_Estimation_Status = 1))
				THEN 
      CASE
        WHEN ((@sqlmajorver = 11 /*SQL2012*/ AND @sqlbuild >= 5532 /*CU1 SP2*/)
					        OR (@sqlmajorver = 11 /*SQL2012*/ AND @sqlbuild >= 3431 /*CU10 SP1*/ AND @sqlbuild < 5058 /*SP2*/)
					        OR @sqlmajorver >= 12 /*SQL2014*/)
             AND ([compatibility_level] >= 120 /*SQL2014*/)
				    THEN 'Warning - Database is using new CE (120 - SQL2014). Found ' + CONVERT(VarChar, t1.NumberOfTablesWithStationaryOrUnkown) + ' stats with inserts beyond the highest RANGE_HI_KEY value in the histogram but still set to Stationary of Unknown. Consider enabling TF4139 to automatically generate quick statistics (histogram amendment) regardless of key column status'
        ELSE 'OK'
      END
    WHEN (NOT EXISTS (SELECT TraceFlag
		                   FROM @tracestatus
		                   WHERE [Global] = 1 AND TraceFlag = 4139)
          AND EXISTS(SELECT * FROM #dbScopedConfig WHERE databases.name = #dbScopedConfig.database_name AND Legacy_Cardinality_Estimation_Status = 1))
				THEN 
      CASE
        WHEN ((@sqlmajorver = 11 /*SQL2012*/ AND @sqlbuild >= 5532 /*CU1 SP2*/)
					        OR (@sqlmajorver = 11 /*SQL2012*/ AND @sqlbuild >= 3431 /*CU10 SP1*/ AND @sqlbuild < 5058 /*SP2*/)
					        OR @sqlmajorver >= 12 /*SQL2014*/) 
				    THEN 'Warning - Database is set to use Legacy_Cardinality_Estimation scoped configuration. TF4139 is not valid for those DBs, use trace flags 2389 and 2390 instead.'
        ELSE 'OK'
      END
    ELSE 'OK'
  END AS [comment]
INTO tempdb.dbo.tmpStatisticCheck11
FROM #tmpCheck11
INNER JOIN sys.databases
ON #tmpCheck11.database_name = QUOTENAME(databases.name)
OUTER APPLY (SELECT COUNT(*) AS NumberOfTablesWithStationaryOrUnkown
             FROM #tmpCheck11 AS a
             WHERE a.database_name = QUOTENAME(databases.name)) AS t1
WHERE databases.state_desc = 'ONLINE'
AND databases.is_read_only = 0 
AND databases.name not in ('tempdb', 'master', 'model', 'msdb')

SELECT * FROM tempdb.dbo.tmpStatisticCheck11
ORDER BY database_name, 
         current_number_of_rows DESC,
         table_name,
         key_column_name,
         stats_name