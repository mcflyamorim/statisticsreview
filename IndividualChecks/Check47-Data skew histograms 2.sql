/* 
Check47 - Data skew histograms 2
Description:
Check 47 - Check data skew histograms for top 10 (by user_seeks + user_lookups) indexes 
Skew is a term from statistics when a normal distribution is not symmetric.  In other words, it simply means that some values appear more often than others. In case of uniformly distributed data, we usually don't have to do anything, but, if data is skewed, we may have to help query optimizer to avoid bad cardinality estimations. In this script, I'm trying to help you to identify those "skewed" cases and I'm pretty much doing it by validating a statistic histogram accuracy.
So, based in a histogram, I'm checking the following: 

1 - RANGE_HI_KEY - EQ_rows - Based in the key value (upper bound value for a histogram step), I'm querying the statistic table to get the actual number of rows for the key value and I'm comparing this with the value stored on EQ_rows (estimated number of rows whose value equals the upper bound of the histogram step).
That would help us to identify cases where a "select * where col = <RANGE_HI_KEY value>" would have bad cardinality estimation because the estimated value stored in the statistic not very accurate.

2 - RANGE_rows - RANGE_rows has the estimated number of rows whose column value falls within a histogram step, excluding the upper bound. So, a histogram with:
STEPNUMBER |RANGE_HI_KEY |RANGE_rows	|EQ_rows	 |DISTINCT_RANGE_rows	|AVG_RANGE_rows
1          |0	           |0	         |232.5905 |0	                  |1
2          |32	          |7310.733	  |154.4944 |31	                 |235.8301
3          |66	          |7772.338	  |183.356	 |33	                 |235.5254
4          |92	          |5983.832	  |40.74578 |25	                 |239.3533
5          |119	         |6380.71	   |50.93223 |26	                 |245.4119
6          |139	         |4728.471	  |91.67802 |19	                 |248.8669

Based on this histogram, we can say that there are 7772.338 rows between values 33 and 65. So, a query "select * where col > 32 AND col < 66" would have a cardinality estimation of 7772.338. Again, I'm checking the statistic table to get the actual number of rows within the histogram step range. If the RANGE_rows is not accurate, you may have not only bad estimations with queries reading the range, but you may have a bad AVG_RANGE_rows as it is based on the range.

3 - DISTINCT_RANGE_rows - DISTINCT_RANGE_rows has the estimated number of rows with a distinct column value within a histogram step, excluding the upper bound. I think this column name should be DISTINCT_RANGE_VALUES, not rows, but, anyway. I'm checking the statistics table to get the actual number of distinct values in the histogram step range. Since this is used to calculate the AVG_RANGE_rows, it is important to get this as much accurate as possible.

4 - AVG_RANGE_rows - AVG_RANGE_rows has the average number of rows with duplicate column values within a histogram step, excluding the upper bound. AVG_RANGE_rows is calculated by dividing RANGE_rows by DISTINCT_RANGE_rows. I'm checking the statistics table to get the actual number of average rows in the histogram step range. I'm also checking what is the value with the biggest difference within the histogram step range. This would give us a sample values that is off the average.

Estimated Benefit:
High
Estimated Effort:
Very High
Recommendation:
Quick recommendation:
Review reported statistics, comments and recommendations.
Detailed recommendation:
- The biggest the diff between the values, the least accurate the histogram. 
- Review the histogram accuracy and if necessary, update the statistic with a bigger sample or create filtered statistics.
*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com

SET LOCK_TIMEOUT -1;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; SET ANSI_WARNINGS OFF;
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 

IF OBJECT_ID('dbo.tmpStatisticCheck47') IS NOT NULL
  DROP TABLE dbo.tmpStatisticCheck47

DECLARE @TOP INT = 10 /* Adjust this to run for TOP n Indexes... */
DECLARE @database_id INT, @object_id INT, @Index_ID INT
DECLARE @sp_CheckHistogramAccuracyCmd VARCHAR(8000), @SQL VARCHAR(8000)
DECLARE @ErrMsg VarChar(8000)
DECLARE @ROWID INT = 0, @NumberOfrows INT = 0

/* 
  Creating table ##tmpHistResults to indicate to sp_CheckHistogramAccuracy that I want to save 
  the results results into this table.
*/
IF OBJECT_ID('tempdb.dbo.##tmpHistResults') IS NOT NULL
  DROP TABLE ##tmpHistResults

CREATE TABLE ##tmpHistResults
  (
    [full_table_name] [varchar] (800) null,
    [index_name] [varchar] (800) null,
    [key_column_name] [varchar] (800) null,
    [modificationcounter] [bigint] null,
    [statistics_update_datetime] [datetime] null,
    [rows] [bigint] null,
    [rows_sampled] [bigint] null,
    [steps] [smallint] null,
    [statistic_sample_pct] [decimal] (5, 2) null,
    [key_column_density] FLOAT,
    [unique_values_on_key_column_based_on_density] BIGINT,
    [estimated_number_of_rows_per_value_based_on_density] NUMERIC(25,4),
    [list_of_top_10_values_and_number_of_rows] XML,
    [stepnumber] [smallint] not null,
    [range_hi_key] sql_variant null,
    [eq_rows] [numeric] (18, 4) null,
    [actual_eq_rows] [bigint] null,
    [eq_rows_diff] [numeric] (24, 4) null,
    [eq_rows_factor_diff] [numeric] (18, 2) null,
    [eq_rows - sample query to show bad cardinality estimation] [xml] null,
    [previous_range_hi_key] sql_variant null,
    [current_range_hi_key] sql_variant null,
    [range_rows] [numeric] (18, 4) null,
    [actual_range_rows] [bigint] null,
    [range_rows_diff] [numeric] (24, 4) null,
    [range_rows_factor_diff] [numeric] (18, 2) null,
    [range_rows - sample query to show bad cardinality estimation] [xml] null,
    [distinct_range_"values"] [bigint] null,
    [actual_distinct_range_"values"] [bigint] null,
    [distinct_range_"values"_diff] [bigint] null,
    [avg_range_rows] [decimal] (28, 4) null,
    [actual_avg_range_rows] [decimal] (28, 4) null,
    [avg_range_rows_diff] [decimal] (28, 4) null,
    [actual_rows_for_value_with_biggest_diff] [bigint] null,
    [value_on_range_with_biggest_diff] sql_variant null,
    [avg_range_rows_factor_diff_for_value_with_biggest_diff] [numeric] (18, 2) null,
    [avg_range_rows - sample query to show bad cardinality estimation] [xml] null,
    [captureddatetime] [datetime] not null,
    [database_name] [varchar] (800) null,
    [schema_name] [varchar] (800) null,
    [table_name] [varchar] (800) null,
    [index_id] [bigint] null,
    [columndefinition] [varchar] (100) null,
    [isclustered] [bit] null,
    [isunique] [bit] null,
    [user_seeks] [bigint] null,
    [user_scans] [bigint] null,
    [user_lookups] [bigint] null,
    [range_scan_count] [bigint] null,
    [singleton_lookup_count] [bigint] null,
    [page_latch_wait_count] [bigint] null,
    [page_io_latch_wait_count] [bigint] null
  )

IF OBJECT_ID('tempdb.dbo.#tmpIndexes') IS NOT NULL
  DROP TABLE #tmpIndexes

CREATE TABLE #tmpIndexes(database_id INT, 
                         object_id INT, 
                         index_id INT,
                         rows BIGINT,
                         seek_lookup BIGINT)

CREATE CLUSTERED INDEX ix1 ON #tmpIndexes(database_id, object_id, index_id)

IF OBJECT_ID('tempdb.dbo.#tmpdb') IS NOT NULL
  DROP TABLE #tmpdb

SELECT database_id
INTO #tmpdb
FROM sys.databases d1
WHERE d1.state_desc = 'ONLINE' 
AND d1.is_read_only = 0
AND d1.name NOT IN('tempdb', 'master', 'msdb', 'model', 'distribution')
AND d1.database_id IN(SELECT DISTINCT database_id FROM dbo.tmpStatisticCheck_stats)

DECLARE c_cursor CURSOR STATIC FOR
    SELECT database_id 
    FROM #tmpdb
OPEN c_cursor

FETCH NEXT FROM c_cursor
INTO @database_id
WHILE @@FETCH_STATUS = 0
BEGIN
  SET @SQL = 'use [' + DB_NAME(@database_id) + ']; ' +
             'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
             DECLARE @err_msg NVARCHAR(4000)

             /* Creating a copy of sys.partitions because unindexed access to it can be very slow */
             IF OBJECT_ID(''tempdb.dbo.#tmp_sys_partitions'') IS NOT NULL
                 DROP TABLE #tmp_sys_partitions;
             SELECT * INTO #tmp_sys_partitions FROM sys.partitions
             CREATE CLUSTERED INDEX ix1 ON #tmp_sys_partitions (object_id, index_id, partition_number)

             IF OBJECT_ID(''tempdb.dbo.#tmp_dm_db_index_usage_stats'') IS NOT NULL
               DROP TABLE #tmp_dm_db_index_usage_stats
             BEGIN TRY
               /* Creating a copy of sys.dm_db_index_usage_stats because this is too slow to access without an index */
               SELECT DB_ID() AS database_id, tables.object_id, indexes.index_id, user_seeks, user_scans, user_lookups, user_updates, last_user_seek, last_user_scan, last_user_lookup
                 INTO #tmp_dm_db_index_usage_stats 
                 FROM sys.tables 
                 INNER JOIN sys.indexes
                 ON indexes.object_id = tables.object_id
                 LEFT OUTER JOIN sys.dm_db_index_usage_stats AS ius WITH(NOLOCK)
                 ON ius.object_id = tables.object_id
                 AND ius.index_id = indexes.index_id
             END TRY
             BEGIN CATCH
               SET @err_msg = ''['' + CONVERT(VARCHAR(200), GETDATE(), 120) + ''] - '' + ''Error while trying to read data from sys.dm_db_index_usage_stats. You may see limited results because of it.''
               RAISERROR (@err_msg, 0,0) WITH NOWAIT
             END CATCH

             CREATE CLUSTERED INDEX ix1 ON #tmp_dm_db_index_usage_stats (database_id, object_id, index_id)

              SELECT db_id() as database_id, dm_db_index_usage_stats.object_id, dm_db_index_usage_stats.index_id, partitions.rows, user_seeks + user_lookups AS seek_lookup
              FROM #tmp_dm_db_index_usage_stats  AS dm_db_index_usage_stats
              INNER JOIN #tmp_sys_partitions AS partitions
              ON partitions.object_id = dm_db_index_usage_stats.object_id
              AND partitions.index_id = dm_db_index_usage_stats.index_id
              WHERE database_id = DB_ID()
              AND partitions.index_id > 0 /*ignoring heaps*/
              AND partitions.partition_number = 1
              AND EXISTS(SELECT 1 FROM sys.tables WHERE dm_db_index_usage_stats.object_id = tables.object_id AND tables.name NOT LIKE ''tmpStatisticCheck%'')
              ORDER BY user_seeks + user_lookups DESC'

  INSERT INTO #tmpIndexes(database_id, object_id, index_id, rows, seek_lookup)
  EXEC (@SQL)
  FETCH NEXT FROM c_cursor
  INTO @database_id
END
CLOSE c_cursor
DEALLOCATE c_cursor

IF OBJECT_ID('tempdb.dbo.#tmp1') IS NOT NULL
  DROP TABLE #tmp1

SELECT TOP (@TOP) /* Adjust this to run for TOP n Indexes... */
       database_id, object_id, index_id, 
       DB_NAME(database_id) AS database_name, 
       OBJECT_SCHEMA_NAME(object_id, database_id) AS schema_name,
       OBJECT_NAME(object_id, database_id) AS table_name,
       QUOTENAME(DB_NAME(database_id)) + N'.' + QUOTENAME(OBJECT_SCHEMA_NAME(object_id, database_id)) + N'.' +  QUOTENAME(OBJECT_NAME(object_id, database_id)) AS Fulltable_name,
       rows, 
       seek_lookup,
       'EXEC sp_CheckHistogramAccuracy @database_name = '''+ DB_NAME(database_id) + '''' + 
       ', @schema_name = '''+ OBJECT_SCHEMA_NAME(object_id, database_id) + '''' + 
       ', @table_name = '''+ OBJECT_NAME(object_id, database_id) + '''' + 
       ', @index_id = '+ CONVERT(VARCHAR, index_id) +
       ', @insert_data_into_temp_table = ''Y'';' AS sp_CheckHistogramAccuracyCmd
INTO #tmp1
FROM #tmpIndexes
ORDER BY seek_lookup DESC, rows DESC

SELECT @NumberOfrows = COUNT(*) FROM #tmp1
SET @ROWID = 0

DECLARE @Fulltable_name VARCHAR(800), @RowNumber BIGINT, @IndexID INT, @SeekLookup BIGINT

DECLARE c_cursor2 CURSOR STATIC FOR
  SELECT sp_CheckHistogramAccuracyCmd, Fulltable_name, rows, seek_lookup, index_id FROM #tmp1
  ORDER BY seek_lookup ASC

OPEN c_cursor2

FETCH NEXT FROM c_cursor2
INTO @sp_CheckHistogramAccuracyCmd, @Fulltable_name, @RowNumber, @SeekLookup, @IndexID
WHILE @@FETCH_STATUS = 0
BEGIN
  SET @ROWID = @ROWID + 1
  SET @ErrMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Progress ' + '(' + CONVERT(VARCHAR(200), CONVERT(NUMERIC(25, 2), (CONVERT(NUMERIC(25, 2), @ROWID) / CONVERT(NUMERIC(25, 2), @NumberOfrows)) * 100)) + '%%) - ' 
             + Convert(VarChar, @ROWID) + ' of ' + Convert(VarChar, @NumberOfrows) + ' - Obj = ' + @Fulltable_name + '(' + CONVERT(VARCHAR, @RowNumber) + ' rows)' + 
             ', index_id = ' + CONVERT(VARCHAR, @IndexID)
  IF (@ROWID % 1 = 0)
  BEGIN
    RAISERROR (@ErrMsg, 0, 0) WITH NOWAIT
  END
  BEGIN TRY 
    --PRINT @sp_CheckHistogramAccuracyCmd
    EXEC (@sp_CheckHistogramAccuracyCmd)
  END TRY 
  BEGIN CATCH 
    SET @ErrMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to run command ' + @sp_CheckHistogramAccuracyCmd
    RAISERROR (@ErrMsg, 10, 1) WITH NOWAIT

    SET @ErrMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE()
    RAISERROR (@ErrMsg, 10, 1) WITH NOWAIT
  END CATCH;

  FETCH NEXT FROM c_cursor2
  INTO @sp_CheckHistogramAccuracyCmd, @Fulltable_name, @RowNumber, @SeekLookup, @IndexID
END
CLOSE c_cursor2
DEALLOCATE c_cursor2

SELECT 'Check data skew histograms for top 10 (by user_seeks + user_lookups) indexes' AS [info],
       *,
       'DBCC SHOW_STATISTICS (' + '''' + database_name + '.' + schema_name + '.' + table_name + '''' + ',' + index_name + ')' AS [dbcc_command]
INTO dbo.tmpStatisticCheck47
FROM ##tmpHistResults

SELECT * FROM dbo.tmpStatisticCheck47
ORDER BY database_name, schema_name, table_name, index_name, stepnumber


/*
-- Script to test check
USE Northwind
GO
IF OBJECT_ID('OrdersBig') IS NOT NULL
  DROP TABLE OrdersBig
GO
SELECT TOP 1500000
       IDENTITY(Int, 1,1) AS OrderID,
       ABS(CheckSUM(NEWID()) / 10000000) AS CustomerID,
       CONVERT(Date, GETDATE() - (CheckSUM(NEWID()) / 1000000)) AS OrderDate,
       ISNULL(ABS(CONVERT(Numeric(18,2), (CheckSUM(NEWID()) / 1000000.5))),0) AS Value
  INTO OrdersBig
  FROM Orders A
 CROSS JOIN Orders B CROSS JOIN Orders C CROSS JOIN Orders D
GO
INSERT INTO OrdersBig WITH(TABLOCK)
SELECT TOP 500000
       ABS(CheckSUM(NEWID()) / 10000000) AS CustomerID,
       '20220101' AS OrderDate,
       ISNULL(ABS(CONVERT(Numeric(18,2), (CheckSUM(NEWID()) / 1000000.5))),0) AS Value
  FROM Orders A
 CROSS JOIN Orders B CROSS JOIN Orders C CROSS JOIN Orders D
GO
INSERT INTO OrdersBig WITH(TABLOCK)
SELECT TOP 4000000
       99999 AS CustomerID,
       '20220101' AS OrderDate,
       ISNULL(ABS(CONVERT(Numeric(18,2), (CheckSUM(NEWID()) / 1000000.5))),0) AS Value
  FROM Orders A
 CROSS JOIN Orders B CROSS JOIN Orders C CROSS JOIN Orders D
GO
ALTER TABLE OrdersBig ADD CONSTRAINT xpk_OrdersBig PRIMARY KEY(OrderID)
GO
CREATE INDEX ixCustomerID ON OrdersBig(CustomerID)
CREATE INDEX ixOrderDate ON OrdersBig(OrderDate)
GO
UPDATE STATISTICS OrdersBig WITH SAMPLE
GO

SELECT COUNT(*) FROM OrdersBig
WHERE OrderDate = '20250101'
AND 1 = (SELECT 1)
SELECT COUNT(*) FROM OrdersBig
WHERE CustomerID <= 1
AND 1 = (SELECT 1)
GO 10

*/