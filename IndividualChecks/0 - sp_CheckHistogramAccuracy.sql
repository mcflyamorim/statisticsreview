USE master
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'sp_CheckHistogramAccuracy')
	 EXEC ('CREATE PROC dbo.sp_CheckHistogramAccuracy AS SELECT 1')
GO

ALTER PROCEDURE dbo.sp_CheckHistogramAccuracy @database_name               VARCHAR(800),
                                              @schema_name                 VARCHAR(800) = '[dbo]',
                                              @table_name                  VARCHAR(800),
                                              @index_name                  VARCHAR(800) = NULL,
                                              @index_id                    BIGINT       = NULL,
                                              @insert_data_into_temp_table CHAR(1)      = 'N',
                                              @cleanup_temp_table          CHAR(1)      = 'N',
                                              @debug                       CHAR(1)      = 'N'
/****************************************************************************************
Name: sp_CheckHistogramAccuracy
Description: Skew is a term from statistics when a normal distribution is not symmetric. 
In other words, it simply mean that some values appear more often than others.
In case of uniformly distributed data, we usually don't have to do anything, but, if data is skewed, 
we may have to help query optimizer to avoid bad cardinality estimations.

In this script, I'm trying to help you to identify those "skewed" cases and I'm pretty much doing it by 
validating a statistic histogram accuracy.

So, based in a histogram, I'm checking the following: 
-----------------------------------------------------------------------------------------------------
1 - range_hi_key - eq_rows
Based in the key value (upper bound value for a histogram step), I'm querying the statistic table
to get the actual number of rows for the key value and I'm comparing this with the value
stored on eq_rows (estimated number of rows whose value equals the upper bound of the histogram step).
That would help us to identify cases where a "select * where col = <range_hi_key value>" would have
bad cardinality estimation because the estimated value stored in the statistic not very accurate.

2 - range_rows
range_rows has the estimated number of rows whose column value falls within a histogram step, 
excluding the upper bound. So, a histogram with:

stepnumber |range_hi_key |range_rows	|eq_rows	 |distinct_range_rows	|avg_range_rows
1          |0	           |0	         |232.5905 |0	                  |1
2          |32	          |7310.733	  |154.4944 |31	                 |235.8301
3          |66	          |7772.338	  |183.356	 |33	                 |235.5254
4          |92	          |5983.832	  |40.74578 |25	                 |239.3533
5          |119	         |6380.71	   |50.93223 |26	                 |245.4119
6          |139	         |4728.471	  |91.67802 |19	                 |248.8669

Based on this histogram, we can say that there are 7772.338 rows between values 33 and 65.
So, a query "select * where col > 32 AND col < 66" would have a cardinality estimation of 
7772.338.
Again, I'm checking the statistic table to get the actual number of rows within the histogram 
step range.
If the range_rows is not accurate, you may have not only bad estimations with queries reading
the range, but you may have a bad avg_range_rows as it is based on the range.

3 - distinct_range_rows
distinct_range_rows has the estimated number of rows with a distinct column value within a 
histogram step, excluding the upper bound. I think this column name should be DISTINCT_RANGE_VALUES, 
not rows, but, anyway.
I'm checking the statistics table to get the actual number of distinct values in the histogram 
step range.
Since this is used to calculate the avg_range_rows, it is important to get this as much accurate
as possible.

4 - avg_range_rows
avg_range_rows has the average number of rows with duplicate column values within a histogram step, 
excluding the upper bound. avg_range_rows is calculated by dividing range_rows by distinct_range_rows.
I'm checking the statistics table to get the actual number of average rows in the histogram 
step range.
I'm also checking what is the value with the biggest difference within the histogram step range.
This would give us a sample values that is off the average.
-----------------------------------------------------------------------------------------------------

The biggest the diff between the values, the least accurate the histogram.

This proc heavily based on awesome Kimberly?s scripts analyzes data skew
https://www.sqlskills.com/blogs/kimberly/sqlskills-procs-analyze-data-skew-create-filtered-statistics/

THANK YOU Kimberly!

-- Sample code on how to use the proc:

-- Create a test table
USE tempdb
GO
IF OBJECT_ID('OrdersBig') IS NOT NULL
  DROP TABLE OrdersBig
GO
SELECT TOP 5000000
       IDENTITY(Int, 1,1) AS OrderID,
       ABS(CheckSUM(NEWID()) / 10000000) AS CustomerID,
       CONVERT(Date, GETDATE() - (CheckSUM(NEWID()) / 1000000)) AS OrderDate,
       ISNULL(ABS(CONVERT(NUMERIC(25,2), (CheckSUM(NEWID()) / 1000000.5))),0) AS Value,
       CONVERT(VarChar(250), NEWID()) AS Col1
  INTO OrdersBig
  FROM master.dbo.spt_values A
 CROSS JOIN master.dbo.spt_values B CROSS JOIN master.dbo.spt_values C CROSS JOIN master.dbo.spt_values D
GO
ALTER TABLE OrdersBig ADD CONSTRAINT xpk_OrdersBig PRIMARY KEY(OrderID)
GO
CREATE INDEX ixOrderDate ON OrdersBig(OrderDate)
GO

DBCC SHOW_STATISTICS ('tempdb.dbo.OrdersBig', ixOrderDate)
GO
SELECT index_id, name FROM tempdb.sys.indexes
WHERE object_id = OBJECT_ID('tempdb.dbo.OrdersBig')
GO

UPDATE STATISTICS tempdb.dbo.OrdersBig ixOrderDate WITH SAMPLE
GO
EXEC sp_CheckHistogramAccuracy 
  @database_name = 'tempdb', 
  @schema_name = 'dbo', 
  @table_name = 'OrdersBig', 
  @index_name = 'ixOrderDate', 
  @debug = 'Y'
GO


-- Sanity Note: DBs with compat level 120 (SQL2014) or 130 (SQL2016) will have estimation diff than
-- value on range_rows.
-- On compat level 100, 110, 140 and 150 it will use the range row, as expected.

Author: Fabiano Amorim
http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
****************************************************************************************/
AS
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 

DECLARE @sql                        VARCHAR(8000),
        @key_column_name            VARCHAR(800),
        @full_table_name            VARCHAR(800),
	       @columndefinition           VARCHAR(100),
        @isclustered                BIT,
        @isunique                   BIT,
        @iscolumnstore              BIT,
        @modificationcounter        BIGINT,
        @statistics_update_datetime DATETIME,
        @user_seeks                 BIGINT, 
        @user_scans                 BIGINT, 
        @user_lookups               BIGINT,
        @range_scan_count           BIGINT,
        @singleton_lookup_count     BIGINT,
        @page_latch_wait_count      BIGINT,
        @page_io_latch_wait_count   BIGINT;

DECLARE @tmp TABLE(key_column_name            VARCHAR(800), 
                   columndefinition           VARCHAR(800),
                   isclustered                BIT,
                   isunique                   BIT,
                   iscolumnstore              BIT,
                   modificationcounter        BIGINT,
                   statistics_update_datetime DATETIME,
                   user_seeks                 BIGINT, 
                   user_scans                 BIGINT, 
                   user_lookups               BIGINT,
                   range_scan_count           BIGINT,
                   singleton_lookup_count     BIGINT,
                   page_latch_wait_count      BIGINT,
                   page_io_latch_wait_count   BIGINT);

DECLARE @tmpindex_name TABLE(Index_ID INT, index_name VARCHAR(800));

-- If input variables are not quoted, do it now.
IF SUBSTRING(@database_name, 1, 1) NOT IN ('[', '"')
  SET @database_name = QUOTENAME(@database_name)
IF SUBSTRING(@schema_name, 1, 1) NOT IN ('[', '"')
  SET @schema_name = QUOTENAME(@schema_name)
IF SUBSTRING(@table_name, 1, 1) NOT IN ('[', '"')
  SET @table_name = QUOTENAME(@table_name)

SET @full_table_name = @database_name + N'.' + @schema_name + N'.' + @table_name

IF @index_id IS NULL AND @index_name IS NULL
BEGIN
  RAISERROR ('You have to specify at least one of @index_id or @index_name input parameters', 16, -1)
	 RETURN
END

IF @index_id IS NOT NULL
BEGIN
  SET @sql = '
  USE ' + @database_name + ';
  SELECT index_id, name
  FROM sys.indexes
  WHERE object_id = object_id('''+ @full_table_name +''')
  AND index_id = ' + CONVERT(VARCHAR, @index_id) + ';'

  INSERT INTO @tmpindex_name (Index_ID, index_name)
  EXEC (@sql)

  IF NOT EXISTS(SELECT * FROM @tmpindex_name)
  BEGIN
    DECLARE @Err VARCHAR(200)
    SET @Err = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Unable to find index_id = ' + CONVERT(VARCHAR, @index_id)
    RAISERROR (@Err, 16, -1)
	   RETURN
  END

  SELECT @index_id = Index_ID, @index_name = QUOTENAME(index_name)
  FROM @tmpindex_name
END

IF @index_name IS NOT NULL
BEGIN
  -- If index_name is using a quotename, change if before search on sys.indexes
  IF SUBSTRING(@index_name, 1, 1) IN ('[', '"')
    SET @index_name = SUBSTRING(SUBSTRING(@index_name, 2, LEN(@index_name)), 1, LEN(@index_name) -2)

  SET @sql = '
  USE ' + @database_name + ';
  SELECT index_id, name
  FROM sys.indexes
  WHERE object_id = object_id('''+ @full_table_name +''')
  AND name = ''' + @index_name + ''';'

  INSERT INTO @tmpindex_name (Index_ID, index_name)
  EXEC (@sql)

  IF NOT EXISTS(SELECT * FROM @tmpindex_name)
  BEGIN
    RAISERROR ('Unable to find index_name = %s', 16, -1, @index_name)
	   RETURN
  END

  SELECT @index_id = Index_ID, @index_name = QUOTENAME(index_name)
  FROM @tmpindex_name
END

SET @sql = '
USE ' + @database_name + ';
SET LOCK_TIMEOUT 5;
SELECT QUOTENAME(t1.key_column_name) AS columnname,
       t3.columndefinition,
       INDEXPROPERTY(indexes.object_id, indexes.name, ''isclustered'')   AS isclustered,
       INDEXPROPERTY(indexes.object_id, indexes.name, ''isclustered'')   AS isunique,
       INDEXPROPERTY(indexes.object_id, indexes.name, ''iscolumnstore'') AS iscolumnstore,
       INDEXPROPERTY(indexes.object_id, indexes.name, ''rowmodcnt80'')   AS modificationcounter,
       STATS_DATE(indexes.object_id, indexes.index_id)                   AS statistics_update_datetime,
       ius.user_seeks, 
       ius.user_scans, 
       ius.user_lookups,
       ios.range_scan_count,
       ios.singleton_lookup_count,
       ios.page_latch_wait_count,
       ios.page_io_latch_wait_count
FROM (SELECT indexes.* 
        FROM sys.indexes
       INNER JOIN sys.objects
          ON objects.object_id = indexes.object_id
       WHERE objects.type IN (''U'', ''V'')) AS indexes
LEFT OUTER JOIN sys.dm_db_index_usage_stats AS ius WITH(NOLOCK)
ON ius.database_id = db_id()
AND ius.object_id = indexes.object_id
and ius.index_id = indexes.index_id
OUTER APPLY sys.dm_db_index_operational_stats(DB_ID(), indexes.object_id, indexes.index_id, 1) AS ios
OUTER APPLY (SELECT TOP 1 name AS key_column_name
               FROM sys.index_columns
              INNER JOIN sys.all_columns
                 ON all_columns.object_id = index_columns.object_id
                AND all_columns.column_id = index_columns.column_id
               WHERE indexes.object_id = index_columns.object_id
                 AND indexes.index_id = index_columns.index_id
                 AND index_columns.key_ordinal = 1) as t1
OUTER APPLY (SELECT columndefinition = 
		                  CASE 
			                  WHEN [isc].[DATA_TYPE] IN (''tinyint'', ''smallint'', ''int'', ''bigint'')
				                  THEN [isc].[DATA_TYPE]
			                  WHEN [isc].[DATA_TYPE] IN (''char'', ''varchar'', ''nchar'', ''VARCHAR'')
				                  THEN [isc].[DATA_TYPE] 
					                  + ''('' 
					                  + CONVERT(varchar, [isc].[CHARACTER_MAXIMUM_LENGTH])
					                  + '') COLLATE '' 
					                  + [isc].[COLLATION_NAME]
			                  WHEN [isc].[DATA_TYPE] IN (''datetime2'', ''datetimeoffset'', ''time'')
				                  THEN [isc].[DATA_TYPE]
					                  + ''(''
					                  + CONVERT(varchar, [isc].[DATETIME_PRECISION])
					                  + '')''
			                  WHEN [isc].[DATA_TYPE] IN (''numeric'', ''decimal'')
				                  THEN [isc].[DATA_TYPE]
					                  + ''(''
					                  + CONVERT(varchar, [isc].[NUMERIC_PRECISION])
					                  + '', '' 
					                  + CONVERT(varchar, [isc].[NUMERIC_SCALE])
					                  + '')''
			                  WHEN [isc].[DATA_TYPE] IN (''float'', ''decimal'')
				                  THEN [isc].[DATA_TYPE]
					                  + ''(''
					                  + CONVERT(varchar, [isc].[NUMERIC_PRECISION])
					                  + '')''
			                  WHEN [isc].[DATA_TYPE] = ''uniqueidentifier''
				                  THEN ''char(36)''			
			                  --WHEN [isc].[DATA_TYPE] IN (''bit'', ''money'', ''smallmoney'', ''date'', ''datetime'', ''real'', ''smalldatetime'', ''hierarchyid'', ''sql_variant'')
			                  ELSE [isc].[DATA_TYPE]
		                  END
                  FROM [INFORMATION_SCHEMA].[COLUMNS] AS [isc]
                  WHERE QUOTENAME(TABLE_CATALOG) + N''.'' + QUOTENAME(TABLE_SCHEMA) + N''.'' + QUOTENAME(TABLE_NAME) = ''' + @full_table_name + '''
	                  AND [isc].[COLUMN_NAME] = t1.key_column_name) AS t3
WHERE QUOTENAME(indexes.name) = ''' + @index_name + '''' + 
' AND indexes.object_id = OBJECT_ID(''' + @full_table_name + ''')' + 
' OPTION (MAXDOP 1)'

IF @debug = 'Y'
BEGIN
  PRINT @sql
END

BEGIN TRY
  INSERT INTO @tmp
  EXEC (@sql)
END TRY
BEGIN CATCH
  RAISERROR ('Error while trying to read data from sys.indexes, sys.dm_db_index_usage_stats and sys.dm_db_index_operational_stats', 0, 0) WITH NOWAIT
  DECLARE @ErrMessage VARCHAR(8000)
  SET @ErrMessage = ERROR_MESSAGE()
  RAISERROR ('Error_Message() = %s', 16, -1, @ErrMessage) WITH NOWAIT
  RETURN
END CATCH

SELECT @key_column_name            = key_column_name, 
       @columndefinition           = columndefinition,
       @isclustered                = isclustered,
       @isunique                   = isunique,
       @iscolumnstore              = iscolumnstore,
       @modificationcounter        = modificationcounter,
       @statistics_update_datetime = statistics_update_datetime,
       @user_seeks                 = user_seeks, 
       @user_scans                 = user_scans, 
       @user_lookups               = user_lookups,
       @range_scan_count           = range_scan_count,
       @singleton_lookup_count     = singleton_lookup_count,
       @page_latch_wait_count      = page_latch_wait_count,
       @page_io_latch_wait_count   = page_io_latch_wait_count
FROM @tmp

IF @iscolumnstore = 1
BEGIN
  RAISERROR ('Stat_ID is a ColumnStore index, ignoring it...', 16, -1)
	 RETURN
END

SET @sql = 'IF OBJECT_ID(''tempdb.dbo.##tblHistogram'') IS NOT NULL
              DROP TABLE ##tblHistogram
            SELECT TOP 0
                   IDENTITY(SMALLINT, 1, 1)                 AS stepnumber,
                   COALESCE(' + @key_column_name + ', NULL) AS range_hi_key,
                   CONVERT(NUMERIC(25,4), 0)                AS range_rows,
                   CONVERT(NUMERIC(25,4), 0)                AS eq_rows,
                   CONVERT(BIGINT, 0)                       AS distinct_range_rows,
                   CONVERT(DECIMAL(28,4), 0)                AS avg_range_rows,
                   CONVERT(BIGINT, 0)                       AS actual_eq_rows,
                   CONVERT(BIGINT, 0)                       AS actual_range_rows,
                   CONVERT(BIGINT, 0)                       AS actual_distinct_range_rows,
                   CONVERT(DECIMAL(28,4), 0)                AS actual_avg_range_rows,
                   COALESCE(' + @key_column_name + ', NULL) AS value_on_range_with_biggest_diff,
                   CONVERT(BIGINT, 0)                       AS actual_rows_for_value_with_biggest_diff,
                   COALESCE(' + @key_column_name + ', NULL) AS previous_range_hi_key
            INTO ##tblHistogram FROM ' + @full_table_name
            
IF @debug = 'Y'
BEGIN
  PRINT @sql
END
EXEC (@sql)

IF OBJECT_ID('tempdb.dbo.#Stat_Header') IS NOT NULL
  DROP TABLE #Stat_Header;

CREATE TABLE #Stat_Header
(
    name sysname,
    updated datetime,
    rows bigint,
    rows_sampled bigint,
    steps smallint,
    density real,
    averagekeylength int,
    stringindex varchar(10),
    filterexpression varchar(8000),
    unfiltered_rows bigint,
    persisted_sample_percent float null
);

SET @sql = 'DBCC SHOW_STATISTICS (' + '''' + @full_table_name + '''' + ',' + 
                                  @index_name + ') WITH STAT_HEADER, NO_INFOMSGS;'

IF @debug = 'Y'
BEGIN
  PRINT @sql
END

BEGIN TRY 
  INSERT INTO #Stat_Header (name,updated,rows,rows_sampled,steps,density,averagekeylength,stringindex,filterexpression,unfiltered_rows,persisted_sample_percent)
  EXEC (@sql)
END TRY 
BEGIN CATCH 
  INSERT INTO #Stat_Header (name,updated,rows,rows_sampled,steps,density,averagekeylength,stringindex,filterexpression,unfiltered_rows)
  EXEC (@sql)
END CATCH;

SET @sql = 'DBCC SHOW_STATISTICS (' + '''' + @full_table_name + '''' + ',' + 
                                  @index_name + ') WITH HISTOGRAM, NO_INFOMSGS;'

IF @debug = 'Y'
BEGIN
  PRINT @sql
END
INSERT INTO ##tblHistogram
(
    range_hi_key,
    range_rows,
    eq_rows,
    distinct_range_rows,
    avg_range_rows
)
EXEC (@sql)

/*
  Fixing avg_range_rows as if distinct_range_rows is equal to Zero
  this may report wrong values... internally, if distinct_range_rows
  is zero, then avg_range_rows is same as range_rows, or,
  avg_range_rows / (distinct_range_rows + 1).

  Even though BOL says: 
  "When distinct_range_rows is 0, avg_range_rows returns 1 for the histogram step."
  this is not entirely true as I've seen cases where reported value was not 1.
*/
SET @sql = '
-- Adjusting avg_range_rows
UPDATE ##tblHistogram
SET avg_range_rows = CASE 
                       WHEN distinct_range_rows = 0 THEN range_rows
                       ELSE avg_range_rows
                     END
FROM ##tblHistogram h;
'

IF @debug = 'Y'
BEGIN
  PRINT @sql
END
EXEC (@sql)


SET @sql = '
-- eq_rows
UPDATE ##tblHistogram
SET actual_eq_rows =
    (
        SELECT COUNT(*)
        FROM '+@full_table_name+' WITH (NOLOCK, FORCESEEK)
        WHERE '+@key_column_name+' = h.range_hi_key
    )
FROM ##tblHistogram h
OPTION (MAXDOP 4, RECOMPILE);
'

IF @debug = 'Y'
BEGIN
  PRINT @sql
END
EXEC (@sql)

SET @sql = '
-- eq_rows, NULLs
UPDATE ##tblHistogram
SET actual_eq_rows =
    (
        SELECT COUNT(*)
        FROM '+@full_table_name+' WITH (NOLOCK)
        WHERE '+@key_column_name+' IS NULL
    )
FROM ##tblHistogram h
WHERE range_hi_key IS NULL
OPTION (MAXDOP 4, RECOMPILE);
'

IF @debug = 'Y'
BEGIN
  PRINT @sql
END
EXEC (@sql)

SET @sql = '
-- range_rows
WITH BOUNDS
AS (SELECT [stepnumber], 
           LAG(range_hi_key, 1, range_hi_key) OVER (ORDER BY range_hi_key) AS [previous_range_hi_key],
           range_hi_key
    FROM ##tblHistogram)
UPDATE ##tblHistogram
SET actual_range_rows = cnt,
    actual_distinct_range_rows = CntDistinct
FROM
(
    SELECT [stepnumber], 
           previous_range_hi_key,
           range_hi_key,
           cnt,
           CntDistinct
    FROM BOUNDS
    CROSS APPLY (
                    SELECT COUNT(*) AS cnt, COUNT(DISTINCT '+@key_column_name+') AS CntDistinct
                    FROM '+@full_table_name+' WITH (NOLOCK, FORCESEEK)
                    WHERE '+@key_column_name+' > previous_range_hi_key
                          AND '+@key_column_name+' < range_hi_key
                ) AS ActualRangerows
) AS t
WHERE ##tblHistogram.[stepnumber] = t.[stepnumber]
OPTION (MAXDOP 4, RECOMPILE);
'
IF @debug = 'Y'
BEGIN
  PRINT @sql
END
EXEC (@sql)

SET @sql = '
-- actual_avg_range_rows
UPDATE ##tblHistogram
SET actual_avg_range_rows = actual_range_rows / CASE 
                                                  WHEN ISNULL(actual_distinct_range_rows,0) = 0 THEN 1 
                                                  ELSE actual_distinct_range_rows 
                                                END
FROM ##tblHistogram h;
'
IF @debug = 'Y'
BEGIN
  PRINT @sql
END
EXEC (@sql)

SET @sql = '
-- avg_range_rows
;WITH Bounds
AS 
(
  SELECT LAG(range_hi_key, 1, NULL) OVER (ORDER BY range_hi_key) AS [previous_range_hi_key],
         range_hi_key,
         avg_range_rows,
         stepnumber
      FROM ##tblHistogram
),
Current_Values
AS
(
  SELECT [stepnumber],
         avg_range_rows,
         tMin.minkeycol,
         ISNULL(tMin.mincnt,0) AS mincnt, 
         tMax.maxkeycol,
         ISNULL(tMax.maxcnt,0) AS maxcnt,
         previous_range_hi_key,
         range_hi_key
  FROM Bounds
  OUTER APPLY(SELECT TOP 1
                     '+@key_column_name+' AS keycol,
                     COUNT(*) AS cnt                            
              FROM '+@full_table_name+' WITH (NOLOCK, FORCESEEK)
              WHERE '+@key_column_name+' > previous_range_hi_key
              AND '+@key_column_name+' < range_hi_key
              GROUP BY '+@key_column_name+'
              ORDER BY cnt ASC
            ) AS tMin (minkeycol, mincnt)
  OUTER APPLY(SELECT TOP 1
                     '+@key_column_name+' AS keycol,
                     COUNT(*) AS cnt                            
              FROM '+@full_table_name+' WITH (NOLOCK, FORCESEEK)
              WHERE '+@key_column_name+' > previous_range_hi_key
              AND '+@key_column_name+' < range_hi_key
              GROUP BY '+@key_column_name+'
              ORDER BY cnt DESC
            ) AS tMax (maxkeycol, maxcnt)
),
CTE_1
AS
(
SELECT [stepnumber], avg_range_rows, previous_range_hi_key, range_hi_key, t.*
FROM Current_Values
CROSS APPLY (SELECT TOP 1 *
             FROM (VALUES(ABS(Current_Values.avg_range_rows - mincnt), mincnt, minkeycol),
                         (ABS(Current_Values.avg_range_rows - maxcnt), maxcnt, maxkeycol)) AS t (biggestdiffcnt, actualcnt, biggestdiffkeycol)
             ORDER BY t.biggestdiffcnt DESC) AS t
)
UPDATE ##tblHistogram
SET ##tblHistogram.value_on_range_with_biggest_diff        = CTE_1.biggestdiffkeycol,
    ##tblHistogram.actual_rows_for_value_with_biggest_diff = CTE_1.actualcnt,
    ##tblHistogram.previous_range_hi_key = CTE_1.previous_range_hi_key,
    ##tblHistogram.range_hi_key = CTE_1.range_hi_key
FROM CTE_1
WHERE ##tblHistogram.stepnumber = CTE_1.stepnumber
OPTION (MAXDOP 4, RECOMPILE);
'

IF @debug = 'Y'
BEGIN
  PRINT @sql
END
EXEC (@sql)

IF OBJECT_ID('tempdb.dbo.#tmp1') IS NOT NULL
  DROP TABLE #tmp1

SELECT @full_table_name            AS full_table_name,
       @index_name                 AS index_name,
       @key_column_name            AS key_column_name,
       @modificationcounter        AS modification_counter,
       @statistics_update_datetime AS statistics_update_datetime,
       #Stat_Header.rows,
       #Stat_Header.rows_sampled,
       #Stat_Header.steps,
       CAST((rows_sampled / (rows * 1.00)) * 100.0 AS DECIMAL(5, 2)) AS statistic_sample_pct,
       stepnumber, 
       range_hi_key,
       eq_rows,
       actual_eq_rows,
       ABS(eq_rows - actual_eq_rows) AS eq_rows_Diff,
       CASE 
         WHEN actual_eq_rows > 0 AND ABS(eq_rows - actual_eq_rows) > 0
         THEN CONVERT(NUMERIC(25, 2), eq_rows / actual_eq_rows)
         ELSE 0
       END AS eq_rows_Factor_Diff,
       t2.[eq_rows - sample query to show bad cardinality estimation],
       previous_range_hi_key,
       range_hi_key AS current_range_hi_key,
       range_rows,
       actual_range_rows,
       ABS(range_rows - actual_range_rows) AS range_rows_Diff,
       CASE 
         WHEN actual_range_rows > 0 AND ABS(range_rows - actual_range_rows) > 0
         THEN CONVERT(NUMERIC(25, 2), range_rows / actual_range_rows)
         ELSE 0
       END AS range_rows_factor_diff,
       t4.[range_rows - sample query to show bad cardinality estimation],
       distinct_range_rows AS [distinct_range_"values"],
       actual_distinct_range_rows AS [actual_distinct_range_"values"],
       ABS(distinct_range_rows - actual_distinct_range_rows) AS [distinct_range_"values"_diff],
       avg_range_rows,
       actual_avg_range_rows,
       ABS(avg_range_rows - actual_avg_range_rows) AS [avg_range_rows_diff],
       CASE 
         WHEN actual_rows_for_value_with_biggest_diff = 0 THEN NULL
         ELSE actual_rows_for_value_with_biggest_diff
       END AS actual_rows_for_value_with_biggest_diff,
       value_on_range_with_biggest_diff,
       t6.[avg_range_rows - sample query to show bad cardinality estimation],
       GETDATE()                 AS capture_datetime,
       @database_name            AS database_name,
       @schema_name              AS schema_name,
       @table_name               AS table_name,
       @index_id                 AS index_id,
       @columndefinition         AS column_definition,
       @isclustered              AS is_clustered,
       @isunique                 AS is_unique,
       @user_seeks               AS user_seeks, 
       @user_scans               AS user_scans, 
       @user_lookups             AS user_lookups,
       @range_scan_count         AS range_scan_count,
       @singleton_lookup_count   AS singleton_lookup_count,
       @page_latch_wait_count    AS page_latch_wait_count,
       @page_io_latch_wait_count AS page_io_latch_wait_count
INTO #tmp1
FROM ##tblHistogram
CROSS JOIN #Stat_Header
CROSS APPLY (SELECT CASE 
                      WHEN ABS(eq_rows - actual_eq_rows) > 0
                      THEN 'SELECT COUNT(*) FROM ' + @full_table_name + NCHAR(13) + NCHAR(10) +
                         ' WHERE ' + @key_column_name + ' = ''' + TRY_CONVERT(VARCHAR(800), range_hi_key, 21) + '''' + NCHAR(13) + NCHAR(10) +
                         'OPTION (MAXDOP 1, RECOMPILE);' + NCHAR(13) + NCHAR(10) +
                         '/* Estimated number of rows for this filter is probably going to be ' + TRY_CONVERT(VARCHAR(200), CASE WHEN eq_rows = 0 THEN 1 ELSE eq_rows END) + 
                         ', but, actual number of rows will be ' + TRY_CONVERT(VARCHAR(200), actual_eq_rows) + ' */'
                      ELSE ''
                    END
                    ) AS t1 (query)
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
                                              '--?>'))) AS t2 ([eq_rows - sample query to show bad cardinality estimation])
CROSS APPLY (SELECT CASE 
                      WHEN ABS(range_rows - actual_range_rows) > 0
                      THEN 'SELECT COUNT(*) FROM ' + @full_table_name + NCHAR(13) + NCHAR(10) +
                         ' WHERE ' + @key_column_name + ' > ''' + TRY_CONVERT(VARCHAR(800), previous_range_hi_key, 21) + '''' + 
                         ' AND ' + @key_column_name + ' < ''' + TRY_CONVERT(VARCHAR(800), range_hi_key, 21) + '''' + 
                         NCHAR(13) + NCHAR(10) +
                         'OPTION (MAXDOP 1, RECOMPILE);' + NCHAR(13) + NCHAR(10) +
                         '/* Estimated number of rows for this filter is probably going to be ' + TRY_CONVERT(VARCHAR(200), CASE WHEN range_rows = 0 THEN 1 ELSE range_rows END) + 
                         ', but, actual number of rows will be ' + TRY_CONVERT(VARCHAR(200), actual_range_rows) + ' */'
                      ELSE ''
                    END
                   ) AS t3 (query)
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
								                                                   N'--' + NCHAR(13) + NCHAR(10) + t3.query + NCHAR(13) + NCHAR(10) + N'--' COLLATE Latin1_General_Bin2
							                                                   ),
							                                                   NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
							                                                   NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
							                                                   NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
						                                                   NCHAR(0),
						                                                   N'')
                                                         + '--?>'),
                                              '<?query --' + NCHAR(13) + NCHAR(10) +
                                              'Could not render the query due to XML data type limitations.' + NCHAR(13) + NCHAR(10) +
                                              '--?>'))) AS t4 ([range_rows - sample query to show bad cardinality estimation])
CROSS APPLY (SELECT CASE 
                      WHEN value_on_range_with_biggest_diff IS NOT NULL AND ABS(CONVERT(BIGINT, avg_range_rows - actual_rows_for_value_with_biggest_diff)) > 0
                      THEN 'SELECT COUNT(*) FROM ' + @full_table_name + NCHAR(13) + NCHAR(10) +
                           ' WHERE ' + @key_column_name + ' = ''' + TRY_CONVERT(VARCHAR(800), value_on_range_with_biggest_diff, 21) + '''' + NCHAR(13) + NCHAR(10) +
                           'OPTION (MAXDOP 1, RECOMPILE);' + NCHAR(13) + NCHAR(10) +
                           '/* Estimated number of rows for this filter is probably going to be ' + TRY_CONVERT(VARCHAR(200), CASE WHEN avg_range_rows = 0 THEN 1 ELSE avg_range_rows END) + 
                           ', but, actual number of rows will be ' + TRY_CONVERT(VARCHAR(200), actual_rows_for_value_with_biggest_diff) + ' */'
                      ELSE ''
                    END
                    ) AS t5 (query)
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
								                                                   N'--' + NCHAR(13) + NCHAR(10) + t5.query + NCHAR(13) + NCHAR(10) + N'--' COLLATE Latin1_General_Bin2
							                                                   ),
							                                                   NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
							                                                   NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
							                                                   NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
						                                                   NCHAR(0),
						                                                   N'')
                                                         + '--?>'),
                                              '<?query --' + NCHAR(13) + NCHAR(10) +
                                              'Could not render the query due to XML data type limitations.' + NCHAR(13) + NCHAR(10) +
                                              '--?>'))) AS t6 ([avg_range_rows - sample query to show bad cardinality estimation])
ORDER BY stepnumber

IF @insert_data_into_temp_table = 'Y' AND OBJECT_ID('tempdb.dbo.##tmpHistResults') IS NULL
BEGIN
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
    [avg_range_rows_diff] [decimal] (29, 4) null,
    [actual_rows_for_value_with_biggest_diff] [bigint] null,
    [value_on_range_with_biggest_diff] sql_variant null,
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
    [page_io_latch_wait_count] [bigint] null,
    [cached_queryplansample] xml,
    [cached_querysample] xml
  )
END

IF OBJECT_ID('tempdb.dbo.##tmpHistResults') IS NOT NULL
BEGIN
  IF @cleanup_temp_table = 'Y'
  BEGIN
    EXEC ('TRUNCATE TABLE ##tmpHistResults')
  END
  EXEC ('INSERT INTO ##tmpHistResults (full_table_name,
                                       index_name,
                                       key_column_name,
                                       modificationcounter,
                                       statistics_update_datetime,
                                       rows,
                                       rows_sampled,
                                       steps,
                                       statistic_sample_pct,
                                       stepnumber,
                                       range_hi_key,
                                       eq_rows,
                                       actual_eq_rows,
                                       eq_rows_diff,
                                       eq_rows_factor_diff,
                                       [eq_rows - sample query to show bad cardinality estimation],
                                       previous_range_hi_key,
                                       current_range_hi_key,
                                       range_rows,
                                       actual_range_rows,
                                       range_rows_diff,
                                       range_rows_factor_diff,
                                       [range_rows - sample query to show bad cardinality estimation],
                                       [distinct_range_"values"],
                                       [actual_distinct_range_"values"],
                                       [distinct_range_"values"_diff],
                                       avg_range_rows,
                                       actual_avg_range_rows,
                                       avg_range_rows_diff,
                                       actual_rows_for_value_with_biggest_diff,
                                       value_on_range_with_biggest_diff,
                                       [avg_range_rows - sample query to show bad cardinality estimation],
                                       captureddatetime,
                                       database_name,
                                       schema_name,
                                       table_name,
                                       index_id,
                                       columndefinition,
                                       isclustered,
                                       isunique,
                                       user_seeks,
                                       user_scans,
                                       user_lookups,
                                       range_scan_count,
                                       singleton_lookup_count,
                                       page_latch_wait_count,
                                       page_io_latch_wait_count)
         SELECT *
         FROM #tmp1')
END
ELSE
BEGIN
  EXEC ('SELECT * FROM #tmp1 ORDER BY stepnumber')
END
GO

EXEC [sys].[sp_MS_marksystemobject] 'sp_CheckHistogramAccuracy';