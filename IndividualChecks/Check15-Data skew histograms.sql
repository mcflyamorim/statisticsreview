/*
Check15 - Data skew histograms
Description:
Check 15 - Analyze and find data skew issues with limited histogram causing poor estimations
Skew is a term from statistics when a normal distribution is not symmetric. In other words, it simply means that some values appear more often than others. In case of uniformly distributed data, we usually don't have to do anything, but, if data is skewed, we may have to help query optimizer to avoid bad cardinality estimations.
In this script, I'm reading the maximum value of avg_range_rows in a histogram, and based on this, I'm trying to create a SELECT query to read one value between the range. This will make SQL use the avg_range_rows as the estimation and if value is off the average, the estimation will be bad.
Estimated Benefit:
High
Estimated Effort:
Very High
Recommendation:
Quick recommendation:
Review reported statistics, comments and recommendations.
Detailed recommendation:
- Run the query (column sample_query_to_show_bad_cardinality_estimation) using the filter on the value and check the estimated vs the actual number of rows.
- The first thing you should do it to update the statistic with fullscan, as this may provide a better histogram, if this do not help, try filtered stats.
- Check with the developers if those values are indeed used in a query.
- Columns with dates are good candidates to review as usually, users can query by any date.
- Make sure you review all columns with comments and suggestions.
- To help you identify queries with bad estimations, you can use the following xEvents:
- - - - inaccurate_cardinality_estimate (I would start by tracking this one)
- - - - large_cardinality_misestimate
- - - - query_optimizer_cardinality_guess
- - - - query_optimizer_estimate_cardinality
- - - - large_cardinality_misestimate
- Once you've identified the queries, you can check if bad estimation is due to data skew.
- If we identify "skewed" data sets, it may be worth thinking about:
- - - - Filtered stats
- - - - Update statistics with fullscan
- - - - Use hints to help query optimizer
- Filtered stats can help with those columns, good candidates for this are:
- - - - Big tables (usually over 1mi rows)
- - - - Columns with lots of unique values (low density)
- - - - Statistics already using almost all steps available (200 + 1 for NULL)
- Kimberly's scripts can help to analyze analyzes data skew and identify where you can create filtered statistics to provide more Information to the Query Optimizer.
https://www.sqlskills.com/blogs/kimberly/sqlskills-procs-analyze-data-skew-create-filtered-statistics/ 

Note 1: Kimberly's scripts will only analyze data if a base index is available, this may be good for almost all cases since we're expecting you to have indexes on filtered columns, but you may want to check it manually for non-indexed columns

Note 2: Kimberly's scripts will NOT check for wrong estimations due to data that doesn't exist on statistic. In other words, if estimated number of rows is 1000 and actual number of rows is 0, it will not identify those cases.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck15') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck15

SELECT 
  'Check 15 - Analyze and find data skew issues with limited histogram causing poor estimations' AS [info],
  a.database_name,
  a.table_name, 
  a.stats_name, 
  key_column_name, 
  key_column_data_type,
  statistic_type,
  b.leading_column_type,
  last_updated AS last_updated_datetime,
  a.plan_cache_reference_count,
  current_number_of_rows,
  rows_sampled AS number_of_rows_sampled_on_last_update_create_statistic,
  statistic_percent_sampled,
  a.steps AS number_of_steps_on_histogram,
  c.all_density AS key_column_density,
  CONVERT(BigInt, 1.0 / CASE c.all_density WHEN 0 THEN 1 ELSE c.all_density END) AS unique_values_on_key_column_based_on_density,
  CONVERT(BigInt, c.all_density * current_number_of_rows) AS estimated_number_of_rows_per_value_based_on_density,
  user_seeks + user_scans + user_lookups AS number_of_reads_on_index_table_since_last_restart,
  user_seeks + user_scans + user_lookups / 
  CASE DATEDIFF(hh, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), getdate())
    WHEN 0 THEN 1
    ELSE DATEDIFF(hh, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), getdate())
  END AS avg_of_reads_per_hour,
  user_updates AS number_of_modifications_on_index_table_since_last_restart,
  user_updates /
  CASE DATEDIFF(hh, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), getdate())
    WHEN 0 THEN 1
    ELSE DATEDIFF(hh, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), getdate())
  END AS avg_of_modifications_per_hour,
  range_scan_count AS number_of_range_scans_since_last_restart_rebuild,
  singleton_lookup_count AS number_of_singleton_lookups_since_last_restart_rebuild,
  page_latch_wait_count AS number_of_page_latch_since_last_restart_rebuild,
  page_io_latch_wait_count AS number_of_page_i_o_latch_since_last_restart_rebuild,
  '' AS starting_columns_with_info_about_avg_range_rows,
  tMax_By_avg_range_rows.range_hi_key,
  tMax_By_avg_range_rows.avg_range_rows AS max_avg_range_rows,
  '(' + QUOTENAME(key_column_name) + ' > [' + CONVERT(VARCHAR(800), tMax_By_avg_range_rows_PreviousStep.range_hi_key, 21) + '] AND ' + QUOTENAME(key_column_name) + ' < [' + CONVERT(VARCHAR(800), tMax_By_avg_range_rows.range_hi_key, 21) + '] )' AS range_that_will_use_the_avg_range_rows_as_estimation,
  t4.distinct_range_values_based_on_hi_keys,
  tMax_By_avg_range_rows.[distinct_range_values],
  t5.comment_1,
  CASE 
    WHEN (key_column_data_type NOT LIKE 'FLOAT%') AND (key_column_data_type NOT LIKE 'REAL%') /*Convert FLOAT/REAL to NUMERIC is causing some issues... So, I'm ignoring those to avoid errors*/
    THEN 'SELECT COUNT(*) FROM ' + a.database_name + '.' + a.schema_name + '.' + a.table_name + 
         ' WHERE ' + QUOTENAME(key_column_name) + 
         CASE
           WHEN (key_column_data_type LIKE '%CHAR%' OR key_column_data_type LIKE '%uniqueidentifier%') AND LEN(CONVERT(VARCHAR(800), tMax_By_avg_range_rows.range_hi_key)) > 0 THEN ' = ' + '''' + LTRIM(RTRIM(SUBSTRING(CONVERT(VARCHAR(800), tMax_By_avg_range_rows.range_hi_key), 1, LEN(CONVERT(VARCHAR(800), tMax_By_avg_range_rows.range_hi_key)) -1) + CHAR(ASCII(SUBSTRING(CONVERT(VARCHAR(800), tMax_By_avg_range_rows.range_hi_key), LEN(CONVERT(VARCHAR(800), tMax_By_avg_range_rows.range_hi_key)), 1)) - 1))) + ''''
           WHEN key_column_data_type LIKE 'Date%' THEN ' = ' +  '''' + CONVERT(VARCHAR(800), DATEADD(d, -1, CONVERT(DATETIME, tMax_By_avg_range_rows.range_hi_key)), 21) + ''''
           WHEN (key_column_data_type LIKE '%numeric%' OR key_column_data_type LIKE '%decimal%'
                 OR key_column_data_type LIKE '%tinyint%' OR key_column_data_type LIKE '%smallint%'
                 OR key_column_data_type LIKE '%int%' OR key_column_data_type LIKE '%bigint%'
                 OR key_column_data_type LIKE '%float%' OR key_column_data_type LIKE '%decimal%') THEN ' = ' + CONVERT(VARCHAR(800), CONVERT(NUMERIC(28, 0), tMax_By_avg_range_rows.range_hi_key) - CONVERT(NUMERIC(28, 0), 1))
           ELSE ' = ''<change this to use a value between the range>'''
         END + '/*estimated number of rows for this filter is probably going to be ' + CONVERT(VARCHAR(200), tMax_By_avg_range_rows.avg_range_rows) + '*/'
         + ' OPTION (MAXDOP 1, RECOMPILE, QUERYTRACEON 9130);'
    ELSE NULL
  END AS sample_query_to_show_bad_cardinality_estimation,
  tMax_By_avg_range_rows.stepnumber AS max_avg_range_rows_stepnumber,
  tMax_By_avg_range_rows.range_rows AS max_avg_range_rows_range_rows,
  tMax_By_avg_range_rows.eq_rows AS max_avg_range_rows_eq_rows,
  '' AS finished_columns_with_info_about_avg_range_rows,
  '' AS starting_columns_with_info_about_range_rows,
  tMax_By_range_rows.range_rows,
  CONVERT(NUMERIC(25, 2), t0.avg_range_rows) AS avg_range_rows,
  tMax_By_range_rows.range_rows - t0.avg_range_rows AS range_rows_diff_from_avg,
  t0.sum_range_rows,
  t0.sum_eq_rows_plus_range_rows AS range_rows_sum_eq_rows_plus_range_rows,
  CONVERT(NUMERIC(25, 2), (tMax_By_range_rows.range_rows / CASE WHEN t0.sum_eq_rows_plus_range_rows = 0 THEN 1 ELSE t0.sum_eq_rows_plus_range_rows END) * 100) AS range_rows_percent_from_total,
  '(' + QUOTENAME(key_column_name) + ' > [' + CONVERT(VARCHAR(800), tMax_By_range_rows_PreviousStep.range_hi_key, 21) + '] AND ' + QUOTENAME(key_column_name) + ' < [' + CONVERT(VARCHAR(800), tMax_By_range_rows.range_hi_key, 21) + '] )' AS max_range_rows_range,
  CASE 
    WHEN CONVERT(NUMERIC(25, 2), (tMax_By_range_rows.range_rows / CASE WHEN t0.sum_eq_rows_plus_range_rows = 0 THEN 1 ELSE t0.sum_eq_rows_plus_range_rows END) * 100) >= 5 
     AND a.steps > 1
    THEN 'Warning - The number of rows between range ' + 
         '(' + QUOTENAME(key_column_name) + ' > [' + CONVERT(VARCHAR(800), tMax_By_range_rows_PreviousStep.range_hi_key, 21) + '] AND ' + QUOTENAME(key_column_name) + ' < [' + CONVERT(VARCHAR(800), tMax_By_range_rows.range_hi_key, 21) + '] )' + 
         ' is ' + CONVERT(VARCHAR(800), tMax_By_range_rows.range_rows) + 
         ' which is ' + CONVERT(VARCHAR(800), CONVERT(NUMERIC(25, 2), (tMax_By_range_rows.range_rows / CASE WHEN t0.sum_eq_rows_plus_range_rows = 0 THEN 1 ELSE t0.sum_eq_rows_plus_range_rows END) * 100)) + 
         '% of all rows in the histogram. It looks like this range is a good candidate for a filtered statistic.'
    ELSE 'OK'
  END AS comment_2,
  '' AS finishing_columns_with_info_about_range_rows,
  '' AS starting_columns_with_info_about_eq_rows,
  QUOTENAME(CONVERT(VARCHAR(800), tMax_By_eq_rows.range_hi_key, 21)) AS max_eq_rows_range_hi_key,
  tMax_By_eq_rows.eq_rows,
  t0.avg_eq_rows,
  t0.sum_eq_rows,
  t0.sum_eq_rows_plus_range_rows AS eq_rows_sum_eq_rows_plus_range_rows,
  CONVERT(NUMERIC(25, 2), (tMax_By_eq_rows.eq_rows / CASE WHEN t0.sum_eq_rows_plus_range_rows = 0 THEN 1 ELSE t0.sum_eq_rows_plus_range_rows END) * 100) AS eq_rows_percent_from_total,
  CASE 
    WHEN CONVERT(NUMERIC(25, 2), (tMax_By_eq_rows.eq_rows / CASE WHEN t0.sum_eq_rows_plus_range_rows = 0 THEN 1 ELSE t0.sum_eq_rows_plus_range_rows END) * 100) >= 5 
     AND a.steps > 1
    THEN 'Warning - The number of rows equal to range_hi_key ' + 
         '[' +  CONVERT(VARCHAR(800), tMax_By_eq_rows.range_hi_key, 21) + ']' + 
         ' is ' + CONVERT(VARCHAR(800), tMax_By_eq_rows.eq_rows) + 
         ' which is ' + CONVERT(VARCHAR(800), CONVERT(NUMERIC(25, 2), (tMax_By_eq_rows.eq_rows / CASE WHEN t0.sum_eq_rows_plus_range_rows = 0 THEN 1 ELSE t0.sum_eq_rows_plus_range_rows END) * 100)) + 
         '% of all rows in the histogram. It looks like this is a good candidate for a filtered statistic ignoring this value.'
    ELSE 'OK'
  END AS comment_3,
  CASE 
    WHEN CONVERT(NUMERIC(25, 2), (tMax_By_eq_rows.eq_rows / CASE WHEN t0.sum_eq_rows_plus_range_rows = 0 THEN 1 ELSE t0.sum_eq_rows_plus_range_rows END) * 100) >= 10 
     AND a.steps > 1
     AND tMax_By_eq_rows.range_hi_key IS NULL
    THEN 'Warning - ' + CONVERT(VARCHAR(800), CONVERT(NUMERIC(25, 2), (tMax_By_eq_rows.eq_rows / CASE WHEN t0.sum_eq_rows_plus_range_rows = 0 THEN 1 ELSE t0.sum_eq_rows_plus_range_rows END) * 100)) +
          '% of rows are NULL, consider to create a statistic and an index with IS NOT NULL filter.'
    ELSE 'OK'
  END AS comment_4,
  '' AS finishing_columns_with_info_about_eq_rows,
  'SELECT DISTINCT '+ QUOTENAME(key_column_name) +', COUNT('+ QUOTENAME(key_column_name) +') OVER(PARTITION BY '+ QUOTENAME(key_column_name) +') AS "ColCount", '+CONVERT(VARCHAR(200), current_number_of_rows)+' AS "TotCount", CONVERT(NUMERIC(25,2), (CONVERT(NUMERIC(25,2), COUNT('+ QUOTENAME(key_column_name) +') OVER(PARTITION BY '+ QUOTENAME(key_column_name) +'))) / CONVERT(NUMERIC(25,2), ('+CONVERT(VARCHAR(200), current_number_of_rows)+')) * 100) "% over total" FROM ' + a.database_name + '.' + a.schema_name + '.' + a.table_name + ' ORDER BY "ColCount" DESC;' + '/*Warning - Depending on the table size, this may take a while to run...*/' AS sample_query_to_check_data_distribution,
  CASE 
    WHEN (a.steps >= 190) 
      OR (1.0 / CASE c.all_density WHEN 0 THEN 1 ELSE c.all_density END) >= 1000
      THEN 'This looks like a good candidate to test data skew using Kimberly''s scripts'
    ELSE 'Looks like this is not a good candidate to test data skew using Kimberly''s scripts, but, you know the data, so, final decision is yours.'
  END AS comment_5,
  dbcc_command,
  CASE 
    WHEN (a.steps >= 190) 
      OR (1.0 / CASE c.all_density WHEN 0 THEN 1 ELSE c.all_density END) >= 1000
      THEN 'USE ' + a.database_name + 
           '; EXEC sp_SQLskills_AnalyzeColumnSkew @schema_name = ' + 
           '''' + REPLACE(REPLACE(a.schema_name, '[', ''), ']', '') + '''' +
           ', @objectname = ' + 
           '''' +REPLACE(REPLACE(a.table_name, '[', ''), ']', '') + '''' +
           ', @columnname = ' +
           '''' +REPLACE(REPLACE(a.key_column_name, '[', ''), ']', '') + '''' +
           ', @difference	= 1000, @factor = NULL, @numofsteps = NULL, @percentofsteps = 1;'
    ELSE ''
  END AS command_to_test_kimberly_script
INTO tempdb.dbo.tmpStatisticCheck15
FROM tempdb.dbo.tmp_stats AS a
INNER JOIN tempdb.dbo.tmp_exec_history AS b
ON b.rowid = a.rowid
AND b.history_number = 1
INNER JOIN tempdb.dbo.tmp_density_vector AS c
ON c.rowid = a.rowid
AND c.density_number = 1
CROSS APPLY (SELECT SUM(tmp_histogram.eq_rows) AS sum_eq_rows,
                    SUM(tmp_histogram.range_rows) AS sum_range_rows,
                    SUM(tmp_histogram.range_rows + tmp_histogram.eq_rows) AS sum_eq_rows_plus_range_rows,
                    AVG(tmp_histogram.eq_rows) AS avg_eq_rows,
                    AVG(tmp_histogram.range_rows) AS avg_range_rows
               FROM tempdb.dbo.tmp_histogram
              WHERE tmp_histogram.rowid = a.rowid) AS t0
CROSS APPLY (SELECT TOP 1 
                    tmp_histogram.stepnumber,
                    tmp_histogram.range_hi_key,
                    tmp_histogram.range_rows,
                    tmp_histogram.eq_rows,
                    tmp_histogram.distinct_range_rows AS [distinct_range_values],
                    tmp_histogram.avg_range_rows
               FROM tempdb.dbo.tmp_histogram
              WHERE tmp_histogram.rowid = a.rowid
              ORDER BY tmp_histogram.avg_range_rows DESC) AS tMax_By_avg_range_rows
CROSS APPLY (SELECT TOP 1 tmp_histogram.stepnumber,
                    tmp_histogram.range_hi_key,
                    tmp_histogram.range_rows,
                    tmp_histogram.eq_rows,
                    tmp_histogram.distinct_range_rows AS [distinct_range_values],
                    tmp_histogram.avg_range_rows
               FROM tempdb.dbo.tmp_histogram AS tmp_histogram
              WHERE tmp_histogram.rowid = a.rowid
                AND tmp_histogram.stepnumber = CASE 
                                                  WHEN tMax_By_avg_range_rows.stepnumber = 1 THEN tmp_histogram.stepnumber
                                                  ELSE tMax_By_avg_range_rows.stepnumber - 1
                                                END) AS tMax_By_avg_range_rows_PreviousStep
CROSS APPLY (SELECT TOP 1 
                    tmp_histogram.stepnumber,
                    tmp_histogram.range_hi_key,
                    tmp_histogram.range_rows,
                    tmp_histogram.eq_rows,
                    tmp_histogram.distinct_range_rows AS [distinct_range_values],
                    tmp_histogram.avg_range_rows
               FROM tempdb.dbo.tmp_histogram AS tmp_histogram
              WHERE tmp_histogram.rowid = a.rowid
              ORDER BY tmp_histogram.range_rows DESC) AS tMax_By_range_rows
CROSS APPLY (SELECT TOP 1 tmp_histogram.stepnumber,
                    tmp_histogram.range_hi_key,
                    tmp_histogram.range_rows,
                    tmp_histogram.eq_rows,
                    tmp_histogram.distinct_range_rows AS [distinct_range_values],
                    tmp_histogram.avg_range_rows
               FROM tempdb.dbo.tmp_histogram AS tmp_histogram
              WHERE tmp_histogram.rowid = a.rowid
                AND tmp_histogram.stepnumber = CASE 
                                                  WHEN tMax_By_range_rows.stepnumber = 1 THEN tmp_histogram.stepnumber
                                                  ELSE tMax_By_range_rows.stepnumber -1
                                                END) AS tMax_By_range_rows_PreviousStep
CROSS APPLY (SELECT TOP 1 
                    tmp_histogram.stepnumber,
                    tmp_histogram.range_hi_key,
                    tmp_histogram.range_rows,
                    tmp_histogram.eq_rows,
                    tmp_histogram.distinct_range_rows AS [distinct_range_values],
                    tmp_histogram.avg_range_rows
               FROM tempdb.dbo.tmp_histogram AS tmp_histogram
              WHERE tmp_histogram.rowid = a.rowid
              ORDER BY tmp_histogram.eq_rows DESC) AS tMax_By_eq_rows
CROSS APPLY (SELECT CASE 
                      WHEN key_column_data_type LIKE '%CHAR%' OR key_column_data_type LIKE 'FLOAT%'
                        THEN CONVERT(VARCHAR(800), NULL)
                      WHEN (key_column_data_type LIKE '%numeric%' OR key_column_data_type LIKE '%decimal%'
                            OR key_column_data_type LIKE '%tinyint%' OR key_column_data_type LIKE '%smallint%'
                            OR key_column_data_type LIKE '%int%' OR key_column_data_type LIKE '%bigint%'
                            OR key_column_data_type LIKE '%float%' OR key_column_data_type LIKE '%decimal%')
                        THEN CONVERT(VARCHAR(800), CONVERT(NUMERIC(28,0), tMax_By_avg_range_rows.range_hi_key) - CONVERT(NUMERIC(28,0), tMax_By_avg_range_rows_PreviousStep.range_hi_key))
                      WHEN (key_column_data_type LIKE '%date%' OR key_column_data_type LIKE '%time%')
                        THEN CONVERT(VARCHAR(800), DATEDIFF(DAY, CONVERT(DATETIME, tMax_By_avg_range_rows_PreviousStep.range_hi_key), CONVERT(DATETIME, tMax_By_avg_range_rows.range_hi_key)))
                      ELSE NULL
                    END - CONVERT(NUMERIC(28,0), 1)) AS t4(distinct_range_values_based_on_hi_keys)
CROSS APPLY (SELECT CASE 
                     WHEN (t4.distinct_range_values_based_on_hi_keys IS NOT NULL)
                       AND (tMax_By_avg_range_rows.[distinct_range_values] > 1)
                     THEN 'Warning - The distinct number of values on histogram for the range ' + 
                          '(' + QUOTENAME(key_column_name) + ' > [' + CONVERT(VARCHAR(800), tMax_By_avg_range_rows_PreviousStep.range_hi_key, 21) + '] AND ' + QUOTENAME(key_column_name) + ' < [' + CONVERT(VARCHAR(800), tMax_By_avg_range_rows.range_hi_key, 21) + '] )'
                          +' is ' + 
                          CONVERT(VARCHAR(200), tMax_By_avg_range_rows.[distinct_range_values]) + 
                          '. Based on this, it looks like this histogram step is off and will cause bad cardinality estimations for queries using a predicate value between the range_hi_key'
                     ELSE 'OK'
                   END) AS t5(comment_1)
WHERE a.current_number_of_rows >= 1000 /*Only considering table with more than 1000 rows*/
AND a.is_unique = 0
AND a.key_column_data_type NOT LIKE '%BINARY%'
AND a.key_column_data_type NOT LIKE '%IMAGE%'
AND a.key_column_data_type NOT LIKE '%TIMESTAMP%'

SELECT * FROM tempdb.dbo.tmpStatisticCheck15
ORDER BY current_number_of_rows DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name

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
GO
SELECT COUNT(*) FROM OrdersBig
WHERE CustomerID <= 1
AND 1 = (SELECT 1)
GO 10

*/

/*
How to use Kimberly's scripts:

-- Step 1 - analyze a specific table/column
USE Northwind
EXEC sp_SQLskills_AnalyzeColumnSkew
  @schema_name = 'dbo', 
  @objectname = 'Order_DetailsBig',
  @columnname = 'Shipped_Date',
  @difference	= 1000,
  				-- Looking for the minimum difference between average
						-- and biggest difference in that step
  @factor = 2.5,
						-- Looking for the minimum factor of the difference
						-- against the average
  @numofsteps = 1,
						-- This is the minimum number of steps that have to 
						-- have this @difference or @factor (or both)
  @percentofsteps = 1
						-- This is the minimum PERCENT of steps that have to 
						-- have this @difference or @factor (or both)

-- Results 
-------------------------------------------------------------------------------------------------------------
Begin processing @schema_name = [dbo], @objectname = [Order_DetailsBig], @columnname = [Shipped_Date].
Table: [dbo].[Order_DetailsBig], column: [Shipped_Date] has 2 rows (of 125) with a greater difference than 1000. 
This means that there are 2 steps that will result in row estimations that are off by more than 1000. 
Just analyzing step differences, this table has 1.60 percent skew (minimum of 1 percent required by parameter). 
This table shows signs of skew based on this criteria. You should consider filtered statistics on this column to help cardinality estimates.
Caution: Changing any part of an object name could break scripts and stored procedures.
Either parameter @keeptable = 'TRUE' was chosen OR at least one of your criteria showed skew. 
As a result, we saved the table used for histogram analysis as [tempdb]..[SQLskills_HistogramAnalysisOf_Northwind_dbo_Order_DetailsBig_Shipped_Date]. 
This table will need to be manually dropped or will remain in tempdb until it is recreated. 
If this procedure is run again, this table will be replaced (if @keeptable = 'TRUE') but it will not be dropped unless you drop it.
-------------------------------------------------------------------------------------------------------------

-- Step 2 (optional) - Check tables with skewed data
EXEC [sp_SQLskills_HistogramTempTables] @management = 'QUERY'
EXEC [sp_SQLskills_HistogramTempTables] @management = 'DROP'

-- Step 3 - Create filtered stats on columns you identified to be worthy
USE Northwind
EXEC [sp_SQLskills_CreateFilteredStats]
  @schema_name = 'dbo', 
  @objectname = 'Order_DetailsBig', 
  @columnname = 'Shipped_Date',
	 @filteredstats	= 10,
				-- this is the number of filtered statistics
				-- to create. For simplicity, you cannot
				-- create more filtered stats than there are
				-- steps within the histogram (mostly because
				-- not all data is uniform). Maybe in V2.
				-- And, 10 isn't necessarily 10. Because the 
				-- number might not divide easily there are 
				-- likely to be n + 1. And, if @everincreasing
				-- is 1 then you'll get n + 2. 
				-- (the default of 10 may create 11 or 12 stats)
  @fullscan = 'FULLSCAN'


-- Step 4 - Check stats
select * from sys.stats
cross apply sys.dm_db_stats_properties(stats.object_id, stats.stats_id)
where stats.object_id = OBJECT_ID('Order_DetailsBig')


-- Step 5 (optional) - Drop column stats
USE Northwind
EXEC [dbo].[sp_SQLskills_DropAllColumnStats] 
  @schema_name = 'dbo', 
  @objectname = 'Order_DetailsBig', 
  @columnname = 'Shipped_Date',
  @DropAll = 'true'

-- Step 6 (optional) - Test all key columns on DB
-- Depending on the table sizes this may take a while to run
USE Northwind
EXEC sp_SQLskills_AnalyzeAllLeadingIndexColumnSkew 
  @schema_name = NULL, 
  @objectname = NULL,
  @difference	= 1000,
  @factor = 2.5,
  @numofsteps = 1,
  @percentofsteps = 1
*/

