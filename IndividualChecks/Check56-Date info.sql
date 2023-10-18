/*
Check 56 - Report min value for DateTime/Date columns

Description:
Reporting min and max values for date columns for tables greater than 1mi rows.
This is useful to identify tables storing old data and are good candidates to purged/archive/remove, or maybe apply partitioning.

Estimated Benefit:
Very High

Estimated Effort:
High

Recommendation:
Quick recommendation:
Consider to implement a purge/archive strategy or implement partitioning on large tables.

Detailed recommendation:
Review reported tables and implement a purge/archive strategy.
Review reported tables and consider to implement SQL Server native partitioning or partitioned views.
*/

SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck56') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck56

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb.dbo.#tmp1') IS NOT NULL
  DROP TABLE #tmp1

CREATE TABLE #tmp1 (database_id INT, object_id INT, stats_id INT, cMin DATETIME, cMax DATETIME, CountMin BIGINT, CountMax BIGINT)
CREATE UNIQUE CLUSTERED INDEX ix1 ON #tmp1(database_id, object_id, stats_id)

DECLARE @Database_ID INT, @Object_ID INT, @Index_ID INT, @Cmd NVARCHAR(MAX)
DECLARE @ErrMsg VarChar(8000)

SELECT 'Check 56 - Report min value for DateTime/Date columns' AS [Info],
       database_name,
       table_name,
       stats_name,
       statistic_type,
       key_column_name,
       last_updated AS last_updated_datetime,
       current_number_of_rows,
       rows_sampled,
       statistic_percent_sampled,
       plan_cache_reference_count,
       t_Min.MinDate AS min_datetime,
       t_Max.MaxDate AS max_datetime,
       DATEDIFF(YEAR, t_Min.MinDate, t_Max.MaxDate) AS years_cnt,
       t_Min.EstimatedNumberOfRowsMinValue AS estimated_number_of_rows_min_value,
       t_Max.EstimatedNumberOfRowsMaxValue AS estimated_number_of_rows_max_value
INTO tempdb.dbo.tmpStatisticCheck56
FROM tempdb.dbo.tmp_stats
CROSS APPLY (SELECT TOP 1 
                    CONVERT(DATETIME, range_hi_key) AS MinDate,
                    eq_rows AS EstimatedNumberOfRowsMinValue
               FROM tempdb.dbo.tmp_histogram
              WHERE tmp_histogram.rowid = tmp_stats.rowid
                AND tmp_histogram.range_hi_key IS NOT NULL
              ORDER BY CONVERT(DATETIME, range_hi_key) ASC) AS t_Min
CROSS APPLY (SELECT TOP 1 
                    CONVERT(DATETIME, range_hi_key) AS MaxDate, 
                    eq_rows AS EstimatedNumberOfRowsMaxValue
               FROM tempdb.dbo.tmp_histogram
              WHERE tmp_histogram.rowid = tmp_stats.rowid
                AND tmp_histogram.range_hi_key IS NOT NULL
              ORDER BY CONVERT(DATETIME, range_hi_key) DESC) AS t_Max
WHERE key_column_data_type LIKE '%DATE%'
AND current_number_of_rows >= 1000000 /* Only tables >= 1mi rows */

SELECT * FROM tempdb.dbo.tmpStatisticCheck56
ORDER BY current_number_of_rows,
         database_name,
         table_name,
         statistic_type DESC,
         key_column_name,
         stats_name