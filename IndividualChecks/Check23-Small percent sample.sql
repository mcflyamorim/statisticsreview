/*
Check23 – Statistics with small percent sample
Description:
Check 23 - Check if statistic percent sample is too small
When Microsoft SQL Server creates or updates statistics, if a sampling rate isn't manually specified, SQL Server will calculate a default sampling rate. Depending on the real distribution of data in the underlying table, the default sampling rate may not accurately represent the data distribution. 
This may cause degradation of query plan efficiency. To improve this scenario, a database administrator can choose to manually update statistics by using a fixed (fullscan? anyone?) sampling rate that can better represent the distribution of data.
The sampling algorithm for SQL Server is not entirely "random". First, it samples pages and then uses all rows on the page. Second, it will actually sample the same pages each time, mostly to retain sanity within the test team at Microsoft. So, it is possible that the default sample rate will sample pages that do not contain all of the interesting rows that define the "spikes" in your data distribution. A higher sample rate can capture more of these rows, although it will be at a higher cost.
Estimated Benefit:
High
Estimated Effort:
High
Recommendation:
Quick recommendation:
Update statistics with higher sample rate.
Detailed recommendation:
- If you have data with very "spikey" distributions that is not caught by the default sample rate, then you should consider a higher sample rate.
- If you are getting a bad query plan because the statistics are not accurate or current, consider to update the statistic with fullscan.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck23') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck23

SELECT 'Check 23 - Check if statistic percent sample is too small' AS [info],
       a.database_name,
       a.table_name,
       a.stats_name,
       a.key_column_name,
       a.last_updated AS last_updated_datetime,
       a.current_number_of_rows,
       a.rows_sampled AS number_of_rows_sampled_on_last_update_create_statistic,
       a.statistic_percent_sampled,
       CASE 
         WHEN ISNULL(a.statistic_percent_sampled, 0) < 25 THEN 'Statistics with sampling rates less than 25 pct, consider updating with a larger sample or fullscan if key is not uniformly distributed'
         ELSE 'OK'
       END AS percent_sample_comment,
       dbcc_command
INTO tempdb.dbo.tmpStatisticCheck23
FROM tempdb.dbo.tmp_stats a
WHERE a.current_number_of_rows > 0 /* Ignoring empty tables */

SELECT * FROM tempdb.dbo.tmpStatisticCheck23
ORDER BY current_number_of_rows DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name