/*
Check42 - Statistic sample rate per table
Description:
Check 42 - Estimating default sampling rate to be used for each table
When Microsoft SQL Server creates or updates statistics, if a sampling rate isn't manually specified, SQL Server will calculate a default sampling rate. Depending on the real distribution of data in the underlying table, the default sampling rate may not accurately represent the data distribution. This may cause degradation of query plan efficiency.
The sampling algorithm for SQL Server is not entirely "random". First, it samples pages and then uses all rows on the page. Second, it will actually sample the same pages each time, mostly to retain sanity within the test team at Microsoft. So, it is possible that the default sample rate will sample pages that do not contain all of the interesting rows that define the "spikes" in your data distribution. A higher sample rate can capture more of these rows, although it will be at a higher cost.
Estimated Benefit:
High
Estimated Effort:
High
Recommendation:
Quick recommendation:
Manually update statistics using a bigger sampling rate.
Detailed recommendation:
- Notice that big tables will get a very small sampling rate, and possibly introduce degradation of query plan efficiency.
- To improve this scenario, a database administrator can choose to manually update statistics by using a fixed (fullscan? anyone?) sampling rate that can better represent the distribution of data. 

Note 1: Tables that are smaller than 8MB (1024 pages) are always fully scanned to update/create statistics. SQL only consider in-row data, that is, all data types except LOB data types. In other words, if you have tons of LOB_DATA pages, SQL may still decide to do a scan on table to create/update the stat.

Note 2: Since the sampling algorithm uses number of pages, a compressed index would increase the number of rows sampled, therefore, possible improving the stat.

Note 3: A compressed index, can also avoid page disfavoring as table size will be a lot smaller. This could make update stats a lot faster, as, without disfavoring, it is very likely that a sequential update, I mean, for many stats on same table, would do logical reads instead of physical that would be for disfavored pages. In other words, you may be able to speed up the update stats process by enabling compression and avoiding disfavoring/physical reads.

Note 4: Algorithm may not be 100% because of rounding. but it is very close to actual numbers. But as mentioned, you may see small diff with bigger tables.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0;

IF OBJECT_ID('dbo.tmpStatisticCheck42') IS NOT NULL
  DROP TABLE dbo.tmpStatisticCheck42

SELECT 'Check 42 - Estimating default sampling rate to be used for each table' AS [info],
       a.database_name,
       a.schema_name,
       a.table_name,
       a.stats_name,
       a.plan_cache_reference_count,
       current_number_of_rows,
       current_number_of_modified_rows_since_last_update,
       auto_update_threshold,
       auto_update_threshold_type,
       percent_of_threshold,
       CONVERT(BIGINT, ROUND(current_number_of_rows * t7.PercentSampled, 0)) AS auto_update_create_rows_sample,
       CONVERT(BIGINT, t6.SamplePages) AS auto_update_create_pages_sample,
       CONVERT(NUMERIC(25, 8), t7.PercentSampled * 100) AS auto_update_create_percent_sample,
       CONVERT(NUMERIC(25, 8), t1.RowsPerPage) AS number_of_rows_per_page,
       number_of_in_row_data_pages_on_table,
       CONVERT(NUMERIC(25, 2), (number_of_in_row_data_pages_on_table * 8) / 1024.) AS in_row_data_size_in_mb,
       number_of_lob_data_pages_on_table,
       CONVERT(NUMERIC(25, 2), (number_of_lob_data_pages_on_table * 8) / 1024.) AS lob_data_size_in_mb
INTO dbo.tmpStatisticCheck42
FROM (SELECT DISTINCT 
             database_name,
             schema_name,
             table_name,
             stats_name,
             plan_cache_reference_count,
             number_of_in_row_data_pages_on_table,
             number_of_lob_data_pages_on_table,
             current_number_of_rows,
             current_number_of_modified_rows_since_last_update,
             auto_update_threshold,
             auto_update_threshold_type,
	            CONVERT(DECIMAL(25, 2), (current_number_of_modified_rows_since_last_update / (auto_update_threshold * 1.0)) * 100.0) AS percent_of_threshold
        FROM dbo.tmpStatisticCheck_stats
        WHERE current_number_of_rows > 0 /* Ignoring empty tables */) AS a
CROSS APPLY (SELECT RowsPerPage = CONVERT(NUMERIC(25, 8), current_number_of_rows) / CONVERT(NUMERIC(25, 8), number_of_in_row_data_pages_on_table)) AS t1
CROSS APPLY (SELECT SampleRows  = CONVERT(NUMERIC(25, 8), CEILING(15 * POWER(CONVERT(NUMERIC(25, 8), current_number_of_rows), 0.55)))) AS t2
CROSS APPLY (SELECT SampleRate  = CONVERT(NUMERIC(25, 8), t2.SampleRows / CONVERT(NUMERIC(25, 8), current_number_of_rows))) AS t3
CROSS APPLY (SELECT SamplePages1 = CONVERT(BIGINT, CONVERT(NUMERIC(25, 8), number_of_in_row_data_pages_on_table) * t3.SampleRate) + 1024) AS t4
CROSS APPLY (SELECT SamplePages2 = CASE WHEN CONVERT(NUMERIC(25, 8), number_of_in_row_data_pages_on_table) < t4.SamplePages1 THEN CONVERT(NUMERIC(25, 8), number_of_in_row_data_pages_on_table) ELSE t4.SamplePages1 END) AS t5
CROSS APPLY (SELECT SamplePages = MIN(Tab1.Col1) FROM (VALUES(t4.SamplePages1),(t5.SamplePages2)) AS Tab1(Col1)) AS t6
CROSS APPLY (SELECT PercentSampled = t6.SamplePages / number_of_in_row_data_pages_on_table) AS t7
WHERE current_number_of_rows > 0
AND number_of_in_row_data_pages_on_table > 0

SELECT * FROM dbo.tmpStatisticCheck42
ORDER BY in_row_data_size_in_mb + lob_data_size_in_mb DESC, 
         database_name,
         table_name