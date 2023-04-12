/*
Check10 - Ascending statistics
Description:
Check 10 - Check if there are statistics set as ascending/descending
Statistics on ascending or descending key columns, such as IDENTITY or real-time timestamp columns, might require more frequent statistics updates than the Query Optimizer performs. 
Insert operations append new values to ascending or descending columns. The number of rows added might be too small to trigger a statistics update. If statistics are not up-to-date and queries select from the most recently added rows, the current statistics will not have cardinality estimates for these new values. This can result in inaccurate cardinality estimates and slow query performance.
For example, a query that selects from the most recent sales order dates will have inaccurate cardinality estimates if the statistics are not updated to include cardinality estimates for the most recent sales order dates.
Estimated Benefit:
Medium
Estimated Effort:
High
Recommendation:
Quick recommendation:
Update statistics on ascending columns more often.
Detailed recommendation:
- Check if queries using tables are trying to read latest inserted rows, I mean, check if queries are using predicates beyond the RANGE_HI_KEY value of the existing statistics, if so, make sure you've a script to update the statistic more often to guarantee those queries will have Information about newest records.
- Review column query_plan_associated_with_last_usage that may return a plan associated with the statistic.
- If you can't spend time looking at all queries using those tables, go a-head and create a job to update those statistics more often. Make sure your script is smart enough to only run update if number of modified rows changed. Probably an update with sample is enough, as long as you do a bigger sample in the regular maintenance window update.
Note: On KB3189645 (SQL2014 SP1 CU9(12.00.4474) and SP2 CU2(12.00.5532)) filtered indexes are exempted from quickstats queries because it had a bug with filtered indexes and columnstore, but that ended up fixing another problem that when the quickstats query was issued for filtered index stats it has no filter, which was making a full scan (unless a nonfiltered index with the same first column happens to be around to help).


*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

DECLARE @sqlmajorver INT, @sqlminorver INT, @sqlbuild INT
SELECT @sqlmajorver = CONVERT(INT, (@@microsoftversion / 0x1000000) & 0xff),
	      @sqlminorver = CONVERT(INT, (@@microsoftversion / 0x10000) & 0xff),
 	     @sqlbuild = CONVERT(INT, @@microsoftversion & 0xffff);

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck10') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck10

SELECT 'Check 10 - Check if there are statistics set as ascending/descending' AS [info],
       a.database_name,
       a.table_name,
       a.stats_name,
       a.key_column_name,
       a.statistic_type,
       a.plan_cache_reference_count,
       a.last_updated AS last_updated_datetime,
       a.filter_definition,
       CASE 
         WHEN a.filter_definition IS NOT NULL
          AND @sqlmajorver <= 12 /*SQL2014*/
         THEN 'Be careful, a SQL Server lower SQL2014 SP1 CU9(12.00.4474) and SP2 CU2(12.00.5532) has a bug with filtered indexes that may cause quickstats queries to do a fullscan.'
         ELSE 'OK'
       END AS filter_comment,
       a.current_number_of_rows,
       a.current_number_of_modified_rows_since_last_update,
       CONVERT(NUMERIC(25, 2), b.rows_above) AS number_of_rows_inserted_above,
       CONVERT(NUMERIC(25, 2), b.rows_below) AS number_of_rows_inserted_below,
       b.leading_column_type,
       CASE 
           WHEN b.leading_column_type IN ('Ascending',  'Descending') THEN 'Warning - Statistic brand is ascending/descending, make sure you are using new CE or TFs 2389/2390 to get good cardinality estimates for searches on latest data.'
           ELSE 'OK'
         END AS leading_column_type_comment,
       TabIndexUsage.last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used,
       dbcc_command
INTO tempdb.dbo.tmpStatisticCheck10
FROM tempdb.dbo.tmp_stats a
INNER JOIN tempdb.dbo.tmp_exec_history b
ON b.rowid = a.rowid
AND b.history_number = 1
AND b.leading_column_type IN ('Ascending', 'Descending') 
OUTER APPLY (SELECT MAX(Dt) FROM (VALUES(a.last_user_seek), 
                                        (a.last_user_scan),
                                        (a.last_user_lookup)
                               ) AS t(Dt)) AS TabIndexUsage(last_datetime_index_or_a_table_if_obj_is_not_a_index_statistic_was_used)
WHERE a.current_number_of_rows > 0 /* Ignoring empty tables */

SELECT * FROM tempdb.dbo.tmpStatisticCheck10
ORDER BY current_number_of_rows DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name
