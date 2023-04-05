/*
Check32 - TF7471 parallel update statistic
Description:
Check 32 - Check if there are tables with more than 10mi rows and need to do a parallel update stats with TF7471
If there are big tables, is very likely that the maintenance window is taking a lot of time to run.
To reduce the update stats maintenance duration we can use a parallel statistic maintenance plan that runs multiple UPDATE STATISTICS for different statistics on a single table concurrently.
Estimated Benefit:
High
Estimated Effort:
High
Recommendation:
Quick recommendation:
Consider to enable trace flag 7471 to allow parallel statistics update.
Detailed recommendation:
- If you have big tables, you can leverage of service broker or multiple parallel jobs and Ola's maintenance script to do it.
- Implement a parallel statistics update using a script that can do a parallel command execution and make sure you're enabling TF7471 to be able to run multiple update statistics in a table at same time.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck32') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck32

SELECT 'Check 32 - Check if there are tables with more than 10mi rows and need to do a parallel update stats with TF7471' AS [info],
       database_name, 
       MAX(current_number_of_rows) AS max_number_of_rows_in_a_table,
       COUNT(CASE WHEN current_number_of_rows >= 10000000 /*10mi*/ THEN 1 ELSE NULL END) AS number_of_tables_with_more_than_10mi_rows,
       CASE 
         WHEN MAX(current_number_of_rows) >= 10000000 /*10mi*/
         THEN 'Warning - Database ' + database_name + 
              ' has ' + 
              CONVERT(VarChar, COUNT(CASE WHEN current_number_of_rows >= 10000000 /*10mi*/ THEN 1 ELSE NULL END)) + 
              ' tables with more than 10mi rows. Consider to create a maintenance plan to run update stats in parallel using Service Broker and TF7471.'
         ELSE 'OK'
       END AS [comment]
INTO tempdb.dbo.tmpStatisticCheck32
FROM tempdb.dbo.tmp_stats a
GROUP BY database_name

SELECT * FROM tempdb.dbo.tmpStatisticCheck32