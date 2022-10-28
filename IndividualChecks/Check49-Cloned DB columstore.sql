/* 
Check 49 - Warning about Cloned DB and ColumStore stats
< ---------------- Description ----------------- >

The database clone created using DBCC CLONEDATABASE contains the copy of schema and statistics 
which allows the optimizer to generate same query plan as observed on the production database 
without the actual data.

Unlike traditional B-tree indexes, when a columnstore index is created, there is no index 
statistics created on the columns of the columnstore indexes. However, there is an empty stats 
object created with the same name as columnstore index and an entry is added to sys.stats at 
the time of index creation. 
The stats object is populated on the fly when a query is executed against the columnstore index 
or when executing DBCC SHOW_STATISTICS against the columnstore index, but the columnstore index 
statistics aren't persisted in the storage. 

Since the index statistics is not persisted in storage, the clonedatabase will not contain those 
statistics leading to inaccurate stats and different query plans when same query has run against 
database clone as opposed to production database.

< -------------- What to look for and recommendations -------------- >
- To handle this behavior and to be able to accurately capture the columnstore index statistics 
in the clone database, use the following script in the tiger's team github:
https://github.com/microsoft/tigertoolbox/blob/master/DBCC-CLONEDATABASE/usp_update_CI_stats_before_cloning.sql

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck49') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck49

SELECT 'Check 49 - Warning about Cloned DB and ColumStore stats' AS [info],
       a.database_name,
       a.table_name,
       a.stats_name,
       a.table_index_base_type,
       a.key_column_name,
       a.last_updated AS last_updated_datetime,
       a.current_number_of_rows,
       dbcc_command
INTO tempdb.dbo.tmpStatisticCheck49
FROM tempdb.dbo.tmp_stats a
WHERE a.table_index_base_type LIKE '%COLUMNSTORE%'

SELECT * FROM tempdb.dbo.tmpStatisticCheck49
ORDER BY current_number_of_rows DESC,
         database_name,
         table_name,
         key_column_name,
         stats_name
