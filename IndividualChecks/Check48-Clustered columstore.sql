/* 
Check48 - Clustered columnstore
Description:
Check 48 - Check if there are clustered ColumnStore indexes
Check if there are clustered ColumnStore indexes. There is an issue on how SQL tracks statistics modifications on tables with a clustered columnstore index.  If you have a clustered columnstore, SQL reports more modifications than actually occurred, SQL will include modifications for columns that were not modified. This may trigger unnecessary auto-update statistics and problems with update maintenance script since the modification counter is useful for knowing approximately how much has changed since statistics were last updated.
Estimated Benefit:
High
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Create a job to manually run an update on the reported statistics.
Detailed recommendation:
- Not much we can do about it, since it looks like it is a bug. 
- This script will probably return a high number of modifications for statistics on tables with a clustered columnstore. This confirms there is a bug on SQL.
- If you can't afford to pay for the auto-updates, you may need to update the statistics using NoRecompute and create a job to manually update them.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck48') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck48

SELECT DISTINCT 
       'Check 48 - Check if there are clustered ColumStore indexes' AS [info],
       a.database_name,
       a.table_name,
       a.stats_name,
       a.table_index_base_type,
       a.key_column_name,
       a.last_updated AS last_updated_datetime,
       a.current_number_of_rows,
       a.current_number_of_modified_rows_since_last_update
INTO tempdb.dbo.tmpStatisticCheck48
FROM tempdb.dbo.tmp_stats a
WHERE a.table_index_base_type = 'CLUSTERED COLUMNSTORE'

SELECT * FROM tempdb.dbo.tmpStatisticCheck48
ORDER BY current_number_of_rows DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name

/*
  Script to show issue

USE Northwind
GO
IF OBJECT_ID('Customers_3') IS NOT NULL
  DROP TABLE Customers_3
GO
SELECT TOP 4995
       ISNULL(CONVERT(INT, ROW_NUMBER() OVER(ORDER BY (SELECT 1))),0) AS CustomerID,
       CONVERT(VARCHAR(250), t4.ColumnToShowIssueWithCCC) AS ColumnToShowIssueWithCCC, 
       CONVERT(VARCHAR(250), t3.ContactName) AS ContactName,
       'Info - ' + ISNULL(CONVERT(VARCHAR(MAX),REPLICATE(CONVERT(VARBINARY(MAX), CONVERT(VARCHAR(250), NEWID())), 1000)), '') AS Info
  INTO Customers_3
  FROM master.dbo.spt_values A
 CROSS JOIN master.dbo.spt_values B
 CROSS JOIN master.dbo.spt_values C
 CROSS JOIN master.dbo.spt_values D
 CROSS APPLY (SELECT CRYPT_GEN_RANDOM (10)) AS t1(ContactName)
 CROSS APPLY (SELECT CRYPT_GEN_RANDOM (15)) AS t2(CompanyName)
 CROSS APPLY (SELECT REPLACE(REPLACE(REPLACE(CONVERT(XML, '').value('xs:base64Binary(sql:column("t1.ContactName"))', 'VARCHAR(MAX)'), '=', ''), '/', ''), '+', '')) AS t3(ContactName)
 CROSS APPLY (SELECT REPLACE(REPLACE(REPLACE(CONVERT(XML, '').value('xs:base64Binary(sql:column("t2.CompanyName"))', 'VARCHAR(MAX)'), '=', ''), '/', ''), '+', '')) AS t4(ColumnToShowIssueWithCCC)
OPTION (MAXDOP 4)
GO

DECLARE @Max INT
SELECT @Max = MAX(CustomerID) FROM Customers_3
INSERT INTO Customers_3 WITH (TABLOCK)
(
    CustomerID,
    ColumnToShowIssueWithCCC,
    ContactName,
    Info
)
SELECT TOP 5
       @Max + ISNULL(CONVERT(INT, ROW_NUMBER() OVER(ORDER BY (SELECT 1))),0) AS CustomerID,
       CONVERT(VARCHAR(250), t4.ColumnToShowIssueWithCCC) AS ColumnToShowIssueWithCCC, 
       CONVERT(VARCHAR(250), 'Fabiano Amorim') AS ContactName,
       'Info - Teste -> *' AS Info
  FROM master.dbo.spt_values A
 CROSS JOIN master.dbo.spt_values B
 CROSS JOIN master.dbo.spt_values C
 CROSS JOIN master.dbo.spt_values D
 CROSS APPLY (SELECT CRYPT_GEN_RANDOM (10)) AS t1(ContactName)
 CROSS APPLY (SELECT CRYPT_GEN_RANDOM (15)) AS t2(CompanyName)
 CROSS APPLY (SELECT REPLACE(REPLACE(REPLACE(CONVERT(XML, '').value('xs:base64Binary(sql:column("t1.ContactName"))', 'VARCHAR(MAX)'), '=', ''), '/', ''), '+', '')) AS t3(ContactName)
 CROSS APPLY (SELECT REPLACE(REPLACE(REPLACE(CONVERT(XML, '').value('xs:base64Binary(sql:column("t2.CompanyName"))', 'VARCHAR(MAX)'), '=', ''), '/', ''), '+', '')) AS t4(ColumnToShowIssueWithCCC)
OPTION (MAXDOP 4)
GO
CREATE CLUSTERED COLUMNSTORE INDEX ixColumnStore ON Customers_3
GO
ALTER TABLE Customers_3 ADD CONSTRAINT xpk_Customers_3 PRIMARY KEY NONCLUSTERED(CustomerID)
GO

-- This will trigger auto_create_stats and create
-- a stats on ContactName and on Info columns
-- auto create on column Info will take forever, because 
-- create stats on LOB data is very slow...
SELECT CustomerID, ContactName, Info
  FROM Customers_3
 WHERE ContactName = 'Fabiano Amorim'
   AND Info LIKE 'Info - Test%'
GO
-- two auto created stats... no modifications
SELECT sp.stats_id, name, filter_definition, last_updated, rows, rows_sampled, steps, unfiltered_rows, modification_counter   
FROM sys.stats AS stat   
CROSS APPLY sys.dm_db_stats_properties(stat.object_id, stat.stats_id) AS sp  
WHERE stat.object_id = object_id('Customers_3');
GO

-- What if I change column ColumnToShowIssueWithCCC that has nothing to do with the existing stats?
UPDATE TOP (5) Customers_3 SET ColumnToShowIssueWithCCC = 'Updated - ' + CONVERT(VARCHAR(250), NEWID())
GO

-- doubled modification increased for all stats...
SELECT sp.stats_id, name, filter_definition, last_updated, rows, rows_sampled, steps, unfiltered_rows, modification_counter   
FROM sys.stats AS stat   
CROSS APPLY sys.dm_db_stats_properties(stat.object_id, stat.stats_id) AS sp  
WHERE stat.object_id = object_id('Customers_3');
GO

-- Auto update treshold for this table is 1500 rows...
-- So, update 750 rows should be enough to hit the target
UPDATE TOP (750) Customers_3 SET ColumnToShowIssueWithCCC = 'Updated - ' + CONVERT(VARCHAR(250), NEWID())
GO

-- 1510 modifications... but none for the columns I've used in the query
SELECT sp.stats_id, name, filter_definition, last_updated, rows, rows_sampled, steps, unfiltered_rows, modification_counter   
FROM sys.stats AS stat   
CROSS APPLY sys.dm_db_stats_properties(stat.object_id, stat.stats_id) AS sp  
WHERE stat.object_id = object_id('Customers_3');
GO

-- Run the query again and see the auto_update 
-- for the stats on columns ContactName and Info
SELECT CustomerID, ContactName, Info
  FROM Customers_3
 WHERE ContactName = 'Fabiano Amorim'
   AND Info LIKE 'Info - Test%'
GO

-- 0 modifications for ContactName and Info 
-- and last_updated confirming this was recently updated...
SELECT sp.stats_id, name, filter_definition, last_updated, rows, rows_sampled, steps, unfiltered_rows, modification_counter   
FROM sys.stats AS stat   
CROSS APPLY sys.dm_db_stats_properties(stat.object_id, stat.stats_id) AS sp  
WHERE stat.object_id = object_id('Customers_3');
GO
*/