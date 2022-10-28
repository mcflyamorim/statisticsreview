/* 
Check 50 - Statistics with a bad leading column
< ---------------- Description ----------------- >
Identify multi-column statistics with a bad leading key column

< -------------- What to look for and recommendations -------------- >
- If number of unique values on the leading key column is low, this may lead 
to a poor histogram, consider to reorder the columns and use specify a more 
selective column as the leading key.
*/

/*
TODO - Add logic to show more info based on density vector data already saved on table tempdb.dbo.tmp_stats_Stat_Density_Vector
*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck50') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck50

SELECT 'Check 50 - Statistics with a bad leading column' AS [info],
       a.database_name,
       a.table_name,
       a.stats_name,
       a.key_column_name,
       a.stat_all_columns,
       b.all_density AS key_column_density,
       t.unique_values_on_key_column,
       a.last_updated AS last_updated_datetime,
       a.current_number_of_rows,
       CASE 
         WHEN t.unique_values_on_key_column <= 200
         THEN 'Warning - It looks like the number of unique values on the leading key column of this multi-column statistic very low. This will lead to a poor histogram, consider to reorder the columns and use specify a more selective column as the leading key.'
         ELSE 'OK'
       END AS [comment],
       dbcc_command
INTO tempdb.dbo.tmpStatisticCheck50
FROM tempdb.dbo.tmp_stats AS a
INNER JOIN tempdb.dbo.tmp_density_vector AS b
ON b.rowid = a.rowid
AND b.density_number = 1
CROSS APPLY (SELECT CONVERT(BigInt, 1.0 / CASE b.all_density WHEN 0 THEN 1 ELSE b.all_density END)) AS t(unique_values_on_key_column)
WHERE a.key_column_name <> a.stat_all_columns
AND a.current_number_of_rows >= 100 /* ignoring small tables */

SELECT * FROM tempdb.dbo.tmpStatisticCheck50
ORDER BY unique_values_on_key_column ASC, 
         current_number_of_rows,
         database_name,
         table_name,
         key_column_name,
         stats_name


/*
Tests to show demo of issue:

-- Create a test table
USE tempdb
GO
IF OBJECT_ID('OrdersBig') IS NOT NULL
  DROP TABLE OrdersBig
GO
SELECT TOP 1000000
       IDENTITY(Int, 1,1) AS OrderID,
       ABS(CheckSUM(NEWID()) / 10000000) AS CustomerID,
       CONVERT(Date, GETDATE() - (CheckSUM(NEWID()) / 1000000)) AS OrderDate,
       ISNULL(ABS(CONVERT(NUMERIC(25,2), (CheckSUM(NEWID()) / 1000000.5))),0) AS Value,
       CONVERT(VarChar(250), NEWID()) AS Col1,
       0 AS ColBit
  INTO OrdersBig
  FROM master.dbo.spt_values A
 CROSS JOIN master.dbo.spt_values B CROSS JOIN master.dbo.spt_values C CROSS JOIN master.dbo.spt_values D
GO
UPDATE TOP(10) OrdersBig SET ColBit = 1
GO
ALTER TABLE OrdersBig ADD CONSTRAINT xpk_OrdersBig PRIMARY KEY(OrderID)
GO
CREATE INDEX ixOrderDate ON OrdersBig(OrderDate)
GO
CREATE INDEX ixColBit ON OrdersBig (ColBit, Value, OrderDate)
GO

DBCC SHOW_STATISTICS ('tempdb.dbo.OrdersBig', ixColBit)
GO

-- Bad estimation
SELECT * 
FROM OrdersBig
WHERE ColBit = 0
AND Value <= 10
AND 1 = (SELECT 1)
ORDER BY Col1
OPTION (MAXDOP 1, RECOMPILE)
GO

-- Option 1
-- Recreate the index on Value, ColBit and OrderDate
DROP INDEX ix2 ON OrdersBig
CREATE INDEX ix2 ON OrdersBig(Value, ColBit, OrderDate)
GO
GO
-- Option 2
-- Update the auto-created stats with fullscan
sp_helpstats OrdersBig
GO
DROP STATISTICS OrdersBig._WA_Sys_00000004_60C822F7 
GO
UPDATE STATISTICS OrdersBig _WA_Sys_00000004_60C822F7 WITH FULLSCAN
GO

-- Option 3
-- Recreate the same auto-created stats, but use a filter to get better 
-- histograms
DROP STATISTICS OrdersBig._WA_Sys_00000004_60C822F7 
GO
DROP STATISTICS OrdersBig.Stats1
CREATE STATISTICS Stats1 ON OrdersBig(Value, ColBit, OrderDate)
WHERE ColBit = 0
GO
DROP STATISTICS OrdersBig.Stats2
CREATE STATISTICS Stats2 ON OrdersBig(Value, ColBit, OrderDate)
WHERE ColBit = 1
GO
*/