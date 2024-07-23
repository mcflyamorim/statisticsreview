/*
Check20 - Incremental partitioned statistics
Description:
Check 20 - Check if there are partitioned tables with indexes or statistics not using incremental
When new partitions are added to a large table, statistics should be updated to include the new partitions. However, the time required to scan the entire table (FULLSCAN or SAMPLE option) might be quite long. Also, scanning the entire table isn't necessary because only the statistics on the new partitions might be needed. 
If you assume that only data in the most recent partition is changing, then ideally you only update statistics for that partition. You can do this now with incremental statistics, and what happens is that information is then merged back into the main histogram. The histogram for the entire table will update without having to read through the entire table to update statistics, and this can help with performance of your maintenance tasks.
The other valuable point is that the percentage of data changes required to trigger the automatic update of statistics, 20% of rows changed, will be applied at the partition level.
The query optimizer still just uses the main histogram that represents the entire table. 
Note: This is not a statistic/histogram per partition, QO doesn't use this to get information about each partition. It is used to provide a performance benefit when managing statistics for partitioned tables. If statistics only need to be updated for select partitions, just those can be updated. The new information is then merged into the table-level histogram, providing the optimizer more current information, without the cost of reading the entire table.
Estimated Benefit:
High
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Enable incremental statistics on reported statistics.
Detailed recommendation:
- If there are partitioned tables not using incremental, consider to enable it.
- Applies to: SQL Server 2014 (12.x) and higher builds.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('dbo.tmpStatisticCheck20') IS NOT NULL
  DROP TABLE dbo.tmpStatisticCheck20

DECLARE @sqlmajorver INT, @sqlminorver int, @sqlbuild int
SELECT @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff)
IF @sqlmajorver >= 13 /*SQL2014*/
BEGIN
  SELECT 'Check 20 - Check if there are partitioned tables with indexes or statistics not using incremental' AS [info],
         a.database_name,
         a.table_name,
         a.stats_name,
         a.key_column_name,
         a.current_number_of_rows,
         a.statistic_type,
         a.plan_cache_reference_count,
         a.is_table_partitioned,
         a.is_incremental,
         CASE
           WHEN a.is_incremental = 0
           THEN 'Warning - Table is partitioned but statistic is not set to incremental. Rebuild the index using "WITH(STATISTICS_INCREMENTAL=ON)" or update the stats using "WITH RESAMPLE, INCREMENTAL = OFF"'
           ELSE 'OK'
         END AS [comment],
         CASE
           WHEN a.is_incremental = 0 AND a.statistic_type = 'Index_Statistic'
           THEN 'ALTER INDEX ' + a.stats_name + ' ON ' + a.database_name + '.' + a.schema_name + '.' + a.table_name + 
                ' REBUILD WITH(STATISTICS_INCREMENTAL=ON, ' + 
                CASE 
                  WHEN CONVERT(VarChar(200), SERVERPROPERTY('Edition')) LIKE 'Developer%'
                    OR CONVERT(VarChar(200), SERVERPROPERTY('Edition')) LIKE 'Enterprise%' THEN ' ONLINE=ON)'
                  ELSE ' ONLINE=OFF)'
                END
           WHEN a.is_incremental = 0 AND a.statistic_type <> 'Index_Statistic'
           THEN 'UPDATE STATISTICS ' + a.database_name + '.' + a.schema_name + '.' + a.table_name + ' ' + a.stats_name + 
                ' WITH RESAMPLE, INCREMENTAL = ON;'
           ELSE 'OK'
         END AS command_to_implement_incremental,
         dbcc_command
  INTO dbo.tmpStatisticCheck20
  FROM dbo.tmpStatisticCheck_stats AS a
  WHERE a.is_table_partitioned = 1
END
ELSE
BEGIN
  SELECT 'Check 20 - Check if there are partitioned tables with indexes or statistics not using incremental' AS [info], 
         'Check is not relevant on this SQL version as Incremental stats only applies to SQL Server 2014 (12.x) and higher builds.' AS auto_create_stats_incremental_comment,
         0 AS current_number_of_rows,
         '' AS Comment
  INTO dbo.tmpStatisticCheck20
END

SELECT * FROM dbo.tmpStatisticCheck20
ORDER BY current_number_of_rows DESC

/*

Script to test check

--USE Northwind;
--GO
---- 2 minutes to run...
--IF OBJECT_ID('TabPartition') IS NOT NULL
--  DROP TABLE TabPartition
--GO
--IF OBJECT_ID('TabPartitionElimination') IS NOT NULL
--  DROP TABLE TabPartitionElimination
--GO
--IF EXISTS(SELECT * FROM sys.partition_schemes WHERE name = 'PartitionScheme1')
--  DROP PARTITION SCHEME PartitionScheme1
--GO
--IF EXISTS(SELECT * FROM sys.partition_functions WHERE name = 'PartitionFunction1')
--  DROP PARTITION FUNCTION PartitionFunction1
--GO
--CREATE PARTITION FUNCTION PartitionFunction1 (INT)
--AS RANGE FOR VALUES
--(   2015,
--    2016,
--    2017,
--    2018,
--    2019,
--    2020,
--    2021,
--    2022,
--    2023,
--    2024,
--    2025
--);
--CREATE PARTITION SCHEME PartitionScheme1 AS PARTITION PartitionFunction1 ALL TO ([PRIMARY]);
--GO
--DROP TABLE IF EXISTS TabPartition
--GO
--CREATE TABLE TabPartition
--(
--    ID          INT NOT NULL,
--    Col1        VARCHAR(MAX),
--    Col2        VARCHAR(MAX),
--    Col3        VARCHAR(MAX),
--    ColDate     DATE DEFAULT GETDATE() NOT NULL,
--    ColDateYear AS YEAR(ColDate) PERSISTED NOT NULL,
--)
--GO
--IF OBJECT_ID('fn_GetDate') IS NOT NULL
--  DROP FUNCTION fn_GetDate
--GO
--CREATE FUNCTION dbo.fn_GetDate(@i INT, @dt DATE)
--RETURNS DATE
--AS
--BEGIN
--  DECLARE @GetDate DATE
--  SET @GetDate = DATEADD(DAY, @i, @dt)
--  RETURN(@GetDate)
--END
--GO
--DECLARE @MaxID INT
--SELECT @MaxID = MAX(ID) FROM TabPartition
--INSERT INTO TabPartition WITH(TABLOCK) (ID, ColDate, Col1, Col2, Col3)
--SELECT TOP 10000 ISNULL(@MaxID, 0) + ROW_NUMBER() OVER(ORDER BY(SELECT 1)) AS ID, '20150101', 
--                 CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000)),
--                 CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000)),
--                 CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000))
--FROM master.dbo.spt_values A
--CROSS JOIN master.dbo.spt_values B
--CROSS JOIN master.dbo.spt_values C
--CROSS JOIN master.dbo.spt_values D
--OPTION (MAXDOP 8)
--GO
--DECLARE @MaxID INT
--SELECT @MaxID = MAX(ID) FROM TabPartition
--INSERT INTO TabPartition WITH(TABLOCK) (ID, ColDate, Col1, Col2, Col3)
--SELECT TOP 10000 ISNULL(@MaxID, 0) + ROW_NUMBER() OVER(ORDER BY(SELECT 1)) AS ID, '20160101',
--                 CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000)),
--                 CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000)),
--                 CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000))
--FROM master.dbo.spt_values A
--CROSS JOIN master.dbo.spt_values B
--CROSS JOIN master.dbo.spt_values C
--CROSS JOIN master.dbo.spt_values D
--OPTION (MAXDOP 8)
--GO
--DECLARE @MaxID INT
--SELECT @MaxID = MAX(ID) FROM TabPartition
--INSERT INTO TabPartition WITH(TABLOCK) (ID, ColDate, Col1, Col2, Col3)
--SELECT TOP 10 ISNULL(@MaxID, 0) + ROW_NUMBER() OVER(ORDER BY(SELECT 1)) AS ID, '20170101',
--                CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000)),
--                CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000)),
--                CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000))
--FROM master.dbo.spt_values A
--CROSS JOIN master.dbo.spt_values B
--CROSS JOIN master.dbo.spt_values C
--CROSS JOIN master.dbo.spt_values D
--OPTION (MAXDOP 8)
--GO
--DECLARE @MaxID INT
--SELECT @MaxID = MAX(ID) FROM TabPartition
--INSERT INTO TabPartition WITH(TABLOCK) (ID, ColDate, Col1, Col2, Col3)
--SELECT TOP 10 ISNULL(@MaxID, 0) + ROW_NUMBER() OVER(ORDER BY(SELECT 1)) AS ID, '20180101',
--                CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000)),
--                CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000)),
--                CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000))
--FROM master.dbo.spt_values A
--CROSS JOIN master.dbo.spt_values B
--CROSS JOIN master.dbo.spt_values C
--CROSS JOIN master.dbo.spt_values D
--OPTION (MAXDOP 8)
--GO
--DECLARE @MaxID INT
--SELECT @MaxID = MAX(ID) FROM TabPartition
--INSERT INTO TabPartition WITH(TABLOCK) (ID, ColDate, Col1, Col2, Col3)
--SELECT TOP 10 ISNULL(@MaxID, 0) + ROW_NUMBER() OVER(ORDER BY(SELECT 1)) AS ID, '20190101',
--                CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000)),
--                CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000)),
--                CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000))
--FROM master.dbo.spt_values A
--CROSS JOIN master.dbo.spt_values B
--CROSS JOIN master.dbo.spt_values C
--CROSS JOIN master.dbo.spt_values D
--OPTION (MAXDOP 8)
--GO
--DECLARE @MaxID INT
--SELECT @MaxID = MAX(ID) FROM TabPartition
--INSERT INTO TabPartition WITH(TABLOCK) (ID, ColDate, Col1, Col2, Col3)
--SELECT TOP 10 ISNULL(@MaxID, 0) + ROW_NUMBER() OVER(ORDER BY(SELECT 1)) AS ID, '20200101',
--                CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000)),
--                CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000)),
--                CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000))
--FROM master.dbo.spt_values A
--CROSS JOIN master.dbo.spt_values B
--CROSS JOIN master.dbo.spt_values C
--CROSS JOIN master.dbo.spt_values D
--OPTION (MAXDOP 8)
--GO
--DECLARE @MaxID INT
--SELECT @MaxID = MAX(ID) FROM TabPartition
--INSERT INTO TabPartition WITH(TABLOCK) (ID, ColDate, Col1, Col2, Col3)
--SELECT TOP 10 ISNULL(@MaxID, 0) + ROW_NUMBER() OVER(ORDER BY(SELECT 1)) AS ID, '20210101',
--                CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000)),
--                CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000)),
--                CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000))
--FROM master.dbo.spt_values A
--CROSS JOIN master.dbo.spt_values B
--CROSS JOIN master.dbo.spt_values C
--CROSS JOIN master.dbo.spt_values D
--OPTION (MAXDOP 8)
--GO
--DECLARE @MaxID INT
--SELECT @MaxID = MAX(ID) FROM TabPartition
--INSERT INTO TabPartition WITH(TABLOCK) (ID, ColDate, Col1, Col2, Col3)
--SELECT TOP 10000 ISNULL(@MaxID, 0) + ROW_NUMBER() OVER(ORDER BY(SELECT 1)) AS ID,
--                 '20220101',
--                 CONVERT(VarBinary(MAX),CONVERT(VarChar(250), NEWID())),
--                 CONVERT(VarBinary(MAX),CONVERT(VarChar(250), NEWID())),
--                 CONVERT(VarBinary(MAX),CONVERT(VarChar(250), NEWID()))
--FROM master.dbo.spt_values A
--CROSS JOIN master.dbo.spt_values B
--CROSS JOIN master.dbo.spt_values C
--CROSS JOIN master.dbo.spt_values D
--GO

---- 35 seconds to create the PK
--ALTER TABLE TabPartition ADD CONSTRAINT PK_TabPartition 
--PRIMARY KEY CLUSTERED (ID, ColDateYear)
--ON PartitionScheme1 (ColDateYear);
--GO

-- 26 seconds to auto create stats on Col1, Col2, Col3 and ColDate
SELECT DISTINCT TOP 10 Col1,Col2,Col3,ColDate
FROM TabPartition
WHERE 1 = (SELECT 1)
GO

sp_helpstats TabPartition
GO

-- 4 auto created stats
SELECT a.stats_id, a.name, a.auto_created, a.is_incremental, sp.last_updated, sp.rows, sp.rows_sampled, sp.modification_counter
FROM sys.stats AS a
CROSS APPLY sys.dm_db_stats_properties(a.object_id, a.stats_id) AS sp
WHERE a.object_id = OBJECT_ID('TabPartition')
GO

-- Updating 7000 rows to hit auto update threshold and trigger auto update on next
-- query exec
-- note: updating rows on last partition...
UPDATE TOP(7000) TabPartition SET Col1 = Col1, Col2 = Col2, Col3 = Col3
WHERE ColDate >= '20220101'
GO

-- 7000 modification_counter
SELECT a.stats_id, a.name, a.auto_created, a.is_incremental, sp.last_updated, sp.rows, sp.rows_sampled, sp.modification_counter
FROM sys.stats AS a
CROSS APPLY sys.dm_db_stats_properties(a.object_id, a.stats_id) AS sp
WHERE a.object_id = OBJECT_ID('TabPartition')
ORDER BY a.stats_id
GO

-- this should trigger auto update stats for stats on Col1 and Col2
SELECT DISTINCT TOP 10 Col1,Col2
FROM TabPartition
WHERE 1 = (SELECT 1)
GO

-- modification_counter is back to zero
SELECT a.stats_id, a.name, a.auto_created, a.is_incremental, sp.last_updated, sp.rows, sp.rows_sampled, sp.modification_counter
FROM sys.stats AS a
CROSS APPLY sys.dm_db_stats_properties(a.object_id, a.stats_id) AS sp
WHERE a.object_id = OBJECT_ID('TabPartition')
GO

-- Enabling incremental on Col1 and Col2 stats 
UPDATE STATISTICS [Northwind].[dbo].[TabPartition] _WA_Sys_00000002_09A971A2 WITH RESAMPLE, INCREMENTAL = ON;
UPDATE STATISTICS [Northwind].[dbo].[TabPartition] _WA_Sys_00000003_09A971A2 WITH RESAMPLE, INCREMENTAL = ON;
GO

-- is_incremental = 1
SELECT a.stats_id, a.name, a.auto_created, a.is_incremental, sp.last_updated, sp.rows, sp.rows_sampled, sp.modification_counter
FROM sys.stats AS a
CROSS APPLY sys.dm_db_stats_properties(a.object_id, a.stats_id) AS sp
WHERE a.object_id = OBJECT_ID('TabPartition')
GO

-- Now I can query data using dm_db_incremental_stats_properties to see per partition info 
SELECT a.stats_id, a.name, a.auto_created, a.is_incremental, isp.last_updated, isp.partition_number, isp.rows, isp.rows_sampled, isp.modification_counter
FROM sys.stats AS a
CROSS APPLY sys.dm_db_incremental_stats_properties(a.object_id, a.stats_id) AS isp
WHERE a.object_id = OBJECT_ID('TabPartition')
AND a.name = '_WA_Sys_00000002_09A971A2'
ORDER BY isp.partition_number
GO

-- Updating 7000 rows to hit auto update threshold and trigger auto update on next
-- query exec
-- note: updating rows on last partition...
UPDATE TOP(7000) TabPartition SET Col1 = Col1, Col2 = Col2, Col3 = Col3
WHERE ColDate >= '20220101'
GO


-- is_incremental = 1
SELECT a.stats_id, a.name, a.auto_created, a.is_incremental, sp.last_updated, sp.rows, sp.rows_sampled, sp.modification_counter
FROM sys.stats AS a
CROSS APPLY sys.dm_db_stats_properties(a.object_id, a.stats_id) AS sp
WHERE a.object_id = OBJECT_ID('TabPartition')
GO

-- Now I can query data using dm_db_incremental_stats_properties to see per partition info 
SELECT a.stats_id, a.name, a.auto_created, a.is_incremental, isp.last_updated, isp.partition_number, isp.rows, isp.rows_sampled, isp.modification_counter
FROM sys.stats AS a
CROSS APPLY sys.dm_db_incremental_stats_properties(a.object_id, a.stats_id) AS isp
WHERE a.object_id = OBJECT_ID('TabPartition')
AND a.name = '_WA_Sys_00000002_09A971A2'
ORDER BY isp.partition_number
GO

-- this should trigger auto update stats for stats on Col1 and Col2
-- update stats was A LOT faster, because it only scanned the modified partition

SELECT DISTINCT TOP 10 Col1,Col2
FROM TabPartition
WHERE 1 = (SELECT 1)
GO

-- is_incremental = 1
SELECT a.stats_id, a.name, a.auto_created, a.is_incremental, sp.last_updated, sp.rows, sp.rows_sampled, sp.modification_counter
FROM sys.stats AS a
CROSS APPLY sys.dm_db_stats_properties(a.object_id, a.stats_id) AS sp
WHERE a.object_id = OBJECT_ID('TabPartition')
GO

-- Now I can query data using dm_db_incremental_stats_properties to see per partition info 
SELECT a.stats_id, a.name, a.auto_created, a.is_incremental, isp.last_updated, isp.partition_number, isp.rows, isp.rows_sampled, isp.modification_counter
FROM sys.stats AS a
CROSS APPLY sys.dm_db_incremental_stats_properties(a.object_id, a.stats_id) AS isp
WHERE a.object_id = OBJECT_ID('TabPartition')
AND a.name = '_WA_Sys_00000002_09A971A2'
ORDER BY isp.partition_number
GO

-- Updating 7000 rows to hit auto update threshold and trigger auto update on next
-- query exec
-- note: partition 8...
UPDATE TOP(7000) TabPartition SET Col1 = Col1, Col2 = Col2, Col3 = Col3
WHERE ColDate >= '20220101'
GO
-- Updating 7000 rows to hit auto update threshold and trigger auto update on next
-- query exec
-- note: partition 1...
UPDATE TOP(7000) TabPartition SET Col1 = Col1, Col2 = Col2, Col3 = Col3
WHERE ColDate >= '20150101' AND ColDate <= '20151231'
GO


-- is_incremental = 1
SELECT a.stats_id, a.name, a.auto_created, a.is_incremental, sp.last_updated, sp.rows, sp.rows_sampled, sp.modification_counter
FROM sys.stats AS a
CROSS APPLY sys.dm_db_stats_properties(a.object_id, a.stats_id) AS sp
WHERE a.object_id = OBJECT_ID('TabPartition')
GO

-- Now I can query data using dm_db_incremental_stats_properties to see per partition info 
SELECT a.stats_id, a.name, a.auto_created, a.is_incremental, isp.last_updated, isp.partition_number, isp.rows, isp.rows_sampled, isp.modification_counter
FROM sys.stats AS a
CROSS APPLY sys.dm_db_incremental_stats_properties(a.object_id, a.stats_id) AS isp
WHERE a.object_id = OBJECT_ID('TabPartition')
AND a.name = '_WA_Sys_00000002_09A971A2'
ORDER BY isp.partition_number
GO

-- this should trigger auto update stats for stats on Col1 and Col2
-- update stats was A LOT faster, because it only scanned the modified partitions
SELECT DISTINCT TOP 10 Col1,Col2
FROM TabPartition
WHERE 1 = (SELECT 1)
GO

*/