/*
Check54 - Queries with distinct
Description:
Check 54 - Look on cached plans to find queries with several columns on distinct clause
Use query plan cache to search for queries with several columns on distinct clause. SQL will trigger auto create/update stats for each column specified on distinct. This may cause long compilation time and create unnecessary (or not very useful) statistics.
Estimated Benefit:
Medium
Estimated Effort:
High
Recommendation:
Quick recommendation:
Review reported queries and check distinct is really necessary.
Detailed recommendation:
- Review queries and check if distinct is really necessary, sometimes a query re-write with exists/not exists clause can be used to avoid unnecessary distinct operations.
- Check if auto created statistics are really useful.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck54') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck54

IF OBJECT_ID('tempdb.dbo.#query_plan') IS NOT NULL
  DROP TABLE #query_plan

;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT qs.*
INTO #query_plan
FROM tempdb.dbo.tmpStatsCheckCachePlanData qs
WHERE statement_plan.exist('//p:RelOp[@LogicalOp="Distinct Sort" or @LogicalOp="Flow Distinct"]') = 1

;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT  TOP 1000
        qp.*,
        logical_op = operators.value('@LogicalOp','nvarchar(50)'),
        STUFF((SELECT ', ' + ISNULL(c1.n.value('(@Database)[1]', 'sysname') + '.' +
                      c1.n.value('(@Schema)[1]', 'sysname') + '.' +
                      c1.n.value('(@Table)[1]', 'sysname') + '.' +
                      QUOTENAME(c1.n.value('(@Column)[1]', 'sysname')), 
                     c2.n.value('(@Database)[1]', 'sysname') + '.' +
                                           c2.n.value('(@Schema)[1]', 'sysname') + '.' +
                                           c2.n.value('(@Table)[1]', 'sysname') + '.' +
                                           QUOTENAME(c2.n.value('(@Column)[1]', 'sysname')))
               FROM #query_plan qp1
               OUTER APPLY qp1.statement_plan.nodes('declare namespace p = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
                                                    //p:RelOp[@LogicalOp="Distinct Sort"]/p:Sort/p:OrderBy/p:OrderByColumn/p:ColumnReference') AS c1(n)
               OUTER APPLY qp1.statement_plan.nodes('declare namespace p = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
                                                    //p:RelOp[@LogicalOp="Flow Distinct"]/p:Hash/p:HashKeysBuild/p:ColumnReference') AS c2(n)
               WHERE qp1.plan_handle = qp.plan_handle
               FOR XML PATH('')), 1, 2, '') AS referenced_columns
INTO tempdb.dbo.tmpStatisticCheck54
FROM #query_plan qp
CROSS APPLY statement_plan.nodes('//p:RelOp[@LogicalOp="Distinct Sort" or @LogicalOp="Flow Distinct"]') rel(operators)
ORDER BY query_impact DESC
OPTION (RECOMPILE);

SELECT 'Check 54 - Plans with several columns on distinct clause' AS [info],
       *,
       LEN(referenced_columns) - LEN(REPLACE(referenced_columns, ',', '')) + 1 AS cnt_referenced_columns
FROM tempdb.dbo.tmpStatisticCheck54
ORDER BY query_impact DESC

/*
-- Script to show issue

USE Northwind
GO
IF OBJECT_ID('TabTestStats') IS NOT NULL
  DROP TABLE TabTestStats
GO
CREATE TABLE TabTestStats (ID Int IDENTITY(1,1) PRIMARY KEY,
                   Col1 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())),  5)) ,
                   Col2 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())),  5)) ,
                   Col3 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())),  5)) ,
                   Col4 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())),  5)) ,
                   Col5 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())),  5)) ,
                   Col6 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())),  5)) ,
                   Col7 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())),  5)) ,
                   Col8 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())),  5)) ,
                   Col9 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())),  5)) ,
                   Col10 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col11 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col12 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col13 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col14 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col15 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col16 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col17 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col18 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col19 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col20 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col21 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col22 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col23 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col24 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col25 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col26 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col27 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col28 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col29 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col30 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col31 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col32 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col33 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col34 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col35 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col36 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col37 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col38 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col39 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col40 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col41 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col42 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col43 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col44 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col45 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col46 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col47 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col48 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col49 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5)) ,
                   Col50 VarBinary(MAX) DEFAULT CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000)) ,
                   ColFoto VarBinary(MAX))
GO

-- 6 seconds to run
INSERT INTO TabTestStats (Col1)
SELECT TOP 10000
       CONVERT(VarBinary(MAX),REPLICATE(CONVERT(VarBinary(MAX), CONVERT(VarChar(250), NEWID())), 5000)) AS Col1
  FROM sysobjects a, sysobjects b, sysobjects c, sysobjects d
GO

-- 14 seconds to run
SELECT DISTINCT
       Col1,
       Col2,
       Col3,
       Col4,
       Col5,
       Col6,
       Col7,
       Col8,
       Col9,
       Col10,
       Col11,
       Col12,
       Col13,
       Col14,
       Col15,
       Col16,
       Col17,
       Col18,
       Col19,
       Col20,
       Col21,
       Col22,
       Col23,
       Col24,
       Col25,
       Col26,
       Col27,
       Col28,
       Col29,
       Col30,
       Col31,
       Col32,
       Col33,
       Col34,
       Col35,
       Col36,
       Col37,
       Col38,
       Col39,
       Col40,
       Col41,
       Col42,
       Col43,
       Col44,
       Col45,
       Col46,
       Col47,
       Col48,
       Col49,
       Col50,
       ColFoto
 FROM TabTestStats
WHERE Col50 IS NULL
AND 1 = (SELECT 1)
GO

--EXEC sp_helpstats TabTestStats
--GO

--DROP STATISTICS TabTestStats.[_WA_Sys_00000033_01892CED]
--GO
*/