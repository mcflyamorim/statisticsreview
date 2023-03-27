/*
Check13 - TF4199 – Query Optimizer hotfixes
Description:
Check 13 - Trace flag check - TF4199, enables query optimizer changes released in SQL Server Cumulative Updates and Service Packs
Check TF4199, TF4199 enables query optimizer changes released in SQL Server Cumulative Updates and Service Packs.
Query Optimizer fixes released for previous SQL Server versions under trace flag 4199 become automatically enabled in the default compatibility level of a newer SQL Server version. Post-RTM Query Optimizer fixes still need to be explicitly enabled via QUERY_OPTIMIZER_HOTFIXES option in ALTER DATABASE SCOPED CONFIGURATION or via trace flag 4199.
Following is a sample of a very common query with a fix only applied under TF4199: 
FIX: Slow query performance when using query predicates with UPPER, LOWER or RTRIM with default CE in SQL Server 2017 and 2019 https://support.microsoft.com/en-us/topic/kb4538497-fix-slow-query-performance-when-using-query-predicates-with-upper-lower-or-rtrim-with-default-ce-in-sql-server-2017-and-2019-5619b55c-b0b4-0a8e-2bce-2ffe6b7eb70e 
Estimated Benefit:
Medium
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Consider to enable trace flag 4199.
Detailed recommendation:
- You still need TF4199 (or QUERY_OPTIMIZER_HOTFIXES DB scope config) to get post-RTM Query Optimizer fixes. It is recommended to enable it.
- You can use query store to experiment queries using QUERY_OPTIMIZER_HOTFIXES/TF4199 and see if you got better/regressed plans and confirm whether you have a plan-choice related performance issue or not.
Warning Note: Customers should always test changes related to trace flags or/and to the compatibility level carefully. You should always test and evaluate those changes before apply it in production. Use mitigation technologies, such as the Query Store, if there is a plan-choice related performance issue.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck13') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck13

DECLARE @sqlmajorver INT, @sqlminorver int, @sqlbuild int
SELECT @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff)

DECLARE @tracestatus TABLE(TraceFlag nVarChar(40)
                         , Status    tinyint
                         , Global    tinyint
                         , Session   tinyint)

INSERT INTO @tracestatus
EXEC ('DBCC TRACESTATUS WITH NO_INFOMSGS')

SELECT 
  'Check 13 - Trace flag check - TF4199, enables query optimizer changes released in SQL Server Cumulative Updates and Service Packs' AS [info],
  CASE 
    WHEN NOT EXISTS(SELECT TraceFlag
		                    FROM @tracestatus
		                   WHERE [Global] = 1 AND TraceFlag = 4199)
         AND (@sqlmajorver >= 13 /*SQL2016*/)
				THEN 'Warning - Consider enabling TF4199 or QUERY_OPTIMIZER_HOTFIXES option in ALTER DATABASE SCOPED CONFIGURATION. This will enable query optimizer changes released in SQL Server Cumulative Updates and Service Packs'
    WHEN NOT EXISTS(SELECT TraceFlag
		                    FROM @tracestatus
		                   WHERE [Global] = 1 AND TraceFlag = 4199)
				THEN 'Warning - Consider enabling TF4199 to enable query optimizer changes released in SQL Server Cumulative Updates and Service Packs'
    ELSE 'OK'
  END AS [comment]
INTO tempdb.dbo.tmpStatisticCheck13

SELECT * FROM tempdb.dbo.tmpStatisticCheck13

/*
  Code sample to show issue

--USE Northwind
--GO
--IF OBJECT_ID('vw_NewID', 'V') IS NOT NULL
--  DROP VIEW vw_NewID
--GO
--CREATE VIEW vw_NewID
--AS
--  SELECT NEWID() AS "NewID"
--GO
--IF OBJECT_ID('fn_cKeys') IS NOT NULL
--  DROP FUNCTION fn_cKeys
--GO
--CREATE FUNCTION dbo.fn_cKeys()
--RETURNS VarChar(200)
--AS
--BEGIN
--  DECLARE @Str VarChar(200)
--  SELECT TOP 1 @Str = t1.pdws
--    FROM (VALUES ('teste'), ('TESTE'),('Teste'), ('password'), ('qwerty'),
--                 ('football'), ('baseball'), ('welcome'), ('abc123'),('ABC123'),
--                 ('1qaz2wsx'), ('dragon'), ('master'), ('moncKey'), ('letmein'),
--                 ('login'), ('princess'), ('qwertyuiop'), ('solo'), ('passw0rd'), 
--                 ('starwars'), ('teste123'), ('TESTE123'), ('deuseamor'), ('jesuscristo'),
--                 ('iloveyou'), ('MARCELO'), ('jc2512'), ('maria'), ('jose'), ('batman'),
--                 ('123123'), ('123123123'), ('FaMiLia'), (''), (' '), ('sexy'),
--                 ('abel123'), ('freedom'), ('whatever'), ('qazwsx'), ('trustno1'), ('sucesso'),
--                 ('1q2w3e4r'), ('1qaz2wsx'), ('1qazxsw2'), ('zaq12wsx'), ('! qaz2wsx'),
--                 ('!qaz2wsx'), ('123mudar'), ('gabriel'), ('102030'), ('010203'), ('101010'), ('131313'),
--                 ('vitoria'), ('flamengo'), ('felipe'), ('brasil'), ('felicidade'), ('mariana'), ('101010')) t1(pdws)
--   ORDER BY (SELECT "NewID" FROM vw_NewID)
--  RETURN(@Str)
--END
--GO
--IF OBJECT_ID('CustomersBigTF4199') IS NOT NULL
--  DROP TABLE CustomersBigTF4199

---- 37 seconds to run
--SELECT TOP 500000
--       IDENTITY(Int, 1,1) AS CustomerID,
--       CONVERT(VARCHAR(250), t4.CompanyName) AS CompanyName, 
--       CONVERT(VARCHAR(250), t3.ContactName) AS ContactName,
--       CONVERT(VARCHAR(250), dbo.fn_cKeys()) AS cKey,
--       CONVERT(DATETIME, NULL) AS InsertedDate,
--       CONVERT(IMAGE, CONVERT(VARCHAR(200), NEWID())) AS Col1,
--       CONVERT(VARCHAR(250), NEWID()) AS Col2
--  INTO CustomersBigTF4199
--  FROM master.dbo.spt_values A
-- CROSS JOIN master.dbo.spt_values B
-- CROSS JOIN master.dbo.spt_values C
-- CROSS JOIN master.dbo.spt_values D
-- CROSS APPLY (SELECT CRYPT_GEN_RANDOM (10)) AS t1(ContactName)
-- CROSS APPLY (SELECT CRYPT_GEN_RANDOM (15)) AS t2(CompanyName)
-- CROSS APPLY (SELECT REPLACE(REPLACE(REPLACE(CONVERT(XML, '').value('xs:base64Binary(sql:column("t1.ContactName"))', 'VARCHAR(MAX)'), '=', ''), '/', ''), '+', '')) AS t3(ContactName)
-- CROSS APPLY (SELECT REPLACE(REPLACE(REPLACE(CONVERT(XML, '').value('xs:base64Binary(sql:column("t2.CompanyName"))', 'VARCHAR(MAX)'), '=', ''), '/', ''), '+', '')) AS t4(CompanyName)
--OPTION (MAXDOP 4)
--GO
--UPDATE CustomersBigTF4199 SET InsertedDate = DATEADD(second, CONVERT(BIGINT, CustomerID * (CONVERT(INT, RAND() * (100 - 1) + 1))), CONVERT(DATETIME, '20100101'))
--GO

---- 37 seconds to run
--INSERT INTO CustomersBigTF4199 WITH(TABLOCK)
--(
--    CompanyName,
--    ContactName,
--    cKey,
--    InsertedDate,
--    Col1,
--    Col2
--)
--SELECT TOP 5000000
--       CONVERT(VARCHAR(250), t4.CompanyName) AS CompanyName, 
--       CONVERT(VARCHAR(250), t3.ContactName) AS ContactName,
--       CONVERT(VARCHAR(250), 'ChaveInicial') AS cKey,
--       CONVERT(DATETIME, '20220101') AS InsertedDate,
--       CONVERT(IMAGE, CONVERT(VARCHAR(200), NEWID())) AS Col1,
--       CONVERT(VARCHAR(250), NEWID()) AS Col2
--  FROM master.dbo.spt_values A
-- CROSS JOIN master.dbo.spt_values B
-- CROSS JOIN master.dbo.spt_values C
-- CROSS JOIN master.dbo.spt_values D
-- CROSS APPLY (SELECT CRYPT_GEN_RANDOM (10)) AS t1(ContactName)
-- CROSS APPLY (SELECT CRYPT_GEN_RANDOM (15)) AS t2(CompanyName)
-- CROSS APPLY (SELECT REPLACE(REPLACE(REPLACE(CONVERT(XML, '').value('xs:base64Binary(sql:column("t1.ContactName"))', 'VARCHAR(MAX)'), '=', ''), '/', ''), '+', '')) AS t3(ContactName)
-- CROSS APPLY (SELECT REPLACE(REPLACE(REPLACE(CONVERT(XML, '').value('xs:base64Binary(sql:column("t2.CompanyName"))', 'VARCHAR(MAX)'), '=', ''), '/', ''), '+', '')) AS t4(CompanyName)
--OPTION (MAXDOP 4)
--GO
---- 11 seconds to run
--ALTER TABLE CustomersBigTF4199 ADD CONSTRAINT xpk_CustomersBigTF4199 PRIMARY KEY(CustomerID)
---- DROP INDEX IF EXISTS ixcKey ON CustomersBigTF4199
--CREATE INDEX ixcKey ON CustomersBigTF4199(cKey)
--GO

-- Scan
SELECT CustomerID,
       CompanyName,
       ContactName,
       InsertedDate,
       cKey,
       Col1
  FROM CustomersBigTF4199
 WHERE cKey = LOWER('teste123')
 OPTION(RECOMPILE)
GO

-- Seek
SELECT CustomerID,
       CompanyName,
       ContactName,
       InsertedDate,
       cKey,
       Col1
  FROM CustomersBigTF4199
 WHERE cKey = LOWER('teste123')
OPTION(RECOMPILE, USE HINT('ENABLE_QUERY_OPTIMIZER_HOTFIXES'))
GO

-- Or
DBCC TRACEON(4199)
-- DBCC TRACEOFF(4199)
GO

SELECT CustomerID,
       CompanyName,
       ContactName,
       InsertedDate,
       cKey,
       Col1
  FROM CustomersBigTF4199
 WHERE cKey = LOWER('teste123')
 OPTION(RECOMPILE)
GO
*/