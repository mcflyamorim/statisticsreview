/*
Check9 - TF2371 dynamic threshold
Description:
Check 9 - Trace flag check - TF2371 (Changes the fixed update statistics threshold to a linear update statistics threshold.)
By default, statistics are updated after 20% +500 rows of data have been modified, this may be too much for big tables. 
TF2371 can be used to reduce the number of modifications required for automatic updates to statistics to occur. While recommended for many scenarios, enabling the trace flag is optional. However, you can use the following guidance for enabling the trace flag 2371 in your SQL Server environment:
- If you are on a SAP system, enable this trace flag. Refer to this blog for additional Information.
- If you have to rely on nightly job to update statistics because current automatic update is not triggered frequently enough, consider enabling trace flag 2371 to adjust the threshold to table cardinality.
- In SQL Server 2008 R2 through SQL Server 2014 (12.x), or in SQL Server 2016 (13.x) and later builds, if you have databases under compatibility level 120 and lower, you'll need to enable trace flag 2371 to make SQL Server uses a decreasing, dynamic statistics update threshold.
Estimated Benefit:
Medium
Estimated Effort:
Low
Recommendation:
Quick recommendation:
Consider to enable trace flag 2371.
Detailed recommendation:
- If you have databases under compatibility level 120 and lower, you still need to enable TF2371, even on SQL Server 2016 and newer builds.
- For a large majority of SQL Server installations, it is a best practice to enable TF2371.
- A friendly and good reminder: It is very important and a good practice to update statistics on a regular basis through a scheduled job and leaving the auto update enabled as a safety.
Warning Note: Customers should always test changes related to trace flags or/and to the compatibility level carefully. You should always test and evaluate those changes before apply it in production. Use mitigation technologies, such as the Query Store, if there is a plan-choice related performance issue.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;


IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck9') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck9

DECLARE @sqlmajorver INT, @sqlminorver int, @sqlbuild int
SELECT @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff),
	      @sqlminorver = CONVERT(int, (@@microsoftversion / 0x10000) & 0xff),
 	     @sqlbuild = CONVERT(int, @@microsoftversion & 0xffff);

DECLARE @min_compat_level tinyint
SELECT @min_compat_level = min([compatibility_level])
		from sys.databases

DECLARE @tracestatus TABLE(TraceFlag nVarChar(40)
                         , Status    tinyint
                         , Global    tinyint
                         , Session   tinyint)

INSERT INTO @tracestatus
EXEC ('DBCC TRACESTATUS WITH NO_INFOMSGS')

SELECT 
  'Check 9 - Trace flag check - TF2371 (Changes the fixed update statistics threshold to a linear update statistics threshold.)' AS [info],
  name AS database_name,
  [compatibility_level],
  CASE 
    WHEN NOT EXISTS(SELECT TraceFlag
	                   FROM @tracestatus
	                   WHERE [Global] = 1 AND TraceFlag = 2371) /*TF2371 is not enabled*/
	        AND ((@sqlmajorver = 10 /*SQL2008*/ AND @sqlminorver = 50 /*50 = R2*/ AND @sqlbuild >= 2500 /*SP1*/) OR @sqlmajorver < 13 /*SQL2016*/)
      THEN 'Warning - Consider enabling TF2371 to change the 20pct fixed rate threshold for update statistics into a dynamic percentage rate'
    WHEN NOT EXISTS (SELECT TraceFlag
			                  FROM @tracestatus
			                  WHERE [Global] = 1 AND TraceFlag = 2371) /*TF2371 is not enabled*/
			     AND (@sqlmajorver >= 13 AND [compatibility_level] < 130) /*SQL Server is 2016(13.x) but there are DBs with compatibility level < 130*/
      THEN 'Warning - Database with compatibility level < 130 (SQL2016). Consider enabling TF2371 to change the 20pct fixed rate threshold for update statistics into a dynamic percentage rate'
    WHEN EXISTS(SELECT TraceFlag
			            FROM @tracestatus
			            WHERE [Global] = 1 
               AND TraceFlag = 2371) /*TF2371 is enabled*/
      THEN CASE
             WHEN (@sqlmajorver = 10 /*SQL2008*/ AND @sqlminorver = 50 /*50 = R2*/ AND @sqlbuild >= 2500 /*SP1*/)
                  OR (@sqlmajorver BETWEEN 11 /*SQL2012*/ AND 12 /*SQL2014*/)
                  OR (@sqlmajorver >= 13 /*SQL2016*/ AND [compatibility_level] < 130 /*SQL2016*/) 
               THEN 'Information - TF2371 is enabled, this TF changes the fixed rate of the 20pct threshold for update statistics into a dynamic percentage rate'
             WHEN @sqlmajorver >= 13 /*SQL2016*/ AND [compatibility_level] >= 130 /*SQL2016*/
               THEN 'Warning - TF2371 is not needed in SQL 2016 and above when all databases are at compatibility level 130 and above'
             ELSE 'Warning - Manually verify need to set a Non-default TF with current system build and configuration'
           END
    ELSE 'OK'
  END AS [comment]
INTO tempdb.dbo.tmpStatisticCheck9
FROM sys.databases
WHERE state_desc = 'ONLINE'
AND is_read_only = 0 
AND name not in ('tempdb', 'master', 'model', 'msdb')

SELECT * FROM tempdb.dbo.tmpStatisticCheck9