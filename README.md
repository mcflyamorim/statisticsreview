# SQL Server statistics best practices review

> "Statistics show every two minutes another statistic is created." (Anonymous)

## Do I need this?

You should consider to review SQL Server statistics in the following conditions:

- Query execution times are slow.

That's it, before performing any additional troubleshooting steps, the first thing you should do is to ensure that QO (Query Optimizer) was able to create a ***high-quality query plan***.

To increase chances of a high-quality plan, you should ensure the following:

1. **Accurate** CE (Cardinality Estimations).
2. **Useful** and **up-to-date** statistics about your data distribution. 

CE in SQL Server is derived primarily from statistics, therefore, to make sure you generate optimal query plans, it is best to design queries so that the QO can **accurately** estimate the selectivity of the conditions in your query, and, ensure you have **up-to-date** statistics. This will increase chances of a more precise CE and, in turn, faster execution plans.

If you don't take good care of DB statistics, it is very likely you will have less-than-optimal query plan choices and poor performance.

If you want to track all queries with CE issues, you can use the extended event [inaccurate_cardinality_estimate] (identify queries that may be using sub-optimal plans due to cardinality estimate inaccuracy) or use query store.

## Purpose:
Help users to check a SQL Server instance for some of most common skewed best practices and performance issues related to statistics.

## How to install and run the script

You have two options to run the scripts, choose the one that most suits your needs:

- Option 1 - Powershell script
  1. Open a PowerShell console and run the following command:

    `& "C:\temp\ExportStatisticsChecksToExcel.ps1" -SQLInstance "ServerA\SQL2019" -LogFilePath "C:\temp\" -Force_sp_GetStatisticInfo_Execution`

![ExportStatisticsChecksToExcel.ps1](/Run.gif)

- Option 2 - Manual execution
  1. Create the procedure [sp_GetStatisticInfo](https://github.com/mcflyamorim/statisticsreview/blob/main/IndividualChecks/0%20-%20sp_GetStatisticInfo.sql).
  2. Execute procedure sp_GetStatisticInfo to capture statistics info, for example, to capture info about all statistics on Northwind DB run *EXEC sp_GetStatisticInfo @database_name_filter = 'Northwind'*.
  3. Run [checks](https://github.com/mcflyamorim/statisticsreview/tree/main/IndividualChecks) and analyze the results.

## Results - Excel spreadsheet report

You can use the [ExportStatisticsChecksToExcel.ps1](https://github.com/mcflyamorim/statisticsreview/blob/main/ExportStatisticsChecksToExcel.ps1) file to run the checks and save the results into a Excel report file.

Following is what the results looks like:

![Excel report](/ExcelReport.png)

## Credit: 
* Many of the checks and scripts were based on Brent Ozar sp_blitz scripts, MS Tiger team BP, Glenn Berry's diagnostic queries, Kimberly Tripp queries
and probably a lot of other SQL community folks out there, so, a huge kudos for SQL community.

## Best practices execution and recommendations
- Ideally the statistic data collection (execution of sp_GetStatisticInfo) and report should be executed a few hours after the maintenance window. Check comments on [Check51-Maintenance time win.sql](https://github.com/mcflyamorim/statisticsreview/blob/main/IndividualChecks/Check51-Maintenance%20time%20win.sql) file for more details.
- It may be a good idea to capture extended event [inaccurate_cardinality_estimate]before start the analysis and use it as a baseline to identify number of queries with bad estimations. The counters "Batch Resp Statistics" may also help to have an ideia on what the query response time looks like. After you finish the analysis and apply the fixes/modifications, you can capture those counters again and compare with the baseline to have an bench with results. 

## Known issues and limitations:
* Each [check](https://github.com/mcflyamorim/statisticsreview/tree/main/IndividualChecks) file has comments and recomendations about what you should look for and do with the check resultset. Make sure you read it before you make any decision.
* Not tested and not support on Azure SQL DBs, Amazon RDS and Managed Instances (I’m planning to add support for this in a new release).
* As for v1, there are no specific checks and validations for Memory-Optimized Tables. (I'm planning to add support for this in a new release).
* Tables with Clustered ColumnStore index, may fail to report index usage information. (I still have to test this and find a workaround, 
 should be easy to fix, but, did't dit it yet)
* SQL Server creates and maintains temporary statistics in tempdb for read-only DBs, 
 snapshots or read-only AG replicas. 
 I'm not checking those yet, but, I'm planing to support it in a new release.

## Important notes and pre-requisites:
* Found a bug or want to change something? Please feel free to create an issue on https://github.com/mcflyamorim/StatisticsReview
 or, you can also e-mail (really? I didn't know people were still using this.) me.
* I'm using unsupported/undocumented TF 2388 to check statistic lead column type.
* Depending on the number of statistics, the PS script to generate the excel file may use a lot (a few GBs) of memory.
* You should know about it, but I'm going to say it anyways:
 Before implementing any trace flag in a production environment, carefully review all Microsoft 
 information and recommendations and learn what you can from other reliable sources. 
 Microsoft recommends that you thoroughly test any trace flags that you plan to implement in a 
 production environment before enabling them in that environment. 
 Trace flags can have unpredictable consequences and should be deployed with care.

## Disclaimer:
* This code and information are provided "AS IS" without warranty of any kind, either expressed or implied.
Furthermore, the author shall not be liable for any damages you may sustain by using this information, whether direct, 
indirect, special, incidental or consequential, even if it has been advised of the possibility of such damages.
	
## License:
* Pretty much free to everyone and to do anything you'd like as per MIT License - https://en.wikipedia.org/wiki/MIT_License

With all love and care, Fabiano Amorim.
