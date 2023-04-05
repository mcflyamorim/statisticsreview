/*
Check34 - Statistics full to sample
Description:
Check 34 - Check if there was an event of an auto update stat using a sample smaller than the last sample used
A DBA can choose to manually update statistics by using a fixed sampling rate that can better represent the distribution of data.
However, a subsequent Automatic Update Statistics operation will reset back to the default sampling rate, and possibly introduce degradation of query plan efficiency.
In this check I'm returning all stats that have a diff in the number of steps, make sure you review all of those (not only the ones with a warning) to confirm you identified all the cases. I understand that having more or less steps in a statistic object is not always synonym of better key value coverage and estimations, but, that's a good indication we can use as a starting point to identify those full to sample issue.
Ideally, this check should be executed after the update stat maintenance, the longer the diff after the maintenance the better the chances we capture histogram diff due to an auto update stat.
For instance, if the maintenance plan runs at 12AM, it would be nice to run this at 5PM to see if there was any auto update that caused histogram change during the day.
Estimated Benefit:
High
Estimated Effort:
Very High
Recommendation:
Quick recommendation:
Review reported statistics and make sure cardinality estimates are good even after the statistic change.
Detailed recommendation:
- If number of steps in the current statistic is different than the last update, then, check if the current statistic created is worse than the last one. To compare it, you can use DBCC SHOW_STATISTICS to see the existing histogram and open a new session, run an update statistic with fullscan and DBCC SHOW_STATISTICS again, then, compare the histograms and see if they're different. 
- PERSIST_SAMPLE_PERCENT command can be used to avoid this issue.
- Starting with SQL Server 2016 (13.x) SP1 CU4 and 2017 CU1, you can use the PERSIST_SAMPLE_PERCENT option of CREATE STATISTICS or UPDATE STATISTICS, to set and retain a specific sampling percentage for subsequent statistic updates that do not explicitly specify a sampling percentage.
- Another option is to add a job to manually update the stats more frequently, or to recreate the stats with NO_RECOMPUTE and make sure you have your own job taking care of it.
- You may don't want to take any action now, but it may be a good idea to create a job to be notified if an auto-update run for an important table.
- Another option is to re-create the statistic using NoRecompute clause. That would avoid the statistic to be updated with sample. But, make sure you've a maintenance plan taking care of those statistics.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Preparing tables with statistic info */
EXEC sp_GetStatisticInfo @database_name_filter = N'', @refreshdata = 0

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck34') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck34

SELECT 'Check 34 - Check if there was an event of an auto update stat using a sample smaller than the last sample used' AS [info],
       a.database_name,
       a.table_name,
       a.stats_name,
       a.key_column_name,
       a.statistic_type,
       a.current_number_of_rows,
       a.current_number_of_modified_rows_since_last_update,
       Tab_StatSample1.number_of_modifications_on_key_column_since_previous_update,
       a.auto_update_threshold,
       a.auto_update_threshold_type,
       CONVERT(DECIMAL(25, 2), (a.current_number_of_modified_rows_since_last_update / (a.auto_update_threshold * 1.0)) * 100.0) AS percent_of_threshold,
       a.number_of_rows_at_time_stat_was_updated AS [Number of rows on table at time statistic was updated 1 - most recent],
       Tab_StatSample2.number_of_rows_at_time_stat_was_updated AS  [Number of rows on table at time statistic was updated - 2 - previous update],
       a.rows_sampled AS number_of_rows_sampled_on_last_update_create_statistic,
       a.statistic_percent_sampled,
       a.steps AS number_of_steps_on_histogram_1_most_recent,
       Tab_StatSample2.steps AS number_of_steps_on_histogram_2_previous_update,
       a.last_updated AS update_stat_1_most_recent_datetime,
       Tab_StatSample2.last_updated AS update_stat_2_previous_update_datetime,
       steps_diff_pct,
       CASE
         WHEN 
          (steps_diff_pct < 90) 
          /*Only considering stats where number of steps diff is at least 90%*/
          AND (Tab_StatSample1.number_of_modifications_on_key_column_since_previous_update < 1000000) 
          /*Checking if number of modifications is lower than 1mi, because, if number of modifications
            is greater than 1mi, it may be the reason of why number of steps changed.
            If number of modifications is low and steps is diff, then it is very likely it changed because
            of an update with a lower sample*/
         THEN 'Warning - Number of steps on last update stats is greater than the last update stats. This may indicate that stat was updated with a lower sample.'
         ELSE 'OK'
       END AS [comment],
       dbcc_command
INTO tempdb.dbo.tmpStatisticCheck34
FROM tempdb.dbo.tmp_stats AS a
CROSS APPLY (SELECT CASE 
                      WHEN b.inserts_since_last_update IS NULL AND b.deletes_since_last_update IS NULL
                      THEN NULL
                      ELSE (ABS(ISNULL(b.inserts_since_last_update,0)) + ABS(ISNULL(b.deletes_since_last_update,0)))
                    END AS number_of_modifications_on_key_column_since_previous_update
                FROM tempdb.dbo.tmp_exec_history b 
               WHERE b.rowid = a.rowid
                 AND b.history_number = 1
                ) AS Tab_StatSample1
CROSS APPLY (SELECT b.table_cardinality AS number_of_rows_at_time_stat_was_updated,
                    b.updated as last_updated,
                    b.steps
                FROM tempdb.dbo.tmp_exec_history b 
               WHERE b.rowid = a.rowid
                 AND b.history_number = 2 /* Previous update stat sample */
                ) AS Tab_StatSample2
CROSS APPLY (SELECT CAST((a.steps / (Tab_StatSample2.steps * 1.00)) * 100.0 AS DECIMAL(25, 2))) AS t(steps_diff_pct)
WHERE a.statistic_percent_sampled <> 100 /*Only considering stats not using FULLSCAN*/
AND Tab_StatSample2.steps <> 1 /*Ignoring histograms with only 1 step*/
AND a.steps <> Tab_StatSample2.steps /*Only cases where number of steps is diff*/

SELECT * FROM tempdb.dbo.tmpStatisticCheck34
ORDER BY steps_diff_pct ASC, 
         current_number_of_rows DESC, 
         database_name,
         table_name,
         key_column_name,
         stats_name


/*
--EXEC xp_cmdshell 'del D:\Fabiano\Trabalho\WebCasts, Artigos e Palestras\PASS Summit 2022\Dica151.bak', no_output
--BACKUP DATABASE Dica151 TO 
--DISK = 'D:\Fabiano\Trabalho\WebCasts, Artigos e Palestras\PASS Summit 2022\Dica151.bak'
--WITH INIT , NOUNLOAD , NAME = 'Dica151 backup', NOSKIP , STATS = 10, NOFORMAT, COMPRESSION
--GO

--USE master
--GO
--ALTER DATABASE Dica151 SET SINGLE_USER WITH ROLLBACK IMMEDIATE
--DROP DATABASE Dica151
---- 26 seconds to run
--RESTORE DATABASE [Dica151] FROM DISK = N'D:\Fabiano\Trabalho\WebCasts, Artigos e Palestras\PASS Summit 2022\Dica151.bak' 
--WITH  FILE = 1, MOVE N'ClearTraceFabiano' TO N'C:\DBs\Dica151.mdf',  
--                MOVE N'ClearTraceFabiano_log' TO N'C:\DBs\Dica151_log.ldf',  
--NOUNLOAD,  STATS = 5

USE Dica151
GO

-- Maintenance job ran and updated stat to FULLSCAN
-- 7 seconds to run
UPDATE STATISTICS FATURAMENTO_PROD XIE2FATURAMENTO_PROD WITH FULLSCAN
GO

-- Query takes avg of 1 second to run
SELECT C.CAIXA,
       A.FILIAL,
       A.NF_SAIDA,
       A.SERIE_NF,
       A.NOME_CLIFOR
FROM FATURAMENTO A
    INNER JOIN FATURAMENTO_PROD B
        ON A.NF_SAIDA = B.NF_SAIDA
           AND A.SERIE_NF = B.SERIE_NF
           AND A.FILIAL = B.FILIAL
    INNER JOIN FATURAMENTO_CAIXAS C
        ON B.CAIXA = C.CAIXA
WHERE A.STATUS_NFE = 5
      AND C.NOME_CLIFOR_DESTINO_FINAL IS NOT NULL
      AND C.NOME_CLIFOR_DESTINO_FINAL <> C.NOME_CLIFOR
      AND C.CHAVE_NFE IS NULL
      AND B.PEDIDO IS NOT NULL
GROUP BY C.CAIXA,
         A.FILIAL,
         A.NF_SAIDA,
         A.SERIE_NF,
         A.NOME_CLIFOR
OPTION (RECOMPILE, MAXDOP 1);
GO

-- Yes, this will not actually change anything, but will 
-- increase internal modification counter
BEGIN TRAN
TRUNCATE TABLE FATURAMENTO_PROD
ROLLBACK TRAN
GO

-- Modification counter...
SELECT a.name, a.stats_id, sp.last_updated, sp.rows, sp.rows_sampled, sp.modification_counter 
FROM sys.stats AS a
        CROSS APPLY sys.dm_db_stats_properties(a.object_id, a.stats_id) AS sp
WHERE a.object_id = OBJECT_ID('FATURAMENTO_PROD')
GO

-- This will trigger auto-update stats which 
-- will use sample
-- Query takes 7 seconds to run
SELECT C.CAIXA,
       A.FILIAL,
       A.NF_SAIDA,
       A.SERIE_NF,
       A.NOME_CLIFOR
FROM FATURAMENTO A
    INNER JOIN FATURAMENTO_PROD B
        ON A.NF_SAIDA = B.NF_SAIDA
           AND A.SERIE_NF = B.SERIE_NF
           AND A.FILIAL = B.FILIAL
    INNER JOIN FATURAMENTO_CAIXAS C
        ON B.CAIXA = C.CAIXA
WHERE A.STATUS_NFE = 5
      AND C.NOME_CLIFOR_DESTINO_FINAL IS NOT NULL
      AND C.NOME_CLIFOR_DESTINO_FINAL <> C.NOME_CLIFOR
      AND C.CHAVE_NFE IS NULL
      AND B.PEDIDO IS NOT NULL
GROUP BY C.CAIXA,
         A.FILIAL,
         A.NF_SAIDA,
         A.SERIE_NF,
         A.NOME_CLIFOR
OPTION (RECOMPILE, MAXDOP 1);
GO
*/