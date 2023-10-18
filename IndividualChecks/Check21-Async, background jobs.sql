/*
Check21 - Async, background jobs
Description:
Check 21 - Async - Dump information from sys.dm_exec_background_job_queue
If there are DBs using auto update stats async, info from sys.dm_exec_background_job_queue may be useful.
Before running a query, the server checks the plan to determine if any of the referenced objects exceed the threshold of stale statistics. If the threshold is exceeded, the server will enqueue a job to the background job queue to rebuild the statistics, but will continue with compilation, without waiting for the job to complete.
You can view currently queued jobs via the sys.dm_exec_background_job_queue dynamic management view, which is currently used only for async update statistics jobs. The database_id column tells you what database the job will run it, while the object_id1 column displays the object ID of the table or view, and the object_id2 column displays the statistics ID that is to be updated.
The system will create up to two workers per SOS scheduler (with a fixed maximum of eight workers) to process jobs from the queue. 
If we haven't reached the limit when a new request is enqueued, then the job starts executing immediately on a background worker. Otherwise, it waits for an available worker.
The job queue is limited to, at most, 100 requests.
Estimated Benefit:
Medium
Estimated Effort:
High
Recommendation:
Quick recommendation:
Review reported data and investigate it further.
Detailed recommendation:
- If you see more than 2 rows as a result of this check, this may indicate the number of threads running the update stats in background is not enough. Investigate it further to understand what is happening.

*/

-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;  SET ANSI_WARNINGS OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb.dbo.tmpStatisticCheck21') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpStatisticCheck21

SELECT 'Check 21 - Async - Dump information from sys.dm_exec_background_job_queue' AS [info],
       OBJECT_NAME(dm_exec_background_job_queue.[object_id1], dm_exec_background_job_queue.database_id) AS [object_name],
       dm_exec_background_job_queue.*,
       dm_exec_requests.command,
	      dm_exec_requests.cpu_time, 
	      dm_exec_requests.reads, 
	      dm_exec_requests.writes, 
	      dm_exec_requests.logical_reads, 
	      dm_exec_requests.granted_query_memory,
	      dm_exec_requests.last_wait_type
INTO tempdb.dbo.tmpStatisticCheck21
FROM sys.dm_exec_background_job_queue
    LEFT OUTER JOIN sys.dm_exec_requests
        ON dm_exec_requests.session_id = dm_exec_background_job_queue.session_id

SELECT * FROM tempdb.dbo.tmpStatisticCheck21
ORDER BY time_queued ASC