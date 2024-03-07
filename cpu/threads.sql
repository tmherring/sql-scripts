SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;
PRINT '************************************************************************************';
PRINT '********** Total threads available and in use on an instance.';
PRINT '************************************************************************************';
SELECT dosi.[max_workers_count], dow.[total_worker_threads], dot.[total_threads_used]
  FROM sys.dm_os_sys_info dosi
 CROSS APPLY (SELECT COUNT(1) [total_threads_used]
                FROM sys.dm_os_threads) dot
 CROSS APPLY (SELECT COUNT(1)
                FROM sys.dm_os_workers) dow;

PRINT '************************************************************************************';
PRINT '********** Total workers tied to UMS schedulers';
PRINT '************************************************************************************';
SELECT SUM(current_workers_count) [current_workers_count],
       SUM(active_workers_count) [active_workers_count]
  FROM sys.dm_os_schedulers
 WHERE [status] = 'VISIBLE ONLINE';

PRINT '************************************************************************************';
PRINT '********** Workers tied to individual UMS schedulers.';
PRINT '************************************************************************************';
SELECT [scheduler_id], [status], [cpu_id], [is_online], [is_idle], [current_tasks_count],
       [runnable_tasks_count], [current_workers_count], [active_workers_count], 
       [pending_disk_io_count], [load_factor], [work_queue_count]
  FROM sys.dm_os_schedulers
 WHERE [status] = 'VISIBLE ONLINE'
 ORDER BY [scheduler_id];

PRINT '************************************************************************************';
PRINT '********** Mirroring Threads.';
PRINT '************************************************************************************';
SELECT [session_id], [status], [command], [wait_type]
  FROM sys.dm_exec_requests
 WHERE [command] = 'DB MIRROR';

PRINT '************************************************************************************';
PRINT '********** Status of individual threads.'
PRINT '************************************************************************************';
SELECT [worker_address], [scheduler_address], [state], [is_preemptive], [is_fiber], [is_sick],
       [is_in_cc_exception], [is_fatal_exception], [return_code]
  FROM sys.dm_os_workers;
