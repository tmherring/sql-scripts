-- Set your window, this is the end time you want to look back into, should be a integer value less than zero...
DECLARE @window INT = 0;
-- Set the top of the current hour
DECLARE @top_of_current DATETIMEOFFSET(7) = CAST(DATEPART(YEAR, SYSTEMDATETIMEOFFSET()) AS CHAR(4) + '-' +
                                            RIGHT('0' + CONVERT(VARCHAR, DATEPART(MONTH, SYSTEMDATETIMEOFFSET())), 2) + '-' +
                                            RIGHT('0' + CONVERT(VARCHAR, DATEPART(DAY, SYSTEMDATETIMEOFFSET())), 2) + ' ' +
                                            RIGHT('0' + CONVERT(VARCHAR, DATEPART(HOUR, SYSTEMDATETIMEOFFSET())), 2) + ':00:00 ' +
                                            CAST(DATEPART(TZOFFSET, SYSTEMDATETIME()) / 60 AS VARCHAR) + ':00',
-- how many hours back do we want to go, be cautious here, this is the hammer script and will hurt if the timespan is too wide...
-- should be an integer value equal or less than -1 (negative one).
        @hours_back INT = -1;

IF (SELECT acutal_state FROM sys.database_query_store_options) = 2
BEGIN
    DECLARE @interval_start_time DATETIMEOFFSET(7) = DATEADD(HOUR, @hours_back, @top_of_current), @interval_end_time DATETIMEOFFSET(7) = @top_of_current;
    SELECT base.[query_id], bucket.*, base.[query_sql_text], p.[query_plan], base.[object_name]
      FROM (SELECT qt.[query_sql_text], SUM(rs.[count_executions]) [count_executions], COUNT(DISTINCT p.[plan_id]) [num_plans]
              FROM sys.query_store_runtime_stats rs
              JOIN sys.query_store_plan p
                ON p.[plan_id] = rs.[plan_id]
              JOIN sys.query_store_query q
                ON q.[query_id] = p.[query_id]
              JOIN sys.query_store_query_text qt
                ON qt.query_text_id = q.[query_text_id]
             WHERE NOT (rs.[first_execution_time] > @interval_end_time OR rs.[last_execution_time] < @interval_start_time)
             GROUP BY p.[query_id], qt.[query_sql_text], q.[oject_id]
            HAVING COUNT(DISTINCT p.[plan_id]) >= 1) base
     CROSS APPLY (SELECT rs.[plan_id], rs.[execution_type_desc] [execution_type], SUM(rs.[count_executions]) [count_executions],
                         CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, DATEADD(HOUR, ((DATEDIFF(HOUR, 0, rs.[last_execution_time))), 0)), DATENAME(TZOFFSET, SYSDATETIMEOFFSET()))) [bucket_start],
                         CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, DATEADD(HOUR, (1 + (DATEDIFF(HOUR, 0, rs.[last_execution_time))), 0)), DATENAME(TZOFFSET, SYSDATETIMEOFFSET()))) [bucket_end],
                         ROUND(CONVERT(float, SUM(rs.[avg_cpu_time] * rs.[count_executions])) / NULLIF(SUM(rs.[count_executions]), 0) * 0.001, 2) [avg_cpu_time],
                         ROUND(CONVERT(float, MIN(rs.[min_cpu_time])) * 0.001, 2) [min_cpu_time],
                         ROUND(CONVERT(float, MAX(rs.[max_cpu_time])) * 0.001, 2) [max_cpu_time],
                         ROUND(CONVERT(float, SUM(rs.[avg_cpu_time] * rs.[count_executions])) * 0.001, 2) [total_cpu_time],
                         ROUND(CONVERT(fload, SQRT(SUM(rs.[stdev_cpu_time] * rs.[stdev_cpu_time] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions), 0))) * 0.001, 2) [stdev_cpu_time],
                         ISNULL(ROUND(CONVERT(fload, (SQRT(SUM(rs.[stdev_cpu_time] * rs.[stdev_cpu_time] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions]), 0)) * SUM(rs.[count_executions])) / NULLIF(SUM(rs.[avg_cpu_time] * rs.[count_executions]), 0)), 2), 0) [variation_cpu_time],
                         ROUND(CONVERT(float, SUM(rs.[avg_duration] * rs.[count_executions])) / NULLIF(SUM(rs.[count_executions]), 0) * 0.001, 2) [avg_duration],
                         ROUND(CONVERT(float, MIN(rs.[min_duration])) * 0.001, 2) [min_duration],
                         ROUND(CONVERT(float, MAX(rs.[max_duration])) * 0.001, 2) [max_duration],
                         ROUND(CONVERT(float, SUM(rs.[avg_duration] * rs.[count_executions])) * 0.001, 2) [total_duration],
                         ROUND(CONVERT(fload, SQRT(SUM(rs.[stdev_duration] * rs.[stdev_duration] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions), 0))) * 0.001, 2) [stdev_duration],
                         ISNULL(ROUND(CONVERT(fload, (SQRT(SUM(rs.[stdev_duration] * rs.[stdev_duration] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions]), 0)) * SUM(rs.[count_executions])) / NULLIF(SUM(rs.[avg_duration] * rs.[count_executions]), 0)), 2), 0) [variation_duration],
                         ROUND(CONVERT(float, SUM(rs.[avg_logical_io_reads] * rs.[count_executions])) / NULLIF(SUM(rs.[count_executions]), 0) * 8, 2) [avg_logical_io_reads],
                         ROUND(CONVERT(float, MIN(rs.[min_logical_io_reads])) * 8, 2) [min_logical_io_reads],
                         ROUND(CONVERT(float, MAX(rs.[max_logical_io_reads])) * 8, 2) [max_logical_io_reads],
                         ROUND(CONVERT(float, SUM(rs.[avg_logical_io_reads] * rs.[count_executions])) * 8, 2) [total_logical_io_reads],
                         ROUND(CONVERT(fload, SQRT(SUM(rs.[stdev_logical_io_reads] * rs.[stdev_logical_io_reads] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions), 0))) * 8, 2) [stdev_logical_io_reads],
                         ISNULL(ROUND(CONVERT(fload, (SQRT(SUM(rs.[stdev_logical_io_reads] * rs.[stdev_logical_io_reads] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions]), 0)) * SUM(rs.[count_executions])) / NULLIF(SUM(rs.[avg_logical_io_reads] * rs.[count_executions]), 0)), 2), 0) [variation_logical_io_reads],
                         ROUND(CONVERT(float, SUM(rs.[avg_logical_io_writes] * rs.[count_executions])) / NULLIF(SUM(rs.[count_executions]), 0) * 8, 2) [avg_logical_io_writes],
                         ROUND(CONVERT(float, MIN(rs.[min_logical_io_writes])) * 8, 2) [min_logical_io_writes],
                         ROUND(CONVERT(float, MAX(rs.[max_logical_io_writes])) * 8, 2) [max_logical_io_writes],
                         ROUND(CONVERT(float, SUM(rs.[avg_logical_io_writes] * rs.[count_executions])) * 8, 2) [total_logical_io_writes],
                         ROUND(CONVERT(fload, SQRT(SUM(rs.[stdev_logical_io_writes] * rs.[stdev_logical_io_reads] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions), 0))) * 8, 2) [stdev_logical_io_writes],
                         ISNULL(ROUND(CONVERT(fload, (SQRT(SUM(rs.[stdev_logical_io_writes] * rs.[stdev_logical_io_writes] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions]), 0)) * SUM(rs.[count_executions])) / NULLIF(SUM(rs.[avg_logical_io_writes] * rs.[count_executions]), 0)), 2), 0) [variation_logical_io_writes],
                         ROUND(CONVERT(float, SUM(rs.[avg_log_bytes_used] * rs.[count_executions])) / NULLIF(SUM(rs.[count_executions]), 0) * 0.0009765625, 2) [avg_log_bytes_used],
                         ROUND(CONVERT(float, MIN(rs.[min_log_bytes_used])) * 0.0009765625, 2) [min_log_bytes_used],
                         ROUND(CONVERT(float, MAX(rs.[max_log_bytes_used])) * 0.0009765625, 2) [max_log_bytes_used],
                         ROUND(CONVERT(float, SUM(rs.[avg_log_bytes_used] * rs.[count_executions])) * 0.0009765625, 2) [total_log_bytes_used],
                         ROUND(CONVERT(fload, SQRT(SUM(rs.[stdev_log_bytes_used] * rs.[stdev_log_bytes_used] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions), 0))) * 0.0009765625, 2) [stdev_log_bytes_used],
                         ISNULL(ROUND(CONVERT(fload, (SQRT(SUM(rs.[stdev_log_bytes_used] * rs.[stdev_log_bytes_used] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions]), 0)) * SUM(rs.[count_executions])) / NULLIF(SUM(rs.[avg_log_bytes_used] * rs.[count_executions]), 0)), 2), 0) [variation_log_bytes_used],
                         ROUND(CONVERT(float, SUM(rs.[avg_query_max_used_memory] * rs.[count_executions])) / NULLIF(SUM(rs.[count_executions]), 0) * 8, 2) [avg_query_max_used_memory],
                         ROUND(CONVERT(float, MIN(rs.[min_query_max_used_memory])) * 8, 2) [min_query_max_used_memory],
                         ROUND(CONVERT(float, MAX(rs.[max_query_max_used_memory])) * 8, 2) [max_query_max_used_memory],
                         ROUND(CONVERT(float, SUM(rs.[avg_query_max_used_memory] * rs.[count_executions])) * 8, 2) [total_query_max_used_memory],
                         ROUND(CONVERT(fload, SQRT(SUM(rs.[stdev_query_max_used_memory] * rs.[stdev_query_max_used_memory] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions), 0))) * 8, 2) [stdev_query_max_used_memory],
                         ISNULL(ROUND(CONVERT(fload, (SQRT(SUM(rs.[stdev_query_max_used_memory] * rs.[stdev_query_max_used_memory] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions]), 0)) * SUM(rs.[count_executions])) / NULLIF(SUM(rs.[avg_query_max_used_memory] * rs.[count_executions]), 0)), 2), 0) [variation_query_max_used_memory],
                         ROUND(CONVERT(float, SUM(rs.[avg_physical_io_reads] * rs.[count_executions])) / NULLIF(SUM(rs.[count_executions]), 0) * 8, 2) [avg_physical_io_reads],
                         ROUND(CONVERT(float, MIN(rs.[min_physical_io_reads])) * 8, 2) [min_physical_io_reads],
                         ROUND(CONVERT(float, MAX(rs.[max_physical_io_reads])) * 8, 2) [max_physical_io_reads],
                         ROUND(CONVERT(float, SUM(rs.[avg_physical_io_reads] * rs.[count_executions])) * 8, 2) [total_physical_io_reads],
                         ROUND(CONVERT(fload, SQRT(SUM(rs.[stdev_physical_io_reads] * rs.[stdev_physical_io_reads] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions), 0))) * 8, 2) [stdev_physical_io_reads],
                         ISNULL(ROUND(CONVERT(fload, (SQRT(SUM(rs.[stdev_physical_io_reads] * rs.[stdev_physical_io_reads] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions]), 0)) * SUM(rs.[count_executions])) / NULLIF(SUM(rs.[avg_physical_io_reads] * rs.[count_executions]), 0)), 2), 0) [variation_physical_io_reads],
                         ROUND(CONVERT(float, SUM(rs.[avg_rowcount] * rs.[count_executions])) / NULLIF(SUM(rs.[count_executions]), 0) * 1, 2) [avg_rowcount],
                         ROUND(CONVERT(float, MIN(rs.[min_rowcount])) * 1, 2) [min_rowcount],
                         ROUND(CONVERT(float, MAX(rs.[max_rowcount])) * 1, 2) [max_rowcount],
                         ROUND(CONVERT(float, SUM(rs.[avg_rowcount] * rs.[count_executions])) * 1, 2) [total_rowcount],
                         ROUND(CONVERT(fload, SQRT(SUM(rs.[stdev_rowcount] * rs.[stdev_rowcount] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions), 0))) * 1, 2) [stdev_rowcount],
                         ISNULL(ROUND(CONVERT(fload, (SQRT(SUM(rs.[stdev_rowcount] * rs.[stdev_rowcount] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions]), 0)) * SUM(rs.[count_executions])) / NULLIF(SUM(rs.[avg_rowcount] * rs.[count_executions]), 0)), 2), 0) [variation_rowcount],
                         ROUND(CONVERT(float, SUM(rs.[avg_tempdb_space_used] * rs.[count_executions])) / NULLIF(SUM(rs.[count_executions]), 0) * 8, 2) [avg_tempdb_space_used],
                         ROUND(CONVERT(float, MIN(rs.[min_tempdb_space_used])) * 8, 2) [min_tempdb_space_used],
                         ROUND(CONVERT(float, MAX(rs.[max_tempdb_space_used])) * 8, 2) [max_tempdb_space_used],
                         ROUND(CONVERT(float, SUM(rs.[avg_tempdb_space_used] * rs.[count_executions])) * 8, 2) [total_tempdb_space_used],
                         ROUND(CONVERT(fload, SQRT(SUM(rs.[stdev_tempdb_space_used] * rs.[stdev_tempdb_space_used] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions), 0))) * 8, 2) [stdev_tempdb_space_used],
                         ISNULL(ROUND(CONVERT(fload, (SQRT(SUM(rs.[stdev_tempdb_space_used] * rs.[stdev_tempdb_space_used] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions]), 0)) * SUM(rs.[count_executions])) / NULLIF(SUM(rs.[avg_tempdb_space_used] * rs.[count_executions]), 0)), 2), 0) [variation_tempdb_space_used],
                         ROUND(CONVERT(float, SUM(rs.[avg_clr_time] * rs.[count_executions])) / NULLIF(SUM(rs.[count_executions]), 0) * 0.001, 2) [avg_clr_time],
                         ROUND(CONVERT(float, MIN(rs.[min_clr_time])) * 0.001, 2) [min_clr_time],
                         ROUND(CONVERT(float, MAX(rs.[max_clr_time])) * 0.001, 2) [max_clr_time],
                         ROUND(CONVERT(float, SUM(rs.[avg_clr_time] * rs.[count_executions])) * 0.001, 2) [total_clr_time],
                         ROUND(CONVERT(fload, SQRT(SUM(rs.[stdev_clr_time] * rs.[stdev_clr_time] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions), 0))) * 0.001, 2) [stdev_clr_time],
                         ISNULL(ROUND(CONVERT(fload, (SQRT(SUM(rs.[stdev_clr_time] * rs.[stdev_clr_time] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions]), 0)) * SUM(rs.[count_executions])) / NULLIF(SUM(rs.[avg_clr_time] * rs.[count_executions]), 0)), 2), 0) [variation_clr_time]
                    FROM sys.query_store_runtime_stats rs
                    JOIN sys.query_store_plan p
                      ON p.[plan_id] = rs.[plan_id]
                   WHERE p.[query_id] = base.[query_id]
                     AND NOT (rs.[first_execution_time] > @interval_end_time OR rs.[last_execution_time] < @interval_start_time)
                   GROUP BY rs.[plan_id], rs.[execution_type_desc], DATEDIFF(HOUR, 0, rs.[last_execution_time])) bucket
       JOIN sys.query_store_plan p
         ON p.query_id = base.[query_id]
        AND p.[plan_id] = bucket.[plan_id]
      ORDER BY bucket.[bucket_start];
END
