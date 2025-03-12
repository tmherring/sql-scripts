DECLARE @window int = 0,
        @hours_back int = -1;
DECLARE @top_of_window datetimeoffset(7) = CAST(DATEPART(YEAR, DATEADD(HOUR, @window, SYSDATETIMEOFFSET())) AS char(4)) + '-' +
                                           RIGHT('0' + CONVERT(varchar, DATEPART(MONTH, DATEADD(HOUR, @window, SYSDATETIMEOFFSET()))), 2) + '-' +
                                           RIGHT('0' + CONVERT(varchar, DATEPART(DAY, DATEADD(HOUR, @window, SYSDATETIMEOFFSET()))), 2) + ' ' +
                                           RIGHT('0' + CONVERT(varchar, DATEPART(HOUR, DATEADD(HOUR, @window, SYSDATETIMEOFFSET()))), 2) + ':00:00 ' +
                                           CAST(DATEPART(tzoffset, DATEADD(HOUR, @window, SYSDATETIMEOFFSET())) / 60 AS varchar) + ':00';

IF (SELECT actual_state FROM sys.database_query_store_options) = 2
BEGIN
    DECLARE @interval_start_time datetimeoffset(7), @interval_end_time datetimeoffset(7);
    SELECT base.[query_id], bucket.*, base.[query_sql_text], TRY_CAST(p.[query_plan] AS xml) [query_plan], base.[object_name]
      FROM (SELECT TOP (25) p.[query_id], q.[object_id], COALESCE(OBJECT_NAME(q.[object_id]), '') [object_name],
                   qt.[query_sql_text], ROUND(CONVERT(float, SUM(rs.[avg_duration]*rs.[count_executions]))*0.001,2) [total_duration],
                   SUM(rs.[count_executions]) [count_executions], COUNT(DISTINCT p.[plan_id]) [num_plans]
              FROM sys.query_store_runtime_stats rs
              JOIN sys.query_store_plan p
                ON p.[plan_id] = rs.[plan_id]
              JOIN sys.query_store_query q
                ON q.[query_id] = p.[query_id]
              JOIN sys.query_store_query_text qt
                ON q.[query_text_id] = qt.[query_text_id]
             WHERE NOT (rs.[first_execution_time] > @interval_end_time OR rs.[last_execution_time] < @interval_start_time)
               AND rs.[execution_type] = 3
             GROUP BY p.[query_id], qt.[query_sql_text], q.[object_id]
            HAVING COUNT(DISTINCT p.[plan_id]) >= 1
             ORDER BY [total_duration] DESC) base
     CROSS APPLY (SELECT rs.[plan_id], rs.[execution_type_desc] [execution_type], SUM(rs.[count_executions]) [count_executions],
                         CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, DATEADD(HOUR, ((DATEDIFF(HOUR, 0, rs.[last_execution_time]))), 0)), DATENAME(tzoffset, SYSDATETIMEOFFSET()))) [bucket_start],
                         CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, DATEADD(HOUR, (1 + (DATEDIFF(HOUR, 0, rs.[last_execution_time]))), 0)), DATENAME(tzoffset, SYSDATETIMEOFFSET()))) [bucket_end],
                         ROUND(CONVERT(float, SUM(rs.[avg_duration]*rs.[count_executions]))/NULLIF(SUM(rs.[count_executions]), 0)*0.001,2) [avg_duration],
                         ROUND(CONVERT(float, MAX(rs.[max_duration]))*0.001, 2) [max_duration],
                         ROUND(CONVERT(float, MIN(rs.[min_duration]))*0.001, 2) [min_duration],
                         ROUND(CONVERT(float, SQRT(SUM(rs.[stdev_duration]*rs.[stdev_duration]*rs.[count_executions])/NULLIF(SUM(rs.[count_executions],0)))*0.001,2) [stdev_duration],
                         COALESCE(ROUND(CONVERT(float, (SQRT(SUM(rs.[stdev_duration]*rs.[stdev_duration]*rs.[count_executions])/NULLIF(SUM(rs.[count_executions],0))*SUM(rs.[count_executions]))/NULLIF(SUM(rs.[avg_duration]*rs.[count_executions]),0)),2),0) [variation_duration],
                         ROUND(CONVERT(float, SUM(rs.[avg_duration]*rs.[count_executions]))*0.001,2) [total_duration]
                    FROM sys.query_store_runtime_stats rs
                    JOIN sys.query_store_plan p
                      ON p.[plan_id] = rs.[plan_id]
                   WHERE p.[query_id] = base.[query_id]
                     AND NOT (rs.[first_execution_time] > @interval_end_time OR rs.[last_execution_time] < @interval_start_time)
                     AND rs.[execution_type] = 3
                   GROUP BY rs.[plan_id], rs.[execution_type_desc], DATEDIFF(HOUR, 0, rs.[last_execution_time])) bucket
      JOIN sys.query_store_plan p
        ON p.[query_id] = base.[query_id]
       AND p.[plan_id] = bucket.[plan_id]
     ORDER BY base.[total_duration] DESC;
END
