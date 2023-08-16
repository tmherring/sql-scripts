-- Grab the query store duration query that is desired, along with the variables at the top of this file, and set them prior to
-- executing the query...
-- Set your window, this is the end time you want to look back into, should be a integer value less than zero...
DECLARE @window INT = 0;
-- Set the top of the hour, either current (when @window is set to zero) or whichever hour is chosen
DECLARE @top_of_current DATETIMEOFFSET(7) = CAST(DATEPART(YEAR, DATEADD(HOUR, @window, SYSDATETIMEOFFSET())) AS CHAR(4)) + '-' +
                                            RIGHT('0' + CONVERT(VARCHAR, DATEPART(MONTH, DATEADD(HOUR, @window, SYSDATETIMEOFFSET()))), 2) + '-' +
                                            RIGHT('0' + CONVERT(VARCHAR, DATEPART(DAY, DATEADD(HOUR, @window, SYSDATETIMEOFFSET()))), 2) + ' ' +
                                            RIGHT('0' + CONVERT(VARCHAR, DATEPART(HOUR, DATEADD(HOUR, @window, SYSDATETIMEOFFSET()))), 2) + ':00:00 ' +
                                            CAST(DATEPART(TZOFFSET, DATEADD(HOUR, @window, SYSDATETIMEOFFSET())) / 60 AS VARCHAR) + ':00',
-- how many hours back do we want to go, be a bit cautious here, this the busier your database the more data this query could have to sort through
-- should be an integer value equal or less than -1 (negative one).
        @hours_back INT = -1;

-- Query for pulling the top 25 queries from the query store, by total duration...
IF (SELECT actual_state FROM sys.database_query_store_options) = 2
BEGIN
    DECLARE @interval_start_time DATETIMEOFFSET(7) = DATEADD(HOUR, @hours_back, @top_of_current), @interval_end_time DATETIMEOFFSET(7) = @top_of_current;
    SELECT base.[query_id], bucket.*, base.[query_sql_text], TRY_CAST(p.[query_plan] AS XML) [query_plan], base.[object_name]
      FROM (SELECT TOP (25) p.[query_id], q.[object_id], COALESCE(OBJECT_NAME(q.[object_id]), '') [object_name], qt.[query_sql_text],
                   ROUND(CONVERT(float, SUM(rs.[avg_duration] * rs.[count_executions])) * 0.001, 2) [total_duration],
                   SUM(rs.[count_executions]) [count_executions], COUNT(DISTINCT p.[plan_id]) [number_of_plans]
              FROM sys.query_store_runtime_stats rs
              JOIN sys.query_store_plan p
                ON p.[plan_id] = rs.[plan_id]
              JOIN sys.query_store_query q
                ON q.[query_id] = p.[query_id]
              JOIN sys.query_store_query_text qt
                ON qt.[query_text_id] = q.[query_text_id]
             WHERE NOT (rs.[first_execution_time] > @interval_end_time OR rs.[last_execution_time] < @interval_start_time)
             GROUP BY p.[query_id], qt.[query_sql_text], q.[object_id]
            HAVING COUNT(DISTINCT p.[plan_id]) >= 1
             ORDER BY [total_duration] DESC) base
     CROSS APPLY (SELECT rs.[plan_id], rs.[execution_type_desc] [execution_type], SUM(rs.[count_executions]) [count_executions],
                         CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, DATEADD(HOUR, ((DATEDIFF(HOUR, 0, rs.[last_execution_time]))), 0)), DATENAME(tzoffset, SYSDATETIMEOFFSET()))) [bucket_start],
                         CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, DATEADD(HOUR, (1 + (DATEDIFF(HOUR, 0, rs.[last_execution_time]))), 0)), DATENAME(tzoffset, SYSDATETIMEOFFSET()))) [bucket_end],
                         ROUND(CONVERT(float, SUM(rs.[avg_duration] * rs.[count_executions])) / NULLIF(SUM(rs.[count_executions]), 0) * 0.001, 2) [avg_duration],
                         ROUND(CONVERT(float, MAX(rs.[max_duration])) * 0.001, 2) [max_duration],
                         ROUND(CONVERT(float, MIN(rs.[min_duration])) * 0.001, 2) [min_duration],
                         ROUND(CONVERT(float, SQRT(SUM(rs.[stdev_duration] * rs.[stdev_duration] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions]), 0))) * 0.001, 2) [stdev_duration],
                         COALESCE(ROUND(CONVERT(float, (SQRT(SUM(rs.[stdev_duration] * rs.[stdev_duration] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions]), 0)) * SUM(rs.[count_executions])) / NULLIF(SUM(rs.[avg_duration] * rs.[count_executions]), 0)), 2), 0) [variation_duration],
                         ROUND(CONVERT(float, SUM(rs.[avg_duration] * rs.[count_executions])) * 0.001, 2) [total_duration]
                    FROM sys.query_store_plan p
                    JOIN sys.query_store_runtime_stats rs
                      ON rs.[plan_id] = p.[plan_id]
                     AND NOT (rs.[first_execution_time] > @interval_end_time OR rs.[last_execution_time] < @interval_start_time)
                   WHERE p.[query_id] = base.[query_id]
                   GROUP BY rs.[plan_id], rs.[execution_type_desc], DATEDIFF(HOUR, 0, rs.[last_execution_time])) [bucket]
      JOIN sys.query_store_plan p
        ON p.[query_id] = base.[query_id]
       AND p.[plan_id] = bucket.[plan_id]
     ORDER BY base.[total_duration] DESC;
END

-- Query for pulling the top 25 queries from the query store, by average duration...
IF (SELECT actual_state FROM sys.database_query_store_options) = 2
BEGIN
    DECLARE @interval_start_time DATETIMEOFFSET(7) = DATEADD(HOUR, @hours_back, @top_of_current), @interval_end_time DATETIMEOFFSET(7) = @top_of_current;
    SELECT base.[query_id], bucket.*, base.[query_sql_text], TRY_CAST(p.[query_plan] AS XML) [query_plan], base.[object_name]
      FROM (SELECT TOP (25) p.[query_id], q.[object_id], COALESCE(OBJECT_NAME(q.[object_id]), '') [object_name], qt.[query_sql_text],
                   ROUND(CONVERT(float, SUM(rs.[avg_duration] * rs.[count_executions])) / NULLIF(SUM(rs.[count_executions]), 0) * 0.001, 2) [avg_duration],
                   SUM(rs.[count_executions]) [count_executions], COUNT(DISTINCT p.[plan_id]) [number_of_plans]
              FROM sys.query_store_runtime_stats rs
              JOIN sys.query_store_plan p
                ON p.[plan_id] = rs.[plan_id]
              JOIN sys.query_store_query q
                ON q.[query_id] = p.[query_id]
              JOIN sys.query_store_query_text qt
                ON qt.[query_text_id] = q.[query_text_id]
             WHERE NOT (rs.[first_execution_time] > @interval_end_time OR rs.[last_execution_time] < @interval_start_time)
             GROUP BY p.[query_id], qt.[query_sql_text], q.[object_id]
            HAVING COUNT(DISTINCT p.[plan_id]) >= 1
             ORDER BY [avg_duration] DESC) base
     CROSS APPLY (SELECT rs.[plan_id], rs.[execution_type_desc] [execution_type], SUM(rs.[count_executions]) [count_executions],
                         CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, DATEADD(HOUR, ((DATEDIFF(HOUR, 0, rs.[last_execution_time]))), 0)), DATENAME(tzoffset, SYSDATETIMEOFFSET()))) [bucket_start],
                         CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, DATEADD(HOUR, (1 + (DATEDIFF(HOUR, 0, rs.[last_execution_time]))), 0)), DATENAME(tzoffset, SYSDATETIMEOFFSET()))) [bucket_end],
                         ROUND(CONVERT(float, SUM(rs.[avg_duration] * rs.[count_executions])) / NULLIF(SUM(rs.[count_executions]), 0) * 0.001, 2) [avg_duration],
                         ROUND(CONVERT(float, MAX(rs.[max_duration])) * 0.001, 2) [max_duration],
                         ROUND(CONVERT(float, MIN(rs.[min_duration])) * 0.001, 2) [min_duration],
                         ROUND(CONVERT(float, SQRT(SUM(rs.[stdev_duration] * rs.[stdev_duration] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions]), 0))) * 0.001, 2) [stdev_duration],
                         COALESCE(ROUND(CONVERT(float, (SQRT(SUM(rs.[stdev_duration] * rs.[stdev_duration] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions]), 0)) * SUM(rs.[count_executions])) / NULLIF(SUM(rs.[avg_duration] * rs.[count_executions]), 0)), 2), 0) [variation_duration],
                         ROUND(CONVERT(float, SUM(rs.[avg_duration] * rs.[count_executions])) * 0.001, 2) [total_duration]
                    FROM sys.query_store_plan p
                    JOIN sys.query_store_runtime_stats rs
                      ON rs.[plan_id] = p.[plan_id]
                     AND NOT (rs.[first_execution_time] > @interval_end_time OR rs.[last_execution_time] < @interval_start_time)
                   WHERE p.[query_id] = base.[query_id]
                   GROUP BY rs.[plan_id], rs.[execution_type_desc], DATEDIFF(HOUR, 0, rs.[last_execution_time])) [bucket]
      JOIN sys.query_store_plan p
        ON p.[query_id] = base.[query_id]
       AND p.[plan_id] = bucket.[plan_id]
     ORDER BY base.[avg_duration] DESC;
END

-- Query for pulling the top 25 queries from the query store, by minimum duration...
IF (SELECT actual_state FROM sys.database_query_store_options) = 2
BEGIN
    DECLARE @interval_start_time DATETIMEOFFSET(7) = DATEADD(HOUR, @hours_back, @top_of_current), @interval_end_time DATETIMEOFFSET(7) = @top_of_current;
    SELECT base.[query_id], bucket.*, base.[query_sql_text], TRY_CAST(p.[query_plan] AS XML) [query_plan], base.[object_name]
      FROM (SELECT TOP (25) p.[query_id], q.[object_id], COALESCE(OBJECT_NAME(q.[object_id]), '') [object_name], qt.[query_sql_text],
                   ROUND(CONVERT(float, MIN(rs.[min_duration])) * 0.001, 2) [min_duration],
                   SUM(rs.[count_executions]) [count_executions], COUNT(DISTINCT p.[plan_id]) [number_of_plans]
              FROM sys.query_store_runtime_stats rs
              JOIN sys.query_store_plan p
                ON p.[plan_id] = rs.[plan_id]
              JOIN sys.query_store_query q
                ON q.[query_id] = p.[query_id]
              JOIN sys.query_store_query_text qt
                ON qt.[query_text_id] = q.[query_text_id]
             WHERE NOT (rs.[first_execution_time] > @interval_end_time OR rs.[last_execution_time] < @interval_start_time)
             GROUP BY p.[query_id], qt.[query_sql_text], q.[object_id]
            HAVING COUNT(DISTINCT p.[plan_id]) >= 1
             ORDER BY [min_duration] DESC) base
     CROSS APPLY (SELECT rs.[plan_id], rs.[execution_type_desc] [execution_type], SUM(rs.[count_executions]) [count_executions],
                         CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, DATEADD(HOUR, ((DATEDIFF(HOUR, 0, rs.[last_execution_time]))), 0)), DATENAME(tzoffset, SYSDATETIMEOFFSET()))) [bucket_start],
                         CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, DATEADD(HOUR, (1 + (DATEDIFF(HOUR, 0, rs.[last_execution_time]))), 0)), DATENAME(tzoffset, SYSDATETIMEOFFSET()))) [bucket_end],
                         ROUND(CONVERT(float, SUM(rs.[avg_duration] * rs.[count_executions])) / NULLIF(SUM(rs.[count_executions]), 0) * 0.001, 2) [avg_duration],
                         ROUND(CONVERT(float, MAX(rs.[max_duration])) * 0.001, 2) [max_duration],
                         ROUND(CONVERT(float, MIN(rs.[min_duration])) * 0.001, 2) [min_duration],
                         ROUND(CONVERT(float, SQRT(SUM(rs.[stdev_duration] * rs.[stdev_duration] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions]), 0))) * 0.001, 2) [stdev_duration],
                         COALESCE(ROUND(CONVERT(float, (SQRT(SUM(rs.[stdev_duration] * rs.[stdev_duration] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions]), 0)) * SUM(rs.[count_executions])) / NULLIF(SUM(rs.[avg_duration] * rs.[count_executions]), 0)), 2), 0) [variation_duration],
                         ROUND(CONVERT(float, SUM(rs.[avg_duration] * rs.[count_executions])) * 0.001, 2) [total_duration]
                    FROM sys.query_store_plan p
                    JOIN sys.query_store_runtime_stats rs
                      ON rs.[plan_id] = p.[plan_id]
                     AND NOT (rs.[first_execution_time] > @interval_end_time OR rs.[last_execution_time] < @interval_start_time)
                   WHERE p.[query_id] = base.[query_id]
                   GROUP BY rs.[plan_id], rs.[execution_type_desc], DATEDIFF(HOUR, 0, rs.[last_execution_time])) [bucket]
      JOIN sys.query_store_plan p
        ON p.[query_id] = base.[query_id]
       AND p.[plan_id] = bucket.[plan_id]
     ORDER BY base.[min_duration] DESC;
END

-- Query for pulling the top 25 queries from the query store, by maximum duration...
IF (SELECT actual_state FROM sys.database_query_store_options) = 2
BEGIN
    DECLARE @interval_start_time DATETIMEOFFSET(7) = DATEADD(HOUR, @hours_back, @top_of_current), @interval_end_time DATETIMEOFFSET(7) = @top_of_current;
    SELECT base.[query_id], bucket.*, base.[query_sql_text], TRY_CAST(p.[query_plan] AS XML) [query_plan], base.[object_name]
      FROM (SELECT TOP (25) p.[query_id], q.[object_id], COALESCE(OBJECT_NAME(q.[object_id]), '') [object_name], qt.[query_sql_text],
                   ROUND(CONVERT(float, MAX(rs.[max_duration])) * 0.001, 2) [max_duration],
                   SUM(rs.[count_executions]) [count_executions], COUNT(DISTINCT p.[plan_id]) [number_of_plans]
              FROM sys.query_store_runtime_stats rs
              JOIN sys.query_store_plan p
                ON p.[plan_id] = rs.[plan_id]
              JOIN sys.query_store_query q
                ON q.[query_id] = p.[query_id]
              JOIN sys.query_store_query_text qt
                ON qt.[query_text_id] = q.[query_text_id]
             WHERE NOT (rs.[first_execution_time] > @interval_end_time OR rs.[last_execution_time] < @interval_start_time)
             GROUP BY p.[query_id], qt.[query_sql_text], q.[object_id]
            HAVING COUNT(DISTINCT p.[plan_id]) >= 1
             ORDER BY [max_duration] DESC) base
     CROSS APPLY (SELECT rs.[plan_id], rs.[execution_type_desc] [execution_type], SUM(rs.[count_executions]) [count_executions],
                         CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, DATEADD(HOUR, ((DATEDIFF(HOUR, 0, rs.[last_execution_time]))), 0)), DATENAME(tzoffset, SYSDATETIMEOFFSET()))) [bucket_start],
                         CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, DATEADD(HOUR, (1 + (DATEDIFF(HOUR, 0, rs.[last_execution_time]))), 0)), DATENAME(tzoffset, SYSDATETIMEOFFSET()))) [bucket_end],
                         ROUND(CONVERT(float, SUM(rs.[avg_duration] * rs.[count_executions])) / NULLIF(SUM(rs.[count_executions]), 0) * 0.001, 2) [avg_duration],
                         ROUND(CONVERT(float, MAX(rs.[max_duration])) * 0.001, 2) [max_duration],
                         ROUND(CONVERT(float, MIN(rs.[min_duration])) * 0.001, 2) [min_duration],
                         ROUND(CONVERT(float, SQRT(SUM(rs.[stdev_duration] * rs.[stdev_duration] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions]), 0))) * 0.001, 2) [stdev_duration],
                         COALESCE(ROUND(CONVERT(float, (SQRT(SUM(rs.[stdev_duration] * rs.[stdev_duration] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions]), 0)) * SUM(rs.[count_executions])) / NULLIF(SUM(rs.[avg_duration] * rs.[count_executions]), 0)), 2), 0) [variation_duration],
                         ROUND(CONVERT(float, SUM(rs.[avg_duration] * rs.[count_executions])) * 0.001, 2) [total_duration]
                    FROM sys.query_store_plan p
                    JOIN sys.query_store_runtime_stats rs
                      ON rs.[plan_id] = p.[plan_id]
                     AND NOT (rs.[first_execution_time] > @interval_end_time OR rs.[last_execution_time] < @interval_start_time)
                   WHERE p.[query_id] = base.[query_id]
                   GROUP BY rs.[plan_id], rs.[execution_type_desc], DATEDIFF(HOUR, 0, rs.[last_execution_time])) [bucket]
      JOIN sys.query_store_plan p
        ON p.[query_id] = base.[query_id]
       AND p.[plan_id] = bucket.[plan_id]
     ORDER BY base.[max_duration] DESC;
END

-- Query for pulling the top 25 queries from the query store, by standard deviation of duration...
IF (SELECT actual_state FROM sys.database_query_store_options) = 2
BEGIN
    DECLARE @interval_start_time DATETIMEOFFSET(7) = DATEADD(HOUR, @hours_back, @top_of_current), @interval_end_time DATETIMEOFFSET(7) = @top_of_current;
    SELECT base.[query_id], bucket.*, base.[query_sql_text], TRY_CAST(p.[query_plan] AS XML) [query_plan], base.[object_name]
      FROM (SELECT TOP (25) p.[query_id], q.[object_id], COALESCE(OBJECT_NAME(q.[object_id]), '') [object_name], qt.[query_sql_text],
                   ROUND(CONVERT(float, SQRT(SUM(rs.[stdev_duration] * rs.[stdev_duration] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions]), 0))) * 0.001, 2) [stdev_duration],
                   SUM(rs.[count_executions]) [count_executions], COUNT(DISTINCT p.[plan_id]) [number_of_plans]
              FROM sys.query_store_runtime_stats rs
              JOIN sys.query_store_plan p
                ON p.[plan_id] = rs.[plan_id]
              JOIN sys.query_store_query q
                ON q.[query_id] = p.[query_id]
              JOIN sys.query_store_query_text qt
                ON qt.[query_text_id] = q.[query_text_id]
             WHERE NOT (rs.[first_execution_time] > @interval_end_time OR rs.[last_execution_time] < @interval_start_time)
             GROUP BY p.[query_id], qt.[query_sql_text], q.[object_id]
            HAVING COUNT(DISTINCT p.[plan_id]) >= 1
             ORDER BY [stdev_duration] DESC) base
     CROSS APPLY (SELECT rs.[plan_id], rs.[execution_type_desc] [execution_type], SUM(rs.[count_executions]) [count_executions],
                         CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, DATEADD(HOUR, ((DATEDIFF(HOUR, 0, rs.[last_execution_time]))), 0)), DATENAME(tzoffset, SYSDATETIMEOFFSET()))) [bucket_start],
                         CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, DATEADD(HOUR, (1 + (DATEDIFF(HOUR, 0, rs.[last_execution_time]))), 0)), DATENAME(tzoffset, SYSDATETIMEOFFSET()))) [bucket_end],
                         ROUND(CONVERT(float, SUM(rs.[avg_duration] * rs.[count_executions])) / NULLIF(SUM(rs.[count_executions]), 0) * 0.001, 2) [avg_duration],
                         ROUND(CONVERT(float, MAX(rs.[max_duration])) * 0.001, 2) [max_duration],
                         ROUND(CONVERT(float, MIN(rs.[min_duration])) * 0.001, 2) [min_duration],
                         ROUND(CONVERT(float, SQRT(SUM(rs.[stdev_duration] * rs.[stdev_duration] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions]), 0))) * 0.001, 2) [stdev_duration],
                         COALESCE(ROUND(CONVERT(float, (SQRT(SUM(rs.[stdev_duration] * rs.[stdev_duration] * rs.[count_executions]) / NULLIF(SUM(rs.[count_executions]), 0)) * SUM(rs.[count_executions])) / NULLIF(SUM(rs.[avg_duration] * rs.[count_executions]), 0)), 2), 0) [variation_duration],
                         ROUND(CONVERT(float, SUM(rs.[avg_duration] * rs.[count_executions])) * 0.001, 2) [total_duration]
                    FROM sys.query_store_plan p
                    JOIN sys.query_store_runtime_stats rs
                      ON rs.[plan_id] = p.[plan_id]
                     AND NOT (rs.[first_execution_time] > @interval_end_time OR rs.[last_execution_time] < @interval_start_time)
                   WHERE p.[query_id] = base.[query_id]
                   GROUP BY rs.[plan_id], rs.[execution_type_desc], DATEDIFF(HOUR, 0, rs.[last_execution_time])) [bucket]
      JOIN sys.query_store_plan p
        ON p.[query_id] = base.[query_id]
       AND p.[plan_id] = bucket.[plan_id]
     ORDER BY base.[stdev_duration] DESC;
END
