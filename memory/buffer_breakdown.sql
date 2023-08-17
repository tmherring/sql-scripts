SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;
SELECT TOP 20 LEFT([name], 128) [name], [type],
       CAST((([pages_kb] * 0.0078125) / (SELECT CAST([value_in_use] AS REAL) FROM sys.configurations WHERE [name] = 'max server memory (MB)')) * 100 AS REAL) [%_server_memory],
       CAST([pages_kb] * 0.0078125 AS REAL) [cache_size_mb], [entries_count]
  FROM sys.dm_os_memory_cache_counters
 UNION ALL
SELECT COALESCE(DB_NAME([database_id]), 'MS_Resource') [name], 'DATABASE_BUFFER' [type],
       CAST(((COUNT(*) * 0.0078125) / (SELECT CAST([value_in_use] AS REAL) FROM sys.configurations WHERE [name] = 'max server memory (MB)')) * 100.0 AS REAL) [%_server_memory],
       CAST(COUNT(*) * 0.0078125 AS REAL) [cache_size_mb], NULL [entries_count]
  FROM sys.dm_os_buffer_descriptors
 --WHERE database_id > 4      -- exclude system databases
 --  AND database_id <> 32767 -- exclude the ResourceDB
 GROUP BY DB_NAME(database_id)
 ORDER BY [cache_size_mb] DESC;
