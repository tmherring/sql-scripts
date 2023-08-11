SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;
-- Need to be cautious with executing this script on servers that exceed 256GB as this has to
-- aggregate the pages, the more pages available the longer this will take
SELECT CASE
         WHEN total.[database_id] = 32767 THEN 'MSResource'
         ELSE DB_NAME(total.[database_id])
       END [database_name], clean.[memory_mb] [clean_memory_mb], dirty.[memory_mb] [dirty_memory_mb],
       total.[memory_mb] [total_memory_mb]
  FROM (SELECT [database_id], CAST(COUNT(*) * 0.0078125 AS real) [memory_mb]
          FROM sys.dm_os_buffer_descriptors
         GROUP BY [database_id]) [total]
  LEFT JOIN (SELECT [database_id], CAST(COUNT(*) * 0.0078125 AS real) [memory_mb]
               FROM sys.dm_os_buffer_descriptors
              WHERE [is_modified] = 0
              GROUP BY [database_id]) [clean]
    ON clean.[database_id] = total.[database_id]
  LEFT JOIN (SELECT [database_id], CAST(COUNT(*) * 0.0078125 AS real) [memory_mb]
               FROM sys.dm_os_buffer_descriptors
              WHERE [is_modified] = 1
              GROUP BY [database_id]) [dirty]
    ON dirty.[database_id] = total.[database_id];
