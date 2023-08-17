SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;
SELECT nm.counter_value_mb [buffer_size_mb], ple.[counter_value] [page_life],
       CASE
         WHEN (nm.[counter_value_mb] / 1024 / 4) * 300 = 0 THEN 300
         ELSE (nm.[counter_value_mb] / 1024 / 4) * 300
       END [baseline_ple],
       (nm.[counter_value_mb] * 1.0) / (ple.[counter_value] * 1.0) [mb_per_s],
       nm.[instance_name] [numa_node], SYSDATETIMEOFFSET() [snapshot]
  FROM (SELECT [cntr_value] / 1024 [counter_value_mb], [instance_name]
          FROM sys.dm_os_performance_counters
         WHERE [object_name] LIKE '%Memory Node%'
           AND LOWER([counter_name]) = 'database node memory (kb)') [nm]
  JOIN (SELECT [cntr_value] [counter_value], [instance_name]
          FROM sys.dm_os_performance_counters
         WHERE [object_name] LIKE '%Buffer Node%'
           AND LOWER([counter_name]) = 'page life expectancy') [ple]
    ON ple.[instance_name] = nm.[instance_name];
