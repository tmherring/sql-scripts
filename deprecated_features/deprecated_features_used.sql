-- Quick and dirty method for identifying what deprecated features are beind used on an instnace
USE [master]
GO
SELECT [object_name], [instance_name] [deprecated_feature], [cntr_value] [use_count]
  FROM sys.dm_os_performance_counters
 WHERE [object_name] LIKE '%Deprecated Features%'
 ORDER BY [cntr_value] DESC;
GO
