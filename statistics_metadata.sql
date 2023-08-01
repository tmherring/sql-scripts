SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;
-- If you wish to target a single table, set the name and schema here, if you just set the schema, it will return
-- statistics for all objects that are in that schema. If you want to gather stats data for not only indexes but
-- also for system generated (i.e. auto created stats), set the @include_sys_gen BIT value to '1'. If you want to 
-- capture stats for system objects, set the @include_system_objects BIT value to '1'.
DECLARE @table_name VARCHAR(128) = NULL,
        @schema_name VARCHAR(128) = NULL,
        @include_sys_gen BIT = '0',
        @include_system_objects BIT = '0';


SELECT OBJECT_SCHEMA_NAME(o.[object_id], DB_ID()) [schema_name], o.[name] [object_name], o.[type_desc],
       s.[name] [stat_name], s.[is_incremental], ddisp.[partition_number], ddisp.[rows], ddisp.[modification_counter],
       ROUND(SQRT(COALESCE(ddps.[row_count], ddisp.[rows]) * 1000), 0) [autostat_target_modifications],
       CAST((ddisp.[modification_counter] / (ddisp.[rows] * 1.0)) * 100 AS REAL) [pct_change], ddisp.[last_updated],
       s.[has_filter], s.[has_persisted_sample], CAST((ddisp.[rows_sampled] / (ddisp.[rows] * 1.0) * 100.0) AS REAL) [used_sample_rate],
       ddsp.[persisted_sample_percent], ddps.[used_page_count] [pages], ddps.[row_count] [current_rows]
  FROM sys.objects o
  JOIN sys.stats s
    ON s.[object_id] = o.[object_id]
   AND s.[auto_created] IN ('0', @include_sys_gen)
  LEFT JOIN sys.dm_db_partition_stats ddps
    ON ddps.[object_id] = s.[object_id]
   AND ddps.[index_id] = s.[stats_id]
 CROSS APPLY sys.dm_db_incremental_stats_properties(s.[object_id], s.[stats_id]) ddisp
 OUTER APPLY sys.dm_db_stats_properties(s.[object_id], s.[stats_id]) ddsp
 WHERE o.[schema_id] = COALESCE(SCHEMA_ID(@schema_name), o.[schema_id])
   AND o.[name] = COALESCE(@table_name, o.[name])
   AND ddisp.[rows] IS NOT NULL
   AND o.is_ms_shipped IN ('0', @include_system_objects)
 UNION ALL
SELECT OBJECT_SCHEMA_NAME(o.[object_id], DB_ID()) [schema_name], o.[name] [object_name], o.[type_desc],
       s.[name] [stat_name], s.[is_incremental], NULL [partition_number], ddsp.[rows], ddsp.[modification_counter],
        ROUND(SQRT(COALESCE(ddps.[row_count], sp.[rows]) * 1000), 0) [autostat_target_modifications],
       CAST((ddsp.[modification_counter] / (ddsp.[rows] * 1.0)) * 100 AS REAL) [pct_change], ddsp.[last_updated],
       s.[has_filter], s.[has_persisted_sample], CAST((ddsp.[rows_sampled] / (ddsp.[rows] * 1.0) * 100.0) AS REAL) [used_sample_rate],
       ddsp.[persisted_sample_percent], ddps.[used_page_count] [pages], ddps.[row_count] [current_rows]
  FROM sys.objects o
  JOIN sys.stats s
    ON s.[object_id] = o.[object_id]
   AND s.[auto_created] IN ('0', @include_sys_gen)
  LEFT JOIN sys.dm_db_partition_stats ddps
    ON ddps.[object_id] = s.[object_id]
   AND ddps.[index_id] = s.[stats_id]
 CROSS APPLY sys.dm_db_stats_properties(s.[object_id], s.[stats_id]) ddsp
 WHERE o.[schema_id] = COALESCE(SCHEMA_ID(@schema_name), o.[schema_id])
   AND o.[name] = COALESCE(@table_name, o.[name])
   AND ddsp.[rows] IS NOT NULL
   AND o.is_ms_shipped IN ('0', @include_system_objects)
 ORDER BY [schema_name], [object_name], [stat_name], [partition_number];
