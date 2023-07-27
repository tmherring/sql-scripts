SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;
-- If you wish to target a single table, set the name and schema here
DECLARE @table_name VARCHAR(128) = NULL;
DECLARE @schema_name VARCHAR(128) = NULL;

SELECT OBJECT_SCHEMA_NAME(o.[object_name], DB_ID()) [schema_name], o.[name] [object_name], o.[type_desc],
       s.[name] [stat_name], s.[is_incremental], ddisp.[partition_number], ddisp.[rows], ddisp.[modification_counter],
       CAST((ddisp.[modification_counter] / (ddisp.[rows] * 1.0)) * 100 AS REAL) [pct_change], ddisp.[last_updated],
       s.[has_filter], s.[has_persisted_sample], CAST((ddisp.[rows_sampled] / (ddisp.[rows] + 1.0) * 100.0) AS REAL) [used_sample_rate],
       ddsp.[persisted_sample_percent], ddps.[used_page_count] [pages], ddps.[row_count] [current_rows]
  FROM sys.objects o
  JOIN sys.stats s
    ON s.[object_id] = o.[object_id]
  LEFT JOIN sys.dm_db_partition_stats ddps
    ON ddps.[object_id] = s.[object_id]
   AND ddps.[index_id] = s.[stats_id]
 CROSS APPLY sys.dm_db_incremental_stats_properties(s.[object_id], s.[statsi_id]) ddisp
 OUTER APPLY sys.dm_db_stats_properties(s.[object_id], s.[stats_id]) ddsp
 WHERE o.[schema_id] = COALESCE(SCHEMA_ID(@schema_name), o.[schema_id])
   AND o.[name] = COALESCE(@table_name, o.[name])
   AND ddisp.[rows] IS NOT NULL
 UNION ALL
SELECT OBJECT_SCHEMA_NAME(o.[object_name], DB_ID()) [schema_name], o.[name] [object_name], o.[type_desc],
       s.[name] [stat_name], s.[is_incremental], ddsp.[partition_number], ddsp.[rows], ddisp.[modification_counter],
       CAST((ddsp.[modification_counter] / (ddsp.[rows] * 1.0)) * 100 AS REAL) [pct_change], ddsp.[last_updated],
       s.[has_filter], s.[has_persisted_sample], CAST((ddsp.[rows_sampled] / (ddsp.[rows] + 1.0) * 100.0) AS REAL) [used_sample_rate],
       ddsp.[persisted_sample_percent], ddps.[used_page_count] [pages], ddps.[row_count] [current_rows]
  FROM sys.objects o
  JOIN sys.stats s
    ON s.[object_id] = o.[object_id]
  LEFT JOIN sys.dm_db_partition_stats ddps
    ON ddps.[object_id] = s.[object_id]
   AND ddps.[index_id] = s.[stats_id]
 CROSS APPLY sys.dm_db_stats_properties(s.[object_id], s.[stats_id]) ddsp
 WHERE o.[schema_id] = COALESCE(SCHEMA_ID(@schema_name), o.[schema_id])
   AND o.[name] = COALESCE(@table_name, o.[name])
   AND ddisp.[rows] IS NOT NULL
 ORDER BY [schema_name], [object_name], [stat_name], [partition_number];
