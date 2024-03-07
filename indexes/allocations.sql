SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;
SELECT QUOTENAME(SCHEMA_NAME(t.[schema_id])) + '.' + QUOTENAME(t.[name]) [object_name],
       i.[name], i.[index_id], i.[type_desc], p.[partition_number],
       au.[type], au.[type_desc], au.[total_pages], au.[used_pages], au.[data_pages]
  FROM sys.tables t
  JOIN sys.indexes i
    ON i.[object_id] = t.[object_id]
   AND i.[type] NOT IN (5, 6)                    -- Ommit Columnstore indexes
  JOIN sys.partitions p
    ON p.[object_id] = i.[object_id]
   AND p.[index_id] = i.[index_id]
  JOIN sys.allocation_units au
    ON (au.[container_id] = p.[hobt_id] AND au.[type] IN (1, 3))
    OR (au.[container_id] = p.[partition_id] AND au.[type] = 2)
 WHERE OBJECTPROPERTY(t.[object_id], 'IsMsShipped') = 0
 UNION ALL
SELECT QUOTENAME(SCHEMA_NAME(t.[schema_id])) + '.' + QUOTENAME(t.[name]) [object_name],
       i.[name], i.[index_id], i.[type_desc], csrg.[partition_number],
       au.[type], au.[type_desc], au.[total_pages], au.[used_pages], au.[data_pages]
  FROM sys.tables t
  JOIN sys.indexes i
    ON i.[object_id] = t.[object_id]
   AND i.[type] IN (5, 6)                        -- Focus on ONLY Columnstore indexes
  JOIN sys.column_store_row_groups csrg
    ON csrg.[object_id] = i.[object_id]
   AND csrg.[index_id] = i.[index_id]
  JOIN sys.allocation_units au
    ON (au.[container_id] = csrg.[delta_store_hobt_id] AND au.[type] IN (1, 3))
 WHERE OBJECTPROPERTY(t.[object_id], 'IsMsShipped') = 0
 ORDER BY [object_name], i.[index_id], [partition_number], au.[type];
GO
