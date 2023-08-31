SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;

DECLARE @schema_name sysname = NULL,
        @table_name sysname = NULL;

CREATE TABLE #columnstore_workflow (
    [schema_id] int,
    [object_id] int,
    [index_id] int,
    [index_name] sysname,
    [partition_number] int,
    [rowgroup_id] int,
    [state] nvarchar(60),
    [deleted_rows] bigint,
    [size_in_bytes] bigint,
    [total_rows] bigint
);

INSERT INTO #columnstore_workflow
SELECT t.[schema_id], t.[object_id], i.[index_id], i.[name], p.[partition_number], csrg.[row_group_id],
       csrg.[state_description], csrg.[deleted_rows], csrg.[size_in_bytes], csrg.[total_rows]
  FROM sys.tables t
  JOIN sys.indexes i 
    ON i.[object_id] = t.[object_id]
   AND i.[type] IN (5, 6)               -- focus on ONLY columnstore index types
  JOIN sys.partitions p 
    ON p.[object_id] = i.[object_id]
   AND p.[index_id] = i.[index_id]
  JOIN sys.column_store_row_groups csrg 
    ON csrg.[object_id] = p.[object_id]
   AND csrg.[index_id] = p.[index_id]
   AND csrg.[partition_number] = p.[partition_number]
 WHERE t.[schema_id] = COALESCE(SCHEMA_ID(@schema_name), t.[schema_id])
   AND t.[name] = COALESCE(@table_name, t.[name])
 ORDER BY p.[partition_number], csrg.[row_group_id];

SELECT QUOTENAME(SCHEMA_NAME([schema_id])) + '.' + QUOTENAME(OBJECT_NAME([object_id])) [table_name],
       [index_name], [partition_number], [rowgroup_id], [state], [deleted_rows], [size_in_bytes], [total_rows],
       CAST(((([total_rows] - [deleted_rows]) * 1.0) / [total_rows]) * 100.0 AS real) [percent_full]
  FROM #columnstore_workflow
 ORDER BY [table_name], [partition_number], [rowgroup_id];

-- Pull the aggregate rows, deleted rows, and calculate the percent full for columnstore index data
SELECT QUOTENAME(SCHEMA_NAME([schema_id])) + '.' + QUOTENAME(OBJECT_NAME([object_id])) [table_name],
       [index_name], SUM([total_rows]) [ci_total_rows], SUM([deleted_rows]) [ci_deleted_rows],
       CAST((((SUM([total_rows]) - SUM([deleted_rows])) * 1.0) / SUM([total_rows])) * 100.0 AS real) [ci_percent_full]
  FROM #columnstore_workflow
 GROUP BY [schema_id], [object_id], [index_name]
 ORDER BY [table_name], [index_name];

-- pull the number of rowgroups and the number of rowgroups that are not full
SELECT QUOTENAME(SCHEMA_NAME(base.[schema_id])) + '.' + QUOTENAME(OBJECT_NAME(base.[object_id])) [table_name],
       base.[index_name], base.[partition_number], COUNT(base.[total_rows]) [fragments], total.[total_rowgroups]
  FROM #columnstore_workflow base
  JOIN (SELECT [schema_id], [object_id], [index_id], [partition_number], COUNT([total_rows]) [total_rowgroups]
          FROM #columnstore_workflow
         WHERE [state] <> 'OPEN'
         GROUP BY [schema_id], [object_id], [index_id], [partition_number]) total
    ON total.[schema_id] = base.[schema_id]
   AND total.[object_id] = base.[object_id]
   AND total.[index_id] = base.[index_id]
   AND total.[partition_number] = base.[partition_number]
 WHERE base.[total_rows] < 1048576
   AND base.[state] <> 'OPEN'
 GROUP BY base.[schema_id], base.[object_id], base.[index_name], base.[partition_number], total.[total_rowgroups]
 ORDER BY [table_name], base.[index_name], base.[partition_number];