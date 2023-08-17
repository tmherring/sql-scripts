SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;

-- Pull the space used information for a given database, or, depending on the inputs a specific database object
-- @object - the qualified (i.e. three-part name) or non-qualified name of a table, indexed view, or queue for which
--           space usage information is requested. Bracketing the object in quotes is only required if a qualified 
--           object name is specified. If a qualifed name is provided, the database name must be the name of the
--           current database.
--
--           If object is not specified, results for the entire database will be returned.
-- @update_usage - DEPRECATED - indicates DBCC UPDATEUSAGE should be run to update space usage information.
-- @summary - indicates that a summary for the current database should be generated.
-- @system_objects - indicates that data for system objects should be included in the results
DECLARE @object nvarchar(386) = NULL,
        --@update_usage char(5) = 'false',
        @summary char(5) = 'true',
        @system_objects char(5) = 'false',
        @schema_data char(5) = 'true';

DECLARE @dbname sysname, @type char(2), @id int, @ms_shipped bit,
        @message nvarchar(2048) = NULL;

--IF @update_usage IS NOT NULL
--BEGIN
--    IF LOWER(TRIM(@update_usage)) NOT IN ('true', 'false')
--    BEGIN
--        SET @message = N'''' + LOWER(TRIM(@update_usage)) + N''' is not a valid option for the @update_usage parameter. Enter either ''true'' or ''false''.';
--        THROW 51000, @message, 1;
--    END
--END

IF @summary IS NOT NULL
BEGIN
    IF LOWER(TRIM(@summary)) NOT IN ('true', 'false')
    BEGIN
        SET @message =  N'''' + LOWER(TRIM(@summary)) + N''' is not a valid option for the @summery parameter. Enter either ''true'' or ''false''.';
        THROW 51000, @message, 1;
    END
END

IF @system_objects IS NOT NULL AND LOWER(TRIM(@system_objects)) NOT IN ('true', 'false')
BEGIN
    SET @message = N'''' + LOWER(TRIM(@system_objects)) + N''' is not a valid option for the @system_objects parameter. Enter either ''true'' or ''false''.';
    THROW 51000, @message, 1;
END
ELSE
BEGIN
    SELECT @ms_shipped = CASE
                           WHEN LOWER(TRIM(COALESCE(@system_objects, 'false'))) = 'true' THEN 1
                           ELSE NULL
                         END;
END

IF @object IS NOT NULL
BEGIN
    SELECT @dbname = PARSENAME(@object, 3);

    IF @dbname IS NOT NULL AND @dbname <> DB_NAME()
    BEGIN
        RAISERROR(15250, -1, -1);
    END

    IF @dbname IS NULL
    BEGIN
        SET @dbname = DB_NAME();
    END

    SELECT @id = [object_id], @type = [type]
      FROM sys.objects
     WHERE [object_id] = OBJECT_ID(@object);

    IF @type = 'SQ'
    BEGIN
        SELECT @id = [object_id]
          FROM sys.internal_tables
         WHERE [parent_id] = @id
           AND [internal_type] = 201;
    END

    IF @id IS NULL
    BEGIN
        RAISERROR(15009, -1, -1, @object, @dbname);
    END

    -- If it is not a table, view, or queue...
    IF @type NOT IN ('U', 'S', 'V', 'SQ', 'IT')
    BEGIN
        RAISERROR(15234, -1, -1);
    END
END

--IF LOWER(TRIM(@update_usage)) = 'true'
--BEGIN
--    IF @object IS NULL
--    BEGIN
--        DBCC UPDATEUSAGE(0) WITH NO_INFOMSGS;
--    END
--    ELSE
--    BEGIN
--        DBCC UPDATEUSAGE(0, @object) WITH NO_INFOMSGS;
--    END
--END

IF LOWER(TRIM(@summary)) = 'true'
BEGIN
    DECLARE @pages bigint, @dbsize bigint, @logsize bigint, @reserved_pages bigint, @used_pages bigint;

    SELECT @dbsize = SUM(CONVERT(bigint, CASE WHEN [type] = 0 THEN [size] ELSE 0 END)),
           @logsize = SUM(CONVERT(bigint, CASE WHEN [type] = 1 THEN [size] ELSE 0 END))
      FROM sys.database_files;

    SELECT @reserved_pages = SUM(au.[total_pages]), @used_pages = SUM(au.[used_pages]),
           @pages = SUM(CASE
                          WHEN it.[internal_type] IN (202, 204, 211, 212, 213, 214, 215, 216) THEN 0
                          WHEN au.[type] <> 1 THEN au.[used_pages]
                          WHEN p.index_id < 2 THEN au.[data_pages]
                          ELSE 0
                        END)
      FROM sys.partitions p
      JOIN sys.allocation_units au
        ON au.[container_id] = p.[partition_id]
      LEFT JOIN sys.internal_tables it
        ON it.[object_id] = p.[object_id];

    -- unallocated space should not be negative
    SELECT DB_NAME() [database_name], LTRIM(STR((CONVERT(decimal(19, 2), @dbsize) + CONVERT(decimal(19, 2), @logsize)) * 8192 / 1048576, 19, 2) + ' MB') [database_size],
           LTRIM(STR((CASE
                        WHEN @dbsize >= @reserved_pages THEN (CONVERT(decimal(19, 2), @dbsize) - CONVERT(decimal(19, 2), @reserved_pages)) * 8192 / 1048576
                        ELSE 0
                      END), 19, 2) + ' MB') [unallocated_space];

    -- calculate summary data
    SELECT TRIM(STR(@reserved_pages * 8192 / 1024.0, 19, 0) + ' KB') [reserved],
           TRIM(STR(@pages * 8192 / 1024.0, 19, 0) + ' KB') [data],
           TRIM(STR((@used_pages - @pages) * 8192 / 1024.0, 19, 0) + ' KB') [index_size],
           TRIM(STR((@reserved_pages - @used_pages) * 8192 / 1024.0, 19, 0) + ' KB') [unused];
END

IF LOWER(TRIM(@schema_data)) = 'true'
BEGIN
    SELECT QUOTENAME(s.[name]) [schema_name], COUNT(o.name) [user_table_count], @@VERSION [SQL Server Version Information]
      FROM sys.objects o
      JOIN sys.schemas s
        ON s.[schema_id] = o.[schema_id]
     WHERE o.[type] = 'U'
       AND o.[is_ms_shipped] = 0
     GROUP BY s.[name]
     ORDER BY [schema_name];
END

SELECT QUOTENAME(OBJECT_SCHEMA_NAME(core1.[object_id])) [schema_name], QUOTENAME(OBJECT_NAME(core1.[object_id])) [name],
       CASE COALESCE(td.[index_id], 0)
         WHEN 1 THEN 'CLUSTERED'
         ELSE 'HEAP'
       END [type_desc], core1.[partition_number], core1.[rows],
       TRIM(STR((core1.[reserved] + COALESCE(core2.[reserved], 0)) * 8, 19, 0) + ' KB') [reserved],
       TRIM(STR(core1.[pages] * 8, 19, 0) + ' KB') [data],
       TRIM(STR((CASE
                   WHEN (core1.[used] + COALESCE(core2.[used], 0)) > core1.[pages] THEN ((core1.[used] + COALESCE(core2.[used], 0)) - core1.[pages])
                   ELSE 0
                 END) * 8, 19, 0) + ' KB') [index_size],
       TRIM(STR((CASE
                   WHEN (core1.[reserved] + COALESCE(core2.[reserved], 0)) > (core1.[used] + COALESCE(core2.[used], 0)) THEN ((core1.[reserved] + COALESCE(core2.[reserved], 0)) - (core1.[used] + COALESCE(core2.[used], 0)))
                   ELSE 0
                 END) * 8, 19, 0) + ' KB') [unused]
  FROM (SELECT base.[object_id], SUM(base.[reserved_page_count]) [reserved], SUM(base.[used_page_count]) [used],
               SUM(CASE
                     WHEN base.[index_id] < 2 THEN (base.[in_row_data_page_count] + base.[lob_used_page_count] + base.[row_overflow_used_page_count])
                     ELSE base.[lob_used_page_count] + base.[row_overflow_used_page_count]
                   END) [pages],
               SUM(CASE
                     WHEN base.[index_id] < 2 THEN base.[row_count]
                     ELSE 0
                   END) [rows], base.[partition_number]
          FROM sys.dm_db_partition_stats base
          --JOIN sys.objects obj
          --  ON obj.[object_id] = base.[object_id]
         WHERE base.[object_id] = COALESCE(@id, base.[object_id])
         GROUP BY base.[object_id], base.[partition_number]) AS [core1]
  LEFT JOIN (SELECT ps.[object_id], SUM(ps.[reserved_page_count]) [reserved], SUM(ps.[used_page_count]) [used], ps.[partition_number]
               FROM sys.dm_db_partition_stats ps
               --JOIN sys.objects obj
               --  ON obj.[object_id] = ps.[object_id]
               JOIN sys.internal_tables it
                 ON it.[object_id] = ps.[object_id]
                AND it.[parent_id] = ps.[object_id]
                AND it.[internal_type] IN (202, 204, 211, 212, 213, 214, 215, 216)
              WHERE ps.[object_id] = COALESCE(@id, ps.[object_id])
              GROUP BY ps.[object_id], ps.[partition_number]) AS [core2]
    ON core2.[object_id] = core1.[object_id]
   AND core2.[partition_number] = core1.[partition_number]
  JOIN sys.objects obj
    ON obj.[object_id] = core1.[object_id]
   AND obj.[is_ms_shipped] IN (0, COALESCE(@ms_shipped, 0))
  LEFT JOIN (SELECT part.[object_id], part.[index_id], part.[partition_number]
               FROM sys.partitions part
              WHERE part.[object_id] = COALESCE(@id, part.[object_id])
                AND part.[index_id] = 1) td
    ON td.[object_id] = core1.[object_id]
   AND td.[partition_number] = core1.[partition_number]
 ORDER BY (core1.[reserved] + COALESCE(core2.[reserved], 0)) * 8 DESC;
