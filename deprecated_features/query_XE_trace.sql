-- T-SQL to crack the event data produced by the deprecated features extended events trace
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;
DECLARE @trace_name varchar(256) = 'find_deprecated_features',
        @file_pattern varchar(256);
SELECT @file_pattern = REVERSE(SUBSTRING(REVERSE(n.value('(File/@name)[1]', 'varchar(256)')), CHARINDEX('\', REVERSE(n.value('(File/@name)[1]', 'varchar(256)'))), LEN(n.value('(File/@name)[1]', 'varchar(256)')))) +
                       REVERSE(SUBSTRING(REVERSE([file_pattern]), 1, CHARINDEX('.', REVERSE([file_pattern]))) + '*' + SUBSTRING(REVERSE([file_pattern]), CHARINDEX('.', REVERSE([file_pattern])) + 1, LEN([file_pattern])))
  FROM (SELECT xs.[name], xsoc.[column_value] [file_pattern], TRY_CAST(xst.[target_data] AS xml) [target_data]
          FROM sys.dm_xe_sessions xs
          JOIN sys.dm_xe_session_object_columns xsoc
            ON xsoc.[event_session_address] = xs.[address]
           AND xsoc.[column_name] = 'filename'
          JOIN sys.dm_xe_session_targets xst
            ON xst.[event_session_address] = xsoc.[event_session_address]
           AND xst.[target_name] = 'event_file'
         WHERE xs.[name] = @trace_name) [trace_file]
 CROSS APPLY [trace_file].[target_data].nodes('EventFileTarget') AS q(n);
