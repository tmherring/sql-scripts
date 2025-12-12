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

-- Perform a raw grab of the XML records from the trace, snagging just the first 100 records
SELECT TOP 100 CAST(event_data AS xml) [event_data]
  FROM sys.fn_xe_file_target_read_file(@file_pattern, null, null, null);

-- Perform a more targeted crack of the trace data
SELECT TOP 100 xevents.[event_data],                                                                                                                      -- Raw XML of a current row
       xevents.[event_data].value('(event/@name)[1]', 'varchar(128)') [event_type],                                                                       -- Type of event for the current row
       DATEADD(MINUTE, DATEDIFF(MINUTE, GETUTCDATE(), CURRENT_TIMESTAMP), xevents.[event_data].value('(event/@timestamp)[1]', 'datetime2')) [event_time], -- datetime of the current event, converted to server local
       DB_NAME(xevents.[event_data].value('(action[@name="database_id"]/value)[1]', 'int')) [database_name],                                              -- database name where the event was targeted
       xevents.[event_data].value('(action[@name="client_hostname"]/value)[1]', 'nvarchar(256)') [client_hostname],                                       -- client hostname that submitted the request
       xevents.[event_data].value('(action[@name="client_app_name"]/value)[1]', 'nvarchar(256)') [client_app_name],                                       -- app name that submited the request
       xevents.[event_data].value('(action[@name="session_id"]/value)[1]', 'int') [session_id],                                                           -- session ID of the request
       xevents.[event_data].value('(action[@name="username"]/value)[1]', 'nvarchar(128)') [username],                                                     -- user name associated with the request
       xevents.[event_data].value('(action[@name="sql_text"]/value)[1]', 'nvarchar(max)') [sql_text]                                                      -- full t-sql for the request
  FROM sys.fn_xe_file_target_read_file(@file_pattern, null, null, null) xe
 CROSS APPLY (SELECT CAST(xe.[event_data] AS xml) [event_data]) [xevents]
 ORDER BY [event_time] DESC;
GO
