SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;

-- Pull the top 100 deadlock events from the system health extended events trace, if the trace is active
IF EXISTS(SELECT TOP 1 1 FROM sys.dm_xe_sessions WHERE [name] = 'system_health')
BEGIN
    WITH system_health_xe AS (
        SELECT CAST(dxst.[target_data] AS xml).value('(/EventFileTarget/File/@name)[1]', 'varchar(255)') [file_name]
          FROM sys.dm_xe_sessions dxs
          JOIN sys.dm_xe_session_targets dxst
            ON dxst.[event_session_address] = dxs.[address]
         WHERE dxs.[name] = 'system_health'
    ), file_pattern AS (
        SELECT REVERSE(SUBSTRING(REVERSE([file_name]), CHARINDEX(N'\', REVERSE([file_name])), 255)) + N'system_health*.xel' [pattern]
          FROM system_health_xe
         WHERE [file_name] IS NOT NULL
    ), deadlock_reports AS (
        SELECT CAST(trf.[event_data] AS xml) [event_data]
          FROM file_pattern fp
        CROSS APPLY sys.fn_xe_file_target_read_file(fp.[pattern], NULL, NULL, NULL) trf
         WHERE trf.[object_name] LIKE 'xml_deadlock_report'
    )
    SELECT TOP 100 dlr.[event_data] [raw_report],
           DATEADD(HOUR, DATEDIFF(HOUR, SYSUTCDATETIME(), SYSDATETIME()), dlr.[event_data].value('(/event/@timestamp)[1]', 'datetime2')) [server_time],
           dl.r.query('.') [deadlock_graph_xml]
      FROM deadlock_reports dlr
     CROSS APPLY dlr.[event_data].nodes('//event/data/value/deadlock') dl(r)
     ORDER BY [server_time] DESC;
END