SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;

-- Pull the top 100 deadlock events from the system health extended events trace, if the trace is active
IF EXISTS(SELECT TOP 1 1 FROM sys.dm_xe_sessions WHERE [name] = 'system_health')
BEGIN
    DECLARE @begin DATETIME = DATEADD(DAY, -5, DATEADD(DAY, DATEDIFF(DAY, '19000101', GETDATE()), '19000101'));
    CREATE TABLE #deadlock_reports (
        [entry_datetime] datetime2
    );

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

    INSERT INTO #deadlock_reports
    SELECT [event_data].value('(/event/@timestamp)[1]', 'datetime2') [entry_datetime]
      FROM deadlock_reports;

    SELECT COUNT(*) [occurances], DATENAME(WEEKDAY, [entry_datetime]) [name], DATEPART(WEEKDAY, [entry_datetime]) [day],
           DATEPART(HOUR, [entry_datetime]) [hour]
      FROM #deadlock_reports
     WHERE [entry_datetime] >= @begin
     GROUP BY DATENAME(WEEKDAY, [entry_datetime]), DATEPART(WEEKDAY, [entry_datetime]), DATEPART(HOUR, [entry_datetime])
     ORDER BY 3 DESC, 4 DESC;

    SELECT TOP 1 [entry_datetime] [last_deadlock_utc], SYSDATETIMEOFFSET() [current_local_time]
      FROM #deadlock_reports
     ORDER BY [entry_datetime] DESC;

     DROP TABLE IF EXISTS #deadlock_reports;
END 