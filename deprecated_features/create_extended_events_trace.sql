-- Create an extended events trace to identify what T-SQL is being submitted which uses deprecated features
CREATE EVENT SESSION [find_deprecated_features] ON SERVER
   ADD EVENT sqlserver.deprecation_announcement (
       ACTION (
           sqlserver.client_app_name,
           sqlserver.client_hostname,
           sqlserver.database_id,
           sqlserver.session_id,
           sqlserver.sql_text,
           sqlserver.username
       )
   ),
   ADD EVENT sqlserver.deprecation_final_support (
       ACTION (
           sqlserver.client_app_name,
           sqlserver.client_hostname,
           sqlserver.database_id,
           sqlserver.session_id,
           sqlserver.sql_text,
           sqlserver.username
       )
   )
   ADD TARGET package0.event_file (
       SET filename = N'find_deprecated_features',
           max_file_size = (50)
   )
  WITH (
       MAX_DISPATCH_LATENCY = 5 SECONDS,
       TRACK_CAUSALITY = ON,
       STARTUP_STATE = ON
  );
GO
