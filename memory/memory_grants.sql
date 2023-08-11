SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;
SELECT deqmg.[session_id], DB_NAME(der.[database_id]) [database_name], COALESCE(CASE des.[host_name] WHEN '' THEN NULL ELSE des.[host_name] END, dec.[client_net_address]),
       COALESCE(CASE des.[login_name] WHEN '' THEN NULL ELSE des.[login_name] END, des.[original_login_name]) [login_name], deqmg.[granted_memory_kb],
       deqmg.[requested_memory_kb], deqmg.[ideal_memory_kb], deqmg.[used_memory_kb], deqmg.[max_used_memory_kb], deqmg.[dop], deqmg.[request_time],
       CASE
         WHEN deqmg.[grant_time] IS NULL THEN RIGHT('000' + CONVERT(VARCHAR, ((DATEDIFF(MILLISECOND, deqmg.[request_time], GETDATE())) / 1000 / 3600 / 24)), 3) + ' ' +
                                              RIGHT('0' + CONVERT(VARCHAR, ((DATEDIFF(MILLISECOND, deqmg.[request_time], GETDATE())) / 1000 / 3600 % 24)), 2) + ':' +
                                              RIGHT('0' + CONVERT(VARCHAR, ((DATEDIFF(MILLISECOND, deqmg.[request_time], GETDATE())) / 1000 % 3600 / 60)), 2) + ':' +
                                              RIGHT('0' + CONVERT(VARCHAR, ((DATEDIFF(MILLISECOND, deqmg.[request_time], GETDATE())) / 1000 % 60)), 2) + ':' +
                                              RIGHT('000' + CONVERT(VARCHAR, ((DATEDIFF(MILLISECOND, deqmg.[request_time], GETDATE())) % 1000)), 2)
         ELSE NULL
       END [pending_grant_duration (DDD HH:MM:SS.FFF)], deqmg.[grant_time], deib.[event_info] [text], TRY_CAST(deqp.[query_plan] AS XML) [query_plan]
  FROM sys.dm_exec_query_memory_grants deqmg
  JOIN sys.dm_exec_requests der 
    ON er.[session_id] = mg.[session_id]
  JOIN sys.dm_exec_sessions des
    ON des.[session_id] = der.[session_id]
  JOIN sys.dm_exec_connections dec
    ON dec.[most_recent_session_id] = des.[session_id]
 CROSS APPLY sys.dm_exec_input_buffer(des.[session_id], der.[request_id]) deib
 CROSS APPLY sys.dm_exec_query_plan(deqmg.[plan_handle]) deqp
 WHERE deqp.[session_id] <> @@SPID
 ORDER BY 1
OPTION (MAXDOP 1);