SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;
SELECT deqmg.[session_id], DB_NAME(der.[database_id]) [database_name], COALESCE(CASE es.[host_name] WHEN '' THEN NULL ELSE es.[host_name] END, ec.[client_net_address]),
       COALESCE(CASE es.[login_name] WHEN '' THEN NULL ELSE es.[login_name] END, es.[original_login_name]) [login_name], deqmg.[granted_memory_kb],
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
    ON der.[session_id] = deqmg.[session_id]
  JOIN sys.dm_exec_sessions es
    ON es.[session_id] = der.[session_id]
  JOIN sys.dm_exec_connections ec
    ON ec.[most_recent_session_id] = es.[session_id]
 CROSS APPLY sys.dm_exec_input_buffer(es.[session_id], der.[request_id]) deib
 CROSS APPLY sys.dm_exec_query_plan(deqmg.[plan_handle]) deqp
 WHERE deqmg.[session_id] <> @@SPID
 ORDER BY 1
OPTION (MAXDOP 1);
