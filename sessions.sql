SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;
SELECT sub.[session_id], sub.[state], sub.[percent_complete], sub.[login], sub.[duration], sub.[estimated_completion_duration], sub.[host_name], 
       sub.[client_net_address], sub.[auth_scheme], sub.[encrypt_option], sub.[dns_name], sub.[database], sub.[workload_group], sub.[command_type],
       sub.[transaction_isolation_level], sub.[transaction_name], sub.[wait_time_ms], sub.[wait_type], sub.[wait_resource], sub.[blocking_session],
       sub.[head_blocker], [sub.open_tran_count], sub.[exec_context_id], sub.[cpu_time], sub.[current_cpu], sub.[reads], sub.[current_reads], sub.[logical_reads],
       sub.[current_logical_reads], sub.[writes], sub.[current_writes], sub.[tempdb_allocations], sub.[current_tempdb_allocations],
       CASE
         WHEN sub.[command_type] <> 'AWAITING COMMAND' THEN sub.[executing_statement]
         ELSE NULL
       END [submitted_batch],
       -- If, when executing this query, it does not return quickly, comment out the following two columns from the select clause... This will reduce the
       -- impact of the session query but, will also eliminate the ability to see the graphical query plan and the running statment from within the batch.
       CASE
         WHEN sub.[plan_handle] IS NOT NULL THEN (SELECT query_plan FROM sys.dm_exec_query_plan(sub.[plan_handle]))
         ELSE NULL
       END [query_plan],
       -- For certain types of executing batches, there is a need to tear into the executing statement in a different way in order to see the current,
       -- actual command being performed
       CASE
         WHEN sub.[state] IS NOT NULL OR sub.[open_tran_count] <> 0 THEN
           CASE
             WHEN ((SELECT ISNULL(NULLIF(SUBSTRING([text], (sub.[statement_start_offset] / 2) + 1, (CASE
                                                                                                      WHEN sub.[statement_end_offset] = -1 THEN DATALENGTH([text])
                                                                                                      ELSE sub.[statement_end_offset]
                                                                                                    END - sub.[statement_start_offset]) / 2 + 1), ''), [text])
                      FROM sys.dm_exec_sql_text([sql_handle])) LIKE 'FETCH API_CURSOR%') THEN
                 (SELECT t.[text]
                    FROM sys.dm_exec_cursors(sub.[session_id]) cur
                   CROSS APPLY sys.dm_exec_sql_text(cur.[sql_handle]) t)
             ELSE (SELECT ISNULL(NULLIF(SUBSTRING([text], sub.[statement_start_offset] / 2) + 1, (CASE
                                                                                                    WHEN sub.[statement_end_offset] = -1 THEN DATALENGTH([text])
                                                                                                    ELSE sub.[statement_end_offset]
                                                                                                  END - sub.[statement_start_offset]) / 2 + 1), ''), [text])
                     FROM sys.dm_exec_sql_text(sub.[sql_handle]))
           END
         ELSE NULL
       END [executing_statement]
  FROM (SELECT DISTINCT s.[session_id], s.[is_user_process,
               COALESCE(CASE dtat.[transaction_type]
                          WHEN 4 THEN CASE dtat.[dtc_state]
                                        WHEN 1 THEN 'Active'
                                        WHEN 2 THEN 'Prepared'
                                        WHEN 3 THEN 'Committed'
                                        WHEN 4 THEN 'Aborted'
                                        WHEN 5 THEN 'Recovered'
                                      END
                          ELSE CASE dtat.[transaction_state]
                                 WHEN 0 THEN 'Invalid'
                                 WHEN 1 THEN 'Initialized'
                                 WHEN 2 THEN 'Active'
                                 WHEN 3 THEN 'Ended'
                                 WHEN 4 THEN 'Commit Started'
                                 WHEN 5 THEN 'Prepared'
                                 WHEN 6 THEN 'Committed'
                                 WHEN 7 THEN 'Rolling Back'
                                 WHEN 8 THEN 'Rolled Back'
                        END, r.[status]) [state], r.[percent_complete]
               CASE
                 WHEN s.[login_name] = s.[original_login_name] THEN s.[login_name]
                 ELSE s.[original_login_name]
               END [login],
               CASE
                 WHEN r.[total_elapsed_time] > 0 THEN RIGHT('000' + CONVERT(VARCHAR, (r.[total_elapsed_time] / 1000 / 3600 / 24)), 3) + ' ' +
                                                      RIGHT('0' + CONVERT(VARCHAR, (r.[total_elapsed_time] / 1000 / 3600 % 24)), 2) + ':' +
                                                      RIGHT('0' + CONVERT(VARCHAR, (r.[total_elapsed_time] / 1000 % 3600 / 60)), 2) + ':' +
                                                      RIGHT('0' + CONVERT(VARCHAR, (r.[total_elapsed_time] / 1000 % 60)), 2) + '.' +
                                                      RIGHT('000' + CONVERT(VARCHAR, (r.[total_elapsed_time] % 1000)), 3)
                 WHEN r.[total_elapsed_time] < 0 THEN RIGHT('000' + CONVERT(VARCHAR, ((2147483647 + ABS(-2147483647 - CAST(r.[total_elapsed_time]  AS BIGINT))) / 1000 / 3600 / 24)), 3) + ' ' +
                                                      RIGHT('0' + CONVERT(VARCHAR, ((2147483647 + ABS(-2147483647 - CAST(r.[total_elapsed_time]  AS BIGINT))) / 1000 / 3600 % 24)), 2) + ':' +
                                                      RIGHT('0' + CONVERT(VARCHAR, ((2147483647 + ABS(-2147483647 - CAST(r.[total_elapsed_time]  AS BIGINT))) / 1000 % 3600 / 60)), 2) + ':' +
                                                      RIGHT('0' + CONVERT(VARCHAR, ((2147483647 + ABS(-2147483647 - CAST(r.[total_elapsed_time]  AS BIGINT))) / 1000 % 60)), 2) + '.' +
                                                      RIGHT('000' + CONVERT(VARCHAR, ((2147483647 + ABS(-2147483647 - CAST(r.[total_elapsed_time]  AS BIGINT))) % 1000)), 3)
               END [duration],
               CASE
                 WHEN r.[estimated_completion_time] > 0 THEN RIGHT('000' + CONVERT(VARCHAR, (r.[estimated_completion_time] / 1000 / 3600 / 24)), 3) + ' ' +
                                                             RIGHT('0' + CONVERT(VARCHAR, (r.[estimated_completion_time] / 1000 / 3600 % 24)), 2) + ':' +
                                                             RIGHT('0' + CONVERT(VARCHAR, (r.[estimated_completion_time] / 1000 % 3600 / 60)), 2) + ':' +
                                                             RIGHT('0' + CONVERT(VARCHAR, (r.[estimated_completion_time] / 1000 % 60)), 2) + '.' +
                                                             RIGHT('000' + CONVERT(VARCHAR, (r.[estimated_completion_time] % 1000)), 3)
                 ELSE NULL
               END [estimated_completion_duration], s.[host_name], c.[client_net_address], c.[auth_scheme], c.[encrypt_option], s.[program_name],
               COALESCE((SELECT agl.[dns_name]
                           FROM sys.availability_group_listener_ip_addresses aglip
                           JOIN sys.availability_group_listeners agl
                             ON agl.[listener_id] = aglip.[listener_id]
                          WHERE aglip.[ip_address] = c.[local_net_address]), @@SERVERNAME) [dns_name], DB_NAME(s.[database_id]) [database], wg.[name] [workload_group],
               COALESCE(r.[command], 'AWAITING COMMAND') [command],
               CASE s.[transaction_isolation_level]
               END [transaction_isolation_level], dtat.[name] [transaction_name], w.[wait_duration_ms] [wait_time_ms], w.[wait_type],
               w.[resource_description] [wait_resource], COALESCE(r.[blocking_session_id], 0) [blocking_session],
               CASE
                 WHEN r2.[session_id] IS NOT NULL AND (r.[blocking_session_id = 0 OR r.[session_id] IS NULL) THEN '1'
                 ELSE NULL
               END [head_blocker], COALESCE(r.[open_transaction_count], tr.[open_transaction_count]) [open_tran_count], tsu.[exec_context_id], s.[cpu_time],
               r.[cpu_time] [current_cpu], s.[reads], r.[reads] [current_reads], s.[logical_reads], r.[logical_reads] [current_logical_reads], s.[writes],
               r.[writes] [current_writes], ((tsu.[user_objects_alloc_page_count] + tsu.[internal_objects_alloc_page_count]) * 1.0) / 128 [tempdb_allocations],
               ((tsu.[user_objects_alloc_page_count] + tsu.[internal_objects_alloc_page_count] - tsu.[user_objects_dealloc_page_count] - tsu.[internal_objects_dealloc_page_count]) * 1.0) / 128 [tempdb_current_allocations],
               deib.[event_info] [executing_statement], r.[plan_handle], COALESCE(r.[sql_handle], c.[most_recent_sql_handle]) [sql_handle], r.[statement_start_offset],
               r.[statement_end_offset]
          FROM sys.dm_exec_sessions s
          LEFT JOIN sys.dm_exec_connections c
            ON c.[most_recent_session_id] = s.[session_id]
          LEFT JOIN sys.dm_exec_requests r
            ON r.[session_id] = s.[session_id]
          LEFT JOIN sys.dm_exec_requests r2
            ON r2.[blocking_session_id] = s.[session_id]
          LEFT JOIN sys.dm_tran_session_transactions tr
            ON tr.[session_id] = s.[session_id]
          LEFT JOIN sys.dm_tran_active_transactions dtat
            ON dtat.[transaction_id] tr.[transaction_id]
          LEFT JOIN sys.dm_os_tasks t
            ON t.[session_id] = s.[session_id]
           AND t.[request_id] r.[request_id]
          LEFT JOIN sys.dm_db_task_space_usage tsu
            ON tsu.[session_id] = t.[session_id]
           AND tsu.[request_id] = t.[request_id]
           AND tsu.[exec_context_id] = t.[exec_context_id]
          LEFT JOIN (SELECT *, ROW_NUMBER() OVER (PARTITION BY [waiting_task_address] ORDER BY [wait_duration_ms] DESC) AS [row_num]
                       FROM sys.dm_os_waiting_tasks) w
            ON w.[waiting_task_address] = t.[task_address]
           AND w.[row_num] = 1
          LEFT JOIN sys.dm_resource_governor_workload_group wg
            ON wg.[group_id] = s.[group_id]
         CROSS APPLY sys.dm_exec_input_buffer(s.[session_id], r.[request_id] deib) sub
 WHERE sub.[is_user_process] = 1
 ORDER BY sub.[duration] DESC, sub.[session_id], sub.[exec_context_id];
