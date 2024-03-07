SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;
DECLARE @ag_name nvarchar(128) = NULL;
SELECT ar.[replica_server_name] [AvailabilityReplicaServerName], dbcs.[database_name] [AvailabilityDatabaseName], dbcs.[group_database_id] [AvailabilityDatabaseId],
       ar.[group_id] [AvailabilityGroupId], ag.[name] [AvailabilityGroupName], ar.[replica_id] [AvailabilityReplicaId], COALESCE(dbr.[database_id], 0) [DatabaseId],
       COALESCE(dbr.[end_of_log_lsn], 0) [EndOfLogLSN],
       CASE
         WHEN dbr.[is_primary_replica] = 1 THEN 1
         WHEN dbcs.[is_failover_ready] = 1 THEN 0
         ELSE COALESCE(DATEDIFF(SECOND, dbr.[last_commit_time], dbrp.[last_commit_time]), -2)
       END [EstimatedDataLoss],
       COALESCE(CASE
                  WHEN dbr.[is_primary_replica] = 1 THEN -1
                  WHEN dbr.[redo_queue_size] IS NULL THEN -2
                  WHEN dbr.[redo_queue_size] = 0 THEN 0
                  WHEN dbr.[redo_rate] IS NULL OR dbr.[redo_rate] = 0 THEN -2
                  ELSE CAST(dbr.[redo_queue_size] AS real) / dbr.[redo_rate]
                END, -2) [EstimatedRecoveryTime],
       COALESCE(dbr.[filestream_send_rate], -1) [FileStreamSendRate], COALESCE(dbcs.[is_failover_ready], 0) [IsFailoverReady], COALESCE(dbcs.[is_database_joined], 0) [IsJoined],
       arstates.[is_local] [IsLocal], COALESCE(dbr.[is_suspended], 0) [IsSuspended], COALESCE(dbr.[last_commit_lsn], 0) [LastCommitLSN], COALESCE(dbr.[last_commit_time], 0) [LastCommitTime],
       COALESCE(dbr.[last_hardened_lsn], 0) [LastHardenedLSN], COALESCE(dbr.[last_hardened_time], 0) [LastHardenedTime], COALESCE(dbr.[last_received_lsn], 0) [LastReceivedLSN],
       COALESCE(dbr.[last_receieved_time], 0) [LastReceivedTime], COALESCE(dbr.[last_redone_lsn], 0) [LastRedoneLSN], COALESCE(dbr.[last_redone_time], 0) [LastRedoneTime],
       COALESCE(dbr.[last_sent_lsn], 0) [LastSentLSN], COALESCE(dbr.[last_sent_time], 0) [LastSentTime], COALESCE(dbr.[log_send_queue_size], -1) [LogSendQueueSize],
       COALESCE(dbr.[log_send_rate], -1) [LogSendRate], COALESCE(dbr.[recovery_lsn]), 0) [RecoveryLSN], COALESCE(dbr.[redo_queue_size], -1) [RedoQueueSize],
       COALESCE(ar.[availability_mode], 2) [ReplicaAvailabilityMode], COALESCE(arstates.[role], 3) [ReplicaRole], COALESCE(dbr.[suspend_reason], 7) [SuspendReason],
       COALESCE(CASE dbr.[log_send_rate]
                  WHEN 0 THEN -1
                  ELSE CAST(dbr.[log_send_queue_size] AS real) / dbr.[log_send_rate]
                END, -1) [SynchronizationPerformance],
       COALESCE(dbr.[synchronization_state], 0) [SynchronizationState], COALESCE(dbr.[truncation_lsn], 0) [TruncationLSN]
  FROM sys.availability_groups ag
  JOIN sys.availability_replicas ar
    ON ar.[group_id] = ag.[group_id]
   AND ar.[availability_mode] <> 4
  JOIN sys.dm_hadr_database_replica_cluster_states dbcs
    ON dbcs.[replica_id] = ar.[replica_id]
  JOIN sys.dm_hadr_availability_replica_states arstates
    ON arstates.[replica_id] = ar.[replica_id]
  LEFT JOIN sys.dm_hadr_database_replica_states dbr
    ON dbr.[replica_id] = dbcs.[replica_id]
   AND dbr.[group_database_id] = dbcs.[group_database_id]
  LEFT JOIN (SELECT ars.[role], drs.[database_id], drs.[replica_id], drs.[last_commit_time]
               FROM sys.dm_hadr_database_replica_states drs
               LEFT JOIN sys.dm_hadr_availability_replica_states ars
                 ON ars.[replica_id] = drs.[replica_id]
              WHERE ars.[role] = 1) dbrp
 WHERE ag.[name] = COALESCE(@ag_name, ag.[name])
 ORDER BY [AvailabilityReplicaServerName], [AvailabilityDatabaseName];
GO
