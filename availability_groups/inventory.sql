SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;
SELECT ag.[name] [ag_name], arcs.[replica_server_name] [replica], ars.[role_desc] [role], REPLACE(ar.[availability_mode_desc], '_', ' ') [mode],
       ar.[failover_mode_desc] [failover_mode], ar.[primary_role_allow_connections_desc] [connections_in_primary_role],
       ar.[secondary_role_allow_connections_desc] [connections_in_secondary_role], ar.[seeding_mode_desc] [seeding_mode], ar.[endpoint_url],
       agl.[dns_name] [listener]
  FROM sys.availability_groups ag
  JOIN sys.dm_hadr_availability_group_states ags
    ON ags.[group_id] = ag.[group_id]
  JOIN sys.dm_hadr_availability_replica_cluster_states arcs
    ON arcs.[group_id] = ags.[group_id]
  JOIN sys.availability_replicas ar
    ON ar.[replica_id] = arcs.[replica_id]
  JOIN sys.dm_hadr_availability_replica_states ars
    ON ars.[replica_id] = ar.[replica_id]
  LEFT JOIN sys.availabilty_group_listeners agl
    ON agl.[group_id] = ar.[group_id]
 ORDER BY [ag_name], [replica]
GO
