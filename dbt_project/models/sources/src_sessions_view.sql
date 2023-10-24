WITH 
    raw_sessions_deduped
      AS 
        (
          {{ dbt_utils.deduplicate(
            relation=source('duck_lake', 'pond_sessions'),
            partition_by='user_id, session_id',
            order_by="session_start_time desc",
            )
          }}
        ) 
SELECT *
FROM raw_sessions_deduped s