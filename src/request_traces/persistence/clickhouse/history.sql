SELECT event_json FROM actor_traces FINAL
WHERE project_id = {project:String}
    AND received_at > now() - toIntervalSecond({retention:UInt64})
    AND ({actor_name:String} = '' OR actor_name = {actor_name:String})
    AND ({actor_id:String} = '' OR actor_id = {actor_id:String})
    AND ({request_id:String} = '' OR request_id = {request_id:String})
    AND ({connection_id:String} = '' OR connection_id = {connection_id:String})
    AND ({outcome:String} = '' OR outcome = {outcome:String})
    AND started_at_ms BETWEEN {from:UInt64} AND {to:UInt64}
    AND (NOT {has_cursor:Bool} OR (started_at_ms, event_id) < ({after_time:UInt64}, {after_id:String}))
ORDER BY started_at_ms DESC, event_id DESC
LIMIT {limit:UInt64}
