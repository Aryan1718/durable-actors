SELECT connection_id, actor_name, actor_id,
    argMin(host_id, (operation != 'onConnect', started_at_ms, event_id)) AS host_id,
    minOrNullIf(started_at_ms, operation = 'onConnect') AS opened_at_ms,
    maxOrNullIf(started_at_ms, operation = 'onDisconnect') AS closed_at_ms,
    max(started_at_ms) AS last_seen_ms,
    countIf(operation = 'onMessage') AS messages,
    countIf(outcome NOT IN ('completed', 'rerouted')) AS failures,
    argMinIf(event_json, (started_at_ms, event_id), operation = 'onConnect') AS connect_event
FROM actor_traces FINAL
WHERE project_id = {project:String}
    AND received_at > now() - toIntervalSecond({retention:UInt64})
    AND kind = 'websocket' AND connection_id IS NOT NULL
GROUP BY connection_id, actor_name, actor_id
HAVING max(started_at_ms) >= {from:UInt64} AND min(started_at_ms) <= {to:UInt64}
ORDER BY coalesce(opened_at_ms, last_seen_ms) DESC, connection_id
LIMIT 500
