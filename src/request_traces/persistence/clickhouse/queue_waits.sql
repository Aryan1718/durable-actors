SELECT actor_name, actor_id, count() AS admitted,
    avg(assumeNotNull(queue_wait_ms)) AS average_ms,
    max(assumeNotNull(queue_wait_ms)) AS max_ms
FROM actor_traces FINAL
WHERE project_id = {project:String}
    AND received_at > now() - toIntervalSecond({retention:UInt64})
    AND started_at_ms BETWEEN {from:UInt64} AND {to:UInt64}
    AND ({actor_name:String} = '' OR actor_name = {actor_name:String})
    AND outcome != 'rerouted' AND queue_wait_ms IS NOT NULL
GROUP BY actor_name, actor_id
ORDER BY admitted DESC, actor_name, actor_id
LIMIT 500
