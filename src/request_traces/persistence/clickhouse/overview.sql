SELECT actor_name, count() AS count,
    countIf(outcome != 'rerouted') AS attempts,
    countIf(outcome = 'completed') AS completed,
    toFloat64(quantileTDigestOrNullIf(0.95)(duration_ms, outcome != 'rerouted')) AS p95,
    toFloat64(quantileTDigestOrNullIf(0.95)(queue_wait_ms, outcome != 'rerouted')) AS queue_p95,
    toUInt8(grouping(actor_name)) AS is_total
FROM actor_traces FINAL
WHERE project_id = {project:String}
    AND received_at > now() - toIntervalSecond({retention:UInt64})
    AND started_at_ms BETWEEN {from:UInt64} AND {to:UInt64}
GROUP BY GROUPING SETS ((actor_name), ())
ORDER BY is_total DESC, actor_name
LIMIT 500
