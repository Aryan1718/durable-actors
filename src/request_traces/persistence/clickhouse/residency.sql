WITH source AS (
    SELECT actor_id, event_id, owner_epoch, sequence, event_json,
           JSONExtractUInt(event_json, 'occurredAtMs') AS occurred_at_ms
    FROM actor_lifecycle FINAL
    WHERE project_id = {project:String} AND actor_name = {actor_name:String}
      AND JSONExtractUInt(event_json, 'occurredAtMs') <= {to:UInt64}
    UNION ALL
    SELECT actor_id, JSONExtractString(event_json, 'eventId') AS event_id,
           JSONExtractUInt(event_json, 'ownerEpoch') AS owner_epoch,
           JSONExtractUInt(event_json, 'sequence') AS sequence, event_json,
           JSONExtractUInt(event_json, 'occurredAtMs') AS occurred_at_ms
    FROM (
        SELECT actor_id, argMaxMerge(event) AS event_json FROM actor_inventory
        WHERE project_id = {project:String} AND actor_name = {actor_name:String}
        GROUP BY actor_id
    )
    WHERE JSONExtractUInt(event_json, 'occurredAtMs') <= {to:UInt64}
), retained AS (
    SELECT DISTINCT * FROM source
), prior AS (
    SELECT argMax(event_id, tuple(owner_epoch, sequence)) AS event_id
    FROM retained WHERE occurred_at_ms < {from:UInt64}
    GROUP BY actor_id
)
SELECT event_json FROM retained
WHERE occurred_at_ms >= {from:UInt64} OR event_id IN (SELECT event_id FROM prior)
ORDER BY occurred_at_ms DESC, owner_epoch DESC, sequence DESC
LIMIT 10001
