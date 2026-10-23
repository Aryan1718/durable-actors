local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[2], 'LIMIT', 0, ARGV[4])
local events = {}
for _, id in ipairs(due) do
    local key = ARGV[1] .. id
    local value = redis.call('GET', key)
    if value then
        local state = cjson.decode(value)
        state.estimated = true
        redis.call('SET', key, cjson.encode(state), 'PX', ARGV[3])
        table.insert(events, state.event)
    end
    redis.call('ZREM', KEYS[1], id)
end
return events
