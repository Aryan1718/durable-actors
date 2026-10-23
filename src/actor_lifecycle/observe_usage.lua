local event = cjson.decode(ARGV[1])
local previous = redis.call('GET', KEYS[1])
local corrected = 0
if previous then
    local state = cjson.decode(previous)
    local old = cjson.decode(state.event)
    if event.sequence <= old.sequence or old.kind == 'stopped' then return 0 end
    if state.estimated then corrected = 1 end
end
redis.call('SET', KEYS[1], cjson.encode({event = ARGV[1], estimated = false}), 'PX', ARGV[4])
if event.kind == 'stopped' then
    redis.call('ZREM', KEYS[2], ARGV[2])
else
    redis.call('ZADD', KEYS[2], event.receivedAtMs + tonumber(ARGV[3]), ARGV[2])
end
return corrected
