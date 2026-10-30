if redis.call('EXISTS', KEYS[3]) == 1 then
    if redis.call('HGET', KEYS[3], 'stopped') == '1' then return {3, 0} end
    return {0, 0}
end
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[2]) then return {1, 0} end
local rate, burst = tonumber(ARGV[3]), tonumber(ARGV[4])
local previous = redis.call('HMGET', KEYS[2], 'tokens', 'updated')
local updated = tonumber(previous[2]) or now
local tokens = math.min(burst, (tonumber(previous[1]) or burst) + math.max(0, now - updated) * rate / 1000)
if tokens < 1 then return {2, math.max(1, math.ceil((1 - tokens) / rate))} end
redis.call('HSET', KEYS[2], 'tokens', tokens - 1, 'updated', math.max(now, updated))
redis.call('PEXPIRE', KEYS[2], math.max(1000, math.ceil(burst / rate * 1000)))
redis.call('ZADD', KEYS[1], now + tonumber(ARGV[5]), ARGV[1])
redis.call('HSET', KEYS[3], 'epoch', 0, 'sequence', 0, 'stopped', 0)
redis.call('PEXPIRE', KEYS[3], ARGV[6])
return {0, 0}
