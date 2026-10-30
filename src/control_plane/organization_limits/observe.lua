local previous = redis.call('HMGET', KEYS[2], 'epoch', 'sequence', 'stopped')
if previous[3] == '1' then return 0 end
local epoch, sequence = previous[1] or '0', tonumber(previous[2]) or 0
if (epoch ~= '0' and epoch ~= ARGV[2]) or tonumber(ARGV[3]) <= sequence then return 0 end
redis.call('HSET', KEYS[2], 'epoch', ARGV[2], 'sequence', ARGV[3], 'stopped', ARGV[4])
redis.call('PEXPIRE', KEYS[2], ARGV[5])
if ARGV[4] == '1' then
    redis.call('ZREM', KEYS[1], ARGV[1])
else
    local time = redis.call('TIME')
    local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
    redis.call('ZADD', KEYS[1], now + tonumber(ARGV[6]), ARGV[1])
end
return 1
