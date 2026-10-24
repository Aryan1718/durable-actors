local previous = redis.call('HMGET', KEYS[2], 'epoch', 'sequence', 'stopped')
if previous[3] == '1' then return 0 end
local epoch, sequence = previous[1] or '0', tonumber(previous[2]) or 0
if (epoch ~= '0' and epoch ~= ARGV[2]) or tonumber(ARGV[3]) <= sequence then return 0 end
redis.call('HSET', KEYS[2], 'epoch', ARGV[2], 'sequence', ARGV[3], 'stopped', ARGV[4])
if ARGV[4] == '1' then
    redis.call('SREM', KEYS[1], ARGV[1])
    redis.call('PEXPIRE', KEYS[2], ARGV[5])
else
    redis.call('SADD', KEYS[1], ARGV[1])
    redis.call('PERSIST', KEYS[2])
end
return 1
