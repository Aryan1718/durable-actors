redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('DEL', KEYS[2])
return 1
