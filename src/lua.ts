export const LUA_MARK_DONE = `
-- KEYS[1] = status key status key for jog
-- KEYS[2] = stream key
-- KEYS[3] = group name
-- KEYS[4] = metrics key
-- KEYS[5] = total metrics key(persistent)

-- ARGV[1] = route name
-- ARGV[2] = timestamp
-- ARGV[3] = msgId - redis stream item ID

-- 1 Ack the stream message
redis.call('XACK', KEYS[2], KEYS[3], ARGV[3])

-- 2 in status key mark the current route as done by saving timestamp
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])

-- 3 Increment throughput metric
if KEYS[5] then
    redis.call('INCR', KEYS[4])
    redis.call('EXPIRE', KEYS[4], 86400)
end

-- 4 Increment Total Metric
redis.call('INCR', KEYS[5])

-- 5 Check for completed routes
local current_fields = redis.call('HLEN', KEYS[1])

-- 6 Get the target completed routes
local target_str = redis.call('HGET', KEYS[1], '__target')
local target = tonumber(target_str)

if not target then
    return 0
end

-- 7 If completed routes is status hash length - 1 -> all were done and we can cleanup
if current_fields >= (target + 1) then
    redis.call('DEL', KEYS[1]) -- Only delete status key
    redis.call('XDEL', KEYS[2], ARGV[3])
    return 1 -- Cleanup, DONE
end

return 0 -- Some routes are not done yet
`

export const LUA_FINALIZE_COMPLEX = `
-- KEYS[1] = status key
-- KEYS[2] = stream key
-- ARGV[1] = group name
-- ARGV[2] = timestamp
-- ARGV[3] = msgId

-- 1. Update status
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])

-- 2. Check completions
local current_fields = redis.call('HLEN', KEYS[1])
local target_str = redis.call('HGET', KEYS[1], '__target')
local target = tonumber(target_str)

if not target then
    return 0
end

-- 3. Cleanup if done
if current_fields >= (target + 1) then
    redis.call('DEL', KEYS[1])
    redis.call('XDEL', KEYS[2], ARGV[3])
    return 1
end

return 0
`
