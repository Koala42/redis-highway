export const LUA_MARK_DONE = `
-- KEYS[1] = status key status key for jog
-- KEYS[2] = data key for job
-- KEYS[3] = stream key
-- KEYS[4] = group name
-- KEYS[5] = metrics key
-- KEYS[6] = total metrics key(persistent)

-- ARGV[1] = route name
-- ARGV[2] = timestamp
-- ARGV[3] = msgId - redis stream item ID

-- 1. Ack the stream message
redis.call('XACK', KEYS[3], KEYS[4], ARGV[3])

-- 2. in status key mark the current route as done by saving timestamp
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])

-- 3. Increment throughput metric
if KEYS[5] then
    redis.call('INCR', KEYS[5])
    redis.call('EXPIRE', KEYS[5], 86400)
end

-- 3.1 Increment Total Metric
if KEYS[6] then
    redis.call('INCR', KEYS[6])
end

-- 4. Check for completed routes
local current_fields = redis.call('HLEN', KEYS[1])

-- 5. Get the target completed routes
local target_str = redis.call('HGET', KEYS[1], '__target')
local target = tonumber(target_str)

if not target then
    return 0
end

-- 6. If completed routes is status hash length - 1 -> all were done and we can cleanup
if current_fields >= (target + 1) then
    redis.call('DEL', KEYS[1], KEYS[2])
    redis.call('XDEL', KEYS[3], ARGV[3])
    return 1 -- Cleanup, DONE
end

return 0 -- Some routes are not done yet
`

export const LUA_FINALIZE_COMPLEX = `
-- KEYS[1] = status key
-- KEYS[2] = data key
-- KEYS[3] = stream key
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
    redis.call('DEL', KEYS[1], KEYS[2])
    redis.call('XDEL', KEYS[3], ARGV[3])
    return 1
end

return 0
`
