export const LUA_FINALIZE = `
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
