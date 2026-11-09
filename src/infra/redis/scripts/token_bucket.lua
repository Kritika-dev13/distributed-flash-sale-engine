-- token_bucket.lua
-- Distributed token bucket. State lives in a Redis hash {tokens, ts}.
-- Time comes from Redis (TIME), not from clients, so clock skew between API pods is irrelevant.
--
-- KEYS[1] bucket key
-- ARGV[1] capacity (burst size)
-- ARGV[2] refill rate (tokens / second)
-- ARGV[3] tokens requested for this call
--
-- Returns {allowed (1|0), tokensRemaining (floor), retryAfterMs}

local capacity  = tonumber(ARGV[1])
local refill    = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local data   = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])

if tokens == nil then
  tokens = capacity
  ts     = now
end

local elapsed = math.max(0, now - ts)
tokens = math.min(capacity, tokens + (elapsed / 1000) * refill)

local allowed    = 0
local retryAfter = 0

if tokens >= requested then
  tokens  = tokens - requested
  allowed = 1
else
  retryAfter = math.ceil(((requested - tokens) / refill) * 1000)
end

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', now)
-- Idle buckets expire after twice the time needed to refill fully.
redis.call('PEXPIRE', KEYS[1], math.ceil((capacity / refill) * 1000) * 2)

return {allowed, math.floor(tokens), retryAfter}
