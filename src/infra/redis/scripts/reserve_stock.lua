-- reserve_stock.lua
-- Atomically: (1) dedupe by idempotency key, (2) enforce per-user limit,
--             (3) check stock, (4) decrement stock, (5) record the reservation.
-- Redis runs the whole script as one uninterruptible unit, so there is no window in
-- which two requests can both observe "1 left" and both succeed. No SQL row locks needed.
--
-- KEYS[1] stock counter             e.g. stock:{sku-1}
-- KEYS[2] idempotency record        e.g. idem:{sku-1}:user42:<key>      (value = orderId)
-- KEYS[3] per-user purchased count  e.g. purchased:{sku-1}:user42
--   (all keys share the {sku-1} hash tag so the script is Redis Cluster safe)
--
-- ARGV[1] quantity requested
-- ARGV[2] orderId to bind to the idempotency key
-- ARGV[3] idempotency TTL (seconds)
-- ARGV[4] per-user purchase limit (0 = unlimited)
--
-- Returns {code, orderId, remaining}
--    1  reserved                 -> orderId = new order, remaining = stock left
--    0  sold out
--   -1  duplicate request        -> orderId = the ORIGINAL order id
--   -2  stock key missing (sale not pre-warmed)
--   -3  per-user limit exceeded

local existing = redis.call('GET', KEYS[2])
if existing then
  return {-1, existing, -1}
end

local stock = redis.call('GET', KEYS[1])
if not stock then
  return {-2, '', -1}
end

local qty   = tonumber(ARGV[1])
local limit = tonumber(ARGV[4])

if limit > 0 then
  local purchased = tonumber(redis.call('GET', KEYS[3]) or '0')
  if purchased + qty > limit then
    return {-3, '', tonumber(stock)}
  end
end

if tonumber(stock) < qty then
  return {0, '', tonumber(stock)}
end

local remaining = redis.call('DECRBY', KEYS[1], qty)
redis.call('INCRBY', KEYS[3], qty)
redis.call('SET', KEYS[2], ARGV[2], 'EX', tonumber(ARGV[3]))

return {1, ARGV[2], remaining}
