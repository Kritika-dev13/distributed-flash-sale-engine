-- release_stock.lua
-- Compensating action: give units back when an order cannot complete
-- (broker publish failed, payment declined, retries exhausted).
-- Idempotent: a "released" flag guarantees stock is returned at most once per order,
-- even if the API and a worker both try to release. The worker also reads this flag
-- to skip orders whose reservation was already cancelled.
--
-- KEYS[1] stock counter
-- KEYS[2] released flag for this order   e.g. released:{sku-1}:<orderId>
-- KEYS[3] per-user purchased count
-- KEYS[4] idempotency record
--
-- ARGV[1] quantity to return
-- ARGV[2] released-flag TTL (seconds)
-- ARGV[3] "1" to delete the idempotency record (lets the client retry with the SAME key)
--
-- Returns 1 if stock was released, 0 if it had already been released.

local first = redis.call('SET', KEYS[2], '1', 'NX', 'EX', tonumber(ARGV[2]))
if not first then
  return 0
end

redis.call('INCRBY', KEYS[1], tonumber(ARGV[1]))

local purchased = redis.call('DECRBY', KEYS[3], tonumber(ARGV[1]))
if purchased < 0 then
  redis.call('SET', KEYS[3], 0)
end

if ARGV[3] == '1' then
  redis.call('DEL', KEYS[4])
end

return 1
