"""
Flash-sale load test.

Run (UI):        locust -f loadtest/locustfile.py --host http://localhost:3000
Run (headless):  locust -f loadtest/locustfile.py --host http://localhost:3000 \
                        --headless -u 2500 -r 500 -t 2m --processes -1
Docker:          docker compose --profile loadtest up locust      # http://localhost:8089
Burst profile:   USE_SHAPE=1 locust -f loadtest/locustfile.py --host http://localhost:3000 --headless

Each virtual user:
  * has its own X-User-Id (so per-user rate limits and purchase limits behave realistically),
  * sends a FRESH Idempotency-Key per request,
  * replays its previous key ~REPLAY_RATIO of the time and asserts the API returns the SAME orderId
    (verifies idempotency under load).

With the default config (1 request/s/user) 2,000 users ~= 2,000 req/s.
Expected statuses:  202 accepted | 409 sold out / purchase limit | 429 rate limited.
Anything else (5xx, connection errors, wrong replay orderId) is counted as a FAILURE.

Env knobs: PRODUCT_ID, RPS_PER_USER, REPLAY_RATIO, USE_SHAPE, SHAPE_USERS, SHAPE_RAMP_SEC, SHAPE_HOLD_SEC
"""
import os
import random
import uuid

from locust import FastHttpUser, LoadTestShape, constant_throughput, task

PRODUCT_ID = os.getenv("PRODUCT_ID", "sku-flash-001")
RPS_PER_USER = float(os.getenv("RPS_PER_USER", "1"))
REPLAY_RATIO = float(os.getenv("REPLAY_RATIO", "0.10"))
CHECKOUT_PATH = "/api/v1/orders/checkout"

BUSINESS_REJECTIONS = {409, 429}  # expected under a flash sale: not errors


class FlashSaleUser(FastHttpUser):
    # constant_throughput(1) => each user fires ~1 request per second
    wait_time = constant_throughput(RPS_PER_USER)
    connection_timeout = 5.0
    network_timeout = 5.0

    def on_start(self) -> None:
        self.user_id = f"load-{uuid.uuid4().hex[:16]}"
        self.last_key = None       # last idempotency key that produced a 202
        self.last_order_id = None  # orderId returned for that key

    @task
    def checkout(self) -> None:
        replay = self.last_key is not None and random.random() < REPLAY_RATIO
        key = self.last_key if replay else str(uuid.uuid4())

        headers = {
            "Content-Type": "application/json",
            "Idempotency-Key": key,
            "X-User-Id": self.user_id,
        }
        payload = {"productId": PRODUCT_ID, "quantity": 1}

        with self.client.post(
            CHECKOUT_PATH,
            json=payload,
            headers=headers,
            name=f"POST {CHECKOUT_PATH} [{'replay' if replay else 'new'}]",
            catch_response=True,
        ) as resp:
            status = resp.status_code

            if status == 202:
                order_id = (resp.json() or {}).get("orderId")
                if not order_id:
                    resp.failure("202 without orderId")
                elif replay and order_id != self.last_order_id:
                    resp.failure(f"idempotency violated: {order_id} != {self.last_order_id}")
                else:
                    if not replay:
                        self.last_key, self.last_order_id = key, order_id
                    resp.success()
            elif status in BUSINESS_REJECTIONS:
                resp.success()
            else:
                resp.failure(f"unexpected status {status}")


if os.getenv("USE_SHAPE", "0") == "1":

    class FlashSaleBurst(LoadTestShape):
        """Doors-open spike: ramp to SHAPE_USERS in a few seconds, hold, then stop."""

        users = int(os.getenv("SHAPE_USERS", "2500"))
        ramp = float(os.getenv("SHAPE_RAMP_SEC", "5"))
        hold = float(os.getenv("SHAPE_HOLD_SEC", "60"))

        def tick(self):
            t = self.get_run_time()
            if t < self.ramp + self.hold:
                return (self.users, self.users / self.ramp)  # (user_count, spawn_rate)
            return None
