#!/usr/bin/env python3
"""Generate realistic MySQL row changes for the Flink CDC ingestion path."""

import argparse
import os
import random
import signal
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import mysql.connector
from mysql.connector import Error as MySQLError


ORDER_ACTIONS = ("CREATE", "PAY", "CONFIRM", "REFUND", "CANCEL")
ORDER_WEIGHTS = (45, 30, 12, 8, 5)
CHANGE_TYPES = ("BILL", "ORDER", "DIM")
CHANGE_WEIGHTS = (60, 38, 2)
MEDIA = ("douyin", "kuaishou", "weibo", "toutiao")
SCENES = ("ecommerce", "short_video", "live")


def db_now() -> datetime:
    """Return a naive UTC timestamp matching this project's MySQL/Flink CDC timezone."""
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


@dataclass
class OrderState:
    order_id: int
    create_time: datetime
    pay_time: datetime | None = None
    confirm_time: datetime | None = None
    pay_ready_at: datetime | None = None
    cancel_ready_at: datetime | None = None
    confirm_ready_at: datetime | None = None
    refund_ready_at: datetime | None = None


class MonotonicId:
    def __init__(self, floor: int):
        self.value = max(floor, int(time.time() * 1000) * 1000)

    def next(self) -> int:
        self.value += 1
        return self.value


class MySQLChangeGenerator:
    def __init__(self, rate: float, duration: float | None, seed: int):
        self.rate = rate
        self.duration = duration
        self.rng = random.Random(seed)
        self.running = True
        self.conn = None
        self.stats = Counter()
        self.started_at = time.monotonic()
        self.last_report_at = self.started_at
        self.last_report_total = 0
        self.pending_changes = 0
        self.last_commit_at = self.started_at
        self.users = []
        self.products = []
        self.ad_keys = []
        self.dim_targets = {}
        self.created_orders = {}
        self.paid_orders = {}
        self.confirmed_orders = {}
        self.order_ids = None
        self.bill_ids = None

    def connect(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "127.0.0.1"),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            database=os.getenv("MYSQL_DATABASE", "ad_ods"),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", "root"),
            autocommit=False,
            connection_timeout=10,
        )

    def cursor(self, dictionary=False):
        if self.conn is None or not self.conn.is_connected():
            self.connect()
        return self.conn.cursor(dictionary=dictionary)

    def load_reference_data(self):
        with self.cursor(dictionary=True) as cur:
            cur.execute("SELECT uid FROM user_info")
            self.users = [int(row["uid"]) for row in cur.fetchall()]
            cur.execute(
                "SELECT product_id, shop_id, price FROM product_info "
                "WHERE shop_id IS NOT NULL"
            )
            self.products = cur.fetchall()
            cur.execute(
                """
                SELECT a.advertiser_id, c.campaign_id, u.unit_id, cr.creative_id,
                       u.bid_type, u.bid
                FROM advertiser_info a
                JOIN campaign_info c ON c.advertiser_id=a.advertiser_id
                JOIN unit_info u ON u.campaign_id=c.campaign_id
                JOIN creative_info cr ON cr.unit_id=u.unit_id
                """
            )
            self.ad_keys = cur.fetchall()
            for table, key in (
                ("advertiser_info", "advertiser_id"),
                ("campaign_info", "campaign_id"),
                ("unit_info", "unit_id"),
                ("creative_info", "creative_id"),
                ("product_info", "product_id"),
                ("user_info", "uid"),
                ("shop_info", "shop_id"),
            ):
                cur.execute(f"SELECT {key} FROM {table}")
                self.dim_targets[table] = [int(row[key]) for row in cur.fetchall()]
            cur.execute("SELECT COALESCE(MAX(order_id),0) AS max_id FROM order_detail")
            max_order = int(cur.fetchone()["max_id"])
            cur.execute("SELECT COALESCE(MAX(bill_id),0) AS max_id FROM bill_detail")
            max_bill = int(cur.fetchone()["max_id"])
        if not self.users or not self.products or not self.ad_keys:
            raise RuntimeError("MySQL seed data is incomplete: users/products/ad hierarchy required")
        self.order_ids = MonotonicId(max_order)
        self.bill_ids = MonotonicId(max_bill)
        self.reload_order_pools()

    def reload_order_pools(self):
        self.created_orders.clear()
        self.paid_orders.clear()
        self.confirmed_orders.clear()
        cutoff = db_now() - timedelta(hours=24)
        with self.cursor(dictionary=True) as cur:
            cur.execute(
                """
                SELECT order_id, order_status, create_time, pay_time, confirm_time
                FROM order_detail
                WHERE order_status IN (1,3,4) AND create_time >= %s
                ORDER BY create_time DESC LIMIT 20000
                """,
                (cutoff,),
            )
            for row in cur.fetchall():
                state = OrderState(
                    int(row["order_id"]), row["create_time"],
                    row["pay_time"], row["confirm_time"],
                )
                now = db_now()
                if state.create_time >= now:
                    continue
                if row["order_status"] == 1:
                    state.pay_ready_at = now
                    state.cancel_ready_at = now
                    self.created_orders[state.order_id] = state
                elif row["order_status"] == 3:
                    if state.pay_time is None or state.pay_time >= now:
                        continue
                    state.confirm_ready_at = now
                    state.refund_ready_at = now
                    self.paid_orders[state.order_id] = state
                else:
                    reference = state.confirm_time or state.pay_time
                    if reference is None or reference >= now:
                        continue
                    state.refund_ready_at = now
                    self.confirmed_orders[state.order_id] = state

    def create_order(self):
        now = db_now()
        order_id = self.order_ids.next()
        uid = self.rng.choice(self.users)
        product = self.rng.choice(self.products)
        quantity = self.rng.choices((1, 2, 3), weights=(82, 14, 4), k=1)[0]
        price = Decimal(product["price"])
        total = price * quantity
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO order_detail
                  (order_id,user_id,product_id,shop_id,product_price,product_num,
                   total_amount,payment_method,receiver_name,receiver_phone,
                   shipping_address,tracking_number,order_status,create_time,
                   cancel_time,pay_time,confirm_time,refund_time)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,1,%s,
                        NULL,NULL,NULL,NULL)
                """,
                (
                    order_id, uid, int(product["product_id"]), int(product["shop_id"]),
                    price, quantity, total, self.rng.choice((1, 2)),
                    f"收货人{uid % 10000}", f"138{uid % 100000000:08d}",
                    f"示例配送地址-{int(product['shop_id'])}", now,
                ),
            )
        self.created_orders[order_id] = OrderState(
            order_id=order_id,
            create_time=now,
            pay_ready_at=now + timedelta(seconds=self.rng.uniform(1, 10)),
            cancel_ready_at=now + timedelta(seconds=self.rng.uniform(2, 20)),
        )
        self.stats["order_create"] += 1

    def eligible_created(self, action, now):
        ready_field = "pay_ready_at" if action == "PAY" else "cancel_ready_at"
        return [state for state in self.created_orders.values()
                if getattr(state, ready_field) and now >= getattr(state, ready_field)]

    def pay_order(self):
        now = db_now()
        candidates = self.eligible_created("PAY", now)
        if not candidates:
            return False
        state = self.rng.choice(candidates)
        with self.cursor() as cur:
            cur.execute(
                "UPDATE order_detail SET order_status=3,pay_time=%s "
                "WHERE order_id=%s AND order_status=1 AND pay_time IS NULL "
                "AND cancel_time IS NULL",
                (now, state.order_id),
            )
            if cur.rowcount != 1:
                self.created_orders.pop(state.order_id, None)
                return False
        state.pay_time = now
        state.confirm_ready_at = now + timedelta(seconds=self.rng.uniform(5, 30))
        state.refund_ready_at = now + timedelta(seconds=self.rng.uniform(5, 60))
        self.created_orders.pop(state.order_id, None)
        self.paid_orders[state.order_id] = state
        self.stats["order_pay"] += 1
        return True

    def cancel_order(self):
        now = db_now()
        candidates = self.eligible_created("CANCEL", now)
        if not candidates:
            return False
        state = self.rng.choice(candidates)
        with self.cursor() as cur:
            cur.execute(
                "UPDATE order_detail SET order_status=2,cancel_time=%s "
                "WHERE order_id=%s AND order_status=1 AND pay_time IS NULL "
                "AND cancel_time IS NULL",
                (now, state.order_id),
            )
            if cur.rowcount != 1:
                self.created_orders.pop(state.order_id, None)
                return False
        self.created_orders.pop(state.order_id, None)
        self.stats["order_cancel"] += 1
        return True

    def confirm_order(self):
        now = db_now()
        candidates = [state for state in self.paid_orders.values()
                      if state.confirm_ready_at and now >= state.confirm_ready_at]
        if not candidates:
            return False
        state = self.rng.choice(candidates)
        with self.cursor() as cur:
            cur.execute(
                "UPDATE order_detail SET order_status=4,confirm_time=%s,"
                "tracking_number=%s WHERE order_id=%s AND order_status=3 "
                "AND confirm_time IS NULL AND refund_time IS NULL",
                (now, f"SF{state.order_id}", state.order_id),
            )
            if cur.rowcount != 1:
                self.paid_orders.pop(state.order_id, None)
                return False
        state.confirm_time = now
        state.refund_ready_at = now + timedelta(seconds=self.rng.uniform(5, 60))
        self.paid_orders.pop(state.order_id, None)
        self.confirmed_orders[state.order_id] = state
        self.stats["order_confirm"] += 1
        return True

    def refund_order(self):
        now = db_now()
        candidates = []
        for state in self.paid_orders.values():
            if state.refund_ready_at and now >= state.refund_ready_at:
                candidates.append((state, 3))
        for state in self.confirmed_orders.values():
            if state.refund_ready_at and now >= state.refund_ready_at:
                candidates.append((state, 4))
        if not candidates:
            return False
        state, prior_status = self.rng.choice(candidates)
        with self.cursor() as cur:
            cur.execute(
                "UPDATE order_detail SET order_status=5,refund_time=%s "
                "WHERE order_id=%s AND order_status=%s AND pay_time IS NOT NULL "
                "AND refund_time IS NULL",
                (now, state.order_id, prior_status),
            )
            if cur.rowcount != 1:
                self.paid_orders.pop(state.order_id, None)
                self.confirmed_orders.pop(state.order_id, None)
                return False
        self.paid_orders.pop(state.order_id, None)
        self.confirmed_orders.pop(state.order_id, None)
        self.stats["order_refund"] += 1
        return True

    def change_order(self):
        action = self.rng.choices(ORDER_ACTIONS, weights=ORDER_WEIGHTS, k=1)[0]
        changed = {
            "CREATE": lambda: (self.create_order() or True),
            "PAY": self.pay_order,
            "CONFIRM": self.confirm_order,
            "REFUND": self.refund_order,
            "CANCEL": self.cancel_order,
        }[action]()
        if not changed:
            self.create_order()

    def insert_bill(self):
        key = self.rng.choice(self.ad_keys)
        uid = self.rng.choice(self.users)
        mode = str(key["bid_type"]).upper().replace("-", "")
        billing_type = 1 if mode in {"CPM", "OCPM"} else 3 if mode in {"CPA", "OCPA"} else 2
        bid = Decimal(key["bid"])
        if billing_type == 1:
            raw_cost = bid / Decimal(1000)
        else:
            raw_cost = bid
        cost = max(15000, int(raw_cost * Decimal(str(self.rng.uniform(0.75, 1.25)))))
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bill_detail
                  (bill_id,advertiser_id,campaign_id,unit_id,creative_id,user_id,
                   slot_id,billing_type,media,commerce_scene,cost,bill_time)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    self.bill_ids.next(), int(key["advertiser_id"]),
                    int(key["campaign_id"]), int(key["unit_id"]),
                    int(key["creative_id"]), uid, self.rng.randint(101, 512),
                    billing_type, self.rng.choice(MEDIA), self.rng.choice(SCENES),
                    cost, db_now(),
                ),
            )
        self.stats["bill_insert"] += 1

    def update_dimension(self):
        operations = [
            ("advertiser_info", "advertiser_id", "status", lambda: self.rng.choice((1, 2))),
            ("campaign_info", "campaign_id", "daily_budget", lambda: self.rng.randint(100000, 900000000)),
            ("campaign_info", "campaign_id", "status", lambda: self.rng.choice((3, 4))),
            ("unit_info", "unit_id", "daily_budget", lambda: self.rng.randint(100000, 500000000)),
            ("unit_info", "unit_id", "bid", lambda: self.rng.randint(15000, 950000)),
            ("unit_info", "unit_id", "delivery_type", lambda: self.rng.choice((1, 2, 3))),
            ("creative_info", "creative_id", "status", lambda: self.rng.choice((0, 1))),
            ("product_info", "product_id", "price", lambda: self.rng.randint(500000, 500000000)),
            ("user_info", "uid", "user_level", lambda: self.rng.randint(0, 5)),
            ("shop_info", "shop_id", "status", lambda: self.rng.choice((0, 1))),
        ]
        table, key, column, value_factory = self.rng.choice(operations)
        targets = self.dim_targets.get(table) or []
        if not targets:
            return False
        with self.cursor() as cur:
            cur.execute(
                f"UPDATE {table} SET {column}=%s WHERE {key}=%s",
                (value_factory(), self.rng.choice(targets)),
            )
        self.stats["dim_update"] += 1
        return True

    def make_change(self):
        change_type = self.rng.choices(CHANGE_TYPES, weights=CHANGE_WEIGHTS, k=1)[0]
        if change_type == "BILL":
            self.insert_bill()
        elif change_type == "ORDER":
            self.change_order()
        else:
            self.update_dimension()
        self.stats["total_changes"] += 1
        self.pending_changes += 1

    def commit_if_needed(self, force=False):
        now = time.monotonic()
        if self.pending_changes and (force or self.pending_changes >= 25 or now - self.last_commit_at >= 0.75):
            self.conn.commit()
            self.pending_changes = 0
            self.last_commit_at = now

    def report(self, final=False):
        now = time.monotonic()
        interval = max(0.001, now - self.last_report_at)
        current_total = self.stats["total_changes"]
        current_rate = (current_total - self.last_report_total) / interval
        label = "final" if final else "mysql-generator"
        print(
            f"[{label}] elapsed={now-self.started_at:.1f}s "
            f"total_changes={current_total} current_rate={current_rate:.1f}/s\n"
            f"bill_insert={self.stats['bill_insert']}\n"
            f"order_create={self.stats['order_create']} order_pay={self.stats['order_pay']} "
            f"order_confirm={self.stats['order_confirm']} "
            f"order_refund={self.stats['order_refund']} "
            f"order_cancel={self.stats['order_cancel']}\n"
            f"dim_update={self.stats['dim_update']}\n"
            f"created_pending={len(self.created_orders)} "
            f"paid_pending={len(self.paid_orders)} "
            f"confirmed={len(self.confirmed_orders)}",
            flush=True,
        )
        self.last_report_at = now
        self.last_report_total = current_total

    def stop(self, *_):
        self.running = False

    def run(self):
        while self.running:
            try:
                self.connect()
                self.load_reference_data()
                break
            except Exception as exc:
                print(f"[mysql-generator] waiting for MySQL seed data: {exc}", flush=True)
                time.sleep(2)
        print(
            f"[mysql-generator] started target_rate={self.rate:g}/s "
            f"duration={'unlimited' if self.duration is None else f'{self.duration:g}s'}",
            flush=True,
        )
        next_window = time.monotonic()
        while self.running:
            elapsed = time.monotonic() - self.started_at
            if self.duration is not None and elapsed >= self.duration:
                break
            window_start = next_window
            window_end = window_start + 1.0
            target = max(1, round(self.rate * self.rng.uniform(0.88, 1.12)))
            offsets = sorted(self.rng.uniform(0.0, 0.94) for _ in range(target))
            for offset in offsets:
                if not self.running:
                    break
                if self.duration is not None and time.monotonic() - self.started_at >= self.duration:
                    self.running = False
                    break
                delay = window_start + offset - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                try:
                    self.make_change()
                    self.commit_if_needed()
                except (MySQLError, ValueError, RuntimeError) as exc:
                    print(f"[mysql-generator] change failed, rolling back: {exc}", flush=True)
                    try:
                        self.conn.rollback()
                    except Exception:
                        pass
                    self.pending_changes = 0
                    try:
                        self.connect()
                        self.reload_order_pools()
                    except Exception as reconnect_exc:
                        print(f"[mysql-generator] reconnect failed: {reconnect_exc}", flush=True)
                        time.sleep(1)
            try:
                self.commit_if_needed(force=True)
            except MySQLError as exc:
                print(f"[mysql-generator] commit failed: {exc}", flush=True)
                self.conn.rollback()
                self.pending_changes = 0
            now = time.monotonic()
            if now - self.last_report_at >= 5:
                self.report()
            next_window = window_end
            remaining = next_window - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)
            elif remaining < -1:
                next_window = time.monotonic()
        try:
            self.commit_if_needed(force=True)
        finally:
            if self.conn is not None:
                self.conn.close()
            self.report(final=True)


def parse_args():
    parser = argparse.ArgumentParser(description="Generate MySQL changes for Flink CDC")
    parser.add_argument("--rate", type=float, default=30.0, help="target row changes per second")
    parser.add_argument("--duration", type=float, default=None, help="run duration in seconds")
    parser.add_argument(
        "--seed", type=int,
        default=int(os.getenv("MYSQL_GENERATOR_RANDOM_SEED", "20260824")),
        help="random seed",
    )
    args = parser.parse_args()
    if args.rate <= 0:
        parser.error("--rate must be greater than zero")
    if args.duration is not None and args.duration <= 0:
        parser.error("--duration must be greater than zero")
    return args


def main():
    args = parse_args()
    generator = MySQLChangeGenerator(args.rate, args.duration, args.seed)
    signal.signal(signal.SIGINT, generator.stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, generator.stop)
    generator.run()


if __name__ == "__main__":
    main()
