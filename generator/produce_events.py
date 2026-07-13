import json
import hashlib
import math
import os
import random
import time
import uuid
from datetime import datetime, timezone, timedelta

import mysql.connector
from kafka import KafkaProducer


BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("TOPIC", "ods_log")
INTERVAL = float(os.getenv("EVENT_INTERVAL_SECONDS", "0.25"))
NODE_ID = os.getenv("GENERATOR_NODE_ID", "ingest-node-1")
RANDOM_SEED = int(os.getenv("GENERATOR_RANDOM_SEED", "20260713"))
HISTORY_DAYS = int(os.getenv("GENERATOR_HISTORY_DAYS", "0"))
HISTORY_EVENTS_PER_DAY = int(os.getenv("GENERATOR_HISTORY_EVENTS_PER_DAY", "1800"))
FRAUD_INJECTION_ENABLED = os.getenv("FRAUD_INJECTION_ENABLED", "true").lower() == "true"
FRAUD_BURST_EVERY = int(os.getenv("FRAUD_BURST_EVERY", "180"))
FRAUD_BURST_SIZE = int(os.getenv("FRAUD_BURST_SIZE", "36"))
FRAUD_USER_POOL = int(os.getenv("FRAUD_USER_POOL", "3"))
TZ = timezone(timedelta(hours=8))
MEDIA = ["douyin", "kuaishou", "bilibili", "xiaohongshu", "toutiao", "weibo"]
MEDIA_PROFILES = {
    "douyin": (28, 1.10, 1.06, 1.12),
    "kuaishou": (18, 1.02, 1.08, 0.94),
    "bilibili": (13, 0.82, 1.13, 1.08),
    "xiaohongshu": (17, 0.96, 1.24, 1.18),
    "toutiao": (14, 0.88, 0.90, 0.86),
    "weibo": (10, 0.91, 0.86, 0.98),
}
INDUSTRY_PROFILES = {
    "ecommerce": (1.08, 1.15, 168.0),
    "game": (1.18, 0.78, 88.0),
    "education": (0.86, 1.04, 428.0),
    "local_service": (0.93, 1.12, 116.0),
    "beauty": (1.03, 1.20, 238.0),
    "technology": (0.92, 1.08, 228.0),
    "aerospace": (0.72, 0.68, 680.0),
    "short_video": (1.22, 0.92, 96.0),
    "sportswear": (1.02, 1.17, 358.0),
    "apparel": (1.01, 1.16, 298.0),
    "beverage": (1.10, 1.08, 48.0),
    "fmcg": (0.98, 1.18, 128.0),
    "household_care": (0.96, 1.20, 86.0),
    "consumer_electronics": (0.96, 1.10, 1880.0),
    "coffee": (1.06, 1.22, 36.0),
    "automotive": (0.78, 0.72, 3200.0),
    "travel": (0.90, 1.05, 860.0),
}
REGIONS = [
    "Beijing", "Tianjin", "Hebei", "Shanxi", "Inner Mongolia",
    "Liaoning", "Jilin", "Heilongjiang", "Shanghai", "Jiangsu",
    "Zhejiang", "Anhui", "Fujian", "Jiangxi", "Shandong",
    "Henan", "Hubei", "Hunan", "Guangdong", "Guangxi",
    "Hainan", "Chongqing", "Sichuan", "Guizhou", "Yunnan",
    "Tibet", "Shaanxi", "Gansu", "Qinghai", "Ningxia", "Xinjiang"
]


def mysql_conn():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "mysql"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        database=os.getenv("MYSQL_DATABASE", "ad_ods"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", "root"),
    )


def load_creatives():
    with mysql_conn() as conn:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT a.advertiser_id, a.industry, a.tier,
                   c.campaign_id, c.objective, c.budget,
                   u.unit_id, u.bid_amount, cr.creative_id
              FROM advertiser a
              JOIN campaign c ON a.advertiser_id = c.advertiser_id
              JOIN `unit` u ON c.campaign_id = u.campaign_id
              JOIN creative cr ON u.unit_id = cr.unit_id
            """
        )
        return cursor.fetchall()


def maybe_write_order(event):
    if event["event_type"] != "order":
        return
    with mysql_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO ad_order
            (order_id, advertiser_id, creative_id, user_id, gmv, order_status, create_time, payment_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE order_status=VALUES(order_status), gmv=VALUES(gmv)
            """,
            (
                event["order_id"],
                event["advertiser_id"],
                event["creative_id"],
                event["user_id"],
                event["gmv"],
                "paid",
                event["ts"].replace("T", " ").split("+")[0],
                event["ts"].replace("T", " ").split("+")[0],
            ),
        )
        conn.commit()


def stable_factor(value, low=0.82, high=1.18):
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    ratio = int.from_bytes(digest[:4], "big") / 0xFFFFFFFF
    return low + (high - low) * ratio


def traffic_intensity(moment):
    """Typical programmatic-ad traffic: lunch bump and a stronger evening peak."""
    hour = moment.hour + moment.minute / 60.0
    overnight = 0.25 + 0.10 * math.cos((hour - 3.0) * math.pi / 6.0)
    morning = 0.55 * math.exp(-((hour - 9.0) / 2.2) ** 2)
    lunch = 0.85 * math.exp(-((hour - 12.5) / 1.7) ** 2)
    evening = 1.45 * math.exp(-((hour - 20.5) / 2.5) ** 2)
    weekday_factor = 1.08 if moment.weekday() < 5 else 0.92
    return max(0.20, (overnight + morning + lunch + evening) * weekday_factor)


def daily_market_factor(day):
    trend = 1.0 + 0.035 * math.sin(day.toordinal() * 0.71)
    weekday = [1.04, 1.08, 1.06, 1.10, 1.16, 0.94, 0.88][day.weekday()]
    shock = stable_factor(day.isoformat(), 0.84, 1.20)
    return trend * weekday * shock


def choose_key(keys, rng):
    weights = []
    for key in keys:
        tier_lift = {"KA": 1.30, "Growth": 1.08, "SMB": 0.82}.get(key.get("tier"), 1.0)
        weights.append(max(1.0, float(key.get("budget") or 10000)) ** 0.5 * tier_lift)
    return rng.choices(keys, weights=weights, k=1)[0]


def make_event(keys, event_time=None, rng=random):
    moment = event_time or datetime.now(TZ)
    key = choose_key(keys, rng)
    media = rng.choices(MEDIA, weights=[MEDIA_PROFILES[name][0] for name in MEDIA], k=1)[0]
    _, media_click, media_conversion, media_cost = MEDIA_PROFILES[media]
    industry_click, industry_conversion, average_order = INDUSTRY_PROFILES.get(
        key.get("industry"), (1.0, 1.0, 150.0)
    )
    advertiser_factor = stable_factor(key["advertiser_id"])
    objective = key.get("objective") or "CTR"
    objective_click = 1.18 if objective == "CTR" else 1.0
    objective_conversion = 1.20 if objective in ("ROI", "GMV") else 0.90
    click_rate = min(0.18, 0.075 * media_click * industry_click * objective_click * advertiser_factor)
    conversion_rate = min(0.32, 0.14 * media_conversion * industry_conversion * objective_conversion)
    order_rate = min(0.72, 0.46 * industry_conversion * objective_conversion)
    event_type = rng.choices(
        ["impression", "click", "conversion", "order"],
        weights=[1.0, click_rate, click_rate * conversion_rate, click_rate * conversion_rate * order_rate],
        k=1,
    )[0]
    spend = 0.0
    gmv = 0.0
    order_id = None
    bid_amount = float(key.get("bid_amount") or 2.5)
    if event_type == "click":
        spend = round(max(0.15, rng.lognormvariate(math.log(bid_amount * media_cost), 0.28)), 4)
    if event_type == "order":
        promotion_lift = 1.35 if moment.day in (1, 8, 18, 28) else 1.0
        gmv = round(max(9.9, rng.lognormvariate(math.log(average_order * promotion_lift), 0.48)), 2)
        order_id = f"ord_{uuid.uuid4().hex[:12]}"
    ts = moment.isoformat(timespec="milliseconds")
    return {
        "event_id": uuid.uuid4().hex,
        "ts": ts,
        "advertiser_id": key["advertiser_id"],
        "campaign_id": key["campaign_id"],
        "unit_id": key["unit_id"],
        "creative_id": key["creative_id"],
        "media": media,
        "region": rng.choices(REGIONS, weights=[8, 2, 5, 3, 2, 5, 2, 2, 9, 8, 8, 5, 4, 3, 7, 7, 5, 5, 10, 4, 2, 4, 7, 3, 3, 1, 4, 2, 1, 1, 2], k=1)[0],
        "user_id": f"user_{rng.randint(1, 12000):05d}",
        "event_type": event_type,
        "bid_price": round(max(0.2, rng.lognormvariate(math.log(bid_amount), 0.22)), 4),
        "spend": spend,
        "gmv": gmv,
        "order_id": order_id,
        "schema_version": 1,
    }


def historical_moments(now, rng):
    for days_ago in range(HISTORY_DAYS, 0, -1):
        day = (now - timedelta(days=days_ago)).date()
        event_count = max(100, round(HISTORY_EVENTS_PER_DAY * daily_market_factor(day)))
        moments = []
        hour_weights = []
        for hour in range(24):
            sample = datetime(day.year, day.month, day.day, hour, 30, tzinfo=TZ)
            hour_weights.append(traffic_intensity(sample))
        for _ in range(event_count):
            hour = rng.choices(range(24), weights=hour_weights, k=1)[0]
            moments.append(datetime(
                day.year, day.month, day.day, hour,
                rng.randint(0, 59), rng.randint(0, 59), rng.randint(0, 999999), tzinfo=TZ,
            ))
        yield day, sorted(moments)


def produce_history(producer, keys, rng):
    if HISTORY_DAYS <= 0:
        return 0
    total = 0
    for day, moments in historical_moments(datetime.now(TZ), rng):
        for moment in moments:
            event = make_event(keys, event_time=moment, rng=rng)
            producer.send(TOPIC, key=event["event_id"], value=event)
            total += 1
        producer.flush(timeout=30)
        print(f"{NODE_ID} historical day ready: date={day} events={len(moments)}", flush=True)
    return total


def make_fraud_burst(keys):
    key = random.choice(keys)
    media = random.choice(MEDIA)
    region = random.choice(REGIONS)
    ts = datetime.now(TZ).isoformat(timespec="milliseconds")
    users = [f"user_fraud_{idx:03d}" for idx in range(1, FRAUD_USER_POOL + 1)]
    events = []

    for idx in range(max(1, FRAUD_BURST_SIZE // 12)):
        events.append(
            {
                "event_id": uuid.uuid4().hex,
                "ts": ts,
                "advertiser_id": key["advertiser_id"],
                "campaign_id": key["campaign_id"],
                "unit_id": key["unit_id"],
                "creative_id": key["creative_id"],
                "media": media,
                "region": region,
                "user_id": users[idx % len(users)],
                "event_type": "impression",
                "bid_price": round(random.uniform(0.6, 8.5), 4),
                "spend": 0.0,
                "gmv": 0.0,
                "order_id": None,
                "schema_version": 1,
            }
        )

    for idx in range(FRAUD_BURST_SIZE):
        spend = round(random.uniform(0.4, 9.0), 4)
        events.append(
            {
                "event_id": uuid.uuid4().hex,
                "ts": ts,
                "advertiser_id": key["advertiser_id"],
                "campaign_id": key["campaign_id"],
                "unit_id": key["unit_id"],
                "creative_id": key["creative_id"],
                "media": media,
                "region": region,
                "user_id": users[idx % len(users)],
                "event_type": "click",
                "bid_price": round(random.uniform(0.6, 8.5), 4),
                "spend": spend,
                "gmv": 0.0,
                "order_id": None,
                "schema_version": 1,
            }
        )
    return events


def main():
    rng = random.Random(RANDOM_SEED + sum(ord(char) for char in NODE_ID))
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )
    keys = []
    while not keys:
        try:
            keys = load_creatives()
        except Exception as exc:
            print(f"waiting for mysql seed data: {exc}", flush=True)
            time.sleep(2)
    print(f"{NODE_ID} producing ad events to {TOPIC} via {BOOTSTRAP}", flush=True)
    historical_count = produce_history(producer, keys, rng)
    if historical_count:
        print(f"{NODE_ID} historical backfill complete: events={historical_count}", flush=True)
    produced = 0
    while True:
        event = make_event(keys, rng=rng)
        producer.send(TOPIC, key=event["event_id"], value=event)
        produced += 1

        if FRAUD_INJECTION_ENABLED and FRAUD_BURST_EVERY > 0 and produced % FRAUD_BURST_EVERY == 0:
            burst = make_fraud_burst(keys)
            for fraud_event in burst:
                producer.send(TOPIC, key=fraud_event["event_id"], value=fraud_event)
            print(
                f"{NODE_ID} injected fraud burst: events={len(burst)} every={FRAUD_BURST_EVERY} size={FRAUD_BURST_SIZE}",
                flush=True,
            )

        producer.flush(timeout=5)
        try:
            maybe_write_order(event)
        except Exception as exc:
            print(f"order cdc side-write failed: {exc}", flush=True)
        live_intensity = traffic_intensity(datetime.now(TZ))
        jitter = rng.uniform(0.82, 1.18)
        time.sleep(max(0.03, INTERVAL / (live_intensity * jitter)))


if __name__ == "__main__":
    main()
