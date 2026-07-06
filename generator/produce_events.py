import json
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
FRAUD_INJECTION_ENABLED = os.getenv("FRAUD_INJECTION_ENABLED", "true").lower() == "true"
FRAUD_BURST_EVERY = int(os.getenv("FRAUD_BURST_EVERY", "180"))
FRAUD_BURST_SIZE = int(os.getenv("FRAUD_BURST_SIZE", "36"))
FRAUD_USER_POOL = int(os.getenv("FRAUD_USER_POOL", "3"))
TZ = timezone(timedelta(hours=8))
MEDIA = ["douyin", "kuaishou", "bilibili", "xiaohongshu", "toutiao", "weibo"]
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
            SELECT a.advertiser_id, c.campaign_id, u.unit_id, cr.creative_id
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


def make_event(keys):
    key = random.choice(keys)
    event_type = random.choices(["impression", "click", "conversion", "order"], weights=[68, 20, 8, 4], k=1)[0]
    spend = 0.0
    gmv = 0.0
    order_id = None
    if event_type in ("click", "conversion", "order"):
        spend = round(random.uniform(0.4, 9.0), 4)
    if event_type == "order":
        gmv = round(spend * random.uniform(4.0, 18.0), 2)
        order_id = f"ord_{uuid.uuid4().hex[:12]}"
    ts = datetime.now(TZ).isoformat(timespec="milliseconds")
    return {
        "event_id": uuid.uuid4().hex,
        "ts": ts,
        "advertiser_id": key["advertiser_id"],
        "campaign_id": key["campaign_id"],
        "unit_id": key["unit_id"],
        "creative_id": key["creative_id"],
        "media": random.choice(MEDIA),
        "region": random.choice(REGIONS),
        "user_id": f"user_{random.randint(1, 1800):04d}",
        "event_type": event_type,
        "bid_price": round(random.uniform(0.6, 8.5), 4),
        "spend": spend,
        "gmv": gmv,
        "order_id": order_id,
        "schema_version": 1,
    }


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
    print(f"producing ad events to {TOPIC} via {BOOTSTRAP}", flush=True)
    produced = 0
    while True:
        event = make_event(keys)
        producer.send(TOPIC, key=event["event_id"], value=event)
        produced += 1

        if FRAUD_INJECTION_ENABLED and FRAUD_BURST_EVERY > 0 and produced % FRAUD_BURST_EVERY == 0:
            burst = make_fraud_burst(keys)
            for fraud_event in burst:
                producer.send(TOPIC, key=fraud_event["event_id"], value=fraud_event)
            print(
                f"injected fraud burst: events={len(burst)} every={FRAUD_BURST_EVERY} size={FRAUD_BURST_SIZE}",
                flush=True,
            )

        producer.flush(timeout=5)
        try:
            maybe_write_order(event)
        except Exception as exc:
            print(f"order cdc side-write failed: {exc}", flush=True)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
