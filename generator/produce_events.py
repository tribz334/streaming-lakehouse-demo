import json
import hashlib
import math
import os
import random
import time
from datetime import datetime, timezone, timedelta

import mysql.connector
from kafka import KafkaProducer


BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-node-1:9092")
TOPIC = os.getenv("TOPIC", "ods_log")
INTERVAL = float(os.getenv("EVENT_INTERVAL_SECONDS", "0.25"))
NODE_ID = os.getenv("GENERATOR_NODE_ID", "ingest-node-1")
NODE_NUMBER = int(os.getenv("GENERATOR_NODE_NUMBER", "1")) & 0x3FF
RANDOM_SEED = int(os.getenv("GENERATOR_RANDOM_SEED", "20260713"))
HISTORY_DAYS = int(os.getenv("GENERATOR_HISTORY_DAYS", "0"))
HISTORY_EVENTS_PER_DAY = int(os.getenv("GENERATOR_HISTORY_EVENTS_PER_DAY", "1800"))
ATTRIBUTION_DEMO_ORDERS = int(os.getenv("GENERATOR_ATTRIBUTION_DEMO_ORDERS", "500"))
LIVE_ORDER_EVERY = int(os.getenv("GENERATOR_LIVE_ORDER_EVERY", "5"))
HISTORY_START_DATE = os.getenv("GENERATOR_HISTORY_START_DATE", "").strip()
HISTORY_END_DATE = os.getenv("GENERATOR_HISTORY_END_DATE", "").strip()
FRAUD_INJECTION_ENABLED = os.getenv("FRAUD_INJECTION_ENABLED", "true").lower() == "true"
FRAUD_BURST_EVERY = int(os.getenv("FRAUD_BURST_EVERY", "180"))
FRAUD_BURST_SIZE = int(os.getenv("FRAUD_BURST_SIZE", "36"))
FRAUD_USER_POOL = int(os.getenv("FRAUD_USER_POOL", "3"))
TZ = timezone(timedelta(hours=8))
BUS_ID = int(os.getenv("GENERATOR_BUS_ID", "10"))
APP_ID = int(os.getenv("GENERATOR_APP_ID", "80"))
AD_LOG_ID = int(os.getenv("GENERATOR_AD_LOG_ID", "1234"))
MEDIA = ["douyin", "kuaishou", "bilibili", "xiaohongshu", "toutiao", "weibo"]
MEDIA_PROFILES = {
    "douyin": (28, 1.10, 1.06, 1.12),
    "kuaishou": (18, 1.02, 1.08, 0.94),
    "bilibili": (13, 0.82, 1.13, 1.08),
    "xiaohongshu": (17, 0.96, 1.24, 1.18),
    "toutiao": (14, 0.88, 0.90, 0.86),
    "weibo": (10, 0.91, 0.86, 0.98),
}
COMMERCE_SCENES = ["live", "short_video", "shop", "external"]
COMMERCE_SCENE_PROFILES = {
    # traffic weight, click lift, conversion lift, average-order-value lift
    "live": (35, 1.12, 1.22, 1.05),
    "short_video": (45, 1.18, 0.96, 0.82),
    "shop": (20, 0.86, 1.12, 1.28),
    "external": (12, 0.92, 0.72, 0.90),
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
ATTRIBUTION_BUCKETS = [
    "natural", "direct_30m",
]
ATTRIBUTION_BUCKET_WEIGHTS = [25, 75]


ID_EPOCH_MS = int(datetime(2026, 1, 1, tzinfo=TZ).timestamp() * 1000)
_last_id_millis = -1
_id_sequence = 0


def next_bigint_id(moment=None):
    """Return a positive 63-bit time/node/sequence ID without hashing or UUIDs."""
    global _last_id_millis, _id_sequence
    current = int((moment or datetime.now(TZ)).timestamp() * 1000)
    if current == _last_id_millis:
        _id_sequence = (_id_sequence + 1) & 0xFFF
        if _id_sequence == 0:
            current += 1
    else:
        _id_sequence = 0
    _last_id_millis = current
    return ((current - ID_EPOCH_MS) << 22) | (NODE_NUMBER << 12) | _id_sequence


def anonymous_user_id(raw_id):
    """Map the simulated user number directly to a BIGINT namespace."""
    return 10_000_000 + int(raw_id)


def epoch_millis(timestamp):
    return int(datetime.fromisoformat(timestamp).timestamp() * 1000)


def simulated_area(region):
    """Return a stable six-digit area code for the simulated region."""
    return f"{100000 + int(hashlib.sha256(region.encode('utf-8')).hexdigest()[:8], 16) % 900000:06d}"


def common_fields(event):
    user_id = event["user_id"]
    numeric_user_id = int(user_id)
    return {
        "area": simulated_area(event.get("region", "unknown")),
        "ip": f"10.{numeric_user_id % 251}.{numeric_user_id // 251 % 251}.{numeric_user_id // 63001 % 251}",
        "uid": user_id,
        "device_id": event.get("device_id", 20_000_000 + numeric_user_id),
        "platform": 0 if event.get("media") != "weibo" else 1,
        "app_version": "1.0.0",
        "browser_version": "" if event.get("media") != "weibo" else "Chrome/128",
        "sdk_version": "3.3.0",
    }


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
            SELECT a.advertiser_id, a.industry_l2_name AS industry,
                CASE WHEN MOD(a.advertiser_id, 3)=0 THEN 'KA' ELSE 'Growth' END AS tier,
                c.campaign_id, '电商下单推广' AS promotion_goal,
                c.budget / 100000 AS budget,
                u.unit_id, u.product_id,
                u.bid_type AS billing_mode,
                u.bid / 100000 AS bid_amount, cr.creative_id
            FROM advertiser_info a
            JOIN campaign_info c ON a.advertiser_id = c.advertiser_id
            JOIN unit_info u ON c.campaign_id = u.campaign_id
            JOIN creative_info cr ON u.unit_id = cr.unit_id
            """
        )
        return cursor.fetchall()


def maybe_write_order(event):
    if event["event_type"] != "order":
        return
    with mysql_conn() as conn:
        cursor = conn.cursor()
        event_dt = datetime.fromisoformat(event["ts"])
        event_time = event_dt.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
        cancel_time = None
        confirm_time = None
        refund_time = None
        refund_finish_time = None
        finish_time = None
        order_status = 3

        # Historical demo orders cover all terminal paths. Live orders stay
        # open so their later CDC transitions can be demonstrated explicitly.
        if datetime.now(TZ) - event_dt >= timedelta(days=2):
            lifecycle_bucket = int(event["order_id"]) % 100
            if lifecycle_bucket < 20:
                confirm_time = (event_dt + timedelta(hours=2)).replace(tzinfo=None)
                finish_time = (event_dt + timedelta(days=1)).replace(tzinfo=None)
                order_status = 7
            elif lifecycle_bucket < 30:
                cancel_time = (event_dt + timedelta(minutes=30)).replace(tzinfo=None)
                order_status = 2
            elif lifecycle_bucket < 40:
                confirm_time = (event_dt + timedelta(hours=2)).replace(tzinfo=None)
                refund_time = (event_dt + timedelta(days=1)).replace(tzinfo=None)
                refund_finish_time = (event_dt + timedelta(days=2)).replace(tzinfo=None)
                order_status = 6
        amount = int(round(float(event["gmv"]) * 100000))
        payment_method = 1 + int(event["order_id"]) % 2
        receiver_name = f"收货人{int(event['user_id']) % 10000}"
        receiver_phone = f"138{int(event['user_id']) % 100000000:08d}"
        tracking_number = f"SF{event['order_id']}" if order_status >= 4 else None
        cursor.execute(
            """
            INSERT INTO user_info
              (uid, user_name, gender, phone_hash, email, user_level, birthday,
               status, created_at, updated_at)
            VALUES (%s, CONCAT('用户', %s), MOD(%s, 2), NULL, NULL, 1, NULL,
                    0, %s, %s)
            ON DUPLICATE KEY UPDATE updated_at=GREATEST(updated_at, VALUES(updated_at))
            """,
            (event["user_id"], event["user_id"], event["user_id"], event_time, event_time),
        )
        cursor.execute(
            "SELECT shop_id FROM product_info WHERE product_id = %s",
            (event["product_id"],),
        )
        product_row = cursor.fetchone()
        shop_id = product_row[0] if product_row else None
        shipping_address = f"示例配送地址-{int(shop_id or 0) % 1000}"
        cursor.execute(
            """
            INSERT INTO order_detail
            (order_id, user_id, product_id, shop_id, product_price, product_num,
             total_amount, payment_method, receiver_name, receiver_phone,
             shipping_address, tracking_number, order_status,
             create_time, cancel_time, payment_time, confirm_time, refund_time,
             refund_finish_time, finish_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              shop_id=VALUES(shop_id),
              order_status=VALUES(order_status),
              product_price=VALUES(product_price),
              product_num=VALUES(product_num),
              total_amount=VALUES(total_amount),
              payment_method=VALUES(payment_method),
              receiver_name=VALUES(receiver_name),
              receiver_phone=VALUES(receiver_phone),
              shipping_address=VALUES(shipping_address),
              tracking_number=VALUES(tracking_number),
              cancel_time=VALUES(cancel_time),
              payment_time=VALUES(payment_time),
              confirm_time=VALUES(confirm_time),
              refund_time=VALUES(refund_time),
              refund_finish_time=VALUES(refund_finish_time),
              finish_time=VALUES(finish_time)
            """,
            (
                event["order_id"],
                event["user_id"],
                event["product_id"],
                shop_id,
                amount,
                1,
                amount,
                payment_method,
                receiver_name,
                receiver_phone,
                shipping_address,
                tracking_number,
                order_status,
                event_time,
                cancel_time,
                event_time,
                confirm_time,
                refund_time,
                refund_finish_time,
                finish_time,
            ),
        )
        conn.commit()


def maybe_write_bill(event):
    """Persist an immutable charge when the event matches its billing mode."""
    if (
        event["event_type"] != billable_event_type(event.get("billing_mode"))
        or float(event.get("spend") or 0) <= 0
    ):
        return
    with mysql_conn() as conn:
        cursor = conn.cursor()
        event_time = event["ts"].replace("T", " ").split("+")[0]
        cursor.execute(
            """
            INSERT INTO user_info
              (uid, user_name, gender, phone_hash, email, user_level, birthday,
               status, created_at, updated_at)
            VALUES (%s, CONCAT('用户', %s), MOD(%s, 2), NULL, NULL, 1, NULL,
                    0, %s, %s)
            ON DUPLICATE KEY UPDATE updated_at=GREATEST(updated_at, VALUES(updated_at))
            """,
            (event["user_id"], event["user_id"], event["user_id"], event_time, event_time),
        )
        cursor.execute(
            """
            INSERT IGNORE INTO bill_detail
            (bill_id, advertiser_id, campaign_id, unit_id, creative_id,
             user_id, slot_id, billing_type, media, commerce_scene, cost, bill_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event["event_id"],
                event["advertiser_id"], event["campaign_id"], event["unit_id"],
                event["creative_id"], event["user_id"], event["slot_id"],
                {"CPM": 1, "OCPM": 1, "CPC": 2, "OCPC": 2,
                 "CPA": 3, "OCPA": 3}[normalize_billing_mode(event["billing_mode"])],
                event["media"], event["commerce_scene"],
                int(round(float(event["spend"]) * 100000)),
                event_time,
            ),
        )
        conn.commit()


def stable_factor(value, low=0.82, high=1.18):
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    ratio = int.from_bytes(digest[:4], "big") / 0xFFFFFFFF
    return low + (high - low) * ratio


def normalize_billing_mode(value):
    """Normalize configured billing labels while preserving common oCPX modes."""
    return str(value or "CPC").strip().upper().replace("-", "")


def billable_event_type(billing_mode):
    """Return the event that creates an immutable charge for one billing mode."""
    mode = normalize_billing_mode(billing_mode)
    if mode in {"CPM", "OCPM"}:
        return "impression"
    if mode in {"CPC", "OCPC"}:
        return "click"
    if mode in {"CPA", "OCPA"}:
        return "conversion"
    raise ValueError(f"unsupported billing mode: {billing_mode}")


def calculate_spend(event_type, billing_mode, bid_price, media_cost):
    """Calculate one bill in yuan; CPM/oCPM prices are quoted per 1,000 views."""
    mode = normalize_billing_mode(billing_mode)
    if event_type != billable_event_type(mode):
        return 0.0
    adjusted_bid = max(0.0, float(bid_price)) * float(media_cost)
    if mode in {"CPM", "OCPM"}:
        return round(adjusted_bid / 1000.0, 4)
    return round(max(0.15, adjusted_bid), 4)


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
    commerce_scene = rng.choices(
        COMMERCE_SCENES,
        weights=[COMMERCE_SCENE_PROFILES[name][0] for name in COMMERCE_SCENES],
        k=1,
    )[0]
    _, media_click, media_conversion, media_cost = MEDIA_PROFILES[media]
    _, scene_click, scene_conversion, scene_order_value = COMMERCE_SCENE_PROFILES[commerce_scene]
    industry_click, industry_conversion, average_order = INDUSTRY_PROFILES.get(
        key.get("industry"), (1.0, 1.0, 150.0)
    )
    advertiser_factor = stable_factor(str(key["advertiser_id"]))
    promotion_goal = key.get("promotion_goal") or "电商下单推广"
    objective_click = 1.18 if promotion_goal in ("品牌活动推广", "快手号推广") else 1.0
    objective_conversion = 1.20 if promotion_goal in ("电商下单推广", "销售线索收集") else 0.90
    click_rate = min(
        0.18,
        0.075 * media_click * scene_click * industry_click * objective_click * advertiser_factor,
    )
    conversion_rate = min(
        0.32,
        0.14 * media_conversion * scene_conversion * industry_conversion * objective_conversion,
    )
    order_rate = min(0.72, 0.46 * industry_conversion * objective_conversion)
    event_type = rng.choices(
        ["impression", "click", "conversion", "order"],
        weights=[1.0, click_rate, click_rate * conversion_rate, click_rate * conversion_rate * order_rate],
        k=1,
    )[0]
    if commerce_scene == "external" and event_type == "order":
        event_type = "conversion"
    gmv = 0.0
    order_id = None
    bid_amount = float(key.get("bid_amount") or 2.5)
    billing_mode = normalize_billing_mode(key.get("billing_mode"))
    bid_price = round(max(0.2, rng.lognormvariate(math.log(bid_amount), 0.22)), 4)
    spend = calculate_spend(event_type, billing_mode, bid_price, media_cost)
    if event_type == "order":
        promotion_lift = 1.35 if moment.day in (1, 8, 18, 28) else 1.0
        gmv = round(max(
            9.9,
            rng.lognormvariate(
                math.log(average_order * scene_order_value * promotion_lift),
                0.48,
            ),
        ), 2)
        order_id = next_bigint_id(moment)
    ts = moment.isoformat(timespec="milliseconds")
    return {
        "event_id": next_bigint_id(moment),
        "ts": ts,
        "advertiser_id": key["advertiser_id"],
        "campaign_id": key["campaign_id"],
        "product_id": key["product_id"],
        "slot_id": (MEDIA.index(media) + 1) * 100 + rng.randint(1, 12),
        "unit_id": key["unit_id"],
        "creative_id": key["creative_id"],
        "media": media,
        "commerce_scene": commerce_scene,
        "traffic_type": "paid",
        "region": rng.choices(REGIONS, weights=[8, 2, 5, 3, 2, 5, 2, 2, 9, 8, 8, 5, 4, 3, 7, 7, 5, 5, 10, 4, 2, 4, 7, 3, 3, 1, 4, 2, 1, 1, 2], k=1)[0],
        "user_id": anonymous_user_id(f"{rng.randint(1, 12000):05d}"),
        "event_type": event_type,
        "billing_mode": billing_mode,
        "bid_price": bid_price,
        "spend": spend,
        "gmv": gmv,
        "order_id": order_id,
    }


def choose_attribution_bucket(rng):
    return rng.choices(ATTRIBUTION_BUCKETS, weights=ATTRIBUTION_BUCKET_WEIGHTS, k=1)[0]


def attach_attribution_journey(order_event, rng, bucket=None, stable_suffix=None):
    """Make every order follow a controlled attribution path.

    Orders get a dedicated user so the intended touchpoint is not overridden by
    unrelated random clicks from the general traffic stream.
    """
    bucket = bucket or choose_attribution_bucket(rng)
    journey_key = stable_suffix or order_event["event_id"]
    order_event["user_id"] = 30_000_000 + int(journey_key) % 10_000_000
    click = make_attribution_click(order_event, rng, bucket=bucket)
    if click:
        click["user_id"] = order_event["user_id"]
        order_event["traffic_type"] = "paid"
    else:
        order_event["traffic_type"] = "organic"
        order_event["commerce_scene"] = "shop"
    return click


def make_attribution_click(order_event, rng, bucket=None):
    """Create a reproducible last-click journey for a generated order.

    Buckets are deliberately generated across the full 30-day horizon so the
    attribution BI page is useful immediately after the historical backfill.
    A missing click represents an organic/direct-store order.
    """
    bucket = bucket or choose_attribution_bucket(rng)
    if bucket == "natural":
        return None

    lag_ranges = {
        # Keep demo touchpoints within the 5-second watermark tolerance while
        # the Flink rule itself still accepts the complete 30-minute window.
        "direct_30m": (1, 4),
    }
    lag_seconds = rng.randint(*lag_ranges[bucket])
    click = dict(order_event)
    click["event_id"] = next_bigint_id(datetime.fromisoformat(order_event["ts"]))
    click["ts"] = (
        datetime.fromisoformat(order_event["ts"]) - timedelta(seconds=lag_seconds)
    ).isoformat(timespec="milliseconds")
    click["event_type"] = "click"
    click["spend"] = calculate_spend(
        "click",
        click.get("billing_mode"),
        click["bid_price"],
        MEDIA_PROFILES[click["media"]][3],
    )
    click["gmv"] = 0.0
    click["order_id"] = None
    return click


def make_attribution_impression(click_event, rng):
    """Create the impression immediately preceding an attributed click."""
    impression = dict(click_event)
    impression["event_id"] = next_bigint_id(datetime.fromisoformat(click_event["ts"]))
    impression["ts"] = (
        datetime.fromisoformat(click_event["ts"]) - timedelta(seconds=rng.randint(2, 20))
    ).isoformat(timespec="milliseconds")
    impression["event_type"] = "impression"
    impression["spend"] = calculate_spend(
        "impression",
        impression.get("billing_mode"),
        impression["bid_price"],
        MEDIA_PROFILES[impression["media"]][3],
    )
    return impression


def send_event(producer, event):
    """Wrap one simulated action in the raw SDK report written to ODS."""
    action_names = {
        "impression": "impression",
        "click": "click",
        "conversion": "conversion",
    }
    event_ts = epoch_millis(event["ts"])
    action = {
        "event_id": event["event_id"],
        "action": action_names[event["event_type"]],
        "creative_id": event["creative_id"],
        "product_id": event["product_id"],
        "slot_id": event["slot_id"],
        "media": event["media"],
        "commerce_scene": event["commerce_scene"],
        "traffic_type": event["traffic_type"],
        "play_during": int(event.get("play_during", 0)),
        "ts": event_ts,
    }
    actions = [action]
    if event["event_type"] == "impression":
        actions.insert(0, {**action, "event_id": next_bigint_id(datetime.fromisoformat(event["ts"])),
                           "action": "delivery", "play_during": 0})

    report = {
        "bus_id": BUS_ID,
        "app_id": APP_ID,
        "log_id": AD_LOG_ID,
        "msg_id": next_bigint_id(datetime.fromisoformat(event["ts"])),
        "common": common_fields(event),
        "actions": actions,
        "ts": event_ts,
    }
    producer.send(TOPIC, key=event["user_id"], value=report)
    try:
        maybe_write_bill(event)
    except Exception as exc:
        print(f"ad bill mysql write failed: {exc}", flush=True)


def send_attribution_journey(producer, click_event, rng):
    if click_event:
        send_event(producer, make_attribution_impression(click_event, rng))
        send_event(producer, click_event)


def make_live_attribution_order(keys, scene, sequence, rng):
    """Guarantee fresh closed-loop GMV for the rolling two-hour dashboard."""
    order = make_event(keys, event_time=datetime.now(TZ), rng=rng)
    order["order_id"] = next_bigint_id(datetime.now(TZ))
    order["event_type"] = "order"
    order["commerce_scene"] = scene
    order["traffic_type"] = "paid"
    order["spend"] = 0.0
    order["gmv"] = round(rng.uniform(120.0, 1800.0), 2)
    click = attach_attribution_journey(order, rng, bucket="direct_30m")
    return order, click


def make_demo_attribution_order(keys, day, bucket, index, rng):
    """Build an idempotent order journey on a fabricated business timeline."""
    moment = datetime(day.year, day.month, day.day, 8 + index % 15, index % 60, tzinfo=TZ)
    order = make_event(keys, event_time=moment, rng=rng)
    stable_id = int(day.strftime("%Y%m%d")) * 1_000_000 + NODE_NUMBER * 100_000 + index
    order["event_id"] = stable_id
    order["order_id"] = stable_id + 10_000_000_000_000
    order["event_type"] = "order"
    order["commerce_scene"] = COMMERCE_SCENES[index % 3]
    order["spend"] = 0.0
    order["gmv"] = round(80.0 + index * 35.0 + stable_factor(str(stable_id), 0.0, 120.0), 2)
    click = attach_attribution_journey(order, rng, bucket=bucket, stable_suffix=stable_id)
    if click:
        click["event_id"] = stable_id + 20_000_000_000_000
    return order, click


def attribution_demo_buckets(total):
    """Allocate a deterministic cohort while preserving the configured weights."""
    if total <= 0:
        return []
    weight_sum = sum(ATTRIBUTION_BUCKET_WEIGHTS)
    exact_counts = [total * weight / weight_sum for weight in ATTRIBUTION_BUCKET_WEIGHTS]
    counts = [math.floor(value) for value in exact_counts]
    remainder = total - sum(counts)
    order = sorted(
        range(len(counts)),
        key=lambda index: exact_counts[index] - counts[index],
        reverse=True,
    )
    for index in order[:remainder]:
        counts[index] += 1
    return [
        bucket
        for bucket, count in zip(ATTRIBUTION_BUCKETS, counts)
        for _ in range(count)
    ]


def historical_dates(now):
    last_complete_day = (now - timedelta(days=1)).date()
    if HISTORY_START_DATE and HISTORY_END_DATE:
        start = datetime.strptime(HISTORY_START_DATE, "%Y-%m-%d").date()
        configured_end = datetime.strptime(HISTORY_END_DATE, "%Y-%m-%d").date()
        end = min(configured_end, last_complete_day)
        if start > end and start <= configured_end:
            return []
        if start > end:
            raise ValueError("GENERATOR_HISTORY_START_DATE must not be after GENERATOR_HISTORY_END_DATE")
        return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]
    return [(now - timedelta(days=days_ago)).date() for days_ago in range(HISTORY_DAYS, 0, -1)]


def historical_moments(days, rng):
    for day in days:
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
    total = 0
    virtual_now = datetime.now(TZ)
    days = historical_dates(virtual_now)

    demo_buckets = attribution_demo_buckets(ATTRIBUTION_DEMO_ORDERS)
    rng.shuffle(demo_buckets)
    demo_events_by_day = {day: [] for day in days}
    for index, bucket in enumerate(demo_buckets):
        day = days[index % len(days)] if days else (virtual_now - timedelta(days=1)).date()
        order, click = make_demo_attribution_order(keys, day, bucket, index, rng)
        if click:
            demo_events_by_day.setdefault(day, []).extend([
                make_attribution_impression(click, rng),
                click,
            ])
        demo_events_by_day.setdefault(day, []).append(order)

    for day, moments in historical_moments(days, rng):
        day_events = []
        for moment in moments:
            event = make_event(keys, event_time=moment, rng=rng)
            if event["event_type"] == "order":
                attribution_click = attach_attribution_journey(event, rng)
                if attribution_click:
                    day_events.extend([
                        make_attribution_impression(attribution_click, rng),
                        attribution_click,
                    ])
            day_events.append(event)

        day_events.extend(demo_events_by_day.get(day, []))
        day_events.sort(key=lambda item: item["ts"])
        ad_log_count = 0
        order_count = 0
        for event in day_events:
            if event["event_type"] == "order":
                maybe_write_order(event)
                order_count += 1
            else:
                send_event(producer, event)
                ad_log_count += 1
        total += ad_log_count
        producer.flush(timeout=30)
        print(
            f"{NODE_ID} historical day ready: date={day} "
            f"ad_log_events={ad_log_count} mysql_orders={order_count}",
            flush=True,
        )

    if demo_buckets:
        print(
            f"{NODE_ID} attribution demo cohort ready: orders={len(demo_buckets)}",
            flush=True,
        )
    return total


def make_fraud_burst(keys):
    key = random.choice(keys)
    media = random.choice(MEDIA)
    media_cost = MEDIA_PROFILES[media][3]
    billing_mode = normalize_billing_mode(key.get("billing_mode"))
    region = random.choice(REGIONS)
    ts = datetime.now(TZ).isoformat(timespec="milliseconds")
    users = [90_000_000 + idx for idx in range(1, FRAUD_USER_POOL + 1)]

    def fraud_event(event_type, index):
        bid_price = round(random.uniform(0.6, 8.5), 4)
        return {
            "event_id": next_bigint_id(datetime.fromisoformat(ts)),
            "ts": ts,
            "advertiser_id": key["advertiser_id"],
            "campaign_id": key["campaign_id"],
            "product_id": key["product_id"],
            "slot_id": (MEDIA.index(media) + 1) * 100 + random.randint(1, 12),
            "unit_id": key["unit_id"],
            "creative_id": key["creative_id"],
            "media": media,
            "commerce_scene": random.choices(
                COMMERCE_SCENES,
                weights=[COMMERCE_SCENE_PROFILES[name][0] for name in COMMERCE_SCENES],
                k=1,
            )[0],
            "traffic_type": "paid",
            "region": region,
            "user_id": users[index % len(users)],
            "event_type": event_type,
            "billing_mode": billing_mode,
            "bid_price": bid_price,
            "spend": calculate_spend(event_type, billing_mode, bid_price, media_cost),
            "gmv": 0.0,
            "order_id": None,
        }

    impression_count = max(1, FRAUD_BURST_SIZE // 12)
    return (
        [fraud_event("impression", index) for index in range(impression_count)]
        + [fraud_event("click", index) for index in range(FRAUD_BURST_SIZE)]
    )


def main():
    rng = random.Random(RANDOM_SEED + sum(ord(char) for char in NODE_ID))
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda value: str(value).encode("utf-8"),
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
        if event["event_type"] == "order":
            attribution_click = attach_attribution_journey(event, rng)
            send_attribution_journey(producer, attribution_click, rng)
            try:
                maybe_write_order(event)
            except Exception as exc:
                print(f"order mysql write failed: {exc}", flush=True)
        else:
            send_event(producer, event)
        produced += 1

        if LIVE_ORDER_EVERY > 0 and produced % LIVE_ORDER_EVERY == 0:
            scene_index = (produced // LIVE_ORDER_EVERY - 1) % len(COMMERCE_SCENES)
            live_order, live_click = make_live_attribution_order(
                keys, COMMERCE_SCENES[scene_index], produced // LIVE_ORDER_EVERY, rng
            )
            send_attribution_journey(producer, live_click, rng)
            try:
                maybe_write_order(live_order)
            except Exception as exc:
                print(f"live order cdc side-write failed: {exc}", flush=True)

        if FRAUD_INJECTION_ENABLED and FRAUD_BURST_EVERY > 0 and produced % FRAUD_BURST_EVERY == 0:
            burst = make_fraud_burst(keys)
            for fraud_event in burst:
                send_event(producer, fraud_event)
            print(
                f"{NODE_ID} injected fraud burst: events={len(burst)} every={FRAUD_BURST_EVERY} size={FRAUD_BURST_SIZE}",
                flush=True,
            )

        producer.flush(timeout=5)
        live_intensity = traffic_intensity(datetime.now(TZ))
        jitter = rng.uniform(0.82, 1.18)
        time.sleep(max(0.03, INTERVAL / (live_intensity * jitter)))


if __name__ == "__main__":
    main()
