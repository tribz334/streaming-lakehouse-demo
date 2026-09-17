import importlib.util
import random
import sys
import types
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]


def load_generator_module():
    fluss = types.ModuleType("fluss")
    mysql = types.ModuleType("mysql")
    mysql.__path__ = []
    connector = types.ModuleType("mysql.connector")
    mysql.connector = connector
    sys.modules.setdefault("fluss", fluss)
    sys.modules.setdefault("mysql", mysql)
    sys.modules.setdefault("mysql.connector", connector)

    spec = importlib.util.spec_from_file_location(
        "closed_loop_event_generator", ROOT / "generator/produce_events.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


GENERATOR = load_generator_module()


def sample_event(event_type="click"):
    return {
        "event_id": 9001,
        "order_id": 8001 if event_type == "order" else None,
        "ts": "2026-08-30T12:00:00.000+08:00",
        "advertiser_id": 11,
        "campaign_id": 22,
        "unit_id": 33,
        "creative_id": 44,
        "product_id": 55,
        "shop_id": 66,
        "slot_id": 101,
        "user_id": 77,
        "event_type": event_type,
        "billing_mode": "CPC",
        "media": "douyin",
        "commerce_channel": "live",
        "region": "Anhui",
        "bid_price": 2.5,
        "spend": 1.23456,
        "product_price": 12.34,
        "product_num": 2,
        "gmv": 24.68,
    }


class GeneratorClosedLoopTest(unittest.TestCase):
    def test_history_defaults_are_bounded_and_cover_35_days(self):
        self.assertEqual(35, GENERATOR.HISTORY_DAYS)
        self.assertEqual(180, GENERATOR.HISTORY_EVENTS_PER_DAY)
        self.assertEqual(15, GENERATOR.ATTRIBUTION_DEMO_ORDERS_PER_DAY)
        self.assertEqual(
            {"DIRECT", "1D", "7D", "30D", "ORGANIC"},
            set(GENERATOR.attribution_demo_buckets(15)),
        )

    def test_amounts_use_one_yuan_equals_100000_raw_units(self):
        self.assertEqual(100_000, GENERATOR.yuan_to_raw_units(1))
        self.assertEqual(1, GENERATOR.yuan_to_raw_units("0.00001"))
        self.assertEqual(1_234_567, GENERATOR.yuan_to_raw_units("12.34567"))

    def test_bill_and_order_rows_are_integer_raw_units(self):
        bill = GENERATOR.bill_mysql_params(sample_event("click"))
        self.assertEqual(2, bill[7])
        self.assertEqual(123_456, bill[10])
        self.assertIsNone(bill[11].tzinfo)

        order = GENERATOR.order_mysql_params(sample_event("order"))
        self.assertEqual(1_234_000, order[4])
        self.assertEqual(2, order[5])
        self.assertEqual(2_468_000, order[6])
        self.assertEqual(3, order[12])
        self.assertLess(order[13], order[15])
        self.assertIsNone(order[15].tzinfo)

    def test_all_attribution_buckets_share_uid_and_product(self):
        lag_limits = {
            "DIRECT": (1, 6 * 60 * 60),
            "1D": (6 * 60 * 60, 24 * 60 * 60),
            "7D": (24 * 60 * 60, 7 * 24 * 60 * 60),
            "30D": (7 * 24 * 60 * 60, 30 * 24 * 60 * 60),
        }
        for index, bucket in enumerate(GENERATOR.ATTRIBUTION_BUCKETS):
            with self.subTest(bucket=bucket):
                order = sample_event("order")
                order["event_id"] += index
                click = GENERATOR.attach_attribution_journey(
                    order, random.Random(100 + index), bucket=bucket
                )
                if bucket == "ORGANIC":
                    self.assertIsNone(click)
                    self.assertEqual("organic", order["traffic_type"])
                    continue
                self.assertEqual(order["user_id"], click["user_id"])
                self.assertEqual(order["product_id"], click["product_id"])
                order_time = datetime.fromisoformat(order["ts"])
                click_time = datetime.fromisoformat(click["ts"])
                lag = int((order_time - click_time).total_seconds())
                lower, upper = lag_limits[bucket]
                self.assertGreaterEqual(lag, lower)
                self.assertLessEqual(lag, upper)

    def test_historical_day_is_deterministic(self):
        keys = [{
            "advertiser_id": 1,
            "industry": "ecommerce",
            "tier": "Growth",
            "campaign_id": 2,
            "promotion_goal": "电商下单推广",
            "budget": 5000,
            "unit_id": 3,
            "product_id": 4,
            "billing_mode": "CPC",
            "bid_amount": 2.5,
            "creative_id": 5,
            "shop_id": 6,
            "product_price": 49.9,
        }]
        first = GENERATOR.build_history_day_events(keys, date(2026, 8, 20))
        second = GENERATOR.build_history_day_events(keys, date(2026, 8, 20))
        project = lambda events: [
            (
                event["event_id"], event["ts"], event["event_type"],
                event["user_id"], event["product_id"], event.get("order_id"),
            )
            for event in events
        ]
        self.assertEqual(project(first), project(second))

    def test_mysql_facts_are_idempotent_and_history_has_checkpoint(self):
        self.assertIn("INSERT INTO bill_info", GENERATOR.BILL_INSERT_SQL)
        self.assertIn("ON DUPLICATE KEY UPDATE", GENERATOR.BILL_INSERT_SQL)
        self.assertIn("INSERT INTO order_info", GENERATOR.ORDER_INSERT_SQL)
        self.assertIn("ON DUPLICATE KEY UPDATE", GENERATOR.ORDER_INSERT_SQL)
        self.assertIn("PRIMARY KEY", GENERATOR.HISTORY_CHECKPOINT_DDL)

    def test_only_the_configured_billable_event_creates_a_bill(self):
        click = sample_event("click")
        impression = sample_event("show")
        impression["spend"] = 0
        with patch.object(GENERATOR, "execute_mysql") as execute:
            self.assertTrue(GENERATOR.persist_bill(click))
            self.assertFalse(GENERATOR.persist_bill(impression))
        execute.assert_called_once()

    def test_send_event_routes_logs_and_orders_to_their_sinks(self):
        with (
            patch.object(GENERATOR, "send_log_with_reconnect") as send_log,
            patch.object(GENERATOR, "persist_bill") as persist_bill,
            patch.object(GENERATOR, "persist_order") as persist_order,
        ):
            GENERATOR.send_event(sample_event("click"))
            send_log.assert_called_once()
            persist_bill.assert_called_once()
            persist_order.assert_not_called()

            GENERATOR.send_event(sample_event("order"))
            persist_order.assert_called_once()
            self.assertEqual(1, send_log.call_count)

    def test_fluss_write_reconnects_and_retries_same_event(self):
        event = sample_event("click")
        append = Mock(side_effect=[RuntimeError("connection lost"), None])
        with (
            patch.object(GENERATOR, "ensure_fluss_connection") as ensure,
            patch.object(GENERATOR, "append_log_to_fluss", append),
            patch.object(GENERATOR, "disconnect_fluss") as disconnect,
            patch.object(GENERATOR.time, "sleep"),
        ):
            GENERATOR.send_log_with_reconnect(event)
        self.assertEqual(2, ensure.call_count)
        self.assertEqual(2, append.call_count)
        self.assertIs(event, append.call_args_list[0].args[0])
        self.assertIs(event, append.call_args_list[1].args[0])
        disconnect.assert_called_once()

    def test_mysql_write_reconnects_and_retries_same_statement(self):
        first_cursor = Mock()
        first_cursor.execute.side_effect = RuntimeError("connection lost")
        second_cursor = Mock()
        first_connection = Mock()
        first_connection.cursor.return_value = first_cursor
        second_connection = Mock()
        second_connection.cursor.return_value = second_cursor
        statement = "INSERT INTO example VALUES (%s)"
        with (
            patch.object(
                GENERATOR,
                "get_mysql_connection",
                side_effect=[first_connection, second_connection],
            ),
            patch.object(GENERATOR, "close_mysql_connection") as close,
            patch.object(GENERATOR.time, "sleep"),
        ):
            GENERATOR.execute_mysql(statement, (7,), description="test write")
        first_cursor.execute.assert_called_once_with(statement, (7,))
        second_cursor.execute.assert_called_once_with(statement, (7,))
        second_connection.commit.assert_called_once()
        close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
