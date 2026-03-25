"""
Tests for NexusManager.process_data()
Verifies that no input causes a crash or inexplicable behavior.
"""
import logging
import pytest
from ex2.nexus_pipeline import (
    NexusManager,
    JSONAdapter,
    CSVAdapter,
    StreamAdapter,
)

logging.disable(logging.CRITICAL)


def make_manager() -> NexusManager:
    manager = NexusManager()
    manager.add_pipeline("json", JSONAdapter("json"))
    manager.add_pipeline("csv", CSVAdapter("csv"))
    manager.add_pipeline("stream", StreamAdapter("stream"))
    return manager


# ---------------------------------------------------------------------------
# Helper: assert process_data never raises
# ---------------------------------------------------------------------------

def assert_no_crash(manager: NexusManager, data, label: str = "") -> None:
    """process_data must not raise any exception for any input."""
    try:
        result = manager.process_data(data)
    except Exception as exc:
        pytest.fail(
            f"process_data raised {type(exc).__name__}: {exc}"
            + (f"  [input: {label}]" if label else "")
        )
    # result must be str or None (backup returns str, valid pipeline returns str)
    assert result is None or isinstance(result, str), (
        f"Expected str or None, got {type(result).__name__}: {result!r}"
        + (f"  [input: {label}]" if label else "")
    )


# ===========================================================================
# 1. 正常系 (happy path)
# ===========================================================================

class TestHappyPath:
    def test_json_valid(self):
        m = make_manager()
        result = m.process_data({"sensor": "temp", "value": 23.5, "unit": "C"})
        assert isinstance(result, str)
        assert "temperature" in result

    def test_csv_activity(self):
        m = make_manager()
        result = m.process_data(
            "user,action,timestamp\nalice,login,20260101120000"
        )
        assert isinstance(result, str)
        assert "activity" in result.lower() or "action" in result.lower()

    def test_csv_sensor(self):
        m = make_manager()
        result = m.process_data("sensor,value,unit\ntemp,23.5,C")
        assert isinstance(result, str)

    def test_csv_transaction(self):
        m = make_manager()
        result = m.process_data("product,price,quantity\nwidget,9.99,3")
        assert isinstance(result, str)

    def test_stream_single_key(self):
        m = make_manager()
        result = m.process_data(["temp:20.0", "temp:21.0", "temp:22.0"])
        assert isinstance(result, str)
        assert "Stream summary" in result

    def test_stream_multiple_keys(self):
        m = make_manager()
        result = m.process_data(
            ["temp:20.0", "humidity:55.0", "pressure:1013.0"]
        )
        assert isinstance(result, str)
        assert "Stream summary" in result

    def test_json_integer_value(self):
        m = make_manager()
        result = m.process_data({"sensor": "temp", "value": 25, "unit": "C"})
        assert isinstance(result, str)

    def test_json_all_valid_units(self):
        m = make_manager()
        valid_units = ["C", "F", "K", "%", "hPa", "Pa", "kPa", "ppm"]
        sensors = ["temp", "humidity", "pressure", "co2"]
        for unit in valid_units:
            result = m.process_data({"sensor": sensors[0], "value": 1.0, "unit": unit})
            assert isinstance(result, str)


# ===========================================================================
# 2. 空・最小入力
# ===========================================================================

class TestEmptyInputs:
    def test_none(self):
        assert_no_crash(make_manager(), None, "None")

    def test_empty_list(self):
        assert_no_crash(make_manager(), [], "[]")

    def test_empty_dict(self):
        assert_no_crash(make_manager(), {}, "{}")

    def test_empty_string(self):
        assert_no_crash(make_manager(), "", '""')

    def test_string_without_comma(self):
        # str without "," → not routed to CSV → backup
        assert_no_crash(make_manager(), "hello", '"hello"')

    def test_csv_header_only(self):
        # No data rows after header
        assert_no_crash(make_manager(), "user,action,timestamp", "header-only CSV")

    def test_csv_empty_data_rows(self):
        assert_no_crash(make_manager(), "sensor,value,unit\n", "CSV with empty data row")


# ===========================================================================
# 3. 型異常 (unexpected types)
# ===========================================================================

class TestUnexpectedTypes:
    @pytest.mark.parametrize("value,label", [
        (42, "int"),
        (3.14, "float"),
        (True, "bool True"),
        (False, "bool False"),
        (b"bytes data", "bytes"),
        ((1, 2, 3), "tuple"),
        ({1, 2, 3}, "set"),
        (frozenset([1, 2]), "frozenset"),
        (object(), "object()"),
        (lambda x: x, "lambda"),
        (type, "type object"),
    ])
    def test_primitive_types(self, value, label):
        assert_no_crash(make_manager(), value, label)

    def test_integer_zero(self):
        assert_no_crash(make_manager(), 0, "0")

    def test_negative_integer(self):
        assert_no_crash(make_manager(), -1, "-1")

    def test_float_nan(self):
        import math
        assert_no_crash(make_manager(), float("nan"), "float nan")

    def test_float_inf(self):
        assert_no_crash(make_manager(), float("inf"), "float inf")


# ===========================================================================
# 4. 内包空リスト / ネスト構造 (nested / embedded empty lists)
# ===========================================================================

class TestNestedAndEmbeddedLists:
    def test_list_with_empty_list_item(self):
        assert_no_crash(make_manager(), [[]], "[[]]")

    def test_list_with_mixed_empty_and_valid(self):
        assert_no_crash(make_manager(), ["temp:20.0", []], '["temp:20.0", []]')

    def test_list_with_none_item(self):
        assert_no_crash(make_manager(), [None], "[None]")

    def test_list_with_int_items(self):
        assert_no_crash(make_manager(), [1, 2, 3], "[1, 2, 3]")

    def test_list_with_dict_items(self):
        assert_no_crash(make_manager(), [{"a": 1}], "[{dict}]")

    def test_deeply_nested_list(self):
        assert_no_crash(make_manager(), [[[["deep"]]]], "deeply nested list")

    def test_list_with_mixed_types(self):
        assert_no_crash(
            make_manager(),
            ["temp:20.0", 42, None, [], {}, "no_colon"],
            "mixed type list",
        )

    def test_list_of_empty_strings(self):
        assert_no_crash(make_manager(), ["", "", ""], '["", "", ""]')

    def test_list_no_colon_strings(self):
        assert_no_crash(make_manager(), ["abc", "def"], '["abc", "def"]')

    def test_list_unknown_stream_key(self):
        assert_no_crash(make_manager(), ["unknown:1.0"], '["unknown:1.0"]')

    def test_list_invalid_value_not_numeric(self):
        assert_no_crash(make_manager(), ["temp:abc"], '["temp:abc"]')

    def test_list_missing_colon(self):
        assert_no_crash(make_manager(), ["temponly"], '["temponly"]')


# ===========================================================================
# 5. 不正フォーマット (malformed valid-type inputs)
# ===========================================================================

class TestMalformedInputs:
    # JSON (dict) — missing required keys
    def test_json_missing_sensor(self):
        assert_no_crash(make_manager(), {"value": 1.0, "unit": "C"}, "missing sensor")

    def test_json_missing_value(self):
        assert_no_crash(make_manager(), {"sensor": "temp", "unit": "C"}, "missing value")

    def test_json_missing_unit(self):
        assert_no_crash(make_manager(), {"sensor": "temp", "value": 1.0}, "missing unit")

    def test_json_invalid_unit(self):
        assert_no_crash(
            make_manager(),
            {"sensor": "temp", "value": 1.0, "unit": "XYZ"},
            "invalid unit",
        )

    def test_json_non_numeric_value(self):
        assert_no_crash(
            make_manager(),
            {"sensor": "temp", "value": "hot", "unit": "C"},
            "non-numeric value",
        )

    def test_json_extra_keys(self):
        result = make_manager().process_data(
            {"sensor": "temp", "value": 25.0, "unit": "C", "extra": "ignored"}
        )
        assert isinstance(result, str)

    def test_json_nested_dict_value(self):
        assert_no_crash(
            make_manager(),
            {"sensor": "temp", "value": {"nested": 1}, "unit": "C"},
            "nested dict as value",
        )

    # CSV — unknown schema / empty headers / column mismatch
    def test_csv_unknown_schema(self):
        assert_no_crash(
            make_manager(), "foo,bar,baz\n1,2,3", "unknown CSV schema"
        )

    def test_csv_empty_header_name(self):
        assert_no_crash(
            make_manager(), ",action,timestamp\nalice,login,now", "empty header name"
        )

    def test_csv_column_mismatch(self):
        assert_no_crash(
            make_manager(),
            "user,action,timestamp\nalice,login",
            "column count mismatch",
        )

    def test_csv_only_commas(self):
        assert_no_crash(make_manager(), ",,,", "only commas")

    def test_csv_whitespace_only(self):
        assert_no_crash(make_manager(), "   ,   ,   \n1,2,3", "whitespace headers")

    # Stream — invalid key or value
    def test_stream_unknown_key(self):
        assert_no_crash(make_manager(), ["wind:5.0"], "unknown stream key")

    def test_stream_non_numeric_value(self):
        assert_no_crash(make_manager(), ["temp:hot"], "non-numeric stream value")

    def test_stream_empty_value(self):
        assert_no_crash(make_manager(), ["temp:"], "empty stream value")

    def test_stream_empty_key(self):
        assert_no_crash(make_manager(), [":20.0"], "empty stream key")

    def test_stream_multiple_colons(self):
        result = make_manager().process_data(["temp:20.0:extra"])
        # value part "20.0:extra" → not numeric → backup
        assert result is None or isinstance(result, str)


# ===========================================================================
# 6. 境界値・大量データ
# ===========================================================================

class TestBoundaryAndScale:
    def test_json_very_large_value(self):
        assert_no_crash(
            make_manager(),
            {"sensor": "temp", "value": 1e308, "unit": "C"},
            "very large float",
        )

    def test_json_negative_value(self):
        result = make_manager().process_data(
            {"sensor": "temp", "value": -273.15, "unit": "C"}
        )
        assert isinstance(result, str)

    def test_json_zero_value(self):
        result = make_manager().process_data(
            {"sensor": "temp", "value": 0.0, "unit": "C"}
        )
        assert isinstance(result, str)

    def test_stream_large_volume(self):
        import random
        data = [f"temp:{random.uniform(0, 100):.2f}" for _ in range(10_000)]
        assert_no_crash(make_manager(), data, "10,000 stream items")

    def test_csv_many_rows(self):
        rows = "\n".join(
            f"user{i},login,20260101{i:06d}" for i in range(1000)
        )
        csv_data = "user,action,timestamp\n" + rows
        assert_no_crash(make_manager(), csv_data, "1000-row CSV")

    def test_json_unicode_sensor_name(self):
        assert_no_crash(
            make_manager(),
            {"sensor": "温度センサー", "value": 25.0, "unit": "C"},
            "unicode sensor name",
        )

    def test_stream_unicode_item(self):
        assert_no_crash(make_manager(), ["温度:20.0"], "unicode stream item")

    def test_stream_single_item(self):
        result = make_manager().process_data(["temp:20.0"])
        assert isinstance(result, str)

    def test_dict_with_none_values(self):
        assert_no_crash(
            make_manager(),
            {"sensor": None, "value": None, "unit": None},
            "dict with None values",
        )


# ===========================================================================
# 7. ルーティング未登録パイプライン
# ===========================================================================

class TestRoutingEdgeCases:
    def test_no_pipelines_registered(self):
        """パイプライン未登録のマネージャーでも crash しない"""
        empty_manager = NexusManager()
        assert_no_crash(empty_manager, {"sensor": "temp", "value": 1.0, "unit": "C"})
        assert_no_crash(empty_manager, "user,action,ts\na,b,c")
        assert_no_crash(empty_manager, ["temp:20.0"])
        assert_no_crash(empty_manager, 42)

    def test_partial_pipeline_registration(self):
        """一部のパイプラインのみ登録"""
        m = NexusManager()
        m.add_pipeline("json", JSONAdapter("json"))
        # csv and stream not registered
        assert_no_crash(m, "user,action,timestamp\na,b,c", "csv without csv pipeline")
        assert_no_crash(m, ["temp:20.0"], "stream without stream pipeline")
        # json should still work
        result = m.process_data({"sensor": "temp", "value": 1.0, "unit": "C"})
        assert isinstance(result, str)

    def test_string_without_comma_not_routed_to_csv(self):
        m = make_manager()
        result = m.process_data("nodatahere")
        # no "," → not csv → no matching pipeline → backup
        assert result is None or isinstance(result, str)
