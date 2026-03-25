import unittest

from data_stream import (
    EventStream,
    SensorStream,
    StreamProcessor,
    TransactionStream,
)


class SensorStreamValidationTests(unittest.TestCase):
    def test_process_batch_counts_only_valid_finite_temperature_readings(self) -> None:
        stream = SensorStream("SENSOR")

        result = stream.process_batch(
            [
                "temp:25.5",
                "temp:nan",
                "temp:inf",
                "humidity:50",
                None,
                "temp:30.0",
            ]
        )

        self.assertEqual("2 readings processed", result)
        self.assertEqual(
            {
                "stream_id": "SENSOR",
                "stream_type": "Environmental Data",
                "processed_data": 2,
                "avg_temp": "27.8℃",
            },
            stream.get_stats(),
        )

    def test_filter_data_uses_only_current_batch_valid_high_priority_items(self) -> None:
        stream = SensorStream("SENSOR")
        stream.process_batch(["temp:50.0"])

        filtered = stream.filter_data(
            ["temp:31.0", "temp:29.0", "temp:nan", "temp:inf"],
            "High-priority",
        )

        self.assertEqual([31.0], filtered)


class TransactionStreamValidationTests(unittest.TestCase):
    def test_process_batch_ignores_invalid_and_negative_operations(self) -> None:
        stream = TransactionStream("TRANS")

        result = stream.process_batch(
            [
                "buy:100",
                "sell:30",
                "sell:-5",
                "buy:-1",
                "sell:1.5",
                "noop:10",
                {},
            ]
        )

        self.assertEqual("2 operations processed", result)
        self.assertEqual(
            {
                "stream_id": "TRANS",
                "stream_type": "Financial Data",
                "processed_data": 2,
                "net_flow": "+70 units",
            },
            stream.get_stats(),
        )

    def test_filter_data_uses_only_current_batch_valid_large_transactions(self) -> None:
        stream = TransactionStream("TRANS")
        stream.process_batch(["buy:500"])

        filtered = stream.filter_data(
            ["buy:300", "sell:100", "buy:-5", "sell:250"],
            "High-priority",
        )

        self.assertEqual([300], filtered)


class EventStreamValidationTests(unittest.TestCase):
    def test_process_batch_counts_only_valid_string_events(self) -> None:
        stream = EventStream("EVENT")

        result = stream.process_batch(["login", "error", 1, None, "logout"])

        self.assertEqual("3 events processed", result)
        self.assertEqual(
            {
                "stream_id": "EVENT",
                "stream_type": "System Events",
                "processed_data": 3,
                "error_detected": 1,
            },
            stream.get_stats(),
        )


class BatchContainerValidationTests(unittest.TestCase):
    def test_non_list_batches_raise_type_error(self) -> None:
        for stream in (
            SensorStream("SENSOR"),
            TransactionStream("TRANS"),
            EventStream("EVENT"),
        ):
            with self.subTest(stream=stream.name):
                with self.assertRaises(TypeError):
                    stream.process_batch("temp:20")  # type: ignore[arg-type]

    def test_stream_processor_requires_matching_batch_count(self) -> None:
        processor = StreamProcessor()
        processor.add_stream(SensorStream("SENSOR"))
        processor.add_stream(TransactionStream("TRANS"))

        with self.assertRaises(ValueError):
            processor.process_stream([["temp:20"]])

        with self.assertRaises(ValueError):
            processor.filter_stream([["temp:20"]], "High-priority")


if __name__ == "__main__":
    unittest.main()
