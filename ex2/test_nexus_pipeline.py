import unittest

from nexus_pipeline import CSVAdapter, JSONAdapter, NexusManager, StreamAdapter


def build_manager() -> NexusManager:
    manager = NexusManager()
    manager.add_pipeline("json", JSONAdapter("json"))
    manager.add_pipeline("csv", CSVAdapter("csv"))
    manager.add_pipeline("stream", StreamAdapter("stream"))
    return manager


class PipelineValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.manager = build_manager()

    def assert_backup(self, data: object, expected: str) -> None:
        self.assertEqual(self.manager.process_data(data), expected)

    def test_json_bool_value_falls_back_to_backup(self) -> None:
        self.assert_backup(
            {"sensor": "temp", "value": True, "unit": "C"},
            "Backup processed: raw data with 3 fields",
        )

    def test_json_non_finite_value_falls_back_to_backup(self) -> None:
        self.assert_backup(
            {"sensor": "temp", "value": float("nan"), "unit": "C"},
            "Backup processed: raw data with 3 fields",
        )

    def test_json_invalid_sensor_falls_back_to_backup(self) -> None:
        self.assert_backup(
            {"sensor": "noise", "value": 12.3, "unit": "C"},
            "Backup processed: raw data with 3 fields",
        )

    def test_json_invalid_unit_type_falls_back_to_backup(self) -> None:
        self.assert_backup(
            {"sensor": "temp", "value": 12.3, "unit": []},
            "Backup processed: raw data with 3 fields",
        )

    def test_json_non_serializable_data_falls_back_to_backup(self) -> None:
        self.assert_backup(
            {"sensor": "temp", "value": {1, 2}, "unit": "C"},
            "Backup processed: raw data with 3 fields",
        )

    def test_csv_invalid_sensor_row_falls_back_to_backup(self) -> None:
        self.assert_backup(
            "sensor,value,unit\nnoise,12.3,C",
            "Backup processed: raw text (30 chars)",
        )

    def test_csv_invalid_transaction_value_falls_back_to_backup(self) -> None:
        self.assert_backup(
            "product,price,quantity\napple,free,2",
            "Backup processed: raw text (35 chars)",
        )

    def test_stream_non_finite_value_falls_back_to_backup(self) -> None:
        self.assert_backup(
            ["temp:nan"],
            "Backup processed: raw stream (1 items)",
        )


if __name__ == "__main__":
    unittest.main()
