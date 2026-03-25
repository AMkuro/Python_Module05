import unittest

from stream_processor import NumericProcessor


class AddOnly:
    def __add__(self, other):
        if other == 0:
            return self
        raise TypeError("unsupported add target")


class NumericProcessorContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.processor = NumericProcessor()

    def test_numeric_validate_accepts_ints_and_floats_in_list_or_tuple(self) -> None:
        self.assertTrue(self.processor.validate([1, 2.5, 3]))
        self.assertTrue(self.processor.validate((1.5, 2, 3.5)))

    def test_numeric_validate_rejects_empty_sequences(self) -> None:
        self.assertFalse(self.processor.validate([]))
        self.assertFalse(self.processor.validate(()))

    def test_numeric_validate_rejects_non_list_or_tuple_inputs(self) -> None:
        self.assertFalse(self.processor.validate({1, 2, 3}))
        self.assertFalse(self.processor.validate("123"))
        self.assertFalse(self.processor.validate(iter([1, 2, 3])))

    def test_numeric_validate_rejects_bool_values(self) -> None:
        self.assertFalse(self.processor.validate([True, False, True]))

    def test_numeric_validate_rejects_complex_values(self) -> None:
        self.assertFalse(self.processor.validate([1 + 2j, 3 + 4j]))

    def test_numeric_validate_rejects_non_numeric_objects(self) -> None:
        self.assertFalse(self.processor.validate([AddOnly()]))

    def test_numeric_process_raises_value_error_for_invalid_numeric_inputs(self) -> None:
        with self.assertRaises(ValueError):
            self.processor.process([True, False])

    def test_numeric_process_keeps_existing_summary_for_valid_input(self) -> None:
        self.assertEqual(
            self.processor.process([1, 2.5, 3.5]),
            "Processed 3 Numeric values, sum=7.0, avg=2.3333333333333335",
        )


if __name__ == "__main__":
    unittest.main()
