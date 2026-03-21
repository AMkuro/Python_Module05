from typing import Any, List, Tuple
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    label: str = ""

    @abstractmethod
    def process(self, data: Any) -> str:
        if not self.validate(data):
            raise ValueError(
                f"[{self.__class__.__name__}] Validation failed for "
                f"{type(data).__name__}: {data!r}"
            )

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"

    def format_input(self, data: Any) -> str:
        return str(data)


class NumericProcessor(DataProcessor):
    label: str = "Numeric data"

    def __str__(self) -> str:
        return "Numeric processor"

    def process(self, data: Any) -> str:
        super().process(data)
        return (
            f"processed {len(data)} Numeric values, "
            f"sum={sum(data)}, avg={sum(data) / len(data)}"
        )

    def validate(self, data: Any) -> bool:
        if not hasattr(data, "__iter__") or not hasattr(data, "__len__"):
            return False
        if len(data) == 0:
            return False
        try:
            sum(data)
            return True
        except TypeError:
            return False


class TextProcessor(DataProcessor):
    label: str = "Text data"

    def __str__(self) -> str:
        return "Text processor"

    def process(self, data: Any) -> str:
        super().process(data)
        word_count: int = len(data.split())
        return (
            f"Processed text: {len(data)} characters, "
            f"{word_count} {'words' if word_count > 1 else 'word'}"
        )

    def validate(self, data: Any) -> bool:
        return hasattr(data, "split") and hasattr(data, "__len__")

    def format_input(self, data: Any) -> str:
        return f'"{data}"'


class LogProcessor(DataProcessor):
    label: str = "Log entry"

    def __str__(self) -> str:
        return "Log processor"

    def process(self, data: Any) -> str:
        super().process(data)
        log_type: str = data.split(":")[0]
        if log_type == "ERROR":
            log_type = "ALERT"
        return (
            f"[{log_type}] {data.split(':')[0]} level detected: "
            f"{data.split(': ', 1)[1]}"
        )

    def validate(self, data: Any) -> bool:
        return hasattr(data, "split") and hasattr(data, "__len__")

    def format_input(self, data: Any) -> str:
        return f'"{data}"'


def demo(processor: DataProcessor, data: Any) -> None:
    if not processor.label:
        raise ValueError("Unknown processor has input")
    try:
        print(f"Initializing {processor.label}...")
        print(f"Processing data: {processor.format_input(data)}")
        processed_data: str = processor.process(data)
        if processor.validate(data):
            print(f"Validation: {processor.label} verified")
        print(processor.format_output(processed_data))
        print()
    except ValueError as e:
        print(e, "\n")


def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    demo(NumericProcessor(), [1, 2, 3, 4, 5])
    demo(TextProcessor(), "Hello Nexus World")
    demo(LogProcessor(), "ERROR: Connection timeout")
    print("=== Polymorphic Processing Demo ===\n")
    print("Processing multiple data types through same interface...")
    pairs: List[Tuple[DataProcessor, Any]] = [
        (NumericProcessor(), [1, 2, 3]),
        (TextProcessor(), "Hello World!"),
        (LogProcessor(), "INFO: System ready"),
    ]
    for i, (processor, data) in enumerate(pairs, 1):
        result: str = processor.process(data)
        print(f"Result {i}: {result}")
    print("\nFoundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
