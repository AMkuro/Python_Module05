from typing import Any, Dict, List, Tuple
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    label: str = ""

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"

    def format_input(self, data: Any) -> str:
        return str(data)

    def ensure_validate(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError(
                f"[{self.__class__.__name__}] Validation failed for "
                f"{type(data).__name__}: {data!r}"
            )


class NumericProcessor(DataProcessor):
    label: str = "Numeric data"

    def __str__(self) -> str:
        return "Numeric processor"

    def process(self, data: Any) -> str:
        super().ensure_validate(data)
        return (
            f"Processed {len(data)} Numeric values, "
            f"sum={sum(data)}, avg={sum(data) / len(data)}"
        )

    def validate(self, data: Any) -> bool:
        try:
            if len(data) == 0:
                return False
            for value in data:
                _ = value + 0
        except (TypeError, AttributeError):
            return False
        return True


class TextProcessor(DataProcessor):
    label: str = "Text data"

    def __str__(self) -> str:
        return "Text processor"

    def process(self, data: Any) -> str:
        super().ensure_validate(data)
        word_count: int = len(data.split())
        return (
            f"Processed text: {len(data)} characters, "
            f"{word_count} {'words' if word_count > 1 else 'word'}"
        )

    def validate(self, data: Any) -> bool:
        match data:
            case str():
                if len(data) > 0:
                    return True
                return False
            case _:
                return False

    def format_input(self, data: Any) -> str:
        return f'"{data}"'


class LogProcessor(DataProcessor):
    label: str = "Log entry"

    def __str__(self) -> str:
        return "Log processor"

    def process(self, data: Any) -> str:
        super().ensure_validate(data)
        level_display: Dict[str, str] = {
            "ERROR": "ALERT",
            "INFO" : "INFO",
        }
        original_level: str = data.split(":")[0]
        display_level: str = level_display.get(original_level, "Unknown")
        return (
            f"[{display_level}] {original_level} level detected: "
            f"{data.split(': ', 1)[1]}"
        )

    def validate(self, data: Any) -> bool:
        try:
            level, message = data.split(": ", 1)
        except (AttributeError, ValueError):
            return False
        return level in {"ERROR", "WARNING", "INFO"} and len(message) > 0

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
    try:
        demo(NumericProcessor(), [1, 2, 3, 4, 5])
        demo(TextProcessor(), "Hello Nexus World")
        demo(LogProcessor(), "ERROR: Connection timeout")
    except ValueError as e:
        print(e)
    print("=== Polymorphic Processing Demo ===\n")
    print("Processing multiple data types through same interface...")
    pairs: List[Tuple[DataProcessor, Any]] = [
        (NumericProcessor(), [1, 2, 3]),
        (TextProcessor(), "Hello World!"),
        (LogProcessor(), "INFO: System ready"),
    ]
    for i, (processor, data) in enumerate(pairs, 1):
        try:
            result: str = processor.process(data)
            print(f"Result {i}: {result}")
        except ValueError as e:
            print(e)
    print("\nFoundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
