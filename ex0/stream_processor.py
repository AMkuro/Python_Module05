from typing import Any, List, Tuple
from abc import ABC, abstractmethod


class DataProcessor(ABC):
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


class NumericProcessor(DataProcessor):
    def __str__(self) -> str:
        return "Numeric processor"

    def process(self, data: Any) -> str:
        super().process(data)
        return (
            f"processed {len(data)} Numeric values, "
            f"sum={sum(data)}, avg={sum(data) / len(data)}"
        )

    def validate(self, data: Any) -> bool:
        return (
            isinstance(data, (list, tuple, set))
            and len(data) > 0
            and all(isinstance(x, (int, float)) for x in data)
        )


class TextProcessor(DataProcessor):
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
        return isinstance(data, str)


class LogProcessor(DataProcessor):
    def __str__(self) -> str:
        return "Log processor"

    def process(self, data: Any) -> str:
        super().process(data)
        log_type: str = data.split(":")[0]
        if log_type == "ERROR":
            log_type = "ALERT"
        elif log_type == "INFO":
            log_type = "INFO"
        return (
            f"[{log_type}] {data.split(':')[0]} level detected: "
            f"{data.split(': ', 1)[1]}"
        )

    def validate(self, data: Any) -> bool:
        return isinstance(data, str)


def demo(processor: Any, data: Any):
    try:
        type_str: str = "Empty"
        if isinstance(processor, NumericProcessor):
            type_str = "Numeric data"
        elif isinstance(processor, TextProcessor):
            type_str = "Text data"
        elif isinstance(processor, LogProcessor):
            type_str = "Log entry"
        else:
            raise ValueError("Unknown processor has input")
        print(f"Initializing {type_str}...")
        print("Processing data: ", end="")
        if type_str == "Numeric data":
            print(f"{data}")
        elif type_str != "Empty":
            print(f'"{data}"')
        processed_data: str = processor.process(data)
        if processor.validate(data):
            print(f"Validation: {type_str} verified")
        print(processor.format_output(processed_data))
        print()
    except ValueError as e:
        print(e, "\n")


def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    demo(NumericProcessor(), [1, 2, 3, 4, 5])
    demo(TextProcessor(), "Hello Nexus World")
    demo(LogProcessor(), "ERROR: Connection timeout")
    print("=== Polymorphic  Processing Demo ===\n")
    print("Processing multiple data types through same interface...")
    pairs: List[Tuple[Any, Any]] = [
        (NumericProcessor(), [1, 2, 3]),
        (TextProcessor(), "Hello World!"),
        (LogProcessor(), "INFO: System ready"),
    ]
    for i, (processor, data) in enumerate(pairs, 1):
        result = processor.process(data)
        print(f"Result {i}: {result}")
    print("\nFoundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
