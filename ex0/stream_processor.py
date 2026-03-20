from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        pass


class NumericProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        return super().process(data)

    def validate(self, data: Any) -> bool:
        return super().validate(data)

    def format_output(self, result: str) -> str:
        return super().format_output(result)


class TextProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        return super().process(data)

    def validate(self, data: Any) -> bool:
        return super().validate(data)

    def format_output(self, result: str) -> str:
        return super().format_output(result)


class LogProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        return super().process(data)

    def validate(self, data: Any) -> bool:
        return super().validate(data)

    def format_output(self, result: str) -> str:
        return super().format_output(result)
