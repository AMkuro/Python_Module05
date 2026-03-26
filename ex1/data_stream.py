import math
from typing import Any, List, Dict, Tuple, Union, Optional
from abc import ABC, abstractmethod


class DataStream(ABC):
    name: str = "Data"

    def __init__(self, stream_id: str, stream_type: str) -> None:
        self.stream_id: str = stream_id
        self.stream_type: str = stream_type
        self.data_count: int = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        self._require_batch(data_batch)
        self.data_count = 0

    def _require_batch(self, data_batch: Any) -> List[Any]:
        if not isinstance(data_batch, list):
            raise TypeError("data_batch must be a list")
        return data_batch

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        return self._require_batch(data_batch)

    def filter_label(self, count: int) -> str:
        return f"{count} filtered item{'s' if count > 1 else ''}"

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "stream_type": self.stream_type,
            "processed_data": self.data_count,
        }


class SensorStream(DataStream):
    name: str = "Sensor"

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Environmental Data")
        self._avg_temp: float = 0.0
        self._temps: List[float] = []

    def _parse_temp(self, item: Any) -> Optional[float]:
        if not isinstance(item, str):
            return None
        parts = item.split(":")
        if len(parts) == 2 and parts[0] == "temp":
            temp = float(parts[1])
            if not math.isfinite(temp):
                raise ValueError(f"non-finite temperature value: {parts[1]!r}")
            return temp
        return None

    def process_batch(self, data_batch: List[Any]) -> str:
        super().process_batch(data_batch)
        valid_temps: List[float] = []
        for data in data_batch:
            try:
                t: Optional[float] = self._parse_temp(data)
                if t is not None:
                    valid_temps.append(t)
            except ValueError:
                pass
        self._temps.extend(valid_temps)
        self.data_count = len(valid_temps)
        if self._temps:
            self._avg_temp = sum(self._temps) / len(self._temps)
        return f"{self.data_count} readings processed"

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria == "High-priority":
            result: List[float] = []
            for item in self._require_batch(data_batch):
                try:
                    temp = self._parse_temp(item)
                except ValueError:
                    continue
                if temp is not None and temp > 30.0:
                    result.append(temp)
            return result
        return super().filter_data(data_batch, criteria)

    def filter_label(self, count: int) -> str:
        return f"{count} critical sensor alert{'s' if count > 1 else ''}"

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats: Dict[str, Union[str, int, float]] = super().get_stats()
        stats["avg_temp"] = f"{self._avg_temp:.1f}℃"
        return stats


class TransactionStream(DataStream):
    name: str = "Transaction"

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Financial Data")
        self._net_flow: int = 0
        self._amounts: List[int] = []

    def _parse_operation(self, item: Any) -> Optional[Tuple[str, int]]:
        if not isinstance(item, str):
            return None
        parts: List[str] = item.split(":")
        if len(parts) == 2 and parts[0] in ("buy", "sell"):
            amount = int(parts[1])
            if amount < 0:
                raise ValueError(f"negative transaction amount: {parts[1]!r}")
            return (parts[0], amount)
        return None

    def process_batch(self, data_batch: List[Any]) -> str:
        super().process_batch(data_batch)
        net: int = 0
        valid_amounts: List[int] = []
        for item in data_batch:
            try:
                op: Optional[Tuple[str, int]] = self._parse_operation(item)
                if op is not None:
                    optype, amount = op
                    temp: int = amount if optype == "buy" else -amount
                    valid_amounts.append(temp)
                    net += temp
            except ValueError:
                pass
        self._amounts.extend(valid_amounts)
        self.data_count = len(valid_amounts)
        self._net_flow += net
        return f"{self.data_count} operations processed"

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria == "High-priority":
            result: List[int] = []
            for item in self._require_batch(data_batch):
                try:
                    op = self._parse_operation(item)
                except ValueError:
                    continue
                if op is None:
                    continue
                optype, amount = op
                signed_amount = amount if optype == "buy" else -amount
                if amount > 200:
                    result.append(signed_amount)
            return result
        return super().filter_data(data_batch, criteria)

    def filter_label(self, count: int) -> str:
        return f"{count} large transaction{'s' if count > 1 else ''}"

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats: Dict[str, Union[str, int, float]] = super().get_stats()
        sign: str = "+" if self._net_flow >= 0 else "-"
        stats["net_flow"] = (
            f"{sign}{self._net_flow} unit{'s' if self._net_flow > 1 else ''}"
        )
        return stats


class EventStream(DataStream):
    name: str = "Event"

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "System Events")
        self._event_errors: int = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        super().process_batch(data_batch)
        valid_events = [
            item
            for item in self._require_batch(data_batch)
            if isinstance(item, str)
        ]
        error_count = sum(1 for item in valid_events if item == "error")
        self.data_count = len(valid_events)
        self._event_errors += error_count
        return f"{self.data_count} events processed"

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria == "High-priority":
            return [
                item
                for item in data_batch
                if isinstance(item, str) and item == "error"
            ]
        return super().filter_data(data_batch, criteria)

    def filter_label(self, count: int) -> str:
        return f"{count} error event{'s' if count > 1 else ''}"

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats: Dict[str, Union[str, int, float]] = super().get_stats()
        stats["error_detected"] = self._event_errors
        return stats


class StreamProcessor:
    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        self.streams.append(stream)

    def process_stream(self, batches: List[List[Any]]) -> List[Any]:
        if len(self.streams) != len(batches):
            raise ValueError("stream count and batch count must match")
        return [
            stream.process_batch(batch_data)
            for stream, batch_data in zip(self.streams, batches)
        ]

    def filter_stream(
        self, batches: List[List[Any]], criteria: Optional[str] = None
    ) -> List[Any]:
        if len(self.streams) != len(batches):
            raise ValueError("stream count and batch count must match")
        return [
            stream.filter_data(batch, criteria)
            for stream, batch in zip(self.streams, batches)
        ]


def demo_individual_streams() -> None:
    streams: List[Tuple[Any, str, List[Any]]] = [
        (
            SensorStream,
            "SENSOR_001",
            ["temp:22.5", "humidity:65", "pressure:1013"],
        ),
        (TransactionStream, "TRANS_001", ["buy:100", "sell:150", "buy:75"]),
        (EventStream, "EVENT_001", ["login", "error", "logout"]),
    ]
    for stream_class, stream_id, data_batch in streams:
        stream: DataStream = stream_class(stream_id)
        print(f"Initializing {stream.name} Stream...")
        print(f"Stream ID: {stream_id}, Type: {stream.stream_type}")
        batch_result: str = stream.process_batch(data_batch)
        print(
            f"Processing {stream.name.lower()} batch:"
            f" [{', '.join(data for data in data_batch)}]"
        )
        extra_items: List[str] = []
        stats: Dict[str, Union[str, int, float]] = stream.get_stats()
        for unique_key, value in stats.items():
            if unique_key not in {
                "stream_id",
                "stream_type",
                "processed_data",
            }:
                format_key: str = unique_key.replace("_", " ")
                if isinstance(value, str):
                    extra_items.append(f"{format_key}: {value}")
                else:
                    extra_items.append(f"{value} {format_key}")
        msg: str = (
            f"{batch_result}, {', '.join(extra_items)}"
            if extra_items
            else batch_result
        )
        print(f"{stream.name.capitalize()} analysis: {msg}\n")


def demo_polymorphic_processing() -> None:
    print("Processing mixed stream types through unified interface...\n")
    processer = StreamProcessor()
    streams: List[DataStream] = [
        SensorStream("SENSOR_002"),
        TransactionStream("TRANS_002"),
        EventStream("EVENT_002"),
    ]
    for stream in streams:
        processer.add_stream(stream)
    batches: List[List[Any]] = [
        ["temp:38.5", "temp:40.0"],
        ["buy:1000", "sell:50", "buy:200", "sell:100"],
        ["login", "logout", "login"],
    ]
    results: List[str] = processer.process_stream(batches)
    print("Batch 1 Results:")
    for stream, result in zip(streams, results):
        print(f"- {stream.name} data: {result}")
    criteria: str = "High-priority"
    print(f"\nStream filtering active: {criteria} data only")
    filtered: List[List[Any]] = processer.filter_stream(batches, criteria)
    parts = [
        stream.filter_label(len(items))
        for stream, items in zip(streams, filtered)
        if items
    ]
    print(f"Filtered results: {', '.join(parts)}")


def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")
    demo_individual_streams()
    print("=== Polymorphic Stream Processing ===")
    demo_polymorphic_processing()
    print("\nAll streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    main()
