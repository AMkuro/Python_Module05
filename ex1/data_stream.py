from typing import Any, List, Dict, Union, Optional  # noqa
from abc import ABC, abstractmethod


class DataStream(ABC):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class SensorStream(DataStream):
    def __str__(self) -> str:
        return "Sensor Stream"

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)

    def process_batch(self, data_batch: List[Any]) -> str:
        return super().process_batch(data_batch)


class TransactionStream(DataStream):
    def __str__(self) -> str:
        return "Transaction Stream"

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)


class EventStream(DataStream):
    def __str__(self) -> str:
        return "Event Stream"

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)


class StreamProcessor:
    def __init__(self) -> None:
        self.streams: List[DataStream] = []
    
    def add

def demo(stream: Any, stream_id: str) -> None:
    print(f"Initializing {stream}...")
    print(f"Stream ID: {stream_id}
