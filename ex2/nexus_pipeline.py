from abc import ABC, abstractmethod
from typing import Any, List, Dict, Protocol, Union, Optional  # noqa

class ProcessingStage(Protocol):
    def process(self,data:Any) -> Any:
        pass

class InputStage:
    def process(data) -> Dict:
        pass

class TransformStage:
    def process(data) -> Dict:
        pass

class OutputStage:
    def process(data) ->str:
        pass

class ProcessingPipeline(ABC):
    def __init__(self,pipeline_id: str) -> None:
        self.stages: List[Stage] = []
        self.pipeline_id: str = ""

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        pass

    def add_stage():
        pass


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)

    def process(self,data: Any) -> Union[str, Any]:
        return super().process(data)


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)
    def process(self,data: Any) -> Union[str, Any]:
        return super().process(data)

class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)
    def process(self, data: Any) -> Union[str, Any]:
        return super().process(data)

class NexusManager:
    def __init__(self) -> None:
        self.pipelines: List[ProcessingPipeline] =[]

    def add_pipeline():
        pass

    def process_data():
        pass

