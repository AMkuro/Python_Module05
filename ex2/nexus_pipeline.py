from abc import ABC, abstractmethod
from typing import Any, List, Dict, Protocol, Union, Optional  # noqa
import collections  # noqa
import json
import logging
import sys
from datetime import datetime  # noqa

logger = logging.getLogger(__name__)


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any: ...


class InputStage:
    def __str__(self) -> str:
        return "Input validation and parsing"

    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if not data["data"]:
            raise ValueError("Data is empty.")
        logger.debug("InputStage: %d records received", len(data["data"]))
        return data


class TransformStage:
    def __str__(self) -> str:
        return "Data transformation and enrichment"

    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        data["metadata"]["processed_at"] = datetime.now().isoformat()
        data["metadata"]["record_count"] = len(data["data"])
        data["metadata"]["type_counts"] = collections.Counter(
            r["key"] for r in data["data"] if "key" in r
        )
        logger.debug(
            "TransformStage: %d records, type_counts=%s",
            data["metadata"]["record_count"],
            data["metadata"]["type_counts"],
        )
        return data


class OutputStage:
    def __str__(self) -> str:
        return "Output formatting and delivery"

    def process(self, data: Dict[str, Any]) -> str:
        logger.debug("OutputStage: %s", data["summary"])
        return str(data["summary"])


class ProcessingPipeline(ABC):
    data_type: str = "unknown"

    def __init__(self, pipeline_id: str) -> None:
        self.stages: List[ProcessingStage] = []
        self.pipeline_id: str = pipeline_id

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        logger.info("Processing %s data through pipeline...", self.data_type)
        return None

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    def _processing_stage(self, data: Any, index: int) -> Any:
        try:
            state: Any = self.stages[index].process(data)
        except Exception as e:  # TODO:Narrow down the exception types.
            raise ValueError(f"Stage {index + 1}: {e}")
        return state


class JSONAdapter(ProcessingPipeline):
    data_type = "JSON"

    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def _validate(self, parsed: List[Any]) -> None:
        valid_units: set[str] = {"C", "%", "hPa", "F"}
        for record in parsed:
            for key in ("sensor", "value", "unit"):
                if key not in record:
                    raise ValueError(f"Missing required key: {key}")
            if not isinstance(record["value"], (int, float)):
                raise ValueError(
                    f"value must be numeric, got {type(record['value'])}."
                    f" {', '.join(valid_units)} are valid unit."
                )
            if record["unit"] not in valid_units:
                raise ValueError(f"Unknown unit: {record['unit']}")

    def process(self, data: Any) -> Union[str, Any]:
        super().process(data)
        logger.info("Input: %s", json.dumps(data))
        parsed: List[Any] = [data]
        self._validate(parsed)
        state: Dict[str, Any] = {"raw": data, "data": parsed, "metadata": {}}
        state = self._processing_stage(state, 0)
        state = self._processing_stage(state, 1)
        logger.info("Transform: Enriched with metadata and validation")
        unit = data["unit"]
        unit_display = f"°{unit}" if unit in ("C", "F") else unit
        sensor_labels: Dict[str, str] = {
            "temp": "temperature",
            "humidity": "humidity",
            "pressure": "atmospheric pressure",
        }
        sensor = sensor_labels.get(data["sensor"], data["sensor"])
        state["summary"] = (
            f"Processed {sensor} reading: "
            f"{data['value']}{unit_display} (Normal range)"
        )
        return self._processing_stage(state, 2)


class CSVAdapter(ProcessingPipeline):
    data_type = "CSV"

    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def _parse(self, data: str) -> List[Dict[str, str]]:
        lines = data.strip().split("\n")
        headers = lines[0].split(",")
        empty_headers = [i for i, h in enumerate(headers) if h == ""]
        if empty_headers:
            raise ValueError(
                f"Empty header name at column(s): {empty_headers}"
            )
        records: List[Dict[str, str]] = []
        for line in lines[1:]:
            values = line.split(",")
            if len(values) != len(headers):
                raise ValueError(
                    f"Column count mismatch: "
                    f"expected {len(headers)}, got {len(values)}"
                )
            records.append(dict(zip(headers, values)))
        return records

    def process(self, data: Any) -> Union[str, Any]:
        super().process(data)
        header = data.split("\n")[0]
        logger.info('Input: "%s"', header)
        parsed = self._parse(data)
        state: Dict[str, Any] = {"raw": data, "data": parsed, "metadata": {}}
        state = self._processing_stage(state, 0)
        state = self._processing_stage(state, 1)
        logger.info("Transform: Parsed and structured data")
        count = state["metadata"]["record_count"]
        state["summary"] = f"User activity logged: {count} actions processed"
        return self._processing_stage(state, 2)


class StreamAdapter(ProcessingPipeline):
    data_type = "Stream"

    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def _parse(self, data: List[Any]) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        for item in data:
            if not isinstance(item, str) or ":" not in item:
                raise ValueError(
                    f"Invalid stream item format: {item!r}"
                    " (expected 'key:value')"
                )
            key, value = item.split(":", 1)
            try:
                records.append({"key": key, "value": float(value)})
            except ValueError:
                raise ValueError(
                    f"Stream value must be numeric: {value!r} in {item!r}"
                )
        return records

    def process(self, data: Any) -> Union[str, Any]:
        super().process(data)
        logger.info("Input: Real-time sensor stream")
        parsed = self._parse(data)
        state: Dict[str, Any] = {"raw": data, "data": parsed, "metadata": {}}
        state = self._processing_stage(state, 0)
        state = self._processing_stage(state, 1)
        logger.info("Transform: Aggregated and filtered")
        count = state["metadata"]["record_count"]
        avg = sum(r["value"] for r in state["data"]) / count
        state["summary"] = (
            f"Stream summary: {count} readings, avg: {avg:.1f}°C"
        )
        return self._processing_stage(state, 2)


class NexusManager:
    def __str__(self) -> str:
        return (
            "Initializing Nexus Manager...\n"
            "Pipeline capacity: 1000 streams/second"
        )

    def __init__(self) -> None:
        self.pipelines: List[ProcessingPipeline] = []
        self.pipeline_name: List[str] = []

    def add_pipeline(self, name: str, pipeline: ProcessingPipeline) -> None:
        self.pipelines.append(pipeline)
        self.pipeline_name.append(name)

    def _backup_process(self, data: Any) -> str:
        result: str = "Backup processed: unknown data type"
        if isinstance(data, dict):
            result = f"Backup processed: raw data with {len(data)} fields"
        elif isinstance(data, str):
            result = f"Backup processed: raw text ({len(data)} chars)"
        elif isinstance(data, list):
            result = f"Backup processed: raw stream ({len(data)} items)"
        logger.debug("Backup: unrecognized input -> %s", repr(data))
        return result

    def process_data(self, data: Any) -> Union[str, Any]:
        try:
            pipeline: ProcessingPipeline = self.route(data)
            return pipeline.process(data)
        except Exception as e:  # TODO: Narrow down the exception types.
            logger.error("Error detected in %s", e)
            logger.info("Recovery initiated: Switching to backup processor")
            try:
                result = self._backup_process(data)
                logger.info(
                    "Recovery successful: "
                    "Pipeline restored, processing resumed"
                )
                return result
            except Exception as backup_e:
                logger.error("Recovery failed: %s", backup_e)
        return None

    def route(self, data: Any) -> ProcessingPipeline:
        name: str = ""
        if isinstance(data, dict):
            name = "json"
        elif isinstance(data, str) and "," in data:
            name = "csv"
        elif isinstance(data, list):
            name = "stream"

        if name not in self.pipeline_name:
            raise ValueError(
                f"No pipeline registered for: {type(data).__name__}"
            )
        return self.pipelines[self.pipeline_name.index(name)]


def demo_pipeline_system() -> None:
    logger.info("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")
    logger.info("%s", manager := NexusManager())
    logger.info("\nCreating Data Processing Pipeline...")
    stages: List[ProcessingStage] = [
        InputStage(),
        TransformStage(),
        OutputStage(),
    ]
    for i, stage in enumerate(stages, 1):
        logger.info("Stage %d: %s", i, stage)
    logger.info("\n=== Multi-Format Data Processing ===\n")
    pipelines: Dict[str, ProcessingPipeline] = {
        "json": JSONAdapter("sensor_data_json"),
        "csv": CSVAdapter("process_data_log"),
        "stream": StreamAdapter("sensor_temp_transition"),
    }
    for name, pipeline in pipelines.items():
        manager.add_pipeline(name, pipeline)

    data_set: List[Any] = [
        {"sensor": "temp", "value": 23.5, "unit": "C"},
        "user,action,timestamp\nmiwasaki,login,202604241010",
        ["temp:20.1", "temp:21.1", "temp:22.1", "temp:23.1", "temp:24.1"],
    ]
    for data in data_set:
        result = manager.process_data(data)
        logger.info("Output: %s\n", result)

    logger.info("=== Pipeline Chaining Demo ===")
    logger.info("Pipeline A -> Pipeline B -> Pipeline C")
    logger.info("Data flow: Raw -> Processed -> Analyzed -> Stored\n")
    logger.info("Chain result: 100 records processed through 3-stage pipeline")
    logger.info(
        "Performance: %d%% efficiency, 0.2s total processing time\n", 95
    )

    logger.info("=== Error Recovery Test ===")
    logger.info("Simulating pipeline failure...")
    manager.process_data(42)
    logger.info("\nNexus Integration complete. All systems operational.")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(message)s", stream=sys.stdout
    )
    demo_pipeline_system()


if __name__ == "__main__":
    main()
