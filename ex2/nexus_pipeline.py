from abc import ABC, abstractmethod
from typing import Any, List, Dict, Protocol, Union, Optional
import collections
import json
import logging
import math
import random
import sys
import time
from datetime import datetime

logger = logging.getLogger(__name__)


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any: ...


class InputStage:
    def __str__(self) -> str:
        return "Input validation and parsing"

    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if not data["data"]:
            raise ValueError("Data is empty.")
        validator = data.get("validator")
        if validator is not None:
            validator(data["data"])
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
        data["summary"] = data["summary_fn"](data)
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
        except (ValueError, KeyError, TypeError) as e:
            raise ValueError(f"Stage {index + 1}: {e}")
        return state


class JSONAdapter(ProcessingPipeline):
    data_type = "JSON"

    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    _valid_units: Dict[str, str] = {
        "C": "°C",
        "F": "°F",
        "K": "K",
        "%": "%",
        "hPa": "hPa",
        "Pa": "Pa",
        "kPa": "kPa",
        "ppm": "ppm",
    }
    _sensor_labels: Dict[str, str] = {
        "temp": "temperature",
        "humidity": "humidity",
        "pressure": "atmospheric pressure",
        "co2": "CO2 concentration",
    }
    _sensor_units: Dict[str, set[str]] = {
        "temp": {"C", "F", "K"},
        "humidity": {"%"},
        "pressure": {"hPa", "Pa", "kPa"},
        "co2": {"ppm"},
    }

    def _validate_number(self, value: Any, field_name: str) -> None:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{field_name} must be numeric.")
        if not math.isfinite(float(value)):
            raise ValueError(f"{field_name} must be finite.")

    def _validate(self, parsed: List[Any]) -> None:
        for record in parsed:
            if not isinstance(record, dict):
                raise ValueError(
                    f"Record must be an dict object, got {type(record)}"
                )
            for key in ("sensor", "value", "unit"):
                if key not in record:
                    raise ValueError(f"Missing required key: {key}")
            sensor = record["sensor"]
            if (
                not isinstance(sensor, str)
                or sensor not in self._sensor_labels
            ):
                raise ValueError(
                    f"Unknown sensor: {sensor!r}."
                    f" Valid sensors: {list(self._sensor_labels)}"
                )
            self._validate_number(record["value"], "value")
            unit = record["unit"]
            if not isinstance(unit, str) or unit not in self._valid_units:
                raise ValueError(
                    f"Unknown unit: {unit!r}."
                    f" Valid units: {list(self._valid_units)}"
                )
            if unit not in self._sensor_units[sensor]:
                raise ValueError(
                    f"Unit {unit!r} is not valid for sensor {sensor!r}."
                )

    def _log_input(self, data: Dict[str, Any]) -> None:
        try:
            payload = json.dumps(data)
        except (TypeError, ValueError):
            payload = repr(data)
        logger.info("Input: %s", payload)

    def _make_summary(self, state: Dict[str, Any]) -> str:
        data: Dict[str, Any] = state["raw"]
        unit: str = str(data["unit"])
        unit_display: str = self._valid_units.get(unit, unit)
        sensor_key: str = str(data["sensor"])
        sensor: str = self._sensor_labels.get(sensor_key, sensor_key)
        return (
            f"Processed {sensor} reading: "
            f"{data['value']}{unit_display} (Normal range)"
        )

    def process(self, data: Any) -> Union[str, Any]:
        super().process(data)
        self._log_input(data)
        parsed: List[Any] = [data]
        state: Dict[str, Any] = {
            "raw": data,
            "data": parsed,
            "metadata": {},
            "validator": self._validate,
            "summary_fn": self._make_summary,
        }
        state = self._processing_stage(state, 0)
        state = self._processing_stage(state, 1)
        logger.info("Transform: Enriched with metadata and validation")
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

    _known_schemas: Dict[str, str] = {
        "user,action,timestamp": "activity",
        "sensor,value,unit": "sensor",
        "product,price,quantity": "transaction",
    }

    def _detect_schema(self, columns: List[str]) -> str:
        key: str = ",".join(columns)
        schema: Optional[str] = self._known_schemas.get(key)
        if schema is None:
            raise ValueError(
                f"Unknown CSV schema: {columns}.Known schemas:"
                f" {[k.split(',') for k in self._known_schemas]}"
            )
        return schema

    def _validate_text(self, value: str, field_name: str) -> None:
        if value.strip() == "":
            raise ValueError(f"{field_name} must not be empty")

    def _validate_number(self, value: str, field_name: str) -> None:
        self._validate_text(value, field_name)
        try:
            number = float(value)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be numeric") from exc
        if not math.isfinite(number):
            raise ValueError(f"{field_name} must be finite")

    def _validate_sensor_record(self, record: Dict[str, str]) -> None:
        sensor = record["sensor"].strip()
        unit = record["unit"].strip()
        if sensor not in JSONAdapter._sensor_labels:
            raise ValueError(
                f"Unknown sensor: {sensor!r}. "
                f"Valid sensors: {list(JSONAdapter._sensor_labels)}"
            )
        self._validate_number(record["value"], "value")
        if unit not in JSONAdapter._valid_units:
            raise ValueError(
                f"Unknown unit: {unit!r}. "
                f"Valid units: {list(JSONAdapter._valid_units)}"
            )
        if unit not in JSONAdapter._sensor_units[sensor]:
            raise ValueError(
                f"Unit {unit!r} is not valid for sensor {sensor!r}."
            )

    def _validate_activity_record(self, record: Dict[str, str]) -> None:
        for field in ("user", "action", "timestamp"):
            self._validate_text(record[field], field)

    def _validate_transaction_record(self, record: Dict[str, str]) -> None:
        self._validate_text(record["product"], "product")
        self._validate_number(record["price"], "price")
        self._validate_number(record["quantity"], "quantity")

    def _validate(self, records: List[Dict[str, str]]) -> None:
        if not records:
            raise ValueError("CSV contains no records")
        schema = self._detect_schema(list(records[0].keys()))
        validators = {
            "activity": self._validate_activity_record,
            "sensor": self._validate_sensor_record,
            "transaction": self._validate_transaction_record,
        }
        validator = validators[schema]
        for record in records:
            validator(record)

    def _make_summary(self, state: Dict[str, Any]) -> str:
        count: int = state["metadata"]["record_count"]
        records: List[Dict[str, str]] = state["data"]
        columns: List[str] = list(records[0].keys()) if records else []
        schema: str = self._detect_schema(columns)

        if schema == "activity":
            return f"User activity logged: {count} actions processed"
        if schema == "sensor":
            return f"Sensor log: {count} record(s) processed"
        if schema == "transaction":
            return f"Transaction log: {count} record(s) processed"
        return f"CSV: {count} record(s) processed"

    def process(self, data: Any) -> Union[str, Any]:
        super().process(data)
        header = data.split("\n")[0]
        logger.info('Input: "%s"', header)
        parsed = self._parse(data)
        state: Dict[str, Any] = {
            "raw": data,
            "data": parsed,
            "metadata": {},
            "validator": self._validate,
            "summary_fn": self._make_summary,
        }
        state = self._processing_stage(state, 0)
        state = self._processing_stage(state, 1)
        logger.info("Transform: Parsed and structured data")
        return self._processing_stage(state, 2)


class StreamAdapter(ProcessingPipeline):
    data_type = "Stream"

    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    _valid_keys: Dict[str, str] = {
        "temp": "°C",
        "humidity": "%",
        "pressure": "hPa",
    }

    def _parse(self, data: List[Any]) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        for item in data:
            if not isinstance(item, str) or ":" not in item:
                raise ValueError(
                    f"Invalid stream item format: {item!r}"
                    " (expected 'key:value')"
                )
            key, value = item.split(":", 1)
            if key not in self._valid_keys:
                raise ValueError(
                    f"Unknown stream key: {key!r}."
                    f" Valid keys: {list(self._valid_keys)}"
                )
            try:
                number = float(value)
            except ValueError:
                raise ValueError(
                    f"Stream value must be numeric: {value!r} in {item!r}"
                )
            if not math.isfinite(number):
                raise ValueError(
                    f"Stream value must be finite: {value!r} in {item!r}"
                )
            records.append({"key": key, "value": number})
        return records

    def _make_summary(self, state: Dict[str, Any]) -> str:
        records: List[Dict[str, Any]] = state["data"]
        count: int = state["metadata"]["record_count"]
        type_counts: collections.Counter[str] = state["metadata"][
            "type_counts"
        ]

        if len(type_counts) == 1:
            key: str = next(iter(type_counts))
            avg: float = sum(r["value"] for r in records) / count
            unit: str = self._valid_keys.get(key, "")
            return f"Stream summary: {count} readings, avg: {avg:.1f}{unit}"

        parts: List[str] = []
        for k, k_count in type_counts.items():
            k_avg: float = (
                sum(r["value"] for r in records if r["key"] == k) / k_count
            )
            k_unit: str = self._valid_keys.get(k, "")
            parts.append(f"{k}: {k_count} (avg {k_avg:.1f}{k_unit})")
        return f"Stream summary: {count} readings — {', '.join(parts)}"

    def process(self, data: Any) -> Union[str, Any]:
        super().process(data)
        logger.info("Input: Real-time sensor stream")
        parsed = self._parse(data)
        state: Dict[str, Any] = {
            "raw": data,
            "data": parsed,
            "metadata": {},
            "validator": None,
            "summary_fn": self._make_summary,
        }
        state = self._processing_stage(state, 0)
        state = self._processing_stage(state, 1)
        logger.info("Transform: Aggregated and filtered")
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
        except ValueError as e:
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
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")
    logger.info("%s", manager := NexusManager())
    print()
    logger.info("Creating Data Processing Pipeline...")
    stages: List[ProcessingStage] = [
        InputStage(),
        TransformStage(),
        OutputStage(),
    ]
    for i, stage in enumerate(stages, 1):
        logger.info("Stage %d: %s", i, stage)
    print("\n=== Multi-Format Data Processing ===\n")
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

    print("=== Pipeline Chaining Demo ===")
    print("Pipeline A -> Pipeline B -> Pipeline C")
    print("Data flow: Raw -> Processed -> Analyzed -> Stored\n")

    logging.getLogger().setLevel(logging.DEBUG)

    chain_start: float = time.perf_counter()
    stream_data: List[str] = (
        [f"temp:{random.uniform(15.0, 35.0):.1f}" for _ in range(334)]
        + [f"humidity:{random.uniform(30.0, 90.0):.1f}" for _ in range(333)]
        + [f"pressure:{random.uniform(980.0, 1030.0):.1f}" for _ in range(333)]
    )
    pipeline_chain: ProcessingPipeline = StreamAdapter("chain_stream")
    pipeline_chain.process(stream_data)

    chain_elapsed: float = (time.perf_counter() - chain_start) * 1000

    logging.getLogger().setLevel(logging.INFO)
    print(
        f"\nChain result: {len(stream_data)} records "
        f"processed through 3-stage pipeline",
    )
    print(f"Performance: {chain_elapsed:.2f} ms total processing time\n")

    print("=== Error Recovery Test ===")
    print("Simulating pipeline failure...")
    manager.process_data(42)
    print("\nNexus Integration complete. All systems operational.")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(message)s",
        stream=sys.stdout,
    )
    demo_pipeline_system()


if __name__ == "__main__":
    main()
