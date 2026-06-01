from abc import ABC, abstractmethod
from typing import Any, Protocol


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[str] = []
        self._total_processed: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        if not self._storage:
            raise IndexError("No data avaliable in processor.")

        rank: int = self._total_processed - len(self._storage)
        value = self._storage.pop(0)
        return (rank, value)

    def remaining(self) -> int:
        return len(self._storage)

    def total_processed(self) -> int:
        return self._total_processed


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, bool):
            return False
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return all(
                isinstance(item, (int, float)) and not isinstance(item, bool)
                for item in data
            )
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise TypeError("Improper numeric data")

        if isinstance(data, list):
            for item in data:
                self._storage.append(str(item))
                self._total_processed += 1
        else:
            self._storage.append(str(data))
            self._total_processed += 1


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(item, str) for item in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise TypeError("Improper text data")

        if isinstance(data, list):
            for item in data:
                self._storage.append(str(item))
                self._total_processed += 1
        else:
            self._storage.append(str(data))
            self._total_processed += 1


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return all(
                isinstance(k, str) and isinstance(v, str)
                for k, v in data
                )
        if isinstance(data, list):
            return all(
                isinstance(item, dict) and
                all(isinstance(k, str) and isinstance(v, str)
                    for k, v in item.items())
                for item in data
                )
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise TypeError("Improper log data")

        if isinstance(data, list):
            for item in data:
                log_level = item.get("log_level", "")
                log_message = item.get("log_message", "")
                self._storage.append(f"{log_level}: {log_message}")
                self._total_processed += 1
        else:
            log_level = data.get("log_level", "")
            log_message = data.get("log_message", "")
            self._storage.append(f"{log_level}: {log_message}")
            self._total_processed += 1


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        values = [value for _, value in data]
        csv_line = ",".join(values)
        print("CSV Output:")
        print(csv_line)


class JSONExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pairs = [f'"item_{rank}": "{value}"' for rank, value in data]
        json_str = "{" + ", ".join(pairs) + "}"
        print("JSON Output:")
        print(json_str)


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            handled = False
            for proc in self._processors:
                if proc.validate(element):
                    proc.ingest(element)
                    handled = True
                    break
            if not handled:
                print(
                    f"DataStream error - Can't process element "
                    f"in stream: {element}"
                )

    def print_processors_stats(self) -> None:
        print("\n== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return
        for proc in self._processors:
            name = type(proc).__name__
            total = proc.total_processed()
            remaining = proc.remaining()
            print(
                f"* {name}: total {total} items processed, "
                f"remaining {remaining} on processor"
            )

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        """Consume nb elements from each processor and export via plugin."""
        for proc in self._processors:
            collected: list[tuple[int, str]] = []
            for _ in range(nb):
                if proc.remaining() == 0:
                    break
                collected.append(proc.output())
            if collected:
                plugin.process_output(collected)


if __name__ == "__main__":
    print("=== Outer Wilds - Data Pipeline ===")

    stream = DataStream()

    print("\nBooting Hearthian Data Stream...")
    stream.print_processors_stats()

    print("\nRegistering Onboard Processors")
    num_proc = NumericProcessor()
    txt_proc = TextProcessor()
    log_proc = LogProcessor()
    stream.register_processor(num_proc)
    stream.register_processor(txt_proc)
    stream.register_processor(log_proc)

    batch1: list[Any] = [
        "The Nomai wrote strange messages on the walls",
        [3.14, -1, 42.0],
        [
            {"log_level": "WARNING",
             "log_message": "Ghost Matter detected near Timber Hearth"},
            {"log_level": "INFO",
             "log_message": "Traveler Feldspar still missing in Dark Bramble"},
        ],
        318,
        ["Signal identified", "Quantum fluctuations increasing"],
    ]

    print(f"\nTransmitting first batch of Hearthian data: {batch1}")
    stream.process_stream(batch1)
    stream.print_processors_stats()

    csv_plugin = CSVExportPlugin()
    print("\nExporting 3 processed entries from each processor to CSV module:")
    stream.output_pipeline(3, csv_plugin)
    stream.print_processors_stats()

    batch2: list[Any] = [
        22,
        ["The Eye is calling", "The loop continues",
         "Hold on to your memories"],
        [
            {"log_level": "ERROR",
             "log_message": "Probe tracking module offline"},
            {"log_level": "NOTICE",
             "log_message": "Quantum Moon location unstable"},
        ],
        [12, 24, 36, 48, 60, 72],
        "The universe is older than we thought",
    ]

    print(f"\nTransmitting second batch of Hearthian data: {batch2}")
    stream.process_stream(batch2)
    stream.print_processors_stats()

    json_plugin = JSONExportPlugin()
    print("\nExporting 5 processed entries "
          "from each processor to JSON module:")
    stream.output_pipeline(5, json_plugin)
    stream.print_processors_stats()
    print("\n=== End of program ===")
