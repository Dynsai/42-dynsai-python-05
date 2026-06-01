from abc import ABC, abstractmethod
from typing import Any


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
                    f"DataStream ERROR - Can't process element "
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


if __name__ == "__main__":
    print("=== Data Stream ===")

    stream = DataStream()

    print("\nBooting Hearthian Data Stream...")
    stream.print_processors_stats()

    print("\nRegistering Numeric Telemetry Processor")
    num_proc = NumericProcessor()
    stream.register_processor(num_proc)

    batch1: list[Any] = [
        "The Nomai left strange messages across the solar system",
        [3.14, -1, 2.71],
        [
            {"log_level": "WARNING",
             "log_message": "Ghost Matter detected near Timber Hearth"},
            {"log_level": "INFO",
             "log_message": "Traveler Feldspar remains unaccounted for"},
        ],
        42,
        ["Quantum fluctuations rising", "Signal identified"],
    ]

    print(f"\nTransmitting first batch of Hearthian data: {batch1}")
    stream.process_stream(batch1)
    stream.print_processors_stats()

    print("\nRegistering additional processors (Text, Log)")
    txt_proc = TextProcessor()
    log_proc = LogProcessor()
    stream.register_processor(txt_proc)
    stream.register_processor(log_proc)

    print("Re-transmitting initial batch for multi-processor ingestion...")
    stream.process_stream(batch1)
    stream.print_processors_stats()

    print(
        "\nConsuming buffered entries: "
        "Numeric 3, Text 2, Log 1"
    )
    for _ in range(3):
        num_proc.output()
    for _ in range(2):
        txt_proc.output()
    log_proc.output()

    stream.print_processors_stats()
    print("\n=== End of program ===")
