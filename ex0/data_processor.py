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
            raise IndexError("No data avaliable in processor!")

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
            raise TypeError("Data is not numeric o not int/float")

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
            raise TypeError("Data is not string")

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
            raise TypeError("Data is not log asociated")

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


if __name__ == "__main__":
    print("=== Data Processor ===")

    # NumericProcessor
    print("\n[NUMERIC] Initializing Numeric Processor subsystem...")
    num_proc = NumericProcessor()

    print(f"Validating numeric input '42': {num_proc.validate(42)}")
    print("Validating invalid numeric input 'Hearthian': "
          f"{num_proc.validate('Hearthian')}")

    print("Attempting invalid ingestion of string 'QuantumMoon' "
          "(expected failure):")
    try:
        num_proc.ingest("QuantumMoon")
    except TypeError as e:
        print(f"Exception caught: {e}")

    data_num: list[int | float] = [318, 22.5, -7, 42, 3.14]
    print(f"Ingesting numeric telemetry: {data_num}")
    num_proc.ingest(data_num)

    print("Extracting 3 numeric values from buffer...")
    for _ in range(3):
        rank, value = num_proc.output()
        print(f"* Numeric entry {rank}: {value}")

    # TextProcessor
    print("\n[TEXT] Initializing Text Processor subsystem...")
    txt_proc = TextProcessor()

    print(f"Validating invalid text input '42': {txt_proc.validate(42)}")

    data_txt = ["The Eye is calling", "Timber Hearth", "End of the cycle"]
    print(f"Ingesting text messages: {data_txt}")
    txt_proc.ingest(data_txt)

    print("Extracting 1 text value...")
    rank, value = txt_proc.output()
    print(f"* Text entry {rank}: {value}")

    # LogProcessor
    print("\n[LOG] Initializing Log Processor subsystem...")
    log_proc = LogProcessor()

    print("Validating invalid log input 'Hello': "
          f"{log_proc.validate('Hello')}")

    data_log = [
        {"log_level": "NOTICE", "log_message": "Probe tracking module online"},
        {"log_level": "ERROR", "log_message": "Ghost Matter "
         "detected in vicinity"},
    ]
    print(f"Ingesting log entries: {data_log}")
    log_proc.ingest(data_log)

    print("Extracting 2 log values...")
    for _ in range(2):
        rank, value = log_proc.output()
        print(f"* Log entry {rank}: {value}")
    print("\n=== End of program ===")
