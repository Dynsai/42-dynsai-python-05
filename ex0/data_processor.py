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
    print("=== Code Nexus - Data Processor ===")

    # NumericProcessor
    print("\nTesting Numeric Processor...")
    num_proc = NumericProcessor()

    print(f"Trying to validate input '42': {num_proc.validate(42)}")
    print(f"Trying to validate input 'Hello!': {num_proc.validate('Hello!')}")

    print("Test invalid ingestion of string 'trythis' "
          "without prior validation:")
    try:
        num_proc.ingest("trythis")
    except TypeError as e:
        print(f"Ecxception: {e}")

    data_num: list[int | float] = [1, 2, 3, 4, 5]
    print(f"Processing data: {data_num}")
    num_proc.ingest(data_num)

    print(" Extracting 3 values...")
    for _ in range(3):
        rank, value = num_proc.output()
        print(f"Numeric value {rank}: {value}")

    # TextProcessor
    print("\nTesting Text Processor...")
    txt_proc = TextProcessor()

    print(f"Trying to validate input '42': {txt_proc.validate(42)}")

    data_txt = ["Hello", "Nexus", "World"]
    print(f"Processing data: {data_txt}")
    txt_proc.ingest(data_txt)

    print("Extracting 1 value...")
    rank, value = txt_proc.output()
    print(f"Text value {rank}: {value}")

    # LogProcessor
    print("\nTesting Log Processor...")
    log_proc = LogProcessor()

    print(f"Trying to validate input 'Hello': {log_proc.validate('Hello')}")

    data_log = [
        {"log_level": "NOTICE", "log_message": "Connecting"},
        {"log_level": "ERROR", "log_message": "Unauthorized!"},
    ]
    print(f"Processing data: {data_log}")
    log_proc.ingest(data_log)

    print("Extracting 2 values...")
    for _ in range(2):
        rank, value = log_proc.output()
        print(f" Log entry {rank}: {value}")
