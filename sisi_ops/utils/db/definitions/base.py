from abc import ABC, abstractmethod
from typing import Mapping
import os

class BaseDBConfig(ABC):
    @property
    @abstractmethod
    def config(self) -> Mapping[str, str]:
        ...

    @abstractmethod
    def create_db_file(self) -> None:
        """ONLY for file-based databases like SQLite."""
        ...

    @staticmethod
    def _require_env(varname: str) -> str:
        value = os.getenv(varname)
        if value is None:
            raise EnvironmentError(f"Missing value for {varname}")
        return value