import os
from typing import Mapping
from .base import BaseDBConfig


class SQLiteConfig(BaseDBConfig):
    """SQLite database configuration loader."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    @property
    def config(self) -> Mapping[str, str]:
        return {"database": self._require_env(self.db_path)}

    def create_db_file(self) -> None:
        """Create the SQLite database file if it does not exist."""
        # _db_path = self._require_env(self.db_path)
        if not os.path.exists(self.db_path):
            open(self.db_path, 'a').close()
