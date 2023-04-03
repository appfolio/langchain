from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, List, Optional

from sqlalchemy.exc import ProgrammingError


class SQLDatabase:
    """SQL wrapper around a database."""

    def __init__(
        self,
        connector,
        schema_dir: str,
        include_tables: Optional[List[str]] = None,
    ):
        self.connector = connector
        self.schema_dir = os.path.expanduser(schema_dir)
        self.dialect = "MySQL"

        all_tables = set([p.stem for p in list(Path(".").rglob("*.txt"))])
        if include_tables:
            self.table_names = all_tables.intersection(set(include_tables))
        else:
            self.table_names = all_tables

        # generate table information cache
        self.table_info_map = {
            t: self.read_info(os.path.join(self.schema_dir, f"{t}.txt"))
            for t in self.table_names
        }

    def get_table_names(self) -> Iterable[str]:
        """Get names of tables available."""
        return self.table_names

    @property
    def table_info(self) -> str:
        """Information about all tables in the database."""
        return self.get_table_info()

    def read_info(self, filepath: str):
        if os.path.isfile(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                return str(f.read())
        else:
            return ""

    def get_table_info(self, table_names: Optional[List[str]] = None) -> str:
        """Get information about specified tables."""
        info = list(map(lambda x: self.table_info_map.get(x, ""), table_names))
        return "\n\n".join(info)

    def run(self, command: str, *args) -> str:
        return self.connector.execute(command)

    def get_table_info_no_throw(self, table_names: Optional[List[str]] = None) -> str:
        try:
            return self.get_table_info(table_names)
        except ValueError as err:
            return f"Error: {err}"

    def run_no_throw(self, command: str, *args) -> str:
        try:
            return self.run(command, *args)
        except ProgrammingError as err:
            return f"Error: {err}"
