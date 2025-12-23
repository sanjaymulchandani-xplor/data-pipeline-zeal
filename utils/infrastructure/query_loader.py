import os
from functools import lru_cache


@lru_cache(maxsize=32)
def load_query(queries_dir: str, name: str) -> str:
    # Load SQL query from file by name
    path = os.path.join(queries_dir, f"{name}.sql")
    with open(path, "r") as f:
        return f.read().strip()

