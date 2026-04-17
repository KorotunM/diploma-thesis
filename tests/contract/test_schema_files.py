import json
from pathlib import Path


def test_json_schema_files_are_valid_json() -> None:
    schema_paths = sorted(Path("schemas").rglob("*.json"))
    assert schema_paths, "Schema directory is empty."

    for path in schema_paths:
        with path.open("r", encoding="utf-8") as schema_file:
            json.load(schema_file)
