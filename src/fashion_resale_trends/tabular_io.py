
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


def read_csv_dicts(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv_dicts(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        fields = fieldnames or []
        if fields:
            with path.open("w", newline="", encoding="utf-8") as handle:
                csv.DictWriter(handle, fieldnames=fields).writeheader()
        else:
            path.write_text("", encoding="utf-8")
        return
    fields = fieldnames or list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def count_csv_rows(path: Path) -> int:
    with path.open(encoding="utf-8") as handle:
        total = sum(1 for _ in handle)
    return max(total - 1, 0)


def read_parquet_dataset(directory: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    parquet_dir = directory / "parquet" if (directory / "parquet").is_dir() else directory
    for part in sorted(parquet_dir.glob("*.parquet")):
        rows.extend(pq.read_table(part).to_pylist())
    return rows


def write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows) if rows else pa.table({"keyword": pa.array([], type=pa.string())})
    pq.write_table(table, path)
