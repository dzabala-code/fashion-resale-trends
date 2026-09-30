from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, Any

from fashion_resale_trends.config import Settings, settings


def utc_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


@dataclass
class DatasetRef:
    bucket: str
    key: str
    local_path: Path

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


class ObjectStore:
    
    def __init__(self, cfg: Settings | None = None):
        self.cfg = cfg or settings()
        self.bucket = self.cfg.minio_bucket
        self.local_root = self.cfg.local_data_dir / self.bucket
        self.local_root.mkdir(parents=True, exist_ok=True)

    def key(self, *parts: str) -> str:
        clean = [str(part).strip("/") for part in parts if str(part).strip("/")]
        if not clean:
            raise ValueError("At least one key part is required.")
        return "/".join(clean)

    def dataset_file(self, layer: str, group: str, entity: str, run_id: str, filename: str) -> DatasetRef:
        key = self.key(layer, group, entity, run_id, filename)
        return DatasetRef(self.bucket, key, self.local_root / key)

    def layer_dir(self, layer: str, group: str, entity: str) -> Path:
        return self.local_root / self.key(layer, group, entity)

    def latest_run_dir(self, layer: str, group: str, entity: str) -> Path | None:
        root = self.layer_dir(layer, group, entity)
        if not root.exists():
            return None
        runs = sorted(path for path in root.iterdir() if path.is_dir())
        return runs[-1] if runs else None

    def write_text(self, ref: DatasetRef, content: str) -> DatasetRef:
        ref.local_path.parent.mkdir(parents=True, exist_ok=True)
        ref.local_path.write_text(content, encoding="utf-8")
        self._upload_if_enabled(ref)
        return ref

    def write_jsonl(self, ref: DatasetRef, records: Iterable[Mapping[str, Any]]) -> int:
        ref.local_path.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        with ref.local_path.open("w", encoding="utf-8") as output_file:
            for record in records:
                output_file.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                count += 1
        self._upload_if_enabled(ref)
        return count

    def upload_tree_if_enabled(self, root: Path, key_prefix: str) -> None:
        if self.cfg.storage_backend != "s3" or not root.exists():
            return
        for path in root.rglob("*"):
            if path.is_file():
                key = self.key(key_prefix, str(path.relative_to(root)))
                ref = DatasetRef(self.bucket, key, path)
                self._upload_if_enabled(ref)

    def read_jsonl(self, layer: str, group: str, entity: str, run_dir: Path | None = None) -> list[dict[str, Any]]:
        selected_run = run_dir or self.latest_run_dir(layer, group, entity)
        if selected_run is None:
            return []
        records: list[dict[str, Any]] = []
        for path in sorted(selected_run.glob("*.jsonl")):
            with path.open(encoding="utf-8") as input_file:
                for line in input_file:
                    if line.strip():
                        records.append(json.loads(line))
        return records

    def _upload_if_enabled(self, ref: DatasetRef) -> None:
        if self.cfg.storage_backend != "s3":
            return
        try:
            import boto3
        except Exception:
            return
        try:
            client = boto3.client(
                "s3",
                endpoint_url=self.cfg.minio_endpoint_url,
                aws_access_key_id=self.cfg.minio_access_key,
                aws_secret_access_key=self.cfg.minio_secret_key,
            )
            client.upload_file(str(ref.local_path), ref.bucket, ref.key)
        except Exception:
            if not self.cfg.use_fixtures_if_source_fail:
                raise
