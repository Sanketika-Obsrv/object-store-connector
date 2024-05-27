from dataclasses import dataclass, field
from datetime import datetime
from typing import List
from uuid import uuid4


@dataclass
class Tag:
    key: str
    value: str

    def to_dict(self):
        return {"key": self.key, "value": self.value}

    def to_aws(self):
        return {"Key": self.key, "Value": self.value}

    def to_gcs(self):
        return {"Key": self.key, "Value": self.value}


@dataclass
class ObjectInfo:
    id: str = field(default_factory=lambda: str(uuid4()))
    connector_id: str = None
    dataset_id: str = None
    location: str = None
    format: str = None
    file_size_kb: int = 0
    in_time: datetime = field(default_factory=datetime.now)
    start_processing_time: datetime = None
    end_processing_time: datetime = None
    download_time: float = 0.0
    file_hash: str = None
    num_of_retries: int = 0
    tags: List[Tag] = field(default_factory=list)

    def to_json(self):
        return {
            "id": self.id,
            "connector_id": self.connector_id,
            "dataset_id": self.dataset_id,
            "location": self.location,
            "format": self.format,
            "file_size_kb": self.file_size_kb,
            "in_time": self.in_time,
            "download_time": self.download_time,
            "start_processing_time": self.start_processing_time,
            "end_processing_time": self.end_processing_time,
            "file_hash": self.file_hash,
            "num_of_retries": self.num_of_retries,
            "tags": [tag.__dict__ for tag in self.tags],
        }
