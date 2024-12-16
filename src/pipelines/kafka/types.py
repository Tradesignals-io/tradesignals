"""Kafka Types."""

from datetime import datetime
from enum import StrEnum
from typing import Any, List, Optional, Tuple

from attrs import define


@define
class ConsumerGroup:
    """A consumer group."""

    name: str
    topics: List[str]


class EntitlementType(StrEnum):
    """Enumeration for data entitlements.

    Attributes:
        FREE: Free entitlement.
            (Available to all registered users level 0, 1, 2, 3)
        BASIC: Paid entitlement.
            (Available to all level 1, 2, 3 paid users)
        PRO: Paid entitlement.
            (Available to all level 2, 3 paid users)
        PRO_PLUS: Paid entitlement.
            (Available to all level 3 paid users)
        NONE: No entitlement. Available to the general public.

    """

    FREE = "free"
    BASIC = "basic"
    PRO = "pro"
    PRO_PLUS = "pro_plus"
    NONE = "none"


@define
class RecordMeta:
    """A record metadata.

    Attributes:
        source_id: str The source ID.
        dataset_id: str The dataset ID.
        ts_out: int The timestamp of the record.
        pipeline_id: str The pipeline ID.
        entitlement_id: EntitlementType The entitlement ID.
    """

    source_id: str | None = None
    dataset_id: str | None = None
    ts_out: int | None = None
    pipeline_id: str | None = None
    entitlement_id: EntitlementType | None = None


@define
class KafkaRecord:
    """A Kafka record.

    Attributes:
        key: str The key of the record.
        value: str The value of the record.
        topic: str The topic of the record.
        partition: int The partition of the record.
        offset: int The offset of the record.
        timestamp: int The timestamp of the record.
        headers: List[Tuple[str, str]] | None The headers of the reco

    """

    key: str
    value: str
    topic: str
    partition: int | None = None
    offset: int | None = None
    timestamp: int | None = None
    headers: List[Tuple[str, str]] | None = None
    metadata: RecordMeta | None = None
    source_id: str | None = None
    dataset_id: str | None = None
    ts_out: int | None = None

    def __init__(
        self,
        key: str,
        value: str,
        topic: str,
        partition: Optional[int] = None,
        offset: Optional[int] = None,
        timestamp: Optional[int] = None,
        source_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
        ts_out: Optional[int] = None,
        headers: Optional[List[Tuple[str, str]]] = None,
        metadata: Optional[RecordMeta] = None,
    ) -> None:
        """Initialize the KafkaRecord.

        Args:
            key : str
                The key of the record.
            value : str
                The value of the record.
            topic : str
                The topic of the record.
            partition : Optional[int]
                The partition of the record.
            offset : Optional[int]
                The offset of the record.
            timestamp : Optional[int]
                The timestamp of the record.
            source_id : Optional[str]
                The source ID of the record.
            dataset_id : Optional[str]
                The dataset ID of the record.
            ts_out : Optional[int]
                The timestamp of the record output.
            headers : Optional[List[Tuple[str, str]]]
                The headers of the record.
            metadata : Optional[RecordMeta]
                The metadata of the record.
        """
        self.key = key
        self.value = value
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.timestamp = timestamp if timestamp is not None else int(datetime.now().timestamp())
        self.source_id = source_id
        self.dataset_id = dataset_id
        self.ts_out = ts_out if ts_out is not None else int(datetime.now().timestamp() * 1000)
        self.headers = headers if headers is not None else []
        self.metadata = metadata if metadata is not None else RecordMeta(
            source_id=source_id,
            dataset_id=dataset_id,
            ts_out=ts_out if ts_out is not None else int(datetime.now().timestamp() * 1000),
            pipeline_id=topic,
            entitlement_id=EntitlementType.NONE,
        )
        for attr in RecordMeta.__annotations__:
            value = getattr(self.metadata, attr)
            self.add_header(key=attr, value=value)

    def __str__(self) -> str:
        """Return a string representation of the KafkaRecord.

        Returns:
            str: A string representation of the KafkaRecord.
        """
        return (
            f"KafkaRecord("
            f"key={self.key}, "
            f"value={self.value}, "
            f"topic={self.topic}, "
            f"partition={self.partition}, "
            f"offset={self.offset}, "
            f"timestamp={self.timestamp}, "
            f"headers={self.headers})"
        )

    def add_header(self, key: str, value: Any) -> None:
        """Add a header to the record.

        Args:
            key: str
                The key of the header.
            value: Any
                The value of the header.
        """
        if self.headers is None:
            self.headers = []
        self.headers.append((key, str(value)))


__all__ = ["KafkaRecord", "RecordMeta", "EntitlementType"]


if __name__ == "__main__":
    record = KafkaRecord(
        key="key",
        value="value",
        topic="topic",
        source_id="custom_source_id",
        dataset_id="custom_dataset_id",
        ts_out=int(datetime.utcnow().timestamp() * 1000),
    )
    print(record)
