"""KIS Tick을 MinIO Landing NDJSON+Zstd micro-batch로 저장한다.

Landing은 파싱 결과가 아닌 재처리 기준 데이터다. 따라서 공통 ``Tick`` 필드와
KIS HDFSCNT0 원본 필드를 함께 보존한다. 인증에 쓰인 WebSocket 메시지는 이
모듈로 전달되지 않으며 기록하지 않는다.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

import boto3
import zstandard as zstd

from tbot.feeds.kis_ws import KISTickEvent

logger = logging.getLogger(__name__)

LANDING_SCHEMA_VERSION = 1
DEFAULT_MAX_RECORDS = 50_000
DEFAULT_FLUSH_INTERVAL_SECONDS = 300.0


class LandingConfigurationError(ValueError):
    """Landing writer 환경 설정이 유효하지 않을 때 발생한다."""


@dataclass(frozen=True)
class MinioSettings:
    """MinIO S3 호환 API 연결 설정.

    값은 로컬 ``.env``에서만 읽고, 예외·로그에는 access key나 secret을 넣지
    않는다.
    """

    endpoint_url: str
    access_key: str
    secret_key: str
    bucket: str
    region: str = "us-east-1"

    @classmethod
    def from_env(cls) -> MinioSettings:
        values = {
            "MINIO_ENDPOINT_URL": os.environ.get("MINIO_ENDPOINT_URL", "").strip(),
            "MINIO_ACCESS_KEY": os.environ.get("MINIO_ACCESS_KEY", "").strip(),
            "MINIO_SECRET_KEY": os.environ.get("MINIO_SECRET_KEY", "").strip(),
            "MINIO_BUCKET": os.environ.get("MINIO_BUCKET", "").strip(),
        }
        missing = [name for name, value in values.items() if not value]
        if missing:
            raise LandingConfigurationError(
                f"필수 MinIO 환경 변수가 비어 있습니다: {', '.join(missing)}"
            )

        return cls(
            endpoint_url=values["MINIO_ENDPOINT_URL"],
            access_key=values["MINIO_ACCESS_KEY"],
            secret_key=values["MINIO_SECRET_KEY"],
            bucket=values["MINIO_BUCKET"],
            region=os.environ.get("MINIO_REGION", "us-east-1").strip() or "us-east-1",
        )

    def __post_init__(self) -> None:
        parsed = urlsplit(self.endpoint_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise LandingConfigurationError("MINIO_ENDPOINT_URL은 http(s) URL이어야 합니다.")
        if not self.bucket.strip():
            raise LandingConfigurationError("MINIO_BUCKET은 비어 있을 수 없습니다.")

    def create_s3_client(self) -> Any:
        """boto3의 S3 호환 클라이언트를 생성한다."""

        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url.rstrip("/"),
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
        )


@dataclass(frozen=True)
class LandingBatchSettings:
    """작은 파일 과다 생성과 데이터 유실 지연의 균형을 맞추는 flush 기준."""

    max_records: int = DEFAULT_MAX_RECORDS
    flush_interval_seconds: float = DEFAULT_FLUSH_INTERVAL_SECONDS

    @classmethod
    def from_env(cls) -> LandingBatchSettings:
        try:
            max_records = int(os.environ.get("TBOT_LANDING_MAX_RECORDS", DEFAULT_MAX_RECORDS))
            flush_interval_seconds = float(
                os.environ.get("TBOT_LANDING_FLUSH_SECONDS", DEFAULT_FLUSH_INTERVAL_SECONDS)
            )
        except ValueError as exc:
            raise LandingConfigurationError(
                "TBOT_LANDING_MAX_RECORDS는 정수이고 TBOT_LANDING_FLUSH_SECONDS는 숫자여야 합니다."
            ) from exc
        return cls(max_records=max_records, flush_interval_seconds=flush_interval_seconds)

    def __post_init__(self) -> None:
        if self.max_records < 1:
            raise LandingConfigurationError("Landing 최대 레코드 수는 1 이상이어야 합니다.")
        if self.flush_interval_seconds <= 0:
            raise LandingConfigurationError("Landing flush 시간은 0보다 커야 합니다.")


@dataclass(frozen=True)
class LandingObject:
    """성공적으로 MinIO에 기록된 Landing 객체의 식별자."""

    key: str
    record_count: int
    event_date: date
    symbol: str


@dataclass
class _PendingBatch:
    key: str
    opened_at: datetime
    events: list[KISTickEvent] = field(default_factory=list)


class LandingWriter:
    """``(UTC event date, symbol)``별 Landing micro-batch writer.

    ``append``는 최대 건수에 도달하면 해당 파티션만 즉시 업로드한다.
    ``flush_due``는 시간 기준 업로드를 담당하므로, 실시간 소비자는 주기적으로
    호출해야 한다. 업로드가 실패하면 메모리 버퍼를 비우지 않아 같은 object key로
    재시도할 수 있다.

    이 클래스의 public 메서드는 동시에 호출하지 않는다. 비동기 소비자는 lock으로
    직렬화한 뒤 ``asyncio.to_thread``로 호출하면 네트워크 업로드가 event loop를
    막지 않는다.
    """

    def __init__(
        self,
        client: Any,
        bucket: str,
        *,
        batch_settings: LandingBatchSettings | None = None,
        part_id_factory: Callable[[], str] | None = None,
    ) -> None:
        if not bucket.strip():
            raise LandingConfigurationError("MinIO bucket은 비어 있을 수 없습니다.")

        self._client = client
        self._bucket = bucket
        self._settings = batch_settings or LandingBatchSettings()
        self._part_id_factory = part_id_factory or (lambda: uuid4().hex)
        self._batches: dict[tuple[date, str], _PendingBatch] = {}

    @classmethod
    def from_env(
        cls,
        *,
        batch_settings: LandingBatchSettings | None = None,
    ) -> LandingWriter:
        """환경 변수 기반 MinIO client와 기본 batch 설정으로 만든다."""

        minio = MinioSettings.from_env()
        return cls(
            minio.create_s3_client(),
            minio.bucket,
            batch_settings=batch_settings or LandingBatchSettings.from_env(),
        )

    @property
    def pending_record_count(self) -> int:
        """아직 MinIO에 성공적으로 기록되지 않은 전체 레코드 수."""

        return sum(len(batch.events) for batch in self._batches.values())

    @property
    def flush_interval_seconds(self) -> float:
        """실시간 소비자가 ``flush_due``를 확인할 기준 시간."""

        return self._settings.flush_interval_seconds

    def append(self, event: KISTickEvent, *, now: datetime | None = None) -> LandingObject | None:
        """이벤트를 버퍼에 넣고 건수 임계값이면 해당 파티션을 flush한다."""

        flushed_at = _as_utc(now or datetime.now(UTC))
        partition = _partition_for(event)
        batch = self._batches.get(partition)
        if batch is None:
            batch = _PendingBatch(
                key=_object_key(
                    *partition,
                    batch_started_at=event.received_at,
                    part_id=self._part_id_factory(),
                ),
                opened_at=flushed_at,
            )
            self._batches[partition] = batch
        batch.events.append(event)

        if len(batch.events) >= self._settings.max_records:
            return self._flush_partition(partition)
        return None

    def flush_due(self, *, now: datetime | None = None) -> list[LandingObject]:
        """시간 임계값을 넘긴 모든 파티션을 MinIO에 기록한다."""

        flushed_at = _as_utc(now or datetime.now(UTC))
        interval = timedelta(seconds=self._settings.flush_interval_seconds)
        due = [
            partition
            for partition, batch in self._batches.items()
            if flushed_at - batch.opened_at >= interval
        ]
        return [self._flush_partition(partition) for partition in due]

    def flush_all(self) -> list[LandingObject]:
        """종료 전 남은 모든 파티션을 기록한다."""

        return [self._flush_partition(partition) for partition in tuple(self._batches)]

    def _flush_partition(self, partition: tuple[date, str]) -> LandingObject:
        batch = self._batches[partition]
        payload = _serialize_events(batch.events)
        self._client.put_object(
            Bucket=self._bucket,
            Key=batch.key,
            Body=payload,
            # ContentEncoding=zstd를 쓰면 HTTP client가 응답을 투명하게 풀 수 있어
            # .jsonl.zst의 raw bytes와 S3 checksum 검증이 어긋난다. 압축 정보는
            # metadata로만 남기고 객체 본문은 항상 Zstd bytes 그대로 보존한다.
            ContentType="application/zstd",
            Metadata={
                "schema-version": str(LANDING_SCHEMA_VERSION),
                "record-count": str(len(batch.events)),
                "source": "KIS",
                "content-format": "ndjson",
                "compression": "zstd",
            },
        )
        del self._batches[partition]

        event_date, symbol = partition
        result = LandingObject(
            key=batch.key,
            record_count=len(batch.events),
            event_date=event_date,
            symbol=symbol,
        )
        logger.info("Landing batch 저장 완료: %s (%d건)", result.key, result.record_count)
        return result


def _partition_for(event: KISTickEvent) -> tuple[date, str]:
    event_ts = _as_utc(event.tick.ts)
    symbol = event.tick.symbol.strip().upper()
    if not symbol or any(character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-" for character in symbol):
        raise LandingConfigurationError(f"Landing 경로에 쓸 수 없는 종목코드입니다: {event.tick.symbol!r}")
    return event_ts.date(), symbol


def _object_key(
    event_date: date,
    symbol: str,
    *,
    batch_started_at: datetime,
    part_id: str,
) -> str:
    if not part_id or "/" in part_id:
        raise LandingConfigurationError("Landing part 식별자가 올바르지 않습니다.")
    batch_timestamp = _as_utc(batch_started_at).strftime("%Y%m%dT%H%M%S.%fZ")
    return (
        f"landing/kis/date={event_date.isoformat()}/symbol={symbol}/"
        f"part-{batch_timestamp}-{part_id}.jsonl.zst"
    )


def _serialize_events(events: list[KISTickEvent]) -> bytes:
    records = [_to_landing_record(event) for event in events]
    ndjson = "".join(
        json.dumps(record, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
        for record in records
    ).encode("utf-8")
    return zstd.ZstdCompressor().compress(ndjson)


def _to_landing_record(event: KISTickEvent) -> dict[str, Any]:
    """한 KIS 이벤트를 Landing NDJSON 한 줄의 안정된 스키마로 바꾼다."""

    return {
        "schema_version": LANDING_SCHEMA_VERSION,
        "symbol": event.tick.symbol,
        "price": str(event.tick.price),
        "event_ts": _as_utc(event.tick.ts).isoformat(),
        "received_at": _as_utc(event.received_at).isoformat(),
        "source": event.tick.source,
        "source_fields": event.source_field_map,
    }


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise LandingConfigurationError("Landing 시각은 timezone-aware datetime이어야 합니다.")
    return value.astimezone(UTC)
