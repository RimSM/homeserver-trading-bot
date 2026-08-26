import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
import zstandard as zstd

from tbot.feeds.kis_ws import KISTickEvent
from tbot.models import Tick
from tbot.storage.landing import (
    LandingBatchSettings,
    LandingConfigurationError,
    LandingWriter,
    MinioSettings,
)


class RecordingS3Client:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.fail = False

    def put_object(self, **kwargs: object) -> None:
        self.calls.append(kwargs)
        if self.fail:
            raise OSError("MinIO에 연결할 수 없습니다.")


def event(
    *,
    symbol: str = "TQQQ",
    event_ts: datetime = datetime(2026, 8, 13, 13, 30, tzinfo=UTC),
) -> KISTickEvent:
    return KISTickEvent(
        tick=Tick(symbol=symbol, price=Decimal("81.10"), ts=event_ts, source="KIS"),
        received_at=datetime(2026, 8, 13, 13, 30, 1, tzinfo=UTC),
        source_fields=(("SYMB", symbol), ("LAST", "81.10"), ("KHMS", "223000")),
    )


def decode_uploaded_records(client: RecordingS3Client, index: int = 0) -> list[dict[str, object]]:
    compressed = client.calls[index]["Body"]
    assert isinstance(compressed, bytes)
    decompressed = zstd.ZstdDecompressor().decompress(compressed).decode("utf-8")
    return [json.loads(line) for line in decompressed.splitlines()]


def test_writer_flushes_zstd_ndjson_when_partition_reaches_max_records() -> None:
    client = RecordingS3Client()
    writer = LandingWriter(
        client,
        "trading-bot",
        batch_settings=LandingBatchSettings(max_records=2, flush_interval_seconds=5),
        part_id_factory=lambda: "fixed-id",
    )
    now = datetime(2026, 8, 13, 13, 31, tzinfo=UTC)

    assert writer.append(event(), now=now) is None
    result = writer.append(event(), now=now + timedelta(seconds=1))

    assert result is not None
    assert result.key == (
        "landing/kis/date=2026-08-13/symbol=TQQQ/"
        "part-20260813T133001.000000Z-fixed-id.jsonl.zst"
    )
    assert result.record_count == 2
    assert writer.pending_record_count == 0
    assert client.calls[0]["Bucket"] == "trading-bot"
    assert client.calls[0]["ContentType"] == "application/zstd"
    assert client.calls[0]["Metadata"] == {
        "compression": "zstd",
        "content-format": "ndjson",
        "schema-version": "1",
        "record-count": "2",
        "source": "KIS",
    }

    records = decode_uploaded_records(client)
    assert records[0] == {
        "event_ts": "2026-08-13T13:30:00+00:00",
        "price": "81.10",
        "received_at": "2026-08-13T13:30:01+00:00",
        "schema_version": 1,
        "source": "KIS",
        "source_fields": {"KHMS": "223000", "LAST": "81.10", "SYMB": "TQQQ"},
        "symbol": "TQQQ",
    }


def test_writer_flushes_each_date_symbol_partition_independently_when_due() -> None:
    client = RecordingS3Client()
    writer = LandingWriter(
        client,
        "trading-bot",
        batch_settings=LandingBatchSettings(max_records=10, flush_interval_seconds=5),
        part_id_factory=iter(("tqqq", "soxl")).__next__,
    )
    now = datetime(2026, 8, 13, 13, 31, tzinfo=UTC)

    writer.append(event(), now=now)
    writer.append(event(symbol="SOXL"), now=now)
    results = writer.flush_due(now=now + timedelta(seconds=5))

    assert {result.key for result in results} == {
        "landing/kis/date=2026-08-13/symbol=TQQQ/part-20260813T133001.000000Z-tqqq.jsonl.zst",
        "landing/kis/date=2026-08-13/symbol=SOXL/part-20260813T133001.000000Z-soxl.jsonl.zst",
    }
    assert writer.pending_record_count == 0


def test_failed_upload_keeps_same_batch_and_object_key_for_retry() -> None:
    client = RecordingS3Client()
    client.fail = True
    writer = LandingWriter(
        client,
        "trading-bot",
        batch_settings=LandingBatchSettings(max_records=1, flush_interval_seconds=5),
        part_id_factory=lambda: "retry-id",
    )

    with pytest.raises(OSError, match="연결할 수 없습니다"):
        writer.append(event())

    assert writer.pending_record_count == 1
    client.fail = False
    result = writer.flush_all()[0]

    assert result.key == (
        "landing/kis/date=2026-08-13/symbol=TQQQ/"
        "part-20260813T133001.000000Z-retry-id.jsonl.zst"
    )
    assert [call["Key"] for call in client.calls] == [result.key, result.key]
    assert writer.pending_record_count == 0


def test_minio_settings_require_complete_valid_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINIO_ENDPOINT_URL", "https://s3.rimsm.com")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "access")
    monkeypatch.setenv("MINIO_SECRET_KEY", "secret")
    monkeypatch.setenv("MINIO_BUCKET", "trading-bot")

    settings = MinioSettings.from_env()

    assert settings.endpoint_url == "https://s3.rimsm.com"
    assert settings.bucket == "trading-bot"

    with pytest.raises(LandingConfigurationError, match="http\\(s\\) URL"):
        MinioSettings("not-a-url", "access", "secret", "trading-bot")
