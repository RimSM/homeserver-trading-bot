import asyncio
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from tbot.feeds.kis_ws import (
    HDFSCNT0,
    KISConfigurationError,
    KISCredentials,
    KISEndpoints,
    KISOverseasSymbol,
    KISProtocolError,
    KISSubscriptionError,
    KISWebSocketFeed,
    build_subscribe_message,
    parse_hdfscnt0,
)


def hdfscnt0_row(*, symbol: str = "SOXL", price: str = "42.75", time: str = "223015") -> str:
    return "^".join(
        [
            f"DNAS{symbol}",
            symbol,
            "2",
            "20260813",
            "20260813",
            "093015",
            "20260813",
            time,
            "41.00",
            "43.00",
            "40.00",
            price,
            "2",
            "1.25",
            "3.01",
            "42.74",
            "42.76",
            "10",
            "20",
            "3",
            "123456",
            "5000000",
            "20",
            "30",
            "100",
            "1",
        ]
    )


def test_parse_hdfscnt0_converts_protocol_data_to_utc_decimal_tick() -> None:
    ticks = parse_hdfscnt0(f"0|{HDFSCNT0}|1|{hdfscnt0_row()}")

    assert len(ticks) == 1
    tick = ticks[0]
    assert tick.symbol == "SOXL"
    assert tick.price == Decimal("42.75")
    assert tick.ts == datetime(2026, 8, 13, 13, 30, 15, tzinfo=UTC)
    assert tick.source == "KIS"


def test_parse_hdfscnt0_supports_multiple_ticks_in_one_frame() -> None:
    raw = (
        f"0|{HDFSCNT0}|2|{hdfscnt0_row(symbol='SOXL')}^{hdfscnt0_row(symbol='TQQQ', price='81.10')}"
    )

    ticks = parse_hdfscnt0(raw)

    assert [(tick.symbol, tick.price) for tick in ticks] == [
        ("SOXL", Decimal("42.75")),
        ("TQQQ", Decimal("81.10")),
    ]


@pytest.mark.parametrize(
    "raw",
    [
        "0|HDFSCNT0|x|not-used",
        "0|HDFSCNT0|1|too^short",
        "0|OTHER|1|not-used",
    ],
)
def test_parse_hdfscnt0_rejects_malformed_frames(raw: str) -> None:
    with pytest.raises(KISProtocolError):
        parse_hdfscnt0(raw)


def test_build_subscribe_message_uses_kis_hdfscnt0_contract() -> None:
    message = build_subscribe_message("approval-key", "DNASSOXL")

    assert message == {
        "header": {
            "approval_key": "approval-key",
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8",
        },
        "body": {"input": {"tr_id": HDFSCNT0, "tr_key": "DNASSOXL"}},
    }


def test_feed_uses_paper_endpoints_and_keeps_explicit_subscription_key() -> None:
    feed = KISWebSocketFeed(
        KISCredentials("key", "secret", environment="paper"),
        [KISOverseasSymbol(symbol="SOXL", tr_key="DNASSOXL")],
    )

    assert feed.subscriptions == (KISOverseasSymbol(symbol="SOXL", tr_key="DNASSOXL"),)
    assert KISEndpoints.for_environment("paper").websocket_url.endswith(":31000")


def test_invalid_environment_and_empty_subscription_are_rejected() -> None:
    with pytest.raises(KISConfigurationError):
        KISEndpoints.for_environment("unknown")
    with pytest.raises(KISConfigurationError):
        KISWebSocketFeed(KISCredentials("key", "secret"), [])


def test_approval_key_request_rejects_non_https_endpoint() -> None:
    feed = KISWebSocketFeed(
        KISCredentials("key", "secret"),
        [KISOverseasSymbol(symbol="SOXL", tr_key="DNASSOXL")],
    )
    feed._endpoints = KISEndpoints("http://example.invalid/approval", "ws://example.invalid")

    with pytest.raises(KISConfigurationError, match="HTTPS"):
        feed._request_approval_key()


def test_subscription_error_is_surfaced_without_retry() -> None:
    feed = KISWebSocketFeed(
        KISCredentials("key", "secret"),
        [KISOverseasSymbol(symbol="SOXL", tr_key="DNASSOXL")],
    )

    class Connection:
        async def pong(self, _raw: str) -> None:
            raise AssertionError("구독 오류에는 pong을 보내면 안 됩니다.")

    async def scenario() -> None:
        with pytest.raises(KISSubscriptionError, match="권한 없음"):
            await feed._handle_system_message(
                Connection(),
                '{"header":{"tr_id":"HDFSCNT0"},"body":{"rt_cd":"1","msg1":"권한 없음"}}',
            )

    asyncio.run(scenario())


def test_invalid_system_message_does_not_include_raw_content_in_exception() -> None:
    feed = KISWebSocketFeed(
        KISCredentials("key", "secret"),
        [KISOverseasSymbol(symbol="SOXL", tr_key="DNASSOXL")],
    )

    class Connection:
        async def pong(self, _raw: str) -> None:
            raise AssertionError("잘못된 JSON에는 pong을 보내면 안 됩니다.")

    async def scenario() -> None:
        with pytest.raises(KISProtocolError) as exc_info:
            await feed._handle_system_message(Connection(), '{"synthetic-sensitive-value"')
        assert "synthetic-sensitive-value" not in str(exc_info.value)

    asyncio.run(scenario())
