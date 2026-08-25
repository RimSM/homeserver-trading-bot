"""KIS 해외주식 실시간지연체결가(HDFSCNT0) WebSocket 어댑터.

KIS가 보내는 원문은 ``0|HDFSCNT0|건수|필드1^필드2^...`` 형태다. 이 모듈은
그 거래소 프로토콜을 이 프로젝트의 불변 ``Tick`` 이벤트로만 바꾼다. 저장,
화면 표시, 전략 실행과 주문은 이 모듈의 책임이 아니다.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import websockets

from tbot.feeds.base import Feed
from tbot.models import Tick

logger = logging.getLogger(__name__)

KIS_SOURCE = "KIS"
HDFSCNT0 = "HDFSCNT0"
KST = ZoneInfo("Asia/Seoul")

# KIS Open API 공식 예제의 HDFSCNT0 컬럼 순서. 원문은 이름 없는 ^ 구분 값이다.
HDFSCNT0_COLUMNS = (
    "RSYM",
    "SYMB",
    "ZDIV",
    "TYMD",
    "XYMD",
    "XHMS",
    "KYMD",
    "KHMS",
    "OPEN",
    "HIGH",
    "LOW",
    "LAST",
    "SIGN",
    "DIFF",
    "RATE",
    "PBID",
    "PASK",
    "VBID",
    "VASK",
    "EVOL",
    "TVOL",
    "TAMT",
    "BIVL",
    "ASVL",
    "STRN",
    "MTYP",
)


class KISFeedError(RuntimeError):
    """KIS feed가 정상 tick을 만들지 못했을 때의 기반 예외."""


class KISConfigurationError(KISFeedError):
    """필수 환경 변수 또는 구독 설정이 없을 때 발생한다."""


class KISProtocolError(KISFeedError):
    """KIS가 문서화된 형식과 다른 메시지를 보냈을 때 발생한다."""


class KISSubscriptionError(KISFeedError):
    """KIS가 구독 요청을 거절했을 때 발생한다."""


@dataclass(frozen=True)
class KISCredentials:
    """KIS 승인키 발급에 필요한 자격증명.

    실제 비밀값은 생성자 인자 또는 환경 변수에서만 받고 로그와 예외에는 절대
    포함하지 않는다.
    """

    app_key: str
    app_secret: str
    environment: str = "paper"

    @classmethod
    def from_env(cls) -> KISCredentials:
        """``KIS_APP_KEY``, ``KIS_APP_SECRET``, ``KIS_ENV``를 읽는다.

        ``.env`` 파일 자체를 읽는 일은 실행 진입점의 책임이다. 예를 들어 uv는
        ``uv run --env-file .env ...``로 필요한 환경만 주입할 수 있다.
        """

        app_key = os.environ.get("KIS_APP_KEY", "").strip()
        app_secret = os.environ.get("KIS_APP_SECRET", "").strip()
        environment = os.environ.get("KIS_ENV", "paper").strip().lower()

        missing = [
            name
            for name, value in (("KIS_APP_KEY", app_key), ("KIS_APP_SECRET", app_secret))
            if not value
        ]
        if missing:
            raise KISConfigurationError(f"필수 KIS 환경 변수가 비어 있습니다: {', '.join(missing)}")

        return cls(app_key=app_key, app_secret=app_secret, environment=environment)


@dataclass(frozen=True)
class KISOverseasSymbol:
    """프로젝트 종목 코드와 KIS 해외시세 구독키의 대응.

    ``tr_key``에는 KIS가 요구하는 시장 접두어까지 포함한다. 예를 들어 일반
    미국 정규장 나스닥 종목은 ``DNAS`` + 티커 형식이다. ETF의 실제 거래소는
    KIS 종목 조회에서 확인해 명시해야 하며, 티커만으로 추측하지 않는다.
    """

    symbol: str
    tr_key: str

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise KISConfigurationError("symbol은 비어 있을 수 없습니다.")
        if not self.tr_key.strip():
            raise KISConfigurationError("KIS 구독키(tr_key)는 비어 있을 수 없습니다.")


@dataclass(frozen=True)
class KISEndpoints:
    """KIS 모의/실전의 승인 API와 WebSocket 주소."""

    approval_url: str
    websocket_url: str

    @classmethod
    def for_environment(cls, environment: str) -> KISEndpoints:
        normalized = environment.lower()
        if normalized in {"paper", "mock", "vps"}:
            return cls(
                approval_url="https://openapivts.koreainvestment.com:29443/oauth2/Approval",
                websocket_url="ws://ops.koreainvestment.com:31000",
            )
        if normalized in {"real", "prod", "live"}:
            return cls(
                approval_url="https://openapi.koreainvestment.com:9443/oauth2/Approval",
                websocket_url="ws://ops.koreainvestment.com:21000",
            )
        raise KISConfigurationError(
            f"KIS_ENV는 paper(모의) 또는 real(실전)이어야 합니다. 받은 값: {environment!r}"
        )


class KISWebSocketFeed(Feed):
    """KIS HDFSCNT0 체결가를 ``Tick``으로 스트리밍한다.

    한 연결 안에서 구독을 모두 복구하고, 네트워크 종료 시 지수 backoff로 다시
    붙는다. ``aclose``가 호출되면 재접속하지 않는다.
    """

    def __init__(
        self,
        credentials: KISCredentials,
        subscriptions: Iterable[KISOverseasSymbol],
        *,
        reconnect_delay_seconds: float = 1.0,
        max_reconnect_delay_seconds: float = 30.0,
        connect_timeout_seconds: float = 15.0,
    ) -> None:
        items = tuple(subscriptions)
        if not items:
            raise KISConfigurationError("최소 한 종목을 구독해야 합니다.")
        if reconnect_delay_seconds <= 0 or max_reconnect_delay_seconds <= 0:
            raise KISConfigurationError("재접속 대기 시간은 0보다 커야 합니다.")

        self._credentials = credentials
        self._endpoints = KISEndpoints.for_environment(credentials.environment)
        self._subscriptions = items
        self._reconnect_delay_seconds = reconnect_delay_seconds
        self._max_reconnect_delay_seconds = max_reconnect_delay_seconds
        self._connect_timeout_seconds = connect_timeout_seconds
        self._stopping = False
        self._connection: Any | None = None

    @classmethod
    def from_env(
        cls, subscriptions: Iterable[KISOverseasSymbol], **kwargs: Any
    ) -> KISWebSocketFeed:
        """환경 변수 기반의 편의 생성자."""

        return cls(KISCredentials.from_env(), subscriptions, **kwargs)

    @property
    def subscriptions(self) -> tuple[KISOverseasSymbol, ...]:
        """현재 연결 시마다 복구하는 읽기 전용 구독 목록."""

        return self._subscriptions

    async def aclose(self) -> None:
        """수신 루프와 재접속을 멈추고 열린 소켓을 닫는다."""

        self._stopping = True
        if self._connection is not None:
            await self._connection.close()

    async def stream(self) -> AsyncIterator[Tick]:
        """KIS에 연결해 HDFSCNT0 데이터를 ``Tick``으로 내보낸다."""

        self._stopping = False
        delay = self._reconnect_delay_seconds

        while not self._stopping:
            try:
                approval_key = await asyncio.to_thread(self._request_approval_key)
                async with websockets.connect(
                    self._endpoints.websocket_url,
                    ping_interval=None,
                    open_timeout=self._connect_timeout_seconds,
                ) as connection:
                    self._connection = connection
                    await self._subscribe_all(connection, approval_key)
                    delay = self._reconnect_delay_seconds

                    async for raw in connection:
                        if raw.startswith("0|"):
                            for tick in parse_hdfscnt0(raw):
                                yield tick
                        else:
                            await self._handle_system_message(connection, raw)

            except asyncio.CancelledError:
                raise
            except KISSubscriptionError:
                # 권한/구독키 오류는 재접속해도 해결되지 않는다.
                raise
            except (KISFeedError, OSError, websockets.WebSocketException) as exc:
                if self._stopping:
                    break
                logger.warning(
                    "KIS WebSocket 연결이 끊겼습니다. %.1f초 뒤 재접속합니다: %s", delay, exc
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._max_reconnect_delay_seconds)
            finally:
                self._connection = None

    async def _subscribe_all(self, connection: Any, approval_key: str) -> None:
        for subscription in self._subscriptions:
            payload = build_subscribe_message(approval_key, subscription.tr_key)
            await connection.send(json.dumps(payload))

    async def _handle_system_message(self, connection: Any, raw: str) -> None:
        """구독 결과와 KIS의 application-level PINGPONG을 처리한다."""

        try:
            message = json.loads(raw)
        except json.JSONDecodeError as exc:
            # 원문에는 공급자가 예기치 않게 민감 정보를 되돌려 줄 가능성도 있으므로 로그에 싣지 않는다.
            raise KISProtocolError("KIS 시스템 메시지가 JSON이 아닙니다.") from exc

        header = message.get("header", {})
        tr_id = header.get("tr_id")
        if tr_id == "PINGPONG":
            await connection.pong(raw)
            return

        body = message.get("body", {})
        if body.get("rt_cd") != "0":
            detail = body.get("msg1", "KIS가 사유를 제공하지 않았습니다.")
            raise KISSubscriptionError(f"KIS 구독 요청 거절 ({tr_id}): {detail}")
        logger.info("KIS 구독 확인: %s - %s", tr_id, body.get("msg1", ""))

    def _request_approval_key(self) -> str:
        """KIS OAuth approval key를 동기 HTTP로 발급한다 (event loop 밖에서 호출)."""

        approval_url = self._endpoints.approval_url
        if urlsplit(approval_url).scheme != "https":
            raise KISConfigurationError("KIS 승인키 발급 주소는 HTTPS여야 합니다.")

        payload = json.dumps(
            {
                "grant_type": "client_credentials",
                "appkey": self._credentials.app_key,
                "secretkey": self._credentials.app_secret,
            }
        ).encode("utf-8")
        request = Request(
            approval_url,
            data=payload,
            headers={"content-type": "application/json"},
            method="POST",
        )

        try:
            # approval_url은 위에서 HTTPS만 허용하고 KISEndpoints가 정한 고정 주소다.
            with urlopen(request, timeout=self._connect_timeout_seconds) as response:  # nosec B310
                response_payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise KISFeedError(f"KIS 승인키 발급 HTTP 오류: {exc.code}") from exc
        except (URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise KISFeedError("KIS 승인키 발급에 실패했습니다.") from exc

        approval_key = response_payload.get("approval_key", "")
        if not isinstance(approval_key, str) or not approval_key:
            raise KISFeedError("KIS 승인키 응답에 approval_key가 없습니다.")
        return approval_key


def build_subscribe_message(
    approval_key: str, tr_key: str
) -> dict[str, dict[str, dict[str, str] | str]]:
    """KIS 공식 WebSocket 구독 요청 구조를 만든다."""

    if not approval_key:
        raise KISConfigurationError("KIS approval key가 비어 있습니다.")
    if not tr_key:
        raise KISConfigurationError("KIS tr_key가 비어 있습니다.")

    return {
        "header": {
            "approval_key": approval_key,
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8",
        },
        "body": {"input": {"tr_id": HDFSCNT0, "tr_key": tr_key}},
    }


def parse_hdfscnt0(raw: str) -> tuple[Tick, ...]:
    """HDFSCNT0 원문 하나를 하나 이상의 ``Tick``으로 변환한다.

    KIS는 한 WebSocket frame에 여러 체결가를 붙여 보낼 수 있으므로 반환값이
    단일 Tick이 아니라 tuple이다.
    """

    envelope = raw.split("|", maxsplit=3)
    if len(envelope) != 4 or envelope[0] != "0" or envelope[1] != HDFSCNT0:
        raise KISProtocolError("HDFSCNT0 데이터 envelope 형식이 올바르지 않습니다.")

    try:
        count = int(envelope[2])
    except ValueError as exc:
        raise KISProtocolError(f"HDFSCNT0 건수 값이 숫자가 아닙니다: {envelope[2]!r}") from exc
    if count < 1:
        raise KISProtocolError(f"HDFSCNT0 건수는 1 이상이어야 합니다: {count}")

    values = envelope[3].split("^")
    expected_value_count = count * len(HDFSCNT0_COLUMNS)
    if len(values) != expected_value_count:
        raise KISProtocolError(
            f"HDFSCNT0 필드 수가 맞지 않습니다: 수신 {len(values)}개, 기대 {expected_value_count}개"
        )

    ticks: list[Tick] = []
    for offset in range(0, len(values), len(HDFSCNT0_COLUMNS)):
        row = dict(
            zip(HDFSCNT0_COLUMNS, values[offset : offset + len(HDFSCNT0_COLUMNS)], strict=True)
        )
        try:
            price = Decimal(row["LAST"])
        except (InvalidOperation, ValueError) as exc:
            raise KISProtocolError(
                f"HDFSCNT0 현재가가 Decimal이 아닙니다: {row['LAST']!r}"
            ) from exc

        symbol = row["SYMB"].strip()
        if not symbol:
            raise KISProtocolError("HDFSCNT0 종목코드(SYMB)가 비어 있습니다.")
        ticks.append(
            Tick(
                symbol=symbol,
                price=price,
                ts=_parse_kis_timestamp(row["KYMD"], row["KHMS"]),
                source=KIS_SOURCE,
            )
        )
    return tuple(ticks)


def _parse_kis_timestamp(korean_date: str, korean_time: str) -> datetime:
    """KIS 한국일자/시간을 UTC aware datetime으로 바꾼다."""

    try:
        local = datetime.strptime(f"{korean_date}{korean_time}", "%Y%m%d%H%M%S").replace(tzinfo=KST)
    except ValueError as exc:
        raise KISProtocolError(
            f"HDFSCNT0 한국 시각 형식이 올바르지 않습니다: {korean_date!r} {korean_time!r}"
        ) from exc
    return local.astimezone(UTC)
