"""KIS WebSocket Tick을 MinIO Landing NDJSON+Zstd로 수집한다.

실행 전 .env에 KIS와 MINIO 환경 변수를 둔다.
    uv run --env-file .env python scripts/collect_kis_landing_ticks.py

Ctrl+C로 종료하면 남은 micro-batch를 먼저 MinIO에 flush한다. Landing은 Tick
재처리용 원본 계층이며, Bronze Parquet 변환은 별도 후속 단계다.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal

from tbot.feeds.kis_ws import KISOverseasSymbol, KISWebSocketFeed
from tbot.storage.landing import LandingWriter

logger = logging.getLogger(__name__)


async def _flush_periodically(writer: LandingWriter, lock: asyncio.Lock) -> None:
    """유입이 잠시 멈춰도 시간 기준 micro-batch를 저장한다."""

    check_interval = min(writer.flush_interval_seconds, 1.0)
    while True:
        await asyncio.sleep(check_interval)
        async with lock:
            await asyncio.to_thread(writer.flush_due)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    symbol = os.environ.get("KIS_SYMBOL", "TQQQ").strip().upper()
    tr_key = os.environ.get("KIS_TR_KEY", "").strip()
    if not tr_key:
        raise SystemExit(
            "KIS_TR_KEY가 없습니다. 실제 구독 성공을 확인한 KIS 해외주식 구독키를 .env에 넣어 주세요."
        )

    feed = KISWebSocketFeed.from_env([KISOverseasSymbol(symbol=symbol, tr_key=tr_key)])
    writer = LandingWriter.from_env()
    writer_lock = asyncio.Lock()
    flush_task = asyncio.create_task(_flush_periodically(writer, writer_lock))
    loop = asyncio.get_running_loop()
    shutdown_requested = False

    def request_shutdown() -> None:
        """Docker stop(SIGTERM)와 Ctrl+C(SIGINT) 모두 정상 종료 경로로 보낸다."""

        nonlocal shutdown_requested
        if shutdown_requested:
            return
        shutdown_requested = True
        logger.info("종료 신호를 받았습니다. KIS 연결을 닫고 남은 Landing batch를 저장합니다.")
        asyncio.create_task(feed.aclose())

    for signal_number in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signal_number, request_shutdown)

    logger.info("KIS %s Tick을 MinIO Landing에 수집합니다. 종료하려면 Ctrl+C를 누르세요.", symbol)
    try:
        async for event in feed.stream_events():
            async with writer_lock:
                await asyncio.to_thread(writer.append, event)
    finally:
        for signal_number in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(signal_number)
        await feed.aclose()
        flush_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await flush_task
        async with writer_lock:
            flushed = await asyncio.to_thread(writer.flush_all)
        if flushed:
            logger.info("종료 전 남은 Landing batch %d개를 저장했습니다.", len(flushed))


if __name__ == "__main__":
    asyncio.run(main())
