"""KIS WebSocket에서 받은 해외주식 Tick을 터미널에 출력한다.

실행 전 .env에 KIS_APP_KEY, KIS_APP_SECRET, KIS_ENV와 KIS_TR_KEY를 둔다.
예: uv run --env-file .env python scripts/print_kis_ticks.py
"""

from __future__ import annotations

import asyncio
import logging
import os

from tbot.feeds.kis_ws import KISOverseasSymbol, KISWebSocketFeed


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    symbol = os.environ.get("KIS_SYMBOL", "SOXL").strip().upper()
    tr_key = os.environ.get("KIS_TR_KEY", "").strip()

    if not tr_key:
        raise SystemExit(
            "KIS_TR_KEY가 없습니다. KIS 종목 조회로 확인한 해외주식 구독키를 .env에 넣어 주세요."
        )

    feed = KISWebSocketFeed.from_env(
        [KISOverseasSymbol(symbol=symbol, tr_key=tr_key)]
    )

    print(f"KIS {symbol} 시세 수신을 시작합니다. 종료하려면 Ctrl+C를 누르세요.")
    try:
        async for tick in feed.stream():
            print(f"{tick.ts.isoformat()} | {tick.symbol:5} | ${tick.price} | {tick.source}")
    except KeyboardInterrupt:
        print("\n사용자가 시세 수신을 멈췄습니다.")
    finally:
        await feed.aclose()


if __name__ == "__main__":
    asyncio.run(main())
