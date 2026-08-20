"""시세 소스가 공통으로 지키는 작은 인터페이스."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from tbot.models import Tick


class Feed(ABC):
    """외부 시세를 ``Tick`` 스트림으로 바꾸는 소스 어댑터.

    소비자는 KIS, 파일 리플레이, 합성 시세인지 알 필요 없이 이 인터페이스만
    사용한다. 다음 단계의 저장기·전략·가짜 주문기는 모두 이 경계 뒤에 붙는다.
    """

    @abstractmethod
    def stream(self) -> AsyncIterator[Tick]:
        """연결된 소스에서 들어오는 tick을 순서대로 내보낸다."""

    @abstractmethod
    async def aclose(self) -> None:
        """스트림을 안전하게 멈춘다."""
