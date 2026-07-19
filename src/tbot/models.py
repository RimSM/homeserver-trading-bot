"""
도메인 모델 = 파이프라인에 흐르는 레코드들의 스키마 정의 (DDL 같은 거).

데이터 엔지니어 시선:
    feed → [Tick] → strategy → [Signal] → gate → [Order] → broker → [Fill] → ledger → [Position]
    각 단계가 뱉는 레코드의 '모양'을 여기서 한 번만 못박는다.

돈/수량은 float 금지 → Decimal. (0.1 + 0.2 != 0.3 부동소수점 오차가 돈에 끼면 재앙)
소수점 주식이라 수량(qty)도 Decimal.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum


# ──────────────────────────────────────────────
# Enum = 카테고리 컬럼 (허용값 고정). 오타로 "buy" "Buy" 섞이는 사고 방지.
# ──────────────────────────────────────────────
class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class Env(str, Enum):
    STAGE = "STAGE"   # 돈 안 나감. mock/paper broker
    PROD = "PROD"     # 실제 돈. real 토스 broker


class Mode(str, Enum):
    BACKTEST = "BACKTEST"  # 과거 데이터 압축 재생
    PAPER = "PAPER"        # 실시간 tick + 가짜 주문 (승격 심사)
    LIVE = "LIVE"          # 실시간 tick + 실주문 (PROD 전용)


class OrderType(str, Enum):
    MARKET = "MARKET"  # 시장가 (토스 소수점 기본)
    LIMIT = "LIMIT"    # 지정가 (토스에서 열렸는지 확인 필요 — Open Issue)


class OrderStatus(str, Enum):
    PENDING = "PENDING"    # 만들었지만 아직 안 보냄
    SUBMITTED = "SUBMITTED"  # broker에 전송됨
    FILLED = "FILLED"      # 체결 완료
    REJECTED = "REJECTED"  # 게이트/broker가 거절
    CANCELED = "CANCELED"


# strategy_id: 어느 전략 몫인지 태그. sub-ledger 자금 분리의 핵심 키.
#   "ddulsa"(무한매수법) / "rl"(FinRL) 등. 일단 str로 둠.
StrategyId = str


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ──────────────────────────────────────────────
# 1) Tick — feed(소스 커넥터)가 뱉는 시세 한 조각
# ──────────────────────────────────────────────
@dataclass(frozen=True)  # frozen = 불변. 들어온 시세는 못 바꿈 (이벤트니까)
class Tick:
    symbol: str            # "SOXL"
    price: Decimal         # 현재가
    ts: datetime           # 시세 시각
    source: str            # "KIS" | "MOCK"  (어느 소스에서 왔나)


# ──────────────────────────────────────────────
# 2) Signal — strategy(transform)가 뱉는 매매 '의도'
#    아직 주문 아님. "이만큼 사고싶다/팔고싶다"는 제안.
# ──────────────────────────────────────────────
@dataclass(frozen=True)
class Signal:
    strategy_id: StrategyId
    symbol: str
    side: Side
    # 토스 매수는 '금액 기반'(얼마어치) / 매도는 '수량 기반'(몇 주).
    # 그래서 둘 다 optional, side에 맞는 쪽만 채운다.
    amount: Decimal | None = None   # BUY: 매수 금액 (예: 100 USD어치)
    qty: Decimal | None = None      # SELL: 매도 수량 (소수점 가능)
    reason: str = ""                # "3분할 매수 / 평단 -5%" 같은 근거 (로그·디버깅용)
    ts: datetime = field(default_factory=_now)


# ──────────────────────────────────────────────
# 3) Order — gate를 통과해 broker(sink)로 나가는 실제 주문
# ──────────────────────────────────────────────
@dataclass
class Order:
    id: str                         # idempotency 키 (중복 주문 방지의 핵심)
    strategy_id: StrategyId
    symbol: str
    side: Side
    order_type: OrderType = OrderType.MARKET
    amount: Decimal | None = None   # BUY 금액 기반
    qty: Decimal | None = None      # SELL 수량 기반
    limit_price: Decimal | None = None  # LIMIT일 때만
    status: OrderStatus = OrderStatus.PENDING
    ts: datetime = field(default_factory=_now)


# ──────────────────────────────────────────────
# 4) Fill — broker가 돌려주는 '체결 결과'
#    시장가라 낸 값 != 체결값 (슬리피지) → 실제 체결가/수량이 여기 담김
# ──────────────────────────────────────────────
@dataclass(frozen=True)
class Fill:
    order_id: str
    strategy_id: StrategyId
    symbol: str
    side: Side
    filled_qty: Decimal     # 실제 체결 수량
    filled_price: Decimal   # 실제 평균 체결가
    fee: Decimal            # 수수료 (토스 0.1%)
    ts: datetime = field(default_factory=_now)


# ──────────────────────────────────────────────
# 5) Position — ledger에 쌓이는 '현재 상태' (state 테이블 1행)
#    (strategy_id, symbol)이 사실상 PK. sub-ledger 자금 분리 단위.
# ──────────────────────────────────────────────
@dataclass
class Position:
    strategy_id: StrategyId
    symbol: str
    qty: Decimal = Decimal("0")        # 보유 수량 (소수점)
    avg_price: Decimal = Decimal("0")  # 평균 단가 (평단)

    @property
    def cost_basis(self) -> Decimal:
        """이 포지션에 들어간 총 원가 = 수량 × 평단."""
        return self.qty * self.avg_price
