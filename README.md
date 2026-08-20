# trading-bot

SOXL(및 다종목) 소수점 자동매매. 규칙 기반(무한매수법) vs RL 챔피언-챌린저를
**환경(stage/prod)** 으로 구현. Kafka 스트리밍 학습이 메인 목적.

> 설계·결정 맥락은 코드가 아니라 vault에 있음:
> `second-brain/01_Projects/99_RIMSM/001.trading-bot/overview.md`

## 환경 × 모드

| 축 | 값 | 의미 |
|---|---|---|
| `ENV` | `stage` | 돈 안 나감. mock/paper broker. 챌린저들 굴리는 곳 |
| | `prod` | 실제 돈. 승격된 전략만. real 토스 broker |
| `MODE` | `backtest` | 과거 데이터 압축 재생. 빠른 초기 검증 |
| | `paper` | 실시간 tick, 실제 시간 흐름, 주문만 가짜. **승격 심사(1주)** |
| | `live` | 실시간 tick + 실주문 (`ENV=prod`에서만) |

**안전 가드**: `ENV=stage`는 실주문 broker에 절대 바인딩 안 됨(하드 차단).

## 폴더 구조 (뼈대만 — 로직 미구현)

```
src/tbot/
  brokers/     주문 실행 추상화. base(ABC) · mock(paper) · toss(real) · kis(stub)
  feeds/       시세 소스 추상화. base(ABC) · mock(합성/리플레이) · kis_ws(KIS 웹소켓 래핑)
  strategies/  전략. base(ABC) · infinite_buy(무한매수법)
  gate/        주문 게이트: 가격괴리·idempotency·손실한도·killswitch + 슬리피지 가드
  ledger/      strategy_id 태그 sub-ledger (MVP sqlite → Postgres)
  risk/        kill switch, 일손실 한도
  metrics/     Sharpe·MDD 계산, 전략 스코어보드 (승격 심사)
  models.py    Signal · Order · Fill · Position 등 (미작성)
  config.py    ENV/MODE 로딩 (미작성)
  runner.py    메인 루프: feed→strategy→gate→broker→ledger (미작성)
scripts/       run_dryrun.py 등 실행 스크립트
config/        config.yaml
tests/
docs/
```

## KIS WebSocket feed (첫 구현)

`feeds/kis_ws.py`는 KIS 해외주식 `HDFSCNT0` 체결가 원문을 공통 `Tick`으로
변환하는 어댑터다. 이 단계는 화면 출력·파일 저장·전략·주문을 수행하지 않는다.

```bash
uv sync --group dev
uv run pytest
```

실제 연결은 `.env`를 환경 변수로 주입한 뒤 사용한다. `tr_key`는 티커만이 아니라
KIS 거래소 접두어가 붙은 코드여야 한다. 종목의 정확한 거래소는 KIS 종목 조회에서
확인한다.

> 보안 주의: KIS 공식 예제의 WebSocket 주소는 현재 `ws://`이며, 구독 메시지에는
> 단기 승인키가 포함된다. 이 단계의 운용 범위는 신뢰할 수 있는 네트워크에서의
> `stage` 검증이다. 실전 환경에 쓰기 전 KIS의 `wss://` 지원 여부와 최신 보안
> 가이드를 반드시 확인한다.

```python
from tbot.feeds.kis_ws import KISOverseasSymbol, KISWebSocketFeed

feed = KISWebSocketFeed.from_env(
    [KISOverseasSymbol(symbol="SOXL", tr_key="<KIS의 SOXL 구독키>")]
)

async for tick in feed.stream():
    # 다음 단계: 화면 표시, 파일 저장, 또는 전략 입력
    print(tick)
```

```bash
uv run --env-file .env python your_consumer.py
```

## 참고
- KIS 공식 샘플: https://github.com/koreainvestment/open-trading-api (`kis_auth.py` 재사용)
- 토스 Open API: https://openapi.tossinvest.com (단계적 롤아웃, 발급 대기)
