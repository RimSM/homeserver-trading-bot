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

## 참고
- KIS 공식 샘플: https://github.com/koreainvestment/open-trading-api (`kis_auth.py` 재사용)
- 토스 Open API: https://openapi.tossinvest.com (단계적 롤아웃, 발급 대기)
