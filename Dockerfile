# Landing 수집기는 장시간 실행되므로, lockfile 기준의 재현 가능한 이미지로 만든다.
FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:0.11.15 /uv /uvx /bin/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

# 의존성 레이어를 소스 코드보다 먼저 만들어, 코드만 바뀌면 Docker cache를 재사용한다.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --no-install-project

COPY src ./src
RUN uv sync --locked --no-dev

COPY scripts ./scripts

# 수집기는 외부 포트를 열지 않는 단일 목적 프로세스다. 이미지·실행 파일을 읽기만
# 하면 되므로 root 권한이나 쓰기 가능한 앱 디렉터리를 줄 이유가 없다.
RUN groupadd --system tbot \
    && useradd --system --gid tbot --no-create-home --home-dir /nonexistent tbot \
    && chown -R tbot:tbot /app
USER tbot

CMD [".venv/bin/python", "scripts/collect_kis_landing_ticks.py"]
