FROM ghcr.io/astral-sh/uv:bookworm-slim

WORKDIR /app

COPY tox.ini .

RUN PYTHON_VERSIONS="$(uvx tox -l |  \
    uv run python -c "import sys;print(' '.join(v[2:][0]+'.'+v[2:][1:] for v in sys.stdin.read().split()))" \
    )" &&  \
    uv python install $PYTHON_VERSIONS

ENV PATH=/root/.local/bin:$PATH

COPY . .

ENTRYPOINT ["uvx", "--with", ".", "tox", "-p"]