FROM python:3.12-slim AS base

LABEL maintainer="drt-hub" \
      description="Reverse ETL for the code-first data stack"

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

ARG DRT_EXTRAS=""
ARG DRT_VERSION="0.4.3"

RUN if [ -z "$DRT_EXTRAS" ]; then \
      pip install --no-cache-dir drt-core==${DRT_VERSION}; \
    else \
      pip install --no-cache-dir "drt-core[${DRT_EXTRAS}]==${DRT_VERSION}"; \
    fi

RUN useradd --create-home drt
USER drt

ENTRYPOINT ["drt"]
CMD ["--help"]
