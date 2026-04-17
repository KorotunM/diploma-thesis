FROM python:3.12-slim

WORKDIR /workspace
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/workspace

COPY pyproject.toml README.md ./
COPY apps ./apps
COPY libs ./libs
COPY schemas ./schemas
COPY docs ./docs

RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir .

EXPOSE 8004
CMD ["uvicorn", "apps.backend.app.main:app", "--host", "0.0.0.0", "--port", "8004"]
