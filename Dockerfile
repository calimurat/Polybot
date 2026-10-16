# Polymarket 5-minute BTC bot — container image (added 2026-10-05).
# Code, databases, Parquet copy and logs live in the project folder on the
# host, mounted at /app by docker-compose.yml, so rebuilding or replacing the
# image never touches data. .env (wallet key) is never baked into the image:
# it is read from that mounted folder at run time.
FROM python:3.14-slim

# Local time for log lines and the Parquet day folders — the same days as
# on the Mac, instead of the container's default UTC.
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*
ENV TZ=Europe/Istanbul \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DASHBOARD_HOST=0.0.0.0 \
    POLYBOT_NO_BROWSER=1

WORKDIR /app
# Runtime dependencies only (development tools are in requirements-dev.txt).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# The code is copied too, so the image also runs on its own; with
# docker-compose the mounted project folder takes its place.
COPY . .

EXPOSE 8765
CMD ["python", "run.py"]
