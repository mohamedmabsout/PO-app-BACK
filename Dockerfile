# ULTIMATE FINAL Dockerfile

FROM python:3.11-slim
WORKDIR /app

# Invalidate the cache by adding a variable that can be changed
ARG CACHE_BUSTER=1

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Use the absolute path. We will confirm which one is correct from the build log.
CMD ["gunicorn", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000", "app.main:app"]