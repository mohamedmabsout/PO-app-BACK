# ULTIMATE FINAL Dockerfile

FROM python:3.11-slim
WORKDIR /app

# Invalidate the cache by adding a variable that can be changed
ARG CACHE_BUSTER=1

COPY requirements.txt .

# Verify gunicorn is in requirements.txt (optional but good)
RUN cat requirements.txt | grep gunicorn

RUN pip install --no-cache-dir -r requirements.txt

# --- THE CRUCIAL DEBUGGING STEP ---
# This command will list the contents of the bin directory.
# We will check this during the build process.
RUN ls -la /usr/local/bin
RUN ls -la /root/.local/bin || echo "no /root/.local/bin"

COPY . .

# Use the absolute path. We will confirm which one is correct from the build log.
CMD ["/usr/local/bin/gunicorn", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000", "app.main:app"]