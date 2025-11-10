# FINAL backend/Dockerfile

FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .

# This PATH environment variable is important
ENV PATH="/root/.local/bin:${PATH}"

RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# The final command to run the application
CMD ["gunicorn", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000", "app.main:app"]