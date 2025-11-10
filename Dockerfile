# Dockerfile for Backend

# 1. Use an official Python image as the base.
# We choose 3.11-slim for a good balance of features and size.
FROM python:3.11-slim

# 2. Set the working directory inside the container.
WORKDIR /app

# 3. Copy the dependency file and install dependencies.
# This is done in a separate step to leverage Docker's caching.
COPY requirements.txt .

ENV PATH="/root/.local/bin:${PATH}"

RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy the rest of the application code into the container.
COPY . .

# 5. The command to run when the container starts.
# We use Gunicorn to run the Uvicorn workers.
# The --bind 0.0.0.0:8000 exposes the port inside the container.
CMD ["ls", "-la", "/usr/local/bin"]