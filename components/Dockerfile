FROM python:3.11-slim

WORKDIR /app

COPY ./components/requirements.txt .

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir -r requirements.txt

# Install curl for debugging
RUN apt-get update && apt-get install -y curl

COPY ./components/src /app/src
COPY ./components/tests /app/tests
