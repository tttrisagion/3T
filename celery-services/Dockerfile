FROM python:3.11-slim

WORKDIR /app

# Install build tools for C++ compilation
RUN apt-get update && apt-get install -y \
    g++ \
    make \
    && rm -rf /var/lib/apt/lists/*

RUN pip install "celery[redis]"

COPY ./celery-services/requirements.txt .
RUN pip install -r requirements.txt

# Copy C++ source files and build the library
COPY ./celery-services/cpp/perm_entropy_cpu_lib.cpp /app/cpp/
RUN mkdir -p /app/worker/bin && \
    g++ -O3 -shared -fPIC -Wall -std=c++17 /app/cpp/perm_entropy_cpu_lib.cpp -o /app/worker/bin/libperm_entropy_cpu.so

COPY ./celery-services/worker /app/worker
