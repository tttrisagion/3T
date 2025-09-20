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
COPY ./celery-services/cpp/src/perm_entropy_cpu_lib.cpp /app/cpp/
RUN g++ -O3 -shared -fPIC -Wall -std=c++17 /app/cpp/perm_entropy_cpu_lib.cpp -o /usr/local/lib/libperm_entropy_cpu.so && \
    ldconfig

COPY ./celery-services/worker /app/worker
