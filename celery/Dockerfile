FROM python:3.11-slim

WORKDIR /app

RUN pip install "celery[redis]"

COPY ./celery/requirements.txt .
RUN pip install -r requirements.txt
COPY ./celery/worker /app/worker
