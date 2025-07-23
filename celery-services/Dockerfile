FROM python:3.11-slim

WORKDIR /app

RUN pip install "celery[redis]"

COPY ./celery-services/requirements.txt .
RUN pip install -r requirements.txt
COPY ./celery-services/worker /app/worker
