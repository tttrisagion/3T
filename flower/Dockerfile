FROM python:3.11-slim

WORKDIR /app

COPY ./celery-services/requirements.txt .
RUN pip install -r requirements.txt
RUN pip install flower
COPY ./celery-services/worker /app/worker
