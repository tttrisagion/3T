FROM python:3.11-slim

WORKDIR /app

COPY ./celery/requirements.txt .
RUN pip install -r requirements.txt
RUN pip install flower
COPY ./celery/worker /app/worker
