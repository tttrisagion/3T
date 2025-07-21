FROM python:3.11-slim

WORKDIR /app

COPY ./components/requirements.txt .

RUN pip install -r requirements.txt

COPY ./components/src /app/src
COPY ./components/tests /app/tests
