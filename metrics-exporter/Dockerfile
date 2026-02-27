FROM python:3.11-slim

WORKDIR /app

COPY metrics-exporter/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY metrics-exporter/exporter.py .

# Copy the shared directory to the container
COPY shared /app/shared

CMD ["python", "exporter.py"]
