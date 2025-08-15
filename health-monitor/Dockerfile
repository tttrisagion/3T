FROM python:3.11-slim

WORKDIR /app

COPY health-monitor/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY health-monitor/monitor.py .

# Copy the shared directory to the container
COPY shared /app/shared

CMD ["python", "monitor.py"]