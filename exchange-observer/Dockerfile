FROM python:3.11-slim

WORKDIR /app

# Create a non-root user and group
RUN groupadd -r appgroup && useradd -r -g appgroup --create-home appuser

# Create data directory and set permissions
RUN mkdir -p /app/data && chown -R appuser:appgroup /app/data

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=appuser:appgroup . .

USER appuser

EXPOSE 8000

CMD ["uvicorn", "observer:app", "--host", "0.0.0.0", "--port", "8000"]
