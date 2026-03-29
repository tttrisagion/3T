FROM python:3.11-slim

WORKDIR /app

# Create a non-root user and group
RUN groupadd -r appgroup && useradd -r -g appgroup --create-home appuser

# Create data directory and set permissions
RUN mkdir -p /app/data && chown -R appuser:appgroup /app/data

# Copy requirements from the observer directory
COPY exchange-observer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy default config
COPY config.yml.example /app/config.yml

# Copy the observer source and the shared library
COPY --chown=appuser:appgroup exchange-observer/ /app/
COPY --chown=appuser:appgroup shared/ /app/shared/

# Copy the static web files into the expected /app/www location
COPY --chown=appuser:appgroup docs/www/ /app/www/

USER appuser

EXPOSE 8001

CMD ["uvicorn", "observer:app", "--host", "0.0.0.0", "--port", "8001"]
