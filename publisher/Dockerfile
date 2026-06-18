FROM python:3.11-slim

# Non-root user setup
RUN adduser --disabled-password --gecos '' appuser
WORKDIR /app

# Install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy publisher code
COPY --chown=appuser:appuser main.py ./

USER appuser

CMD ["python", "main.py"]
