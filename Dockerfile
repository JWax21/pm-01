FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY fees.py auth.py db.py executor.py market.py ws_ingestor.py server.py main.py ./

CMD ["python", "-u", "main.py"]
