FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY cms_monitor.py .

# Create data directory
RUN mkdir -p /app/data

ENV CMS_DATA_DIR=/app/data
ENV PYTHONUNBUFFERED=1

CMD ["python", "cms_monitor.py"]
