FROM python:3.12-slim
WORKDIR /app
COPY generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY generator/produce_events.py .
COPY generator/produce_mysql_changes.py .
CMD ["python", "produce_events.py"]
