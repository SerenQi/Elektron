FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py dashboard.html ./
COPY config.example.yaml ./config.yaml
VOLUME ["/app/buckets"]
ENV OMBRE_TRANSPORT=streamable-http
ENV OMBRE_BUCKETS_DIR=/app/buckets
EXPOSE 8000
CMD ["python", "server.py"]
