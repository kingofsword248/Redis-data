LABEL authors="ngonhattoan"

# Sử dụng image Python chính thức
FROM python:3.11-slim

# Cài Redis client
RUN pip install redis

# Copy code vào container
WORKDIR /app
COPY app.py .

# Chạy app
CMD ["python", "app.py"]
ENTRYPOINT ["top", "-b"]