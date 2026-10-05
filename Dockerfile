# Container for the serving API / local pipeline (Azure Container Apps in prod).
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir pandas PyYAML pyarrow fastapi uvicorn
COPY src ./src
COPY config ./config
COPY data_generator ./data_generator
ENV PYTHONPATH=/app/src:/app
RUN useradd -r mdm && chown -R mdm /app
USER mdm
EXPOSE 8000
CMD ["python", "-m", "mdm", "serve", "--host", "0.0.0.0", "--port", "8000"]
