# A real taskiq worker for the e2e suite: RedisStreamBroker reporting to the
# admin through TaskiqAdminMiddleware. Versions are pinned for reproducibility.
FROM python:3.12-slim
RUN pip install --no-cache-dir --disable-pip-version-check \
    taskiq==0.13.0 taskiq-redis==1.2.4 aiohttp==3.14.5
WORKDIR /app
COPY worker.py .
CMD ["taskiq", "worker", "worker:broker"]
