FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
COPY wheels/ /wheels/
RUN if ls /wheels/*.whl >/dev/null 2>&1; then pip install --no-index --find-links=/wheels -r requirements.txt; else pip install --no-cache-dir --retries 5 -r requirements.txt; fi
COPY . .
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
