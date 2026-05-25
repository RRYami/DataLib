FROM python:3.14-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    polars>=1.40.1 \
    pyarrow>=24.0.0 \
    pandas>=2.0.0 \
    SQLAlchemy>=2.0 \
    psycopg2-binary>=2.9

COPY docker/loader/load_parquet.py /app/

CMD ["python", "/app/load_parquet.py"]
