FROM timescale/timescaledb:latest-pg16

# Install Python for potential inline scripting if needed
RUN apk add --no-cache python3 py3-pip

# Copy initialization scripts (executed in alphabetical order on first startup)
COPY docker/db/init/*.sql /docker-entrypoint-initdb.d/

# Ensure TimescaleDB extension is available
RUN echo "CREATE EXTENSION IF NOT EXISTS timescaledb;" > /docker-entrypoint-initdb.d/000_timescale.sql

EXPOSE 5432
