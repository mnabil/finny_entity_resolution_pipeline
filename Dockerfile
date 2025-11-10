# Use the official PostgreSQL image as base
FROM postgres:15

# Set environment variables for PostgreSQL
ENV POSTGRES_DB=finny_db
ENV POSTGRES_USER=finny_user
ENV POSTGRES_PASSWORD=finny_password
ENV PGPORT=4320

# Copy data files to container
COPY fxf_data.json /docker-entrypoint-initdb.d/fxf_data.json
COPY pdl_data.json /docker-entrypoint-initdb.d/pdl_data.json

# Create initialization script
COPY <<EOF /docker-entrypoint-initdb.d/01-init-db.sql
-- Create tables for JSON data
CREATE TABLE IF NOT EXISTS fxf_data (
    id SERIAL PRIMARY KEY,
    data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pdl_data (
    id SERIAL PRIMARY KEY,
    data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better JSON query performance
CREATE INDEX IF NOT EXISTS idx_fxf_data_gin ON fxf_data USING gin (data);
CREATE INDEX IF NOT EXISTS idx_pdl_data_gin ON pdl_data USING gin (data);
EOF

# Configure PostgreSQL to accept external connections
RUN echo "host all all 0.0.0.0/0 md5" >> /usr/share/postgresql/15/pg_hba.conf
RUN echo "listen_addresses = '*'" >> /usr/share/postgresql/15/postgresql.conf

# Expose PostgreSQL port
EXPOSE 4320