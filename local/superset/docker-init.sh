#!/bin/bash

# Start Superset initialization
superset db upgrade

# Create an admin user
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@superset.local \
    --password admin123

# Initialize Superset roles and permissions
superset init

# Add database connection
superset import-datasources --path /app/predefined-database.json || true

# Start Superset
superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger