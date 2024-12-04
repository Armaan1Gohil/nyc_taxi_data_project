#!/bin/bash
set -e

# Initialize the database
superset db upgrade

# Create an admin user if it doesn't exist
superset fab create-admin \
    --username "$SUPERSET_ADMIN_USER" \
    --firstname "$SUPERSET_ADMIN_FIRST_NAME" \
    --lastname "$SUPERSET_ADMIN_LAST_NAME" \
    --email "$SUPERSET_ADMIN_EMAIL" \
    --password "$SUPERSET_ADMIN_PASSWORD" || true

# Initialize Superset
superset init

# Add Trino as a database connection
# Create a temporary Python script to add the database
cat << EOF > /tmp/add_trino_db.py
from superset import db
from superset.models.core import Database
import os

existing = db.session.query(Database).filter_by(database_name='Trino').first()
if not existing:
    trino_db = Database(database_name='Trino')
    sqlalchemy_uri = f"trino://{os.environ.get('TRINO_USER')}@{os.environ.get('TRINO_HOST')}:{os.environ.get('TRINO_PORT')}/{os.environ.get('TRINO_CATALOG')}/{os.environ.get('TRINO_SCHEMA')}"
    trino_db.sqlalchemy_uri = sqlalchemy_uri
    trino_db.extra = '{"metadata_params": {}, "engine_params": {}, "metadata_cache_timeout": {}, "schemas_allowed_for_csv_upload": []}'
    db.session.add(trino_db)
    db.session.commit()
EOF

# Execute the Python script within the Superset shell context
superset shell < /tmp/add_trino_db.py

# Remove the temporary Python script
rm /tmp/add_trino_db.py

# Start the Superset server
superset run -p 8088 --with-threads --reload --debugger