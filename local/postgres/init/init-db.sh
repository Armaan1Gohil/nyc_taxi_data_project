#!/bin/bash

# Check if the superset database already exists
DB_NAME="superset"
psql -U "$POSTGRES_USER" -d postgres -tc "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME';" | grep -q 1

if [ $? -ne 0 ]; then
  # Database doesn't exist; create it
  echo "Creating database $DB_NAME..."
  psql -U "$POSTGRES_USER" -d postgres -c "CREATE DATABASE $DB_NAME;"
else
  echo "Database $DB_NAME already exists."
fi

# Check if the airflow database already exists
DB_NAME="airflow"
psql -U "$POSTGRES_USER" -d postgres -tc "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME';" | grep -q 1

if [ $? -ne 0 ]; then
  # Database doesn't exist; create it
  echo "Creating database $DB_NAME..."
  psql -U "$POSTGRES_USER" -d postgres -c "CREATE DATABASE $DB_NAME;"
else
  echo "Database $DB_NAME already exists."
fi