CREATE DATABASE superset_db;
CREATE USER superset_user WITH PASSWORD 'superset_password';
GRANT ALL PRIVILEGES ON DATABASE superset_db TO superset_user;