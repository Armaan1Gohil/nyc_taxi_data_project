FROM apache/superset:4.1.1

USER root

COPY docker-init.sh /app/docker-init.sh
COPY predefined-database.json /app/predefined-database.json

RUN pip install sqlalchemy-trino && \
    chmod +rwx /app/docker-init.sh

EXPOSE 8088

ENTRYPOINT ["./docker-init.sh"]