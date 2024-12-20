FROM minio/minio:RELEASE.2024-12-13T22-19-12Z

EXPOSE 9000 9001

CMD [ "server", "/data_project/data", "--console-address", ":9001"]