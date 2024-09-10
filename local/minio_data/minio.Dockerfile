FROM minio/minio

EXPOSE 9000 9001

CMD [ "server", "/data_project/data", "--console-address", ":9001"]