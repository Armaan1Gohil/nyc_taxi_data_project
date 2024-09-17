FROM apache/spark-py:v3.4.0

WORKDIR /data_project/transformation/

USER root

# Install Jupyter and necessary dependencies
ENV PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"

RUN pip install --no-cache-dir jupyter

# Install Hadoop AWS package for S3A support (MinIO)
RUN apt-get update && \
    apt-get install -y wget && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.563/aws-java-sdk-bundle-1.11.563.jar -P /opt/spark/jars/ && \
    mkdir $SPARK_HOME/conf

COPY ./spark-defaults.conf $SPARK_HOME/conf

# Expose Jupyter and Spark ports
EXPOSE 8888 7070 4040

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''"]
