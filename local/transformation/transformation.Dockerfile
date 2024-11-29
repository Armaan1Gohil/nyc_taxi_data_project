# Use the official Spark 3.4.0 image with Python
FROM apache/spark-py:v3.4.0

# Set working directory
WORKDIR /data_project/transformation/

# Switch to root user to install dependencies
USER root

# Install Jupyter and necessary Python packages
ENV PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"

# Install Jupyter Notebook and additional dependencies (optional)
RUN pip install --no-cache-dir jupyter pandas numpy pyspark

# Install Hadoop AWS package for S3A support (MinIO)
RUN apt-get update && \
    apt-get install -y wget && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.563/aws-java-sdk-bundle-1.11.563.jar -P /opt/spark/jars/ && \
    wget https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.5.2/iceberg-spark-runtime-3.4_2.12-1.5.2.jar -P /opt/spark/jars/ && \
    wget https://repo.maven.apache.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.4_2.12/0.100.2/nessie-spark-extensions-3.4_2.12-0.100.2.jar -P /opt/spark/jars/ && \
    mkdir -p $SPARK_HOME/conf

# Copy the configuration files (spark-defaults.conf)
COPY ./spark-defaults.conf $SPARK_HOME/conf

# Expose Jupyter and Spark ports
EXPOSE 8888 7070 4040

# Start Jupyter Notebook with Spark
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''"]