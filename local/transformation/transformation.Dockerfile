FROM apache/spark-py:v3.4.0

WORKDIR /data_project/transformation/

USER root

ENV PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"

RUN pip install --no-cache-dir jupyter

EXPOSE 8888 7070 4040

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''"]