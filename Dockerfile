ARG SPARK_VER=v3.3.1
FROM apache/spark-py:$SPARK_VER
USER root
ARG SPARK_HOME /opt/spark

#spark nlp
ADD https://s3.amazonaws.com/auxdata.johnsnowlabs.com/public/jars/spark-nlp-assembly-4.3.0.jar $SPARK_HOME/jars
RUN chmod -R go+r $SPARK_HOME/jars/*

COPY requirements.txt .
RUN pip install --upgrade pip &&\
    pip install --upgrade --ignore-installed setuptools &&\
    pip install -r requirements.txt

ENV NUMBA_CACHE_DIR=/tmp
ENV TRANSFORMERS_CACHE=/opt/spark/work-dir

USER 185
