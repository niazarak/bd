FROM jupyter/pyspark-notebook

USER root

RUN apt-get update && \
        apt-get -y install curl

RUN mkdir jars
RUN curl -sf -o ./jars/mongo-spark-connector_2.12-2.4.2-assembly.jar -L https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/2.4.2/mongo-spark-connector_2.12-2.4.2-assembly.jar


ADD analytics.ipynb .
