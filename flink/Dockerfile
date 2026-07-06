ARG FLINK_BASE_IMAGE=flink:2.0.2-scala_2.12-java17
FROM ${FLINK_BASE_IMAGE}

ARG PAIMON_VERSION=1.1.1
ARG FLINK_KAFKA_CONNECTOR_VERSION=4.0.1-2.0
ARG FLINK_CDC_VERSION=3.5.0
ARG MYSQL_CONNECTOR_VERSION=9.3.0

USER root
COPY lib/paimon-flink-2.0.jar /opt/flink/lib/paimon-flink-2.0.jar
COPY lib/flink-sql-connector-kafka.jar /opt/flink/lib/flink-sql-connector-kafka.jar
COPY lib/flink-sql-connector-mysql-cdc.jar /opt/flink/lib/flink-sql-connector-mysql-cdc.jar
COPY lib/mysql-connector-j.jar /opt/flink/lib/mysql-connector-j.jar
COPY lib/flink-shaded-hadoop-2-uber.jar /opt/flink/lib/flink-shaded-hadoop-2-uber.jar
COPY lib/flink-connector-jdbc-core.jar /opt/flink/lib/flink-connector-jdbc-core.jar
COPY lib/flink-connector-jdbc-mysql.jar /opt/flink/lib/flink-connector-jdbc-mysql.jar

RUN mkdir -p /warehouse/paimon /opt/flink/usrlib/sql && chown -R flink:flink /warehouse /opt/flink/usrlib
USER flink
