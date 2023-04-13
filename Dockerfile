# VERSION 2.4.0
# AUTHOR: Quang Tran Chi
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t chiquang98/docker-airflow .

FROM python:3.7-slim-buster
LABEL maintainer="chiquang98__"

# Never prompt the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=2.4.0
ARG AIRFLOW_USER_HOME=/opt/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}
ARG SPARK_VERSION="3.2.3"
ARG HADOOP_VERSION="3.2"

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

# Disable noisy "Handling signal" log messages:
# ENV GUNICORN_CMD_ARGS --log-level WARNING

RUN set -ex \
    && buildDeps=' \
    freetds-dev \
    libkrb5-dev \
    libsasl2-dev \
    libssl-dev \
    libffi-dev \
    libpq-dev \
    git \
    ' 
RUN  apt-get clean
RUN apt-get update -y 
RUN apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
    && apt-get install -y gosu \
    $buildDeps \
    freetds-bin \
    build-essential \
    default-libmysqlclient-dev \
    apt-utils \
    curl \
    rsync \
    netcat \
    locales \
    # iputils-ping \
    # telnet \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow
#Fix loi cai dat sasl
RUN apt-get install libsasl2-dev
RUN pip install -U pip setuptools wheel \
    && pip install pytz \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
#Fix loi py4j
    && pip install pyspark==3.2.3 \
    && pip install --no-cache-dir apache-airflow-providers-apache-spark \
    && pip install apache-airflow[crypto,celery,postgres,hive,jdbc,mysql,ssh${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION} \
    && pip install -U celery[redis] \
    && pip install importlib-metadata==4.13.0 \
    && if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/* \
    /usr/share/man \
    /usr/share/doc \
    /usr/share/doc-base
COPY script/entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg
# COPY requirements.txt ${AIRFLOW_USER_HOME}/requirements.txt
RUN chown -R airflow: ${AIRFLOW_USER_HOME}

EXPOSE 8080 5555 8793

# Java is required in order to spark-submit work
# Install OpenJDK-8
#Sua loi E: Unable to locate package openjdk-8-jdk 
RUN mkdir -p /usr/share/man/man1
RUN apt-get update 
RUN apt-get install -y software-properties-common
RUN apt-get install -y gnupg2
RUN add-apt-repository "deb http://security.debian.org/debian-security stretch/updates main"
RUN apt-get update
RUN apt-get install -y openjdk-8-jdk && \
    java -version $$ \
    javac -version
# RUN apt-get install libkrb5-dev -y
# RUN pip install krbcontext==0.10

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
RUN export JAVA_HOME
###############################
## Finish JAVA installation
###############################

ENV SPARK_HOME /opt/spark

#Spark submit binaries and jars (Spark binaries must be the same version of spark cluster)
#Dung de goi SparkSubmitOperator; thay the cho SSH
RUN apt-get install -y wget
RUN cd "/tmp"
RUN wget --no-verbose "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
RUN tar -xvzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
RUN mkdir -p "${SPARK_HOME}/bin" && \
    mkdir -p "${SPARK_HOME}/assembly/target/scala-2.12/jars" && \
    cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/bin/." "${SPARK_HOME}/bin/" && \
    cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/jars/." "${SPARK_HOME}/assembly/target/scala-2.12/jars/" && \
    rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
        
RUN chown -R airflow: ${AIRFLOW_HOME}

# Create SPARK_HOME env var
RUN export SPARK_HOME
ENV PATH $PATH:/opt/spark/bin
# python version of base image must be the same with bitnami image (3.8)
ENV PYSPARK_PYTHON python3
ENV PYSPARK_DRIVER_PYTHON python3
###############################
## Finish SPARK files and variables
###############################

USER airflow
WORKDIR ${AIRFLOW_USER_HOME}
# RUN pip install -r requirements.txt
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] 