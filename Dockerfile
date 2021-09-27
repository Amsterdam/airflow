FROM apache/airflow:2.1.4-python3.9
LABEL maintainer "Gemeente Amsterdam <datapunt@amsterdam.nl>"

USER root
ARG AIRFLOW_PATH=/opt/airflow
ENV AIRFLOW_PATH=/opt/airflow

RUN apt-get update \
 && apt-get dist-upgrade -y \
 && apt-get autoremove -y \
 && apt-get install --no-install-recommends -y \
        unzip \
        gcc \
        libc-dev \
        libpq-dev \
        wget \
        dnsutils \
        vim-tiny \
        net-tools \
        netcat \
        libgeos-3.7 \
        gdal-bin \
        postgresql-client-11 \
        libgdal20 \
        libspatialite7 \
        libfreexl1 \
        libgeotiff2 \
        libwebp6 \
        proj-bin \
        mime-support \
        gettext \
        libwebpmux3 \
        libwebpdemux2 \
        libxml2 \
        libfreetype6 \
        libtiff5 \
        rsync \
        libaio1 \
        supervisor \
        curl \
        libcurl4 \
        zip \
        libdbd-pg-perl \
        postgresql-server-dev-all \
        postgresql-common \
        libterm-readpassword-perl \
  && rm -rf /var/lib/apt/lists/* /var/cache/debconf/*-old

COPY scripts/ ${AIRFLOW_PATH}/scripts/
COPY data/ ${AIRFLOW_PATH}/data/
COPY vars/ ${AIRFLOW_PATH}/vars/
COPY vsd/ ${AIRFLOW_PATH}/vsd/
COPY plugins/ ${AIRFLOW_PATH}/plugins/
COPY requirements* ./

ARG PIP_REQUIREMENTS=requirements.txt
RUN pip install --no-cache-dir -r $PIP_REQUIREMENTS
RUN python ${AIRFLOW_PATH}/scripts/mkvars.py

# Installing Oracle instant client for sources that are Oracle DB's
WORKDIR /opt/oracle
RUN wget https://download.oracle.com/otn_software/linux/instantclient/instantclient-basiclite-linuxx64.zip \
    && unzip instantclient-basiclite-linuxx64.zip \
    && rm -f instantclient-basiclite-linuxx64.zip \
    && cd /opt/oracle/instantclient* \
    && rm -f *jdbc* *occi* *mysql* *README *jar uidrvci genezi adrci \
    && echo /opt/oracle/instantclient* > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig

# setup the permissions so the airflow user can access the Python libaries
RUN chmod -R 755 /root
# setup the permissions so the airflow user can write to the airflow base path (for logs e.g.)
RUN chown -R airflow:airflow ${AIRFLOW_PATH}

WORKDIR ${AIRFLOW_PATH}
USER airflow
ENV PYTHONPATH=/root/.local/lib/python3.9/site-packages/
