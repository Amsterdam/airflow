FROM apache/airflow:2.1.2-python3.8
LABEL maintainer "Gemeente Amsterdam <datapunt@amsterdam.nl>"

ARG AIRFLOW_USER_HOME=/usr/local/airflow
ENV AIRFLOW_USER_HOME=${AIRFLOW_USER_HOME}
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

USER root
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

COPY scripts/mkvars.py ${AIRFLOW_USER_HOME}/scripts/mkvars.py
COPY scripts/mkuser.py ${AIRFLOW_USER_HOME}/scripts/mkuser.py
COPY data/ ${AIRFLOW_USER_HOME}/data/
COPY vars/ ${AIRFLOW_USER_HOME}/vars/
COPY vsd/ ${AIRFLOW_USER_HOME}/vsd/
COPY plugins/ ${AIRFLOW_USER_HOME}/plugins/
COPY scripts/run.sh /run.sh

COPY requirements* ./
ARG PIP_REQUIREMENTS=requirements.txt
RUN pip install --no-cache-dir -r $PIP_REQUIREMENTS
RUN python ${AIRFLOW_USER_HOME}/scripts/mkvars.py

# Installing Oracle instant client
WORKDIR /opt/oracle
RUN wget https://download.oracle.com/otn_software/linux/instantclient/instantclient-basiclite-linuxx64.zip \
    && unzip instantclient-basiclite-linuxx64.zip \
    && rm -f instantclient-basiclite-linuxx64.zip \
    && cd /opt/oracle/instantclient* \
    && rm -f *jdbc* *occi* *mysql* *README *jar uidrvci genezi adrci \
    && echo /opt/oracle/instantclient* > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig

WORKDIR ${AIRFLOW_USER_HOME}

CMD [ "/run.sh" ]
