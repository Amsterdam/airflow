FROM apache/airflow:2.1.2-python3.8
LABEL maintainer "Gemeente Amsterdam <datapunt@amsterdam.nl>"

USER root
ARG AIRFLOW_PATH=/opt/airflow/
ENV AIRFLOW_PATH=/opt/airflow/

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

COPY scripts/mkvars.py ${AIRFLOW_PATH}/scripts/mkvars.py
COPY scripts/mkuser.py ${AIRFLOW_PATH}/scripts/mkuser.py
COPY scripts/run.sh ${AIRFLOW_PATH}/scripts/run.sh
COPY data/ ${AIRFLOW_PATH}/data/
COPY vars/ ${AIRFLOW_PATH}/vars/
COPY vsd/ ${AIRFLOW_PATH}/vsd/
COPY plugins/ ${AIRFLOW_PATH}/plugins/
# COPY src/dags/ ${AIRFLOW_PATH}/dags/

COPY requirements* ./
ARG PIP_REQUIREMENTS=requirements.txt
RUN pip install --no-cache-dir -r $PIP_REQUIREMENTS
RUN python ${AIRFLOW_PATH}/scripts/mkvars.py

COPY scripts/run.sh /opt/airflow/scripts/run.sh
COPY scripts/scheduler_startup.sh /opt/airflow/scripts/scheduler_startup.sh
RUN chmod 777 /opt/airflow/scripts/

#Installing Oracle instant client
WORKDIR /opt/oracle
RUN wget https://download.oracle.com/otn_software/linux/instantclient/instantclient-basiclite-linuxx64.zip \
    && unzip instantclient-basiclite-linuxx64.zip \
    && rm -f instantclient-basiclite-linuxx64.zip \
    && cd /opt/oracle/instantclient* \
    && rm -f *jdbc* *occi* *mysql* *README *jar uidrvci genezi adrci \
    && echo /opt/oracle/instantclient* > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig

# RUN mkdir -p $AIRFLOW_PATH/dags/ $AIRFLOW_PATH/logs/  $AIRFLOW_PATH/plugins/
# RUN chown 50000:50000 $AIRFLOW_PATH/dags/ $AIRFLOW_PATH/logs/  $AIRFLOW_PATH/plugins/

WORKDIR ${AIRFLOW_PATH}
USER airflow

# CMD [ "/opt/airflow/scripts/run.sh" ]
