FROM python:3-bookworm

RUN apt-get update && apt-get install -y \
    less \
    sudo \
    vim

RUN useradd -ms /bin/bash sqlmesh
RUN echo "sqlmesh ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/sqlmesh

RUN pip3 install --upgrade pip setuptools wheel --no-cache-dir
RUN pip3 install --no-cache-dir 'sqlmesh[web,bigquery,databricks,mssql,mysql,postgres,gcppostgres,redshift,snowflake,spark,trino]'

USER sqlmesh

RUN mkdir /home/sqlmesh/project
WORKDIR /home/sqlmesh/project

RUN sqlmesh init duckdb

HEALTHCHECK CMD sqlmesh --version || exit 1

EXPOSE 8000

CMD ["sqlmesh", "ui", "--host", "0.0.0.0"]

