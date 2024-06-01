FROM python:3-bookworm

RUN apt-get update

RUN pip3 install --upgrade pip setuptools wheel --no-cache-dir
RUN pip3 install --no-cache-dir 'sqlmesh[web]'

RUN useradd -ms /bin/bash sqlmesh

USER sqlmesh

RUN mkdir /home/sqlmesh/project
WORKDIR /home/sqlmesh/project

RUN sqlmesh init duckdb

HEALTHCHECK CMD sqlmesh --version || exit 1

EXPOSE 8000

CMD ["sqlmesh", "ui", "--host", "0.0.0.0"]

