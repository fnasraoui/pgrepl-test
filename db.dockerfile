FROM postgres:14.7

RUN apt-get update && apt-get install -y postgresql-14-wal2json

