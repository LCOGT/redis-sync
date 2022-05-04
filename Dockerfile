FROM python:3.10-slim

SHELL ["/bin/bash", "-c"]

RUN apt-get update -y && apt-get -y upgrade

WORKDIR /src

COPY ./ ./

RUN pip install .

ENV PYTHONUNBUFFERED=1 PYTHONFAULTHANDLER=1

ENTRYPOINT ["redis-sync"]
