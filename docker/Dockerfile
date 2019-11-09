FROM python:3
USER root

RUN apt-get update
RUN apt-get install -y vim less
RUN apt-get -y install locales && \
    localedef -f UTF-8 -i ja_JP ja_JP.UTF-8

RUN pip install --upgrade pip
RUN pip install --upgrade setuptools

RUN mkdir -p /script
RUN mkdir -p /log_file

ENV LANG ja_JP.UTF-8
ENV LANGUAGE ja_JP:ja
ENV LC_ALL ja_JP.UTF-8