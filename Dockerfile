FROM python:3.8-slim-buster

RUN apt-get update && adduser --disabled-password --home /app api && update-ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ADD requirements.txt .
RUN pip install -r requirements.txt

ADD server.py .
ADD prepare.py .
ADD entrypoint.sh .

EXPOSE 5000
USER api

ENTRYPOINT [ "/app/entrypoint.sh" ]
