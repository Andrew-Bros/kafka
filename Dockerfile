# syntax=docker/dockerfile:1

FROM python:3.9-buster

WORKDIR kafka-project
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt --no-cache-dir

COPY . .

CMD [ "python" ]
