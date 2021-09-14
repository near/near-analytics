FROM ubuntu:20.04

RUN apt update -yq && apt install -q -y python3-pip libpq-dev python-dev

WORKDIR /usr/app

COPY ./requirements.txt .

RUN pip3 install -r requirements.txt

COPY . .

CMD python3 main.py
