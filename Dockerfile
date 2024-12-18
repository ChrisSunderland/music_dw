FROM python:3.9

ENV PYTHONUNBUFFERED 1

RUN mkdir /code

WORKDIR /code

COPY . /code/

COPY requirements.txt /app/

RUN pip install -r requirements.txt