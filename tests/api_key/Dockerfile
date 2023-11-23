FROM python:3.11-alpine

WORKDIR /

COPY requirements-freeze.txt /
RUN pip install -r requirements-freeze.txt

RUN mkdir /tests
COPY test_grpc.py /tests/
COPY test_rest.py /tests/

