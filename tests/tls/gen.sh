#!/usr/bin/env bash

openssl req \
  -new \
  -newkey rsa:2048 \
  -days 3650 \
  -nodes \
  -x509 \
  -subj "/C=US/ST=State/L=City/O=Qdrant" \
  -keyout cakey.pem \
  -out cacert.pem

openssl genrsa -out key.pem 2048

openssl req \
  -new -key key.pem \
  -out cert.csr \
  -config ./tests/tls/cert.cfg

openssl x509 \
  -req \
  -days 3650 \
  -in cert.csr \
  -CA cacert.pem \
  -CAkey cakey.pem \
  -CAcreateserial \
  -extensions v3_req \
  -extfile ./tests/tls/cert.cfg \
  -out cert.pem
