#!/usr/bin/env bash

openssl req \
  -new \
  -newkey rsa:2048 \
  -days 3650 \
  -nodes \
  -x509 \
  -subj "/C=US/ST=State/L=City/O=Qdrant" \
  -addext "keyUsage = critical, keyCertSign, cRLSign" \
  -addext "basicConstraints = critical, CA:TRUE" \
  -keyout cakey.pem \
  -out cacert.pem

openssl genrsa -out key.pem 2048
chmod 644 key.pem

openssl req \
  -new -key key.pem \
  -out cert.csr \
  -config ./tests/e2e_tests/test_data/cert.cfg

openssl x509 \
  -req \
  -days 3650 \
  -in cert.csr \
  -CA cacert.pem \
  -CAkey cakey.pem \
  -CAcreateserial \
  -extensions v3_req \
  -extfile ./tests/e2e_tests/test_data/cert.cfg \
  -out cert.pem
