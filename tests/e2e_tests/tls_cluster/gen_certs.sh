#!/usr/bin/env bash

# Run once to generate TLS certificates for the local cluster.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERT_DIR="$SCRIPT_DIR/cert"
CERT_CFG="$SCRIPT_DIR/cert.cfg"

mkdir -p "$CERT_DIR"

# CA
openssl req \
    -new -newkey rsa:2048 -days 3650 -nodes -x509 \
    -subj "/C=US/ST=State/L=City/O=Qdrant" \
    -addext "keyUsage = critical, keyCertSign, cRLSign" \
    -addext "basicConstraints = critical, CA:TRUE" \
    -keyout "$CERT_DIR/cakey.pem" \
    -out "$CERT_DIR/cacert.pem"

# Server key + CSR + signed cert
openssl genrsa -out "$CERT_DIR/key.pem" 2048
chmod 644 "$CERT_DIR/key.pem"

openssl req \
    -new -key "$CERT_DIR/key.pem" \
    -out "$CERT_DIR/cert.csr" \
    -config "$CERT_CFG"

openssl x509 \
    -req -days 3650 \
    -in "$CERT_DIR/cert.csr" \
    -CA "$CERT_DIR/cacert.pem" \
    -CAkey "$CERT_DIR/cakey.pem" \
    -CAcreateserial \
    -extensions v3_req \
    -extfile "$CERT_CFG" \
    -out "$CERT_DIR/cert.pem"

echo "Certificates generated in $CERT_DIR"
