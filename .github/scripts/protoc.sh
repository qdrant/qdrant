#!/bin/bash

PB_REL="https://github.com/protocolbuffers/protobuf/releases"
curl -LO $PB_REL/download/v3.12.4/protoc-3.12.4-linux-x86_64.zip
unzip protoc-3.12.4-linux-x86_64.zip -d /tmp/protoc
sudo mv /tmp/protoc/bin/protoc /usr/bin/
