#!/usr/bin/env bash

set -euo pipefail

STATIC_DIR=${STATIC_DIR:-"./static"}
OPENAPI_FILE=${OPENAPI_DIR:-"./docs/redoc/master/openapi.json"}

# Download `dist.zip` from the latest release of https://github.com/qdrant/qdrant-web-ui and unzip given folder

# Get latest dist.zip, assume jq is installed
DOWNLOAD_LINK=$(curl --retry 5 --retry-all-errors --silent "https://api.github.com/repos/qdrant/qdrant-web-ui/releases/latest" | jq -r '.assets[] | select(.name=="dist-qdrant.zip") | .browser_download_url')

if command -v wget &> /dev/null
then
    wget -O dist-qdrant.zip $DOWNLOAD_LINK
else
    curl -L -o dist-qdrant.zip $DOWNLOAD_LINK
fi

rm -rf "${STATIC_DIR}/"*
unzip -o dist-qdrant.zip -d "${STATIC_DIR}"
rm dist-qdrant.zip
cp -r "${STATIC_DIR}/dist/"* "${STATIC_DIR}"
rm -rf "${STATIC_DIR}/dist"

cp "${OPENAPI_FILE}" "${STATIC_DIR}/openapi.json"
