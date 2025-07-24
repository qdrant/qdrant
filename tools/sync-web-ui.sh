#!/usr/bin/env bash

set -euo pipefail

STATIC_DIR=${STATIC_DIR:-"./static"}
OPENAPI_FILE=${OPENAPI_DIR:-"./docs/redoc/master/openapi.json"}

# Download `dist.zip` from the latest release of https://github.com/qdrant/qdrant-web-ui and unzip given folder

# Get latest dist.zip, assume jq is installed
API_RESPONSE=$(curl --retry 5 --retry-all-errors --silent -w "\n%{http_code}" --connect-timeout 10 --max-time 30 "https://api.github.com/repos/qdrant/qdrant-web-ui/releases/latest")
HTTP_CODE=$(echo "$API_RESPONSE" | tail -n1)
API_RESPONSE=$(echo "$API_RESPONSE" | sed '$d')

if [ "$HTTP_CODE" != "200" ]; then
    echo "Error: GitHub API returned HTTP $HTTP_CODE"
    if [ "$HTTP_CODE" = "403" ]; then
        echo "This might be due to rate limiting. API Response:"
        echo "$API_RESPONSE" | jq -r '.message // .' 2>/dev/null || echo "$API_RESPONSE"
    fi
    exit 1
fi

if [ -z "$API_RESPONSE" ]; then
    echo "Error: Empty response from GitHub API"
    exit 1
fi

DOWNLOAD_LINK=$(echo "$API_RESPONSE" | jq -r '.assets[] | select(.name=="dist-qdrant.zip") | .browser_download_url' 2>/dev/null)

if [ -z "$DOWNLOAD_LINK" ] || [ "$DOWNLOAD_LINK" = "null" ]; then
    echo "Error: Could not find dist-qdrant.zip in the latest release"
    echo "Available assets:"
    echo "$API_RESPONSE" | jq -r '.assets[]' 2>/dev/null || echo "Failed to parse assets"
    exit 1
fi

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
