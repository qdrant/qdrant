#!/bin/bash

APPDIR="$(dirname "$(readlink -f "$0")")"
export QDRANT__SERVICE__STATIC_CONTENT_DIR="$APPDIR/usr/share/static"
exec "$APPDIR/usr/bin/qdrant" "$@"
