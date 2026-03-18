import os

import schemathesis

ROOT_DIR = os.path.dirname(__file__)
OPENAPI_FILE = os.environ.get("OPENAPI_FILE", os.path.join(os.path.dirname(ROOT_DIR), '../../docs/redoc/master', 'openapi.json'))

SCHEMA = schemathesis.from_file(open(OPENAPI_FILE))
QDRANT_HOST = os.environ.get("QDRANT_HOST", "http://localhost:6333")

QDRANT_HOST_HEADERS = os.environ.get("QDRANT_HOST_HEADERS", "{}")
