import os
import random
import string

import jwt
from consensus_tests.utils import start_cluster

# Each pytest-xdist worker gets its own 100-port window so auth test files can
# run in parallel on different workers without binding the same port.
_worker_id = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
_worker_num = int(_worker_id[2:]) if _worker_id.startswith("gw") and _worker_id[2:].isdigit() else 0
PORT_SEED = 10000 + _worker_num * 100
REST_URI = f"http://127.0.0.1:{PORT_SEED + 2}"
GRPC_URI = f"127.0.0.1:{PORT_SEED + 1}"

SECRET = "my_top_secret_key"
ALT_SECRET = "my_alternative_secret_key"

READ_ONLY_API_KEY = "boo-hoo, this can only read!"

API_KEY_HEADERS = {"Api-Key": SECRET}
API_KEY_METADATA = [("api-key", SECRET)]
READ_ONLY_API_KEY_METADATA = [("api-key", READ_ONLY_API_KEY)]


def start_jwt_protected_cluster(tmp_path, num_peers=1, extra_env=None):
    base_env = {
        "QDRANT__SERVICE__API_KEY": SECRET,
        "QDRANT__SERVICE__ALT_API_KEY": ALT_SECRET,
        "QDRANT__SERVICE__READ_ONLY_API_KEY": READ_ONLY_API_KEY,
        "QDRANT__SERVICE__JWT_RBAC": "true",
        "QDRANT__STORAGE__WAL__WAL_CAPACITY_MB": "1",  # to speed up snapshot tests
    }
    extra_env = {
        **base_env,
        **(extra_env or {}),
    }

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(
        tmp_path,
        num_peers=num_peers,
        port_seed=PORT_SEED,
        extra_env=extra_env,
        headers=API_KEY_HEADERS,
    )

    assert REST_URI in peer_api_uris

    return peer_api_uris, peer_dirs, bootstrap_uri


def encode_jwt(claims: dict, secret: str) -> str:
    return jwt.encode(claims, secret, algorithm="HS256")

def decode_jwt(token: str, secret: str) -> dict:
    return jwt.decode(token, secret, algorithms=["HS256"])

def random_str():
    return "".join(random.choices(string.ascii_lowercase, k=10))
