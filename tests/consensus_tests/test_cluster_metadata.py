import pathlib
from time import sleep
from typing import Any

from .utils import *

CONSENSUS_WAIT_SECONDS = 0.5

def test_cluster_metadata(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(tmp_path, 3, None)

    # Get (empty) metadata key list
    get_metadata_keys(peer_api_uris, [])

    # Put some metadata keys
    put_metadata_key(peer_api_uris, 'string', 'string')

    put_metadata_key(peer_api_uris, 'array', [
        None,
        1337,
        42.69,
        "string",
    ])

    put_metadata_key(peer_api_uris, 'object', {
        'null': None,
        'integer': 1337,
        'float': 42.69,
        'string': 'string',
    })

    # Get metadata key list
    get_metadata_keys(peer_api_uris, ['string', 'array', 'object'])

    # Delete metadata keys
    for key in ['string', 'array', 'object']:
        resp = requests.delete(f"{peer_api_uris[0]}/cluster/metadata/keys/{key}")
        assert_http_ok(resp)
        sleep(CONSENSUS_WAIT_SECONDS)
        get_metadata_key(peer_api_uris, key, None)

    # Get (empty) metadata key list
    get_metadata_keys(peer_api_uris, [])

def get_metadata_keys(peer_uris: list[str], keys: list[str]):
    for peer_uri in peer_uris:
        resp = requests.get(f"{peer_uri}/cluster/metadata/keys")
        assert_http_ok(resp)
        assert set(resp.json()['result']) == set(keys)

def put_metadata_key(peer_uris: list[str], key: str, value: Any):
    resp = requests.put(f"{peer_uris[0]}/cluster/metadata/keys/{key}", json=value)
    assert_http_ok(resp)
    sleep(CONSENSUS_WAIT_SECONDS)
    get_metadata_key(peer_uris, key, value)

def get_metadata_key(peer_uris: list[str], key: str, expected_value: Any):
    for peer_uri in peer_uris:
        resp = requests.get(f"{peer_uri}/cluster/metadata/keys/{key}")
        assert_http_ok(resp)
        assert resp.json()['result'] == expected_value
