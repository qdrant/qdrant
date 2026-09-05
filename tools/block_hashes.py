#!/usr/bin/env python3
"""Block-hashes v1 contract and dependency-free reference client (Python 3.10+).

All hashes are SHA-256; lengths/counts are unsigned big-endian u64 byte encodings.
Encode numeric IDs as 0x00 || u64be(id), UUIDs as 0x01 || 16 RFC 4122 bytes.
Encode the selected string as exact UTF-8, without Unicode normalization.
R = SHA256(b"qdrant:point-fingerprint:v1" || NUL || encoded_id || u64be(len(s)) || s)
B = SHA256(b"qdrant:block-hashes:v1" || NUL || R1 || ... || Rn || u64be(n))
Within each block, order numeric IDs ascending first, then UUID bytes ascending.
R1..Rn are raw 32-byte digests; return B as lowercase hexadecimal. Empty blocks
use the block domain and eight zero bytes. The path and block number are not hashed.

Membership is Qdrant's existing slice_point_id_hash(id) % block_count, using
zero-key SipHash-2-4 over u64 LITTLE-endian numeric IDs or RFC 4122 UUID bytes.
Object paths (including quoted keys) must select a string; missing fields,
non-strings, array traversal and duplicate source IDs are errors. Preserve u64
IDs exactly when decoding JSON. Counts must be in 1..65536.

Input: JSON array of {"id": integer-or-UUID, "payload": {...}} records.
Apply the same Qdrant filter to your source before passing records here.
Refine using --slice TOTAL INDEX and a block count that is a multiple of TOTAL;
retain the original scope filters, then scroll the mismatched slices with
with_payload=[payload_key], with_vector=false. This is a live audit, not a
snapshot: repeat comparisons and revalidate before destructive repairs.
Run --self-test for the shared Rust/Python test vectors in tests/fixtures.
"""

import argparse
import hashlib
import json
import struct
import uuid
from pathlib import Path

POINT_DOMAIN = b"qdrant:point-fingerprint:v1\0"
BLOCK_DOMAIN = b"qdrant:block-hashes:v1\0"
MASK = (1 << 64) - 1


def canonical_id(point_id):
    """Return (sort key, content bytes, slice bytes). Numeric IDs precede UUIDs."""
    if type(point_id) is int and 0 <= point_id <= MASK:
        return (0, point_id), b"\x00" + struct.pack(">Q", point_id), struct.pack("<Q", point_id)
    if isinstance(point_id, str):
        value = uuid.UUID(point_id)
        return (1, value.int), b"\x01" + value.bytes, value.bytes
    raise ValueError(f"Invalid point ID: {point_id!r}")


def siphash24(data):
    """SipHash-2-4 with the all-zero 128-bit key, as in Slice::check."""
    v = [0x736F6D6570736575, 0x646F72616E646F6D, 0x6C7967656E657261, 0x7465646279746573]

    def rot(x, bits):
        return ((x << bits) | (x >> (64 - bits))) & MASK

    def rounds(n):
        for _ in range(n):
            v[0] = (v[0] + v[1]) & MASK
            v[1] = rot(v[1], 13) ^ v[0]
            v[0] = rot(v[0], 32)
            v[2] = (v[2] + v[3]) & MASK
            v[3] = rot(v[3], 16) ^ v[2]
            v[0] = (v[0] + v[3]) & MASK
            v[3] = rot(v[3], 21) ^ v[0]
            v[2] = (v[2] + v[1]) & MASK
            v[1] = rot(v[1], 17) ^ v[2]
            v[2] = rot(v[2], 32)

    end = len(data) - len(data) % 8
    words = [int.from_bytes(data[i:i + 8], "little") for i in range(0, end, 8)]
    words.append(((len(data) & 255) << 56) | int.from_bytes(data[end:], "little"))
    for word in words:
        v[3] ^= word
        rounds(2)
        v[0] ^= word
    v[2] ^= 255
    rounds(4)
    return v[0] ^ v[1] ^ v[2] ^ v[3]


def slice_point_id_hash(point_id):
    return siphash24(canonical_id(point_id)[2])


def object_path(path):
    """Parse Qdrant's object-only JSON path subset, including quoted keys.

    Quoted keys allow dots/spaces but no backslash or quote escapes, matching
    Qdrant JsonPath. Array indices and wildcards are rejected in v1.
    """
    keys = []
    pos = 0
    while pos < len(path):
        if path[pos] == '"':
            end = path.find('"', pos + 1)
            if end < 0 or "\\" in path[pos + 1:end]:
                raise ValueError("Invalid quoted payload key")
            keys.append(path[pos + 1:end])
            pos = end + 1
        else:
            start = pos
            while pos < len(path) and (path[pos].isalnum() or path[pos] in "_-"):
                pos += 1
            if pos == start:
                raise ValueError("Expected an object key; arrays are unsupported")
            keys.append(path[start:pos])
        if pos == len(path):
            return keys
        if path[pos] != "." or pos + 1 == len(path):
            raise ValueError("Expected another object key")
        pos += 1
    raise ValueError("Empty payload path")


def fingerprint(record, keys):
    value = record.get("payload")
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            raise ValueError(f"Missing fingerprint on point {record['id']}")
        value = value[key]
    if not isinstance(value, str):
        raise ValueError(f"Fingerprint must be a string on point {record['id']}")
    return value.encode("utf-8", errors="strict")


def point_digest(point_id, value):
    """Internal contract helper; there is no server endpoint for point digests."""
    return hashlib.sha256(
        POINT_DOMAIN + canonical_id(point_id)[1] + struct.pack(">Q", len(value)) + value
    ).digest()


def block_hashes(records, payload_key, block_count):
    """Hash an already scoped source dataset. Duplicate logical IDs are errors."""
    if type(block_count) is not int or not 1 <= block_count <= 65536:
        raise ValueError("block_count must be between 1 and 65536")
    keys = object_path(payload_key)
    blocks = [hashlib.sha256(BLOCK_DOMAIN) for _ in range(block_count)]
    counts = [0] * block_count
    previous = None
    for record in sorted(records, key=lambda record: canonical_id(record["id"])[0]):
        point_id = record["id"]
        sort_key = canonical_id(point_id)[0]
        if sort_key == previous:
            raise ValueError(f"Duplicate logical point ID: {point_id}")
        previous = sort_key
        block_id = slice_point_id_hash(point_id) % block_count
        blocks[block_id].update(point_digest(point_id, fingerprint(record, keys)))
        counts[block_id] += 1
    result = []
    for block_id, (digest, count) in enumerate(zip(blocks, counts)):
        digest.update(struct.pack(">Q", count))
        result.append({"block_id": block_id, "point_count": count, "hash": digest.hexdigest()})
    return {"blocks": result}


def self_test():
    vectors = json.loads((Path(__file__).resolve().parents[1] / "tests/fixtures/block_hashes.json").read_text())
    for record in vectors["records"]:
        assert slice_point_id_hash(record["id"]) == int(record["slice_hash"])
        value = record["value"].encode("utf-8")
        assert point_digest(record["id"], value).hex() == record["point_digest"]
    points = [{"id": r["id"], "payload": {"sync": {"fingerprint": r["value"]}}} for r in vectors["records"]]
    result = block_hashes(points[::-1], "sync.fingerprint", 16)["blocks"]
    assert [b for b in result if b["point_count"]] == vectors["nonempty_blocks"]
    assert all(b["hash"] == vectors["empty_hash"] for b in result if not b["point_count"])
    assert block_hashes(points, "sync.fingerprint", 1)["blocks"][0]["hash"] == vectors["one_block_hash"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", nargs="?", type=Path)
    parser.add_argument("--payload-key", default="sync.fingerprint")
    parser.add_argument("--block-count", type=int, default=16)
    parser.add_argument("--slice", nargs=2, type=int, metavar=("TOTAL", "INDEX"))
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        print("Cross-language test vectors passed")
    else:
        if args.input is None:
            parser.error("input is required unless --self-test is used")
        records = json.loads(args.input.read_text())
        if args.slice:
            total, index = args.slice
            if not 1 <= total <= (1 << 32) - 1 or not 0 <= index < total:
                parser.error("slice must satisfy 0 <= INDEX < TOTAL <= 2**32 - 1")
            records = [p for p in records if slice_point_id_hash(p["id"]) % total == index]
        print(json.dumps(block_hashes(records, args.payload_key, args.block_count), indent=2))
