"""Decode the Raft fields used by consensus proxy matchers.

These descriptors project the wire schemas in lib/api/src/grpc/proto/raft_service.proto
and raft-rs proto/proto/eraftpb.proto at Qdrant's pinned revision aafb07c.
Unlisted fields are handled as unknown fields by protobuf. No bytes are rewritten.
"""

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory


MSG_APPEND = 3
MSG_APPEND_RESPONSE = 4
ENTRY_CONF_CHANGE_V2 = 2
REMOVE_NODE = 1

_field = descriptor_pb2.FieldDescriptorProto
_schema = descriptor_pb2.FileDescriptorProto(
    name="raft_test.proto", package="raft_test", syntax="proto3",
)


def _message(name, fields):
    message = _schema.message_type.add(name=name)
    for number, name, kind, repeated in fields:
        field = message.field.add(
            name=name, number=number,
            label=_field.LABEL_REPEATED if repeated else _field.LABEL_OPTIONAL,
        )
        if isinstance(kind, str):
            field.type = _field.TYPE_MESSAGE
            field.type_name = f".raft_test.{kind}"
        else:
            field.type = kind


_message("Envelope", [(1, "message", _field.TYPE_BYTES, False)])
_message("Entry", [
    (1, "entry_type", _field.TYPE_UINT32, False),
    (3, "index", _field.TYPE_UINT64, False),
    (4, "data", _field.TYPE_BYTES, False),
])
_message("Message", [
    (1, "msg_type", _field.TYPE_UINT32, False),
    (2, "to", _field.TYPE_UINT64, False),
    (3, "from_peer", _field.TYPE_UINT64, False),
    (6, "index", _field.TYPE_UINT64, False),
    (7, "entries", "Entry", True),
    (8, "commit", _field.TYPE_UINT64, False),
    (10, "reject", _field.TYPE_BOOL, False),
])
_message("ConfChangeSingle", [
    (1, "change_type", _field.TYPE_UINT32, False),
    (2, "node_id", _field.TYPE_UINT64, False),
])
_message("ConfChangeV2", [(2, "changes", "ConfChangeSingle", True)])
_descriptor = descriptor_pool.DescriptorPool().Add(_schema)
_Envelope = message_factory.GetMessageClass(_descriptor.message_types_by_name["Envelope"])
_Message = message_factory.GetMessageClass(_descriptor.message_types_by_name["Message"])
_ConfChangeV2 = message_factory.GetMessageClass(_descriptor.message_types_by_name["ConfChangeV2"])


def decode_raft_message(request):
    return _Message.FromString(_Envelope.FromString(request).message)


def removal_entry_index(message, peer_id):
    if message.msg_type != MSG_APPEND:
        return None
    for entry in message.entries:
        if entry.entry_type != ENTRY_CONF_CHANGE_V2:
            continue
        change = _ConfChangeV2.FromString(entry.data)
        if any(item.change_type == REMOVE_NODE and item.node_id == peer_id for item in change.changes):
            return entry.index
    return None
