"""Decode upsert batch routing fields from points_internal_service.proto."""

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory, text_format


# Project only the routing fields. Proto2 preserves the presence of shard_id,
# so an omitted shard cannot match shard 0. Other fields remain unknown fields.
_schema = text_format.Parse('''
name: "points_test.proto"
package: "points_test"
syntax: "proto2"
message_type {
  name: "UpsertPoints"
  field { name: "collection_name" number: 1 label: LABEL_OPTIONAL type: TYPE_STRING }
}
message_type {
  name: "UpsertPointsInternal"
  field { name: "upsert_points" number: 1 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".points_test.UpsertPoints" }
  field { name: "shard_id" number: 2 label: LABEL_OPTIONAL type: TYPE_UINT32 }
}
message_type {
  name: "UpdateOperation"
  field { name: "upsert" number: 2 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".points_test.UpsertPointsInternal" }
}
message_type {
  name: "UpdateBatchInternal"
  field { name: "operations" number: 1 label: LABEL_REPEATED type: TYPE_MESSAGE type_name: ".points_test.UpdateOperation" }
}
''', descriptor_pb2.FileDescriptorProto())
_descriptor = descriptor_pool.DescriptorPool().Add(_schema)
UpdateBatchInternal = message_factory.GetMessageClass(_descriptor.message_types_by_name["UpdateBatchInternal"])


def is_upsert_batch_for(request, collection_name, shard_id):
    batch = UpdateBatchInternal.FromString(request)
    return bool(batch.operations) and all(
        operation.upsert.upsert_points.collection_name == collection_name
        and operation.upsert.HasField("shard_id")
        and operation.upsert.shard_id == shard_id
        for operation in batch.operations
    )
