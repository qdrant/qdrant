FROM pseudomuto/protoc-gen-doc:latest

WORKDIR /protos

ADD lib/api/src/grpc/proto/ .

RUN rm collections_internal_service.proto \
       points_internal_service.proto \
       shard_snapshots_service.proto \
       raft_service.proto

RUN grep -v 'collections_internal_service.proto' qdrant.proto \
    | grep -v 'points_internal_service.proto' \
    | grep -v 'shard_snapshots_service.proto' \
    | grep -v 'raft_service.proto' \
    > qdrant.proto.tmp \
    && mv qdrant.proto.tmp qdrant.proto

ENTRYPOINT ["protoc", "--doc_out=/out", "--doc_opt=markdown,docs.md", "qdrant.proto"]
