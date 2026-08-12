import 'dart:io';

import 'package:qdrant_edge/qdrant_edge.dart';
import 'package:test/test.dart';

/// A single unnamed 4-dim dot-product vector field — mirrors the Swift/Kotlin
/// SDK tests so behaviour can be compared across bindings.
EdgeConfig singleVectorConfig() => EdgeConfig(
      vectorData: {'': VectorDataConfig(size: 4, distance: Distance.dot)},
    );

/// Upsert `n` axis-aligned points (id 1..n, unit vector on axis i-1) with a
/// `{"label": "<letter>"}` payload.
void seedAxisPoints(EdgeShardInterface shard, int n) {
  final points = <Point>[];
  for (var i = 1; i <= n; i++) {
    final v = List<double>.filled(4, 0.0);
    v[(i - 1) % 4] = 1.0;
    points.add(Point(
      id: NumIdPointId(i),
      vector: SingleVector(v),
      payload: '{"label":"${String.fromCharCode(96 + i)}"}',
    ));
  }
  shard.update(operation: UpdateOperation.upsertPoints(points: points));
}

void main() {
  group('round-trip', () {
    test('load -> upsert -> search returns nearest first', () {
      final dir = Directory.systemTemp.createTempSync('qdrant_edge_test_');
      addTearDown(() => dir.deleteSync(recursive: true));

      final shard = EdgeShard.load(path: dir.path, config: singleVectorConfig());
      addTearDown(shard.unload);
      seedAxisPoints(shard, 3);

      final results = shard.search(
        request: SearchRequest(
          query: NearestQuery(
            vector: DenseNamedVector([1.0, 0.0, 0.0, 0.0]),
            using: null,
          ),
          limit: 10,
          withVector: BoolWithVector(false),
          withPayload: BoolWithPayload(true),
        ),
      );

      expect(results, hasLength(3));
      expect((results.first.id as NumIdPointId).value, 1,
          reason: 'nearest to [1,0,0,0] is point id=1');
      expect(results.first.payload, contains('"label"'));
    });
  });

  group('persistence', () {
    test('data survives flush -> unload -> reload of the same directory', () {
      final dir = Directory.systemTemp.createTempSync('qdrant_edge_persist_');
      addTearDown(() => dir.deleteSync(recursive: true));

      // First session: write, flush to disk, close.
      final first = EdgeShard.load(path: dir.path, config: singleVectorConfig());
      seedAxisPoints(first, 3);
      first.flush();
      first.unload();

      // Second session: reopen the same directory — the points must be there.
      final second = EdgeShard.load(path: dir.path, config: singleVectorConfig());
      addTearDown(second.unload);

      expect(second.count(request: CountRequest()), 3,
          reason: 'all 3 points persisted across reopen');

      final hits = second.search(
        request: SearchRequest(
          query: NearestQuery(
            vector: DenseNamedVector([0.0, 1.0, 0.0, 0.0]),
            using: null,
          ),
          limit: 1,
        ),
      );
      expect((hits.first.id as NumIdPointId).value, 2,
          reason: 'the persisted vectors are still searchable');
    });
  });

  group('create vs load', () {
    test('create opens a fresh shard; a second create on it is rejected', () {
      final dir = Directory.systemTemp.createTempSync('qdrant_edge_create_');
      addTearDown(() => dir.deleteSync(recursive: true));

      final shard = EdgeShard.create(path: dir.path, config: singleVectorConfig());
      seedAxisPoints(shard, 1);
      shard.flush();
      shard.unload();

      // create is not idempotent: re-creating over an existing shard must fail
      // rather than silently clobber or reopen it.
      expect(
        () => EdgeShard.create(path: dir.path, config: singleVectorConfig()),
        throwsA(isA<EdgeException>()),
      );
    });
  });

  group('mutations', () {
    test('delete removes points and updates the count', () {
      final dir = Directory.systemTemp.createTempSync('qdrant_edge_delete_');
      addTearDown(() => dir.deleteSync(recursive: true));

      final shard = EdgeShard.load(path: dir.path, config: singleVectorConfig());
      addTearDown(shard.unload);
      seedAxisPoints(shard, 3);
      expect(shard.count(request: CountRequest()), 3);

      shard.update(
        operation: UpdateOperation.deletePoints(pointIds: [NumIdPointId(2)]),
      );

      expect(shard.count(request: CountRequest()), 2,
          reason: 'one point deleted');
      final remaining = shard.retrieve(
        request: RetrieveRequest(pointIds: [NumIdPointId(2)]),
      );
      expect(remaining, isEmpty, reason: 'deleted id is no longer retrievable');
    });

    test('a UUID point id round-trips through upsert and retrieve', () {
      final dir = Directory.systemTemp.createTempSync('qdrant_edge_uuid_');
      addTearDown(() => dir.deleteSync(recursive: true));

      final shard = EdgeShard.load(path: dir.path, config: singleVectorConfig());
      addTearDown(shard.unload);

      const uuid = '550e8400-e29b-41d4-a716-446655440000';
      shard.update(
        operation: UpdateOperation.upsertPoints(points: [
          Point(id: UuidPointId(uuid), vector: SingleVector([1, 0, 0, 0])),
        ]),
      );

      final got = shard.retrieve(
        request: RetrieveRequest(pointIds: [UuidPointId(uuid)]),
      );
      expect(got, hasLength(1));
      expect((got.first.id as UuidPointId).value, uuid,
          reason: 'the UUID id is preserved, not coerced to an integer');
    });
  });

  group('filter', () {
    test('a payload field condition narrows the search results', () {
      final dir = Directory.systemTemp.createTempSync('qdrant_edge_filter_');
      addTearDown(() => dir.deleteSync(recursive: true));

      final shard = EdgeShard.load(path: dir.path, config: singleVectorConfig());
      addTearDown(shard.unload);
      seedAxisPoints(shard, 3); // labels a, b, c

      final onlyB = shard.search(
        request: SearchRequest(
          query: NearestQuery(
            vector: DenseNamedVector([1.0, 0.0, 0.0, 0.0]),
            using: null,
          ),
          limit: 10,
          filter: Filter(must: [
            FieldConditionVariant(FieldCondition(
              key: 'label',
              match: ValueMatch(StringValueVariants('b')),
            )),
          ]),
        ),
      );

      expect(onlyB, hasLength(1), reason: 'only the point labelled "b" matches');
      expect((onlyB.first.id as NumIdPointId).value, 2);
    });
  });

  group('error paths', () {
    test('a wrong-dimension query vector is rejected', () {
      final dir = Directory.systemTemp.createTempSync('qdrant_edge_dim_');
      addTearDown(() => dir.deleteSync(recursive: true));

      final shard = EdgeShard.load(path: dir.path, config: singleVectorConfig());
      addTearDown(shard.unload);
      seedAxisPoints(shard, 1);

      // The field is 4-dim; a 3-dim query must be rejected, not silently padded.
      // The engine classifies a dimension mismatch as an operation error and
      // surfaces the offending dimensions in the message.
      expect(
        () => shard.search(
          request: SearchRequest(
            query: NearestQuery(
              vector: DenseNamedVector([1.0, 0.0, 0.0]),
              using: null,
            ),
            limit: 1,
          ),
        ),
        throwsA(
          isA<OperationExceptionEdgeException>().having(
            (e) => e.toString(),
            'message',
            contains('dimension'),
          ),
        ),
      );
    });

    test('operating on an unloaded shard throws ShardClosed', () {
      final dir = Directory.systemTemp.createTempSync('qdrant_edge_closed_');
      addTearDown(() => dir.deleteSync(recursive: true));

      final shard = EdgeShard.load(path: dir.path, config: singleVectorConfig());
      seedAxisPoints(shard, 1);
      shard.unload();

      // After unload the handle is dead; further calls must fail loudly rather
      // than use-after-free or silently no-op.
      expect(
        () => shard.count(request: CountRequest()),
        throwsA(isA<ShardClosedEdgeException>()),
      );
    });
  });
}
