import 'dart:io';

import 'package:qdrant_edge/qdrant_edge.dart';

/// Open an on-device shard, upsert three vectors, and run a nearest search.
/// Run with `dart run` (the Native Assets build hook builds the engine).
void main() {
  final dir = Directory.systemTemp.createTempSync('qdrant_edge_example_');
  EdgeShard? shard;
  try {
    shard = EdgeShard.load(
      path: dir.path,
      config: EdgeConfig(
        vectorData: {'': VectorDataConfig(size: 4, distance: Distance.dot)},
      ),
    );

    shard.update(
      operation: UpdateOperation.upsertPoints(points: [
        Point(id: NumIdPointId(1), vector: SingleVector([1, 0, 0, 0]), payload: '{"label":"a"}'),
        Point(id: NumIdPointId(2), vector: SingleVector([0, 1, 0, 0]), payload: '{"label":"b"}'),
        Point(id: NumIdPointId(3), vector: SingleVector([0, 0, 1, 0]), payload: '{"label":"c"}'),
      ]),
    );

    final results = shard.search(
      request: SearchRequest(
        query: NearestQuery(vector: DenseNamedVector([1, 0, 0, 0]), using: null),
        limit: 3,
        withPayload: BoolWithPayload(true),
      ),
    );

    for (final r in results) {
      final id = (r.id as NumIdPointId).value;
      print('id=$id  score=${r.score.toStringAsFixed(3)}  payload=${r.payload}');
    }

  } finally {
    shard?.unload();
    dir.deleteSync(recursive: true);
  }
}
