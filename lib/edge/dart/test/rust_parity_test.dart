// The parts of the Rust edge API the Dart SDK gained to match it: the BM25
// sparse embedder, optimization under a shared resource budget, and the
// search_matrix op. Runs against the real native engine (the Native Assets hook
// builds it on the host).
import 'dart:io';

import 'package:qdrant_edge/qdrant_edge.dart';
import 'package:test/test.dart';

EdgeShard openShard(EdgeConfig config) {
  final dir = Directory.systemTemp.createTempSync('qe_parity_');
  final shard = EdgeShard.load(path: dir.path, config: config);
  addTearDown(() {
    try {
      shard.unload();
    } on EdgeException catch (_) {}
    if (dir.existsSync()) dir.deleteSync(recursive: true);
  });
  return shard;
}

int numId(PointId id) => (id as NumIdPointId).value;

void main() {
  group('Bm25', () {
    test('a query token is one of the document tokens', () {
      final bm25 = Bm25();
      final doc = bm25.embedDocument(text: 'the quick brown fox jumps');
      final query = bm25.embedQuery(text: 'fox fox');
      expect(doc.indices, hasLength(doc.values.length));
      expect(query.indices, hasLength(1));
      expect(query.values, [1.0]);
      expect(doc.indices, contains(query.indices.single));
    });

    test('the config reaches the model', () {
      final english = Bm25(config: Bm25Config(language: 'english'));
      expect(
        english.embedQuery(text: 'running').indices,
        english.embedQuery(text: 'runs').indices,
      );
    });

    test('an invalid config is InvalidArgumentEdgeException', () {
      expect(
        () => Bm25(config: Bm25Config(language: 'klingon')),
        throwsA(isA<InvalidArgumentEdgeException>()),
      );
      expect(
        () => Bm25(config: Bm25Config(k: double.nan)),
        throwsA(isA<InvalidArgumentEdgeException>()),
      );
    });

    test('embeddings search an IDF sparse field, best match first', () {
      final shard = openShard(
        EdgeConfig(
          vectorData: {},
          sparseVectorData: {
            'bm25': SparseVectorDataConfig(modifier: Modifier.idf),
          },
        ),
      );
      final bm25 = Bm25(config: Bm25Config(language: 'english'));
      final docs = {
        1: 'The cat sat on the mat',
        2: 'Cats are chasing cats around the garden',
        3: 'A dog barked at the mailman',
      };
      shard.update(
        operation: UpdateOperation.upsertPoints(
          points: [
            for (final MapEntry(:key, :value) in docs.entries)
              Point(
                id: NumIdPointId(key),
                vector: NamedVectorVariant({
                  'bm25': SparseNamedVector(bm25.embedDocument(text: value)),
                }),
                payload: null,
              ),
          ],
        ),
      );
      final hits = shard.search(
        request: SearchRequest(
          query: NearestQuery(
            vector: SparseNamedVector(bm25.embedQuery(text: 'cats')),
            using: 'bm25',
          ),
          limit: 10,
        ),
      );
      expect([for (final h in hits) numId(h.id)], [2, 1]);
    });
  });

  group('optimizeWithBudget', () {
    test('one budget shared by two shards optimizes both', () {
      final budget = ResourceBudget(cpu: 1, io: 1);
      for (var s = 0; s < 2; s++) {
        final shard = openShard(
          EdgeConfig(
            vectorData: {'': VectorDataConfig(size: 4, distance: Distance.dot)},
          ),
        );
        shard.update(
          operation: UpdateOperation.upsertPoints(
            points: [
              for (var i = 1; i <= 3; i++)
                Point(
                  id: NumIdPointId(i),
                  vector: SingleVector([i.toDouble(), 0, 0, 0]),
                  payload: null,
                ),
            ],
          ),
        );
        shard.optimizeWithBudget(budget: budget);
        expect(shard.count(request: CountRequest()), 3);
      }
    });

    test('a zero budget is InvalidArgumentEdgeException', () {
      expect(
        () => ResourceBudget(cpu: 0, io: 1),
        throwsA(isA<InvalidArgumentEdgeException>()),
      );
    });
  });

  test('searchMatrix returns each sample with its nearest points', () {
    final shard = openShard(
      EdgeConfig(
        vectorData: {'': VectorDataConfig(size: 2, distance: Distance.euclid)},
      ),
    );
    final positions = {1: 0.0, 2: 1.0, 3: 10.0, 4: 11.0};
    shard.update(
      operation: UpdateOperation.upsertPoints(
        points: [
          for (final MapEntry(:key, :value) in positions.entries)
            Point(
              id: NumIdPointId(key),
              vector: SingleVector([value, 0]),
              payload: null,
            ),
        ],
      ),
    );
    final matrix = shard.searchMatrix(
      request: SearchMatrixRequest(sampleSize: 4, limitPerSample: 1),
    );
    expect(matrix.sampleIds, hasLength(4));
    expect(matrix.nearests, hasLength(4));
    // Points come in pairs (1,2) and (3,4); each one's nearest is its pair.
    const pair = {1: 2, 2: 1, 3: 4, 4: 3};
    for (var i = 0; i < 4; i++) {
      final sample = numId(matrix.sampleIds[i]);
      expect(numId(matrix.nearests[i].single.id), pair[sample]);
    }
  });
}
