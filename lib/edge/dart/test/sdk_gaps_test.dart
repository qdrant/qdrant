// Regression suite for four SDK gaps reported by a consumer building on top of
// this package, each reproduced against a scratch directory with no network and
// no fixtures:
//
//   §1  `EdgeShard.load(path, config: null)` tells the rows of the reported
//       load-ambiguity table apart by its outcome: no shard here (a plain
//       `OperationExceptionEdgeException`), a shard that opens, or a shard that
//       is present but will not open (`UnreadableEdgeException` — never recreate
//       over it). Row 3 — config deleted, segments intact — is the one that
//       shipped as a data-loss bug.
//   §2  every exception crossing the API is catchable by name
//       (`EdgeException` or `UniffiInternalError`).
//   §3  a shard held by another handle throws the distinct
//       `ShardLockedEdgeException`, not a generic runtime error.
//   §4  `EdgeShard.clear()` empties a shard in place.
//
// These run against the host-built native library (the native-assets hook
// builds the in-tree crate from source), so they exercise the real engine.
import 'dart:io';
import 'dart:isolate';

import 'package:qdrant_edge/qdrant_edge.dart';
import 'package:test/test.dart';

EdgeConfig _cfg() => EdgeConfig(
      vectorData: {'': VectorDataConfig(size: 4, distance: Distance.cosine)},
    );

Point _pt(int id) => Point(
      id: NumIdPointId(id),
      vector: SingleVector([0.1 * id, 0.2, 0.3, 0.4]),
      payload: null,
    );

/// Create a real, persisted shard at [path] holding [n] points, then release it
/// so the WAL lock is free and the segments are on disk.
void _writePersistedShard(String path, {int n = 1}) {
  final shard = EdgeShard.load(path: path, config: _cfg());
  shard.update(
    operation: UpdateOperation.upsertPoints(
      points: [for (var i = 1; i <= n; i++) _pt(i)],
    ),
  );
  shard.flush();
  shard.unload();
  shard.dispose();
}

void main() {
  late Directory dir;
  setUp(() => dir = Directory.systemTemp.createTempSync('qe_gaps'));
  tearDown(() => dir.deleteSync(recursive: true));

  group('§1 load(path, config: null) — what is at a path', () {
    // "No shard here" is a plain OperationExceptionEdgeException; the engine's
    // wording is pinned by the FFI tests, and hosts telling the rows apart rely
    // on it.
    Matcher noShard() => throwsA(isA<OperationExceptionEdgeException>().having(
          (e) => e.reason,
          'reason',
          contains('no segments were loaded'),
        ));

    test('row 1: empty directory → no shard', () {
      expect(() => EdgeShard.load(path: dir.path, config: null), noShard());
    });

    test('absent path → load refuses it; create makes it', () {
      final path = '${dir.path}/nested/store';
      expect(
        () => EdgeShard.load(path: path, config: null),
        throwsA(isA<OperationExceptionEdgeException>()),
      );
      final shard = EdgeShard.create(path: path, config: _cfg());
      shard.unload();
      shard.dispose();
    });

    test('row 2: unrelated files only → no shard', () {
      File('${dir.path}/hello.txt').writeAsStringSync('not ours');
      expect(() => EdgeShard.load(path: dir.path, config: null), noShard());
    });

    test('a normally-persisted shard → opens', () {
      _writePersistedShard(dir.path);
      final shard = EdgeShard.load(path: dir.path, config: null);
      expect(shard.info().pointsCount, 1);
      shard.unload();
      shard.dispose();
    });

    test('row 3 (the one that cost a release): edge_config.json deleted, '
        'segments intact → opens with the data', () {
      _writePersistedShard(dir.path);
      final cfg = File('${dir.path}/edge_config.json');
      expect(cfg.existsSync(), isTrue,
          reason: 'a real load must have written edge_config.json');
      cfg.deleteSync();

      // The heuristic "no edge_config.json ⇒ empty store" reported a full corpus
      // as empty; load reads the shard from its segments.
      final shard = EdgeShard.load(path: dir.path, config: null);
      expect(shard.info().pointsCount, 1);
      shard.unload();
      shard.dispose();
    });

    test('row 4: corrupt edge_config.json → unreadable with a reason', () {
      _writePersistedShard(dir.path);
      File('${dir.path}/edge_config.json').writeAsStringSync('{ not valid');

      expect(
        () => EdgeShard.load(path: dir.path, config: null),
        throwsA(isA<UnreadableEdgeException>()
            .having((e) => e.reason, 'reason', isNotEmpty)),
      );
    });
  });

  group('§3 a shard held by another handle', () {
    test('second load throws the distinct ShardLockedEdgeException', () {
      final s1 = EdgeShard.load(path: dir.path, config: _cfg());
      s1.update(operation: UpdateOperation.upsertPoints(points: [_pt(1)]));
      try {
        // s1 still holds the WAL lock — a second load must fail as *locked*,
        // not as a generic OperationException (that ambiguity cost a corpus).
        expect(
          () => EdgeShard.load(path: dir.path, config: null),
          throwsA(isA<ShardLockedEdgeException>()),
        );
      } finally {
        s1.unload();
        s1.dispose();
      }

      // Once the holder releases it, the shard opens normally.
      final s2 = EdgeShard.load(path: dir.path, config: null);
      expect(s2.info().pointsCount, 1);
      s2.unload();
      s2.dispose();
    });

  });

  group('§4 EdgeShard.clear', () {
    test('empties the shard, leaves it writable, and the empty state persists',
        () {
      final shard = EdgeShard.load(path: dir.path, config: _cfg());
      shard.update(
          operation: UpdateOperation.upsertPoints(points: [_pt(1), _pt(2)]));
      expect(shard.count(request: CountRequest()), 2);

      shard.clear();
      expect(shard.count(request: CountRequest()), 0);

      // Still writable after clear.
      shard.update(operation: UpdateOperation.upsertPoints(points: [_pt(3)]));
      expect(shard.count(request: CountRequest()), 1);

      shard.clear();
      shard.flush();
      shard.unload();
      shard.dispose();

      final reopened = EdgeShard.load(path: dir.path, config: null);
      expect(reopened.info().pointsCount, 0);
      reopened.unload();
      reopened.dispose();
    });
  });

  group('§2 exception guarantee', () {
    test('EdgeException and UniffiInternalError are both catchable by name', () {
      // A compile-level guarantee: both exception types the API can raise are
      // exported from the public barrel and nameable in `on` clauses. If either
      // ever fell out of the export, this file would not compile.
      String handle(void Function() body) {
        try {
          body();
          return 'ok';
        } on ShardLockedEdgeException {
          return 'locked';
        } on EdgeException {
          return 'edge';
        } on UniffiInternalError {
          return 'internal';
        }
      }

      // A real engine failure lands in the EdgeException arm: an empty
      // directory with no config cannot be loaded.
      expect(handle(() => EdgeShard.load(path: dir.path, config: null)), 'edge');
    });
  });

  group('handle lifecycle', () {
    NearestQuery nearest(List<double> v) =>
        NearestQuery(vector: DenseNamedVector(v), using: null);

    test('an argument that fails to convert does not keep the shard open', () {
      final shard = EdgeShard.load(path: dir.path, config: _cfg());
      shard.update(operation: UpdateOperation.upsertPoints(points: [_pt(1)]));
      expect(
        () => shard.search(
          request: SearchRequest(query: nearest([1, 0, 0, 0]), limit: -1),
        ),
        throwsArgumentError,
      );
      shard.dispose();

      // dispose() released the last reference, so the lock is free.
      final reopened = EdgeShard.load(path: dir.path, config: null);
      expect(reopened.info().pointsCount, 1);
      reopened.unload();
      reopened.dispose();
    });

    test('use after dispose() throws StateError; dispose() is idempotent', () {
      final shard = EdgeShard.load(path: dir.path, config: _cfg());
      shard.unload();
      shard.dispose();
      expect(
        () => shard.info(),
        throwsA(isA<StateError>().having(
          (e) => e.message,
          'message',
          'EdgeShard was used after dispose()',
        )),
      );
      // Arguments are not lowered (and leaked) before the check.
      expect(
        () => shard.update(
          operation: UpdateOperation.upsertPoints(points: [_pt(1)]),
        ),
        throwsStateError,
      );
      shard.dispose();
    });

    test('a shard cannot be sent to another isolate', () async {
      final shard = EdgeShard.load(path: dir.path, config: _cfg());
      try {
        // Two isolates would share one native reference with separate
        // dispose state; the binding marks the class unsendable.
        await expectLater(
          Isolate.run(() => shard.hashCode),
          throwsArgumentError,
        );
      } finally {
        shard.unload();
        shard.dispose();
      }
    });

    test('a second load of a live WAL-less shard is ShardLocked', () {
      final s1 = EdgeShard.create(
        path: dir.path,
        config: EdgeConfig(
          vectorData: {'': VectorDataConfig(size: 4, distance: Distance.cosine)},
          walEnabled: false,
        ),
      );
      try {
        expect(
          () => EdgeShard.load(path: dir.path, config: null),
          throwsA(isA<ShardLockedEdgeException>()),
        );
      } finally {
        s1.unload();
        s1.dispose();
      }
    });

    test('a WAL-less shard whose config was deleted keeps applying updates', () {
      final s1 = EdgeShard.create(
        path: dir.path,
        config: EdgeConfig(
          vectorData: {'': VectorDataConfig(size: 4, distance: Distance.cosine)},
          walEnabled: false,
        ),
      );
      s1.update(
        operation: UpdateOperation.upsertPoints(points: [_pt(1), _pt(2)]),
      );
      s1.unload();
      s1.dispose();
      File('${dir.path}/edge_config.json').deleteSync();

      // The engine used to start a fresh WAL numbered below the stored
      // versions here, and silently skip this delete.
      final s2 = EdgeShard.load(path: dir.path, config: null);
      s2.update(
        operation: UpdateOperation.deletePoints(pointIds: [NumIdPointId(2)]),
      );
      expect(s2.info().pointsCount, 1);
      s2.unload();
      s2.dispose();
    });
  });
}
