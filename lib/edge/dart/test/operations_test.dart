// Full-surface operation coverage for the Qdrant Edge SDK. Exercises every
// EdgeShard method and the major update / query / filter / vector variants
// against the real native engine (the Native Assets hook builds it on the
// host). Companion to qdrant_edge_test.dart, which covers the core round-trip.

import 'dart:io';

import 'package:qdrant_edge/qdrant_edge.dart';
import 'package:test/test.dart';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Unit vector on axis (i-1) mod 4, in a 4-dim space.
List<double> axis(int i) {
  final v = List<double>.filled(4, 0.0);
  v[(i - 1) % 4] = 1.0;
  return v;
}

EdgeConfig denseCfg([int dim = 4, Distance d = Distance.dot]) =>
    EdgeConfig(vectorData: {'': VectorDataConfig(size: dim, distance: d)});

/// Open a fresh temp-dir shard and register teardown (unload + rm).
EdgeShard openShard(EdgeConfig config) {
  final dir = Directory.systemTemp.createTempSync('qe_ops_');
  final shard = EdgeShard.load(path: dir.path, config: config);
  addTearDown(() {
    // Best-effort close: a test may already have unloaded the shard, which the
    // engine reports as an EdgeException. Only that is swallowed — a Dart error
    // (e.g. a bug in cleanup) still surfaces.
    try {
      shard.unload();
    } on EdgeException catch (_) {}
    if (dir.existsSync()) dir.deleteSync(recursive: true);
  });
  return shard;
}

/// Upsert points 1..n on rotating axes with an optional JSON payload builder.
void seed(EdgeShard s, int n, {String? Function(int)? payload}) {
  s.update(
    operation: UpdateOperation.upsertPoints(
      points: [
        for (var i = 1; i <= n; i++)
          Point(
            id: NumIdPointId(i),
            vector: SingleVector(axis(i)),
            payload: payload?.call(i),
          ),
      ],
    ),
  );
}

NearestQuery nearest(List<double> v, {String? using}) =>
    NearestQuery(vector: DenseNamedVector(v), using: using);

Condition fieldEq(String key, ValueVariants v) =>
    FieldConditionVariant(FieldCondition(key: key, match: ValueMatch(v)));

void main() {
  // -------------------------------------------------------------------------
  group('lifecycle & introspection', () {
    test('create opens fresh; config/info/path/flush/optimize/unload', () {
      final dir = Directory.systemTemp.createTempSync('qe_life_');
      addTearDown(() => dir.deleteSync(recursive: true));
      final s = EdgeShard.create(path: dir.path, config: denseCfg());
      seed(s, 3);

      expect(s.config().vectorData.keys, contains(''));
      // Canonicalize both: Directory.systemTemp is under /var, a symlink to
      // /private/var on macOS, and the engine may return the resolved form.
      expect(
        Directory(s.path()).resolveSymbolicLinksSync(),
        Directory(dir.path).resolveSymbolicLinksSync(),
      );
      final info = s.info();
      expect(info.pointsCount, 3);
      s.flush();
      s.optimize(); // returns bool; may be true or false, just must not throw
      s.unload();
    });

    test('load reopens a persisted shard (create → close → load)', () {
      final dir = Directory.systemTemp.createTempSync('qe_reopen_');
      addTearDown(() => dir.deleteSync(recursive: true));
      final a = EdgeShard.create(path: dir.path, config: denseCfg());
      seed(a, 3);
      a.flush();
      a.unload();
      final b = EdgeShard.load(path: dir.path, config: denseCfg());
      addTearDown(b.unload);
      expect(b.count(request: CountRequest()), 3);
    });
  });

  // -------------------------------------------------------------------------
  group('upsert & vector kinds', () {
    test('single + batch upsert accumulate', () {
      final s = openShard(denseCfg());
      seed(s, 1);
      expect(s.count(request: CountRequest()), 1);
      seed(s, 5); // ids 1..5, id 1 replaced (no dup)
      expect(s.count(request: CountRequest()), 5);
    });

    test('named vectors — two dense fields, query one by name', () {
      final cfg = EdgeConfig(
        vectorData: {
          'title': VectorDataConfig(size: 4, distance: Distance.cosine),
          'body': VectorDataConfig(size: 4, distance: Distance.dot),
        },
      );
      final s = openShard(cfg);
      s.update(
        operation: UpdateOperation.upsertPoints(
          points: [
            Point(
              id: NumIdPointId(1),
              vector: NamedVectorVariant({
                'title': DenseNamedVector([1, 0, 0, 0]),
                'body': DenseNamedVector([0, 1, 0, 0]),
              }),
            ),
            Point(
              id: NumIdPointId(2),
              vector: NamedVectorVariant({
                'title': DenseNamedVector([0, 1, 0, 0]),
                'body': DenseNamedVector([1, 0, 0, 0]),
              }),
            ),
          ],
        ),
      );
      final hits = s.search(
        request: SearchRequest(
          query: nearest([1, 0, 0, 0], using: 'title'),
          limit: 1,
        ),
      );
      expect((hits.first.id as NumIdPointId).value, 1);
    });

    test('sparse vector — configured, upserted, queried by name', () {
      final cfg = EdgeConfig(
        vectorData: {'': VectorDataConfig(size: 4, distance: Distance.dot)},
        sparseVectorData: {'sparse': SparseVectorDataConfig()},
      );
      final s = openShard(cfg);
      s.update(
        operation: UpdateOperation.upsertPoints(
          points: [
            Point(
              id: NumIdPointId(1),
              vector: NamedVectorVariant({
                '': DenseNamedVector([1, 0, 0, 0]),
                'sparse': SparseNamedVector(
                  SparseVector(indices: [0, 5], values: [1.0, 2.0]),
                ),
              }),
            ),
          ],
        ),
      );
      final hits = s.search(
        request: SearchRequest(
          query: NearestQuery(
            vector: SparseNamedVector(
              SparseVector(indices: [5], values: [1.0]),
            ),
            using: 'sparse',
          ),
          limit: 5,
        ),
      );
      expect(hits, isNotEmpty);
    });

    test('multi-dense vector (MaxSim) — configured and queried', () {
      final cfg = EdgeConfig(
        vectorData: {
          'multi': VectorDataConfig(
            size: 4,
            distance: Distance.dot,
            multivectorConfig: MultiVectorConfig(
              comparator: MultiVectorComparator.maxSim,
            ),
          ),
        },
      );
      final s = openShard(cfg);
      s.update(
        operation: UpdateOperation.upsertPoints(
          points: [
            Point(
              id: NumIdPointId(1),
              vector: NamedVectorVariant({
                'multi': MultiDenseNamedVector([
                  [1, 0, 0, 0],
                  [0, 1, 0, 0],
                ]),
              }),
            ),
          ],
        ),
      );
      final hits = s.search(
        request: SearchRequest(
          query: NearestQuery(
            vector: MultiDenseNamedVector([
              [1, 0, 0, 0],
            ]),
            using: 'multi',
          ),
          limit: 5,
        ),
      );
      expect(hits, isNotEmpty);
    });

    test('updateVectors replaces a point vector', () {
      final s = openShard(denseCfg());
      seed(s, 2); // 1:[1,0,0,0], 2:[0,1,0,0]
      // Move point 2 onto axis 0 so it becomes the nearest to [1,0,0,0].
      s.update(
        operation: UpdateOperation.updateVectors(
          pointVectors: [
            PointVectors(id: NumIdPointId(2), vector: SingleVector([1, 0, 0, 0])),
          ],
        ),
      );
      final hits = s.search(
        request: SearchRequest(query: nearest([1, 0, 0, 0]), limit: 2),
      );
      expect(hits.map((h) => (h.id as NumIdPointId).value), containsAll([1, 2]));
      expect(hits.first.score, closeTo(1.0, 1e-6));
    });
  });

  // -------------------------------------------------------------------------
  group('payload operations', () {
    String? p(int i) => '{"lang":"en","n":$i}';

    test('setPayload merges; deletePayload removes a key', () {
      final s = openShard(denseCfg());
      seed(s, 1, payload: p);
      s.update(
        operation: UpdateOperation.setPayload(
          pointIds: [NumIdPointId(1)],
          payloadJson: '{"extra":true}',
        ),
      );
      var rec = s.retrieve(
        request: RetrieveRequest(
          pointIds: [NumIdPointId(1)],
          withPayload: BoolWithPayload(true),
        ),
      );
      expect(rec.first.payload, contains('extra'));
      expect(rec.first.payload, contains('lang'));

      s.update(
        operation: UpdateOperation.deletePayload(
          pointIds: [NumIdPointId(1)],
          keys: ['lang'],
        ),
      );
      rec = s.retrieve(
        request: RetrieveRequest(
          pointIds: [NumIdPointId(1)],
          withPayload: BoolWithPayload(true),
        ),
      );
      expect(rec.first.payload, isNot(contains('lang')));
      expect(rec.first.payload, contains('extra'));
    });

    test('overwritePayload replaces; clearPayload empties', () {
      final s = openShard(denseCfg());
      seed(s, 1, payload: p);
      s.update(
        operation: UpdateOperation.overwritePayload(
          pointIds: [NumIdPointId(1)],
          payloadJson: '{"only":"this"}',
        ),
      );
      var rec = s.retrieve(
        request: RetrieveRequest(
          pointIds: [NumIdPointId(1)],
          withPayload: BoolWithPayload(true),
        ),
      );
      expect(rec.first.payload, contains('only'));
      expect(rec.first.payload, isNot(contains('lang')));

      s.update(
        operation: UpdateOperation.clearPayload(pointIds: [NumIdPointId(1)]),
      );
      rec = s.retrieve(
        request: RetrieveRequest(
          pointIds: [NumIdPointId(1)],
          withPayload: BoolWithPayload(true),
        ),
      );
      expect(rec.first.payload == null || rec.first.payload == '{}', isTrue);
    });

    test('setPayloadByFilter targets matching points', () {
      final s = openShard(denseCfg());
      seed(s, 3, payload: (i) => '{"grp":"${i.isEven ? "even" : "odd"}"}');
      s.update(
        operation: UpdateOperation.setPayloadByFilter(
          filter: Filter(must: [fieldEq('grp', StringValueVariants('even'))]),
          payloadJson: '{"tagged":true}',
        ),
      );
      final n = s.count(
        request: CountRequest(
          filter: Filter(must: [fieldEq('tagged', BoolValueVariants(true))]),
        ),
      );
      expect(n, 1); // only id=2 is even
    });
  });

  // -------------------------------------------------------------------------
  group('delete operations', () {
    test('deletePoints and deletePointsByFilter shrink the shard', () {
      final s = openShard(denseCfg());
      seed(s, 4, payload: (i) => '{"keep":${i > 2}}');
      expect(s.count(request: CountRequest()), 4);

      s.update(
        operation: UpdateOperation.deletePoints(pointIds: [NumIdPointId(1)]),
      );
      expect(s.count(request: CountRequest()), 3);

      s.update(
        operation: UpdateOperation.deletePointsByFilter(
          filter: Filter(must: [fieldEq('keep', BoolValueVariants(false))]),
        ),
      );
      // id=2 had keep:false → removed; ids 3,4 remain.
      expect(s.count(request: CountRequest()), 2);
    });
  });

  // -------------------------------------------------------------------------
  group('field index', () {
    test('createFieldIndex registers a schema entry', () {
      final s = openShard(denseCfg());
      seed(s, 2, payload: (i) => '{"lang":"en"}');
      s.update(
        operation: UpdateOperation.createFieldIndex(
          fieldName: 'lang',
          schema: PayloadSchemaType.keyword,
        ),
      );
      expect(s.info().payloadSchema.keys, contains('lang'));

      s.update(operation: UpdateOperation.deleteFieldIndex(fieldName: 'lang'));
      expect(s.info().payloadSchema.keys, isNot(contains('lang')));
    });
  });

  // -------------------------------------------------------------------------
  group('search variants', () {
    test('nearest is ordered; threshold, payload/vector selectors, offset', () {
      final s = openShard(denseCfg());
      seed(s, 4, payload: (i) => '{"n":$i}');

      final hits = s.search(
        request: SearchRequest(
          query: nearest([1, 0, 0, 0]),
          limit: 10,
          withPayload: BoolWithPayload(true),
          withVector: BoolWithVector(true),
        ),
      );
      expect((hits.first.id as NumIdPointId).value, 1);
      expect(hits.first.payload, contains('"n"'));
      expect(hits.first.vector, isNotNull);

      final capped = s.search(
        request: SearchRequest(
          query: nearest([1, 0, 0, 0]),
          limit: 10,
          scoreThreshold: 0.5,
        ),
      );
      expect(capped, hasLength(1)); // only the exact-axis match clears 0.5

      final offset = s.search(
        request: SearchRequest(query: nearest([1, 0, 0, 0]), limit: 10, offset: 1),
      );
      expect((offset.first.id as NumIdPointId).value, isNot(1));
    });

    test('recommend / discover / context run through search()', () {
      final s = openShard(denseCfg());
      seed(s, 4);

      final rec = s.search(
        request: SearchRequest(
          query: RecommendQuery(
            positives: [DenseNamedVector([1, 0, 0, 0])],
            negatives: [DenseNamedVector([0, 0, 1, 0])],
            strategy: RecommendStrategy.bestScore,
            using: null,
          ),
          limit: 4,
        ),
      );
      expect(rec, isNotEmpty);

      final disc = s.search(
        request: SearchRequest(
          query: DiscoverQuery(
            target: DenseNamedVector([1, 0, 0, 0]),
            context: [
              ContextPair(
                positive: DenseNamedVector([1, 0, 0, 0]),
                negative: DenseNamedVector([0, 1, 0, 0]),
              ),
            ],
            using: null,
          ),
          limit: 4,
        ),
      );
      expect(disc, isNotEmpty);

      final ctx = s.search(
        request: SearchRequest(
          query: ContextQuery(
            context: [
              ContextPair(
                positive: DenseNamedVector([1, 0, 0, 0]),
                negative: DenseNamedVector([0, 1, 0, 0]),
              ),
            ],
            using: null,
          ),
          limit: 4,
        ),
      );
      expect(ctx, isNotEmpty);
    });
  });

  // -------------------------------------------------------------------------
  group('query() scoring variants', () {
    test('VectorScoringQuery mirrors search nearest', () {
      final s = openShard(denseCfg());
      seed(s, 4);
      final hits = s.query(
        request: QueryRequest(
          limit: 4,
          query: VectorScoringQuery(nearest([1, 0, 0, 0])),
        ),
      );
      expect((hits.first.id as NumIdPointId).value, 1);
    });

    test('RRF fusion over two prefetches', () {
      final s = openShard(denseCfg());
      seed(s, 4);
      final hits = s.query(
        request: QueryRequest(
          limit: 4,
          prefetches: [
            Prefetch(limit: 4, query: VectorScoringQuery(nearest([1, 0, 0, 0]))),
            Prefetch(limit: 4, query: VectorScoringQuery(nearest([0, 1, 0, 0]))),
          ],
          query: FusionScoringQuery(RrfFusion(k: 60, weights: null)),
        ),
      );
      expect(hits, isNotEmpty);
    });

    test('DBSF fusion, MMR, Sample, OrderBy, Formula', () {
      final s = openShard(denseCfg());
      seed(s, 4, payload: (i) => '{"year":${2000 + i}}');
      s.update(
        operation: UpdateOperation.createFieldIndex(
          fieldName: 'year',
          schema: PayloadSchemaType.integer,
        ),
      );

      final dbsf = s.query(
        request: QueryRequest(
          limit: 4,
          prefetches: [
            Prefetch(limit: 4, query: VectorScoringQuery(nearest([1, 0, 0, 0]))),
            Prefetch(limit: 4, query: VectorScoringQuery(nearest([0, 1, 0, 0]))),
          ],
          query: FusionScoringQuery(DbsfFusion()),
        ),
      );
      expect(dbsf, isNotEmpty);

      final mmr = s.query(
        request: QueryRequest(
          limit: 4,
          query: MmrScoringQuery(
            vector: DenseNamedVector([1, 0, 0, 0]),
            using: null,
            lambda: 0.5,
            candidatesLimit: 10,
          ),
        ),
      );
      expect(mmr, isNotEmpty);

      final sample = s.query(
        request: QueryRequest(limit: 2, query: SampleScoringQuery(Sample.random)),
      );
      expect(sample, hasLength(2));

      final ordered = s.query(
        request: QueryRequest(
          limit: 4,
          query: OrderByScoringQuery(
            OrderBy(key: 'year', direction: Direction.asc),
          ),
        ),
      );
      expect((ordered.first.id as NumIdPointId).value, 1); // year 2001 = id 1

      final formula = s.query(
        request: QueryRequest(
          limit: 4,
          prefetches: [
            Prefetch(limit: 4, query: VectorScoringQuery(nearest([1, 0, 0, 0]))),
          ],
          query: FormulaScoringQuery(
            expression: Expression.sum(
              terms: [Expression.constant(value: 1.0)],
            ),
            defaults: {},
          ),
        ),
      );
      expect(formula, isNotEmpty);
    });

    test('queryBatch returns one result list per request', () {
      final s = openShard(denseCfg());
      seed(s, 4);
      final res = s.queryBatch(
        requests: [
          QueryRequest(limit: 2, query: VectorScoringQuery(nearest([1, 0, 0, 0]))),
          QueryRequest(limit: 3, query: VectorScoringQuery(nearest([0, 1, 0, 0]))),
        ],
      );
      expect(res, hasLength(2));
      expect(res[0], hasLength(2));
      expect(res[1], hasLength(3));
    });
  });

  // -------------------------------------------------------------------------
  group('retrieve / scroll / count / facet / groups', () {
    test('retrieve by ids with payload', () {
      final s = openShard(denseCfg());
      seed(s, 3, payload: (i) => '{"n":$i}');
      final recs = s.retrieve(
        request: RetrieveRequest(
          pointIds: [NumIdPointId(2), NumIdPointId(3)],
          withPayload: BoolWithPayload(true),
        ),
      );
      expect(recs, hasLength(2));
      expect(recs.map((r) => (r.id as NumIdPointId).value), containsAll([2, 3]));
    });

    test('scroll paginates via nextOffset', () {
      final s = openShard(denseCfg());
      seed(s, 5);
      final page1 = s.scroll(request: ScrollRequest(limit: 2));
      expect(page1.records, hasLength(2));
      expect(page1.nextOffset, isNotNull);
      final page2 = s.scroll(
        request: ScrollRequest(limit: 2, offset: page1.nextOffset),
      );
      expect(page2.records, hasLength(2));
    });

    test('scroll with orderBy', () {
      final s = openShard(denseCfg());
      seed(s, 3, payload: (i) => '{"year":${2010 - i}}');
      s.update(
        operation: UpdateOperation.createFieldIndex(
          fieldName: 'year',
          schema: PayloadSchemaType.integer,
        ),
      );
      final res = s.scroll(
        request: ScrollRequest(
          limit: 3,
          orderBy: OrderBy(key: 'year', direction: Direction.asc),
        ),
      );
      // year = 2010-i → id 3 (2007) first ascending.
      expect((res.records.first.id as NumIdPointId).value, 3);
    });

    test('count total and filtered', () {
      final s = openShard(denseCfg());
      seed(s, 4, payload: (i) => '{"lang":"${i.isEven ? "en" : "fr"}"}');
      expect(s.count(request: CountRequest()), 4);
      final en = s.count(
        request: CountRequest(
          filter: Filter(must: [fieldEq('lang', StringValueVariants('en'))]),
        ),
      );
      expect(en, 2);
    });

    test('facet counts distinct values of an indexed keyword field', () {
      final s = openShard(denseCfg());
      seed(s, 4, payload: (i) => '{"lang":"${i.isEven ? "en" : "fr"}"}');
      s.update(
        operation: UpdateOperation.createFieldIndex(
          fieldName: 'lang',
          schema: PayloadSchemaType.keyword,
        ),
      );
      final facets = s.facet(request: FacetRequest(key: 'lang', exact: true));
      final byValue = {for (final h in facets.hits) h.value: h.count};
      expect(byValue['en'], 2);
      expect(byValue['fr'], 2);
    });

    test('queryGroups groups hits by a field', () {
      final s = openShard(denseCfg());
      seed(s, 4, payload: (i) => '{"cat":"${i <= 2 ? "a" : "b"}"}');
      s.update(
        operation: UpdateOperation.createFieldIndex(
          fieldName: 'cat',
          schema: PayloadSchemaType.keyword,
        ),
      );
      final groups = s.queryGroups(
        request: GroupRequest(
          query: QueryRequest(
            limit: 10,
            query: VectorScoringQuery(nearest([1, 0, 0, 0])),
          ),
          groupBy: 'cat',
          groups: 10,
          groupSize: 10,
        ),
      );
      expect(groups.length, 2); // categories a and b
    });
  });

  // -------------------------------------------------------------------------
  group('filter conditions', () {
    EdgeShard filterShard() {
      final s = openShard(denseCfg());
      seed(
        s,
        4,
        payload: (i) => '{"lang":"${i.isEven ? "en" : "fr"}","score":${i * 10},'
            '"tags":["t$i","common"],"empty":${i == 1 ? "[]" : "[1]"}}',
      );
      return s;
    }

    int count(EdgeShard s, Filter f) =>
        s.count(request: CountRequest(filter: f));

    test('ValueMatch (string / int / bool)', () {
      final s = openShard(denseCfg());
      seed(s, 3, payload: (i) => '{"s":"v$i","n":$i,"b":${i == 1}}');
      expect(count(s, Filter(must: [fieldEq('s', StringValueVariants('v2'))])), 1);
      expect(count(s, Filter(must: [fieldEq('n', IntegerValueVariants(3))])), 1);
      expect(count(s, Filter(must: [fieldEq('b', BoolValueVariants(true))])), 1);
    });

    test('AnyMatch and ExceptMatch', () {
      final s = filterShard();
      final any = count(
        s,
        Filter(
          must: [
            FieldConditionVariant(
              FieldCondition(
                key: 'lang',
                match: AnyMatch(StringsAnyVariants(['en'])),
              ),
            ),
          ],
        ),
      );
      expect(any, 2);
      final except = count(
        s,
        Filter(
          must: [
            FieldConditionVariant(
              FieldCondition(
                key: 'lang',
                match: ExceptMatch(StringsAnyVariants(['en'])),
              ),
            ),
          ],
        ),
      );
      expect(except, 2); // the two "fr" docs
    });

    test('RangeFloat and ValuesCount', () {
      final s = filterShard();
      final ranged = count(
        s,
        Filter(
          must: [
            FieldConditionVariant(
              FieldCondition(key: 'score', range: RangeFloat(gte: 30)),
            ),
          ],
        ),
      );
      expect(ranged, 2); // score 30 (id3), 40 (id4)
      final vc = count(
        s,
        Filter(
          must: [
            FieldConditionVariant(
              FieldCondition(key: 'tags', valuesCount: ValuesCount(gte: 2)),
            ),
          ],
        ),
      );
      expect(vc, 4); // every doc has 2 tags
    });

    test('IsEmpty / IsNull', () {
      final s = openShard(denseCfg());
      s.update(
        operation: UpdateOperation.upsertPoints(
          points: [
            Point(
              id: NumIdPointId(1),
              vector: SingleVector(axis(1)),
              payload: '{"arr":[],"nul":null}',
            ),
            Point(
              id: NumIdPointId(2),
              vector: SingleVector(axis(2)),
              payload: '{"arr":[1],"nul":"x"}',
            ),
          ],
        ),
      );
      // is_empty matches the empty array; is_null matches the explicit null.
      // A key that is absent entirely (not stored as null) matches neither.
      expect(count(s, Filter(must: [IsEmptyCondition('arr')])), 1); // id1 []
      expect(count(s, Filter(must: [IsNullCondition('nul')])), 1); // id1 null
      expect(count(s, Filter(must: [IsNullCondition('absent')])), 0);
    });

    test('HasId', () {
      final s = filterShard();
      expect(
        count(
          s,
          Filter(must: [HasIdCondition([NumIdPointId(1), NumIdPointId(3)])]),
        ),
        2,
      );
    });

    test('Nested filter and MinShould', () {
      final s = filterShard();
      final min = s.count(
        request: CountRequest(
          filter: Filter(
            minShould: MinShould(
              conditions: [
                fieldEq('lang', StringValueVariants('en')),
                fieldEq('lang', StringValueVariants('fr')),
              ],
              minCount: 1,
            ),
          ),
        ),
      );
      expect(min, 4); // every doc is en or fr
    });
  });

  // -------------------------------------------------------------------------
  group('config setters', () {
    test('setHnswConfig / setOptimizersConfig / setVectorHnswConfig', () {
      final s = openShard(denseCfg());
      seed(s, 2);
      s.setHnswConfig(hnswConfig: HnswIndexConfig(m: 8, efConstruct: 64));
      s.setOptimizersConfig(
        optimizers: OptimizersConfig(defaultSegmentNumber: 2),
      );
      s.setVectorHnswConfig(vectorName: '', hnswConfig: HnswIndexConfig(m: 12));
      // Still queryable after reconfig.
      expect(
        s.search(request: SearchRequest(query: nearest([1, 0, 0, 0]), limit: 1)),
        isNotEmpty,
      );
    });
  });

  // -------------------------------------------------------------------------
  group('snapshot', () {
    test('snapshotManifest returns a non-empty manifest', () {
      final s = openShard(denseCfg());
      seed(s, 2);
      s.flush();
      expect(s.snapshotManifest(), isNotEmpty);
    });

    test('updateFromSnapshot rejects a bogus path', () {
      final s = openShard(denseCfg());
      expect(
        () => s.updateFromSnapshot(snapshotPath: '/no/such/snapshot'),
        throwsA(isA<EdgeException>()),
      );
    });
  });
}
