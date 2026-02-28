import config;
import distance_metric;

[[vk::binding(VECTOR_STORAGE_SQ_OFFSETS_BINDING, VECTOR_STORAGE_LAYOUT_SET)]] StructuredBuffer<float> vector_offsets;

static float target_offset;

uint sq_dot(vector<uint8_t, 4> a, vector<uint8_t, 4> b) {
    uint4 sum = uint4(a) * uint4(b);
    return sum.x + sum.y + sum.z + sum.w;
}

uint sq_manhattan(vector<uint8_t, 4> a, vector<uint8_t, 4> b) {
    int4 diff = int4(a) - int4(b);
    return abs(diff.x) + abs(diff.y) + abs(diff.z) + abs(diff.w);
}

struct SQManhattan : IDistanceMetric<vector<uint8_t, 4> >
{
    typedef uint ScoreType;
    typedef NoState State;
    static NoState initState() { return {}; }
    static uint scoreElement(vector<uint8_t, 4> a, vector<uint8_t, 4> b, inout NoState s) { return sq_manhattan(a, b); }
    static float postprocessScore(uint accumulated, NoState s, uint pointId)
    {
        float sq_offset = target_offset + vector_offsets[pointId] - SQ_DIFF;
        return SQ_MULTIPLIER * float(WaveActiveSum(accumulated)) + sq_offset;
    }
    override static void onSetTarget(uint pointId) { target_offset = vector_offsets[pointId]; }
};

struct SQDot : IDistanceMetric<vector<uint8_t, 4> >
{
    typedef uint ScoreType;
    typedef NoState State;
    static NoState initState() { return {}; }
    static uint scoreElement(vector<uint8_t, 4> a, vector<uint8_t, 4> b, inout NoState s) { return sq_dot(a, b); }
    static float postprocessScore(uint accumulated, NoState s, uint pointId)
    {
        float sq_offset = target_offset + vector_offsets[pointId] - SQ_DIFF;
        return SQ_MULTIPLIER * float(WaveActiveSum(accumulated)) + sq_offset;
    }
    override static void onSetTarget(uint pointId) { target_offset = vector_offsets[pointId]; }
};
