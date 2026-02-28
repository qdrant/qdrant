import config;
import distance_metric;

[[vk::binding(PQ_CENTROIDS_BINDING, VECTOR_STORAGE_LAYOUT_SET)]] StructuredBuffer<float> pq_centroids;
[[vk::binding(PQ_DIVISIONS_BINDING, VECTOR_STORAGE_LAYOUT_SET)]] StructuredBuffer<uint2> pq_divisions;

struct PQRange {
    uint start;
    uint end;
};

PQRange pq_load_division(uint index) {
    uint2 d = pq_divisions[index];
    PQRange r;
    r.start = d.x;
    r.end = d.y;
    return r;
}

float score_pq(vector<uint8_t, 4> a, vector<uint8_t, 4> b, uint i) {
    float score = 0.0;
    uint index = i * 4;
    uint max_index = min(index + 4, PQ_DIVISIONS_COUNT);
    for (; index < max_index; index++) {
        uint c_a = uint(a[index % 4]) * PQ_CENTROIDS_DIM;
        uint c_b = uint(b[index % 4]) * PQ_CENTROIDS_DIM;
        PQRange range = pq_load_division(index);
        for (uint j = range.start; j < range.end; j++) {
            float data_a = pq_centroids[c_a + j];
            float data_b = pq_centroids[c_b + j];

            if (EUCLID_DISTANCE) {
                score -= (data_a - data_b) * (data_a - data_b);
            } else if (MANHATTAN_DISTANCE) {
                score -= abs(data_a - data_b);
            } else {
                // Cosine and Dot use the same formula.
                score += data_a * data_b;
            }
        }
    }
    return score;
}

struct PQState
{
    uint elementIndex;
};

struct PQMetric : IDistanceMetric<vector<uint8_t, 4> >
{
    typedef float ScoreType;
    typedef PQState State;

    static PQState initState()
    {
        if (SINGLE_WAVE_PER_VECTOR) {
            return { WaveGetLaneIndex() };
        } else {
            return { 0 };
        }
    }

    static float scoreElement(vector<uint8_t, 4> a, vector<uint8_t, 4> b, inout PQState state)
    {
        float result = score_pq(a, b, state.elementIndex);
        if (!SINGLE_WAVE_PER_VECTOR) {
            state.elementIndex += WAVE_SIZE;
        }
        return result;
    }

    static float postprocessScore(float accumulated, PQState state, uint pointId) { return WaveActiveSum(accumulated); }
};
