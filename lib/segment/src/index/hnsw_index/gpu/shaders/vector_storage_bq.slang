import config;
import distance_metric;

static const uint BITS_IN_UINT = 32;

// BQ uses the same XOR + popcount scoring for all distance metrics.
struct BQMetric : IDistanceMetric<uint>
{
    typedef uint ScoreType;
    typedef NoState State;
    static NoState initState() { return {}; }
    static uint scoreElement(uint a, uint b, inout NoState s) { return countbits(a ^ b); }
    static float postprocessScore(uint accumulated, NoState s, uint pointId)
    {
        float xor_product = float(WaveActiveSum(accumulated));
        float zeros_count = float((DIM / PACK_SIZE) * BITS_IN_UINT) - xor_product;
        return zeros_count - xor_product - BQ_SKIP_COUNT;
    }
};
