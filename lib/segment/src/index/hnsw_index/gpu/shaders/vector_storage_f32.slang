import distance_metric;

struct CosineF32 : IDistanceMetric<float4>
{
    typedef float ScoreType;
    typedef NoState State;
    static NoState initState() { return {}; }
    static float scoreElement(float4 a, float4 b, inout NoState s) { return dot(a, b); }
    static float postprocessScore(float accumulated, NoState s, uint pointId) { return WaveActiveSum(accumulated); }
};

struct DotF32 : IDistanceMetric<float4>
{
    typedef float ScoreType;
    typedef NoState State;
    static NoState initState() { return {}; }
    static float scoreElement(float4 a, float4 b, inout NoState s) { return dot(a, b); }
    static float postprocessScore(float accumulated, NoState s, uint pointId) { return WaveActiveSum(accumulated); }
};

struct EuclidF32 : IDistanceMetric<float4>
{
    typedef float ScoreType;
    typedef NoState State;
    static NoState initState() { return {}; }
    static float scoreElement(float4 a, float4 b, inout NoState s)
    {
        float4 v = a - b;
        return -v.x * v.x - v.y * v.y - v.z * v.z - v.w * v.w;
    }
    static float postprocessScore(float accumulated, NoState s, uint pointId) { return WaveActiveSum(accumulated); }
};

struct ManhattanF32 : IDistanceMetric<float4>
{
    typedef float ScoreType;
    typedef NoState State;
    static NoState initState() { return {}; }
    static float scoreElement(float4 a, float4 b, inout NoState s)
    {
        float4 v = abs(a - b);
        return -v.x - v.y - v.z - v.w;
    }
    static float postprocessScore(float accumulated, NoState s, uint pointId) { return WaveActiveSum(accumulated); }
};
