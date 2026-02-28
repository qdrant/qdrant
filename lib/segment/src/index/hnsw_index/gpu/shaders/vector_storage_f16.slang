import distance_metric;

struct CosineF16 : IDistanceMetric<half4>
{
    typedef float ScoreType;
    typedef NoState State;
    static NoState initState() { return {}; }
    static float scoreElement(half4 a, half4 b, inout NoState s) { return dot(a, b); }
    static float postprocessScore(float accumulated, NoState s, uint pointId) { return WaveActiveSum(accumulated); }
};

struct DotF16 : IDistanceMetric<half4>
{
    typedef float ScoreType;
    typedef NoState State;
    static NoState initState() { return {}; }
    static float scoreElement(half4 a, half4 b, inout NoState s) { return dot(a, b); }
    static float postprocessScore(float accumulated, NoState s, uint pointId) { return WaveActiveSum(accumulated); }
};

struct EuclidF16 : IDistanceMetric<half4>
{
    typedef float ScoreType;
    typedef NoState State;
    static NoState initState() { return {}; }
    static float scoreElement(half4 a, half4 b, inout NoState s)
    {
        half4 v = a - b;
        return float(-v.x * v.x - v.y * v.y - v.z * v.z - v.w * v.w);
    }
    static float postprocessScore(float accumulated, NoState s, uint pointId) { return WaveActiveSum(accumulated); }
};

struct ManhattanF16 : IDistanceMetric<half4>
{
    typedef float ScoreType;
    typedef NoState State;
    static NoState initState() { return {}; }
    static float scoreElement(half4 a, half4 b, inout NoState s)
    {
        half4 v = abs(a - b);
        return float(-v.x - v.y - v.z - v.w);
    }
    static float postprocessScore(float accumulated, NoState s, uint pointId) { return WaveActiveSum(accumulated); }
};
