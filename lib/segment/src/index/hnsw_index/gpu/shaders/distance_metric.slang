struct NoState {};

interface IDistanceMetric<T>
{
    associatedtype ScoreType;
    associatedtype State;

    static State initState();
    static ScoreType scoreElement(T a, T b, inout State state);
    static float postprocessScore(ScoreType accumulated, State state, uint pointId);

    // Called after setting the target vector. Default no-op.
    // Overridden by SQ (to load the target offset).
    static void onSetTarget(uint pointId) {}
}
