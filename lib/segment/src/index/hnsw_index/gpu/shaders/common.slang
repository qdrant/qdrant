import config;
import distance_metric;

// Global thread ID, set by entry point at the start of main().
static uint3 gl_GlobalInvocationID;

uint subgroupId() { return gl_GlobalInvocationID.x / SUBGROUP_SIZE; }

static const float positive_infinity = 1.0 / 0.0;
static const float negative_infinity = -1.0 / 0.0;
static const uint UINT_MAX_VALUE = 0xffffffff;

typedef uint POINT_ID;
static const POINT_ID INVALID_POINT_ID = UINT_MAX_VALUE;

struct ScoredPoint {
    POINT_ID id;
    float score;
};

// Helper for subgroupBallotFindLSB equivalent.
// Finds the index of the lowest set bit in a ballot result.
uint ballotFindLSB(uint4 ballot) {
    if (ballot.x != 0) return firstbitlow(ballot.x);
    if (ballot.y != 0) return 32 + firstbitlow(ballot.y);
    if (ballot.z != 0) return 64 + firstbitlow(ballot.z);
    return 96 + firstbitlow(ballot.w);
}
