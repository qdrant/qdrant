import config;
import common;

struct VisitedFlagsParamsStruct {
    uint generation;
};

[[vk::binding(0, VISITED_FLAGS_LAYOUT_SET)]] ConstantBuffer<VisitedFlagsParamsStruct> visited_flags_params;
[[vk::binding(1, VISITED_FLAGS_LAYOUT_SET)]] RWStructuredBuffer<uint8_t> visited_flags;
[[vk::binding(2, VISITED_FLAGS_LAYOUT_SET)]] StructuredBuffer<POINT_ID> visited_flags_remap;

bool check_visited(POINT_ID point_id) {
    if (VISITED_FLAGS_REMAP) {
        point_id = visited_flags_remap[point_id];
    }
    uint wave_index = waveId();
    uint index = wave_index * VISITED_FLAGS_CAPACITY + point_id % VISITED_FLAGS_CAPACITY;
    uint prev_generation = uint(visited_flags[index]);
    if (prev_generation == visited_flags_params.generation) {
        return true;
    } else {
        visited_flags[index] = uint8_t(visited_flags_params.generation);
        return false;
    }
}
