import config;
import common;
import metric_config;

[[vk::binding(0, VECTOR_STORAGE_LAYOUT_SET)]] StructuredBuffer<ElementType> vectors_0;
[[vk::binding(1, VECTOR_STORAGE_LAYOUT_SET)]] StructuredBuffer<ElementType> vectors_1;
[[vk::binding(2, VECTOR_STORAGE_LAYOUT_SET)]] StructuredBuffer<ElementType> vectors_2;
[[vk::binding(3, VECTOR_STORAGE_LAYOUT_SET)]] StructuredBuffer<ElementType> vectors_3;

ElementType loadElement(uint storageIndex, uint elementIndex) {
    switch (storageIndex) {
    case 0: return vectors_0[elementIndex];
    case 1: return vectors_1[elementIndex];
    case 2: return vectors_2[elementIndex];
    case 3: return vectors_3[elementIndex];
    default: return vectors_0[elementIndex];
    }
}

static const uint TARGET_CACHE_SIZE = SINGLE_SUBGROUP_PER_VECTOR ? 1 : SUBGROUPS_COUNT_PER_VECTOR;
static ElementType target_cache[TARGET_CACHE_SIZE];

void set_target_dense(uint point_id) {
    uint index = (point_id / STORAGES_COUNT) * (DIM / ELEMENTS_PER_SUBGROUP) + WaveGetLaneIndex();
    uint storageIndex = point_id % STORAGES_COUNT;
    if (SINGLE_SUBGROUP_PER_VECTOR) {
        target_cache[0] = loadElement(storageIndex, index);
    } else {
        for (uint i = 0; i < SUBGROUPS_COUNT_PER_VECTOR; i++, index += SUBGROUP_SIZE) {
            target_cache[i] = loadElement(storageIndex, index);
        }
    }
    ActiveMetric.onSetTarget(point_id);
}

float similarity_dense(uint point_id) {
    ActiveMetric.State state = ActiveMetric.initState();
    ScoreType result = 0;
    uint index = (point_id / STORAGES_COUNT) * (DIM / ELEMENTS_PER_SUBGROUP) + WaveGetLaneIndex();
    uint storageIndex = point_id % STORAGES_COUNT;
    if (SINGLE_SUBGROUP_PER_VECTOR) {
        result += ActiveMetric.scoreElement(loadElement(storageIndex, index), target_cache[0], state);
    } else {
        for (uint i = 0; i < SUBGROUPS_COUNT_PER_VECTOR; i++, index += SUBGROUP_SIZE) {
            result += ActiveMetric.scoreElement(loadElement(storageIndex, index), target_cache[i], state);
        }
    }
    return ActiveMetric.postprocessScore(result, state, point_id);
}
