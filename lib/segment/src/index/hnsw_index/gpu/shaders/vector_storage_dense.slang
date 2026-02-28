import config;
import common;
import distance_metric;
import vector_storage_f32;
import vector_storage_f16;
import vector_storage_u8;
import vector_storage_bq;
import vector_storage_sq;
import vector_storage_pq;

// ============================================================
// Buffer access: BDA (64-bit pointer) vs ByteAddressBuffer (32-bit offset)
// ============================================================

#ifdef USE_BDA

// BDA mode: storage buffer with 4 device addresses for pointer-based access.
struct VectorAddresses { uint64_t addr[4]; };
[[vk::binding(0, VECTOR_STORAGE_LAYOUT_SET)]] StructuredBuffer<VectorAddresses> vector_addrs;

float4 loadElement_f32(uint storageIndex, uint elementIndex) {
    float4* ptr = (float4*)(vector_addrs[0].addr[storageIndex]);
    return ptr[elementIndex];
}

half4 loadElement_f16(uint storageIndex, uint elementIndex) {
    half4* ptr = (half4*)(vector_addrs[0].addr[storageIndex]);
    return ptr[elementIndex];
}

vector<uint8_t, 4> loadElement_u8(uint storageIndex, uint elementIndex) {
    vector<uint8_t, 4>* ptr = (vector<uint8_t, 4>*)(vector_addrs[0].addr[storageIndex]);
    return ptr[elementIndex];
}

uint loadElement_bq(uint storageIndex, uint elementIndex) {
    uint* ptr = (uint*)(vector_addrs[0].addr[storageIndex]);
    return ptr[elementIndex];
}

#else

// Non-BDA mode: ByteAddressBuffer bindings with 32-bit byte offsets.
[[vk::binding(0, VECTOR_STORAGE_LAYOUT_SET)]] ByteAddressBuffer vectors_0;
[[vk::binding(1, VECTOR_STORAGE_LAYOUT_SET)]] ByteAddressBuffer vectors_1;
[[vk::binding(2, VECTOR_STORAGE_LAYOUT_SET)]] ByteAddressBuffer vectors_2;
[[vk::binding(3, VECTOR_STORAGE_LAYOUT_SET)]] ByteAddressBuffer vectors_3;

float4 loadElement_f32(uint storageIndex, uint elementIndex) {
    uint byteOffset = elementIndex * 16;
    if (storageIndex == 0) return vectors_0.Load<float4>(byteOffset);
    if (storageIndex == 1) return vectors_1.Load<float4>(byteOffset);
    if (storageIndex == 2) return vectors_2.Load<float4>(byteOffset);
    return vectors_3.Load<float4>(byteOffset);
}

half4 loadElement_f16(uint storageIndex, uint elementIndex) {
    uint byteOffset = elementIndex * 8;
    if (storageIndex == 0) return vectors_0.Load<half4>(byteOffset);
    if (storageIndex == 1) return vectors_1.Load<half4>(byteOffset);
    if (storageIndex == 2) return vectors_2.Load<half4>(byteOffset);
    return vectors_3.Load<half4>(byteOffset);
}

vector<uint8_t, 4> loadElement_u8(uint storageIndex, uint elementIndex) {
    uint byteOffset = elementIndex * 4;
    if (storageIndex == 0) return vectors_0.Load<vector<uint8_t, 4>>(byteOffset);
    if (storageIndex == 1) return vectors_1.Load<vector<uint8_t, 4>>(byteOffset);
    if (storageIndex == 2) return vectors_2.Load<vector<uint8_t, 4>>(byteOffset);
    return vectors_3.Load<vector<uint8_t, 4>>(byteOffset);
}

uint loadElement_bq(uint storageIndex, uint elementIndex) {
    uint byteOffset = elementIndex * 4;
    if (storageIndex == 0) return vectors_0.Load<uint>(byteOffset);
    if (storageIndex == 1) return vectors_1.Load<uint>(byteOffset);
    if (storageIndex == 2) return vectors_2.Load<uint>(byteOffset);
    return vectors_3.Load<uint>(byteOffset);
}

#endif

static const uint TARGET_CACHE_SIZE = SINGLE_WAVE_PER_VECTOR ? 1 : WAVES_PER_VECTOR;

// Per-element-type target caches (only one is active per compilation, rest dead-code eliminated)
static float4 target_cache_f32[TARGET_CACHE_SIZE];
static half4 target_cache_f16[TARGET_CACHE_SIZE];
static vector<uint8_t, 4> target_cache_u8[TARGET_CACHE_SIZE];
static uint target_cache_bq[TARGET_CACHE_SIZE];

// ============================================================
// set_target helpers per element type
// ============================================================

void _setTarget_f32(uint point_id) {
    uint index = (point_id / STORAGES_COUNT) * (DIM / PACK_SIZE) + WaveGetLaneIndex();
    uint si = point_id % STORAGES_COUNT;
    if (SINGLE_WAVE_PER_VECTOR) {
        target_cache_f32[0] = loadElement_f32(si, index);
    } else {
        for (uint i = 0; i < WAVES_PER_VECTOR; i++, index += WAVE_SIZE)
            target_cache_f32[i] = loadElement_f32(si, index);
    }
}

void _setTarget_f16(uint point_id) {
    uint index = (point_id / STORAGES_COUNT) * (DIM / PACK_SIZE) + WaveGetLaneIndex();
    uint si = point_id % STORAGES_COUNT;
    if (SINGLE_WAVE_PER_VECTOR) {
        target_cache_f16[0] = loadElement_f16(si, index);
    } else {
        for (uint i = 0; i < WAVES_PER_VECTOR; i++, index += WAVE_SIZE)
            target_cache_f16[i] = loadElement_f16(si, index);
    }
}

void _setTarget_u8(uint point_id) {
    uint index = (point_id / STORAGES_COUNT) * (DIM / PACK_SIZE) + WaveGetLaneIndex();
    uint si = point_id % STORAGES_COUNT;
    if (SINGLE_WAVE_PER_VECTOR) {
        target_cache_u8[0] = loadElement_u8(si, index);
    } else {
        for (uint i = 0; i < WAVES_PER_VECTOR; i++, index += WAVE_SIZE)
            target_cache_u8[i] = loadElement_u8(si, index);
    }
}

void _setTarget_bq(uint point_id) {
    uint index = (point_id / STORAGES_COUNT) * (DIM / PACK_SIZE) + WaveGetLaneIndex();
    uint si = point_id % STORAGES_COUNT;
    if (SINGLE_WAVE_PER_VECTOR) {
        target_cache_bq[0] = loadElement_bq(si, index);
    } else {
        for (uint i = 0; i < WAVES_PER_VECTOR; i++, index += WAVE_SIZE)
            target_cache_bq[i] = loadElement_bq(si, index);
    }
}

// ============================================================
// similarity helpers per metric
// ============================================================

// --- F32 metrics ---

float _similarity_cosine_f32(uint point_id) {
    CosineF32.State state = CosineF32.initState();
    float result = 0;
    uint index = (point_id / STORAGES_COUNT) * (DIM / PACK_SIZE) + WaveGetLaneIndex();
    uint si = point_id % STORAGES_COUNT;
    if (SINGLE_WAVE_PER_VECTOR) {
        result += CosineF32.scoreElement(loadElement_f32(si, index), target_cache_f32[0], state);
    } else {
        for (uint i = 0; i < WAVES_PER_VECTOR; i++, index += WAVE_SIZE)
            result += CosineF32.scoreElement(loadElement_f32(si, index), target_cache_f32[i], state);
    }
    return CosineF32.postprocessScore(result, state, point_id);
}

float _similarity_dot_f32(uint point_id) {
    DotF32.State state = DotF32.initState();
    float result = 0;
    uint index = (point_id / STORAGES_COUNT) * (DIM / PACK_SIZE) + WaveGetLaneIndex();
    uint si = point_id % STORAGES_COUNT;
    if (SINGLE_WAVE_PER_VECTOR) {
        result += DotF32.scoreElement(loadElement_f32(si, index), target_cache_f32[0], state);
    } else {
        for (uint i = 0; i < WAVES_PER_VECTOR; i++, index += WAVE_SIZE)
            result += DotF32.scoreElement(loadElement_f32(si, index), target_cache_f32[i], state);
    }
    return DotF32.postprocessScore(result, state, point_id);
}

float _similarity_euclid_f32(uint point_id) {
    EuclidF32.State state = EuclidF32.initState();
    float result = 0;
    uint index = (point_id / STORAGES_COUNT) * (DIM / PACK_SIZE) + WaveGetLaneIndex();
    uint si = point_id % STORAGES_COUNT;
    if (SINGLE_WAVE_PER_VECTOR) {
        result += EuclidF32.scoreElement(loadElement_f32(si, index), target_cache_f32[0], state);
    } else {
        for (uint i = 0; i < WAVES_PER_VECTOR; i++, index += WAVE_SIZE)
            result += EuclidF32.scoreElement(loadElement_f32(si, index), target_cache_f32[i], state);
    }
    return EuclidF32.postprocessScore(result, state, point_id);
}

float _similarity_manhattan_f32(uint point_id) {
    ManhattanF32.State state = ManhattanF32.initState();
    float result = 0;
    uint index = (point_id / STORAGES_COUNT) * (DIM / PACK_SIZE) + WaveGetLaneIndex();
    uint si = point_id % STORAGES_COUNT;
    if (SINGLE_WAVE_PER_VECTOR) {
        result += ManhattanF32.scoreElement(loadElement_f32(si, index), target_cache_f32[0], state);
    } else {
        for (uint i = 0; i < WAVES_PER_VECTOR; i++, index += WAVE_SIZE)
            result += ManhattanF32.scoreElement(loadElement_f32(si, index), target_cache_f32[i], state);
    }
    return ManhattanF32.postprocessScore(result, state, point_id);
}

// --- F16 metrics ---

float _similarity_cosine_f16(uint point_id) {
    CosineF16.State state = CosineF16.initState();
    float result = 0;
    uint index = (point_id / STORAGES_COUNT) * (DIM / PACK_SIZE) + WaveGetLaneIndex();
    uint si = point_id % STORAGES_COUNT;
    if (SINGLE_WAVE_PER_VECTOR) {
        result += CosineF16.scoreElement(loadElement_f16(si, index), target_cache_f16[0], state);
    } else {
        for (uint i = 0; i < WAVES_PER_VECTOR; i++, index += WAVE_SIZE)
            result += CosineF16.scoreElement(loadElement_f16(si, index), target_cache_f16[i], state);
    }
    return CosineF16.postprocessScore(result, state, point_id);
}

float _similarity_dot_f16(uint point_id) {
    DotF16.State state = DotF16.initState();
    float result = 0;
    uint index = (point_id / STORAGES_COUNT) * (DIM / PACK_SIZE) + WaveGetLaneIndex();
    uint si = point_id % STORAGES_COUNT;
    if (SINGLE_WAVE_PER_VECTOR) {
        result += DotF16.scoreElement(loadElement_f16(si, index), target_cache_f16[0], state);
    } else {
        for (uint i = 0; i < WAVES_PER_VECTOR; i++, index += WAVE_SIZE)
            result += DotF16.scoreElement(loadElement_f16(si, index), target_cache_f16[i], state);
    }
    return DotF16.postprocessScore(result, state, point_id);
}

float _similarity_euclid_f16(uint point_id) {
    EuclidF16.State state = EuclidF16.initState();
    float result = 0;
    uint index = (point_id / STORAGES_COUNT) * (DIM / PACK_SIZE) + WaveGetLaneIndex();
    uint si = point_id % STORAGES_COUNT;
    if (SINGLE_WAVE_PER_VECTOR) {
        result += EuclidF16.scoreElement(loadElement_f16(si, index), target_cache_f16[0], state);
    } else {
        for (uint i = 0; i < WAVES_PER_VECTOR; i++, index += WAVE_SIZE)
            result += EuclidF16.scoreElement(loadElement_f16(si, index), target_cache_f16[i], state);
    }
    return EuclidF16.postprocessScore(result, state, point_id);
}

float _similarity_manhattan_f16(uint point_id) {
    ManhattanF16.State state = ManhattanF16.initState();
    float result = 0;
    uint index = (point_id / STORAGES_COUNT) * (DIM / PACK_SIZE) + WaveGetLaneIndex();
    uint si = point_id % STORAGES_COUNT;
    if (SINGLE_WAVE_PER_VECTOR) {
        result += ManhattanF16.scoreElement(loadElement_f16(si, index), target_cache_f16[0], state);
    } else {
        for (uint i = 0; i < WAVES_PER_VECTOR; i++, index += WAVE_SIZE)
            result += ManhattanF16.scoreElement(loadElement_f16(si, index), target_cache_f16[i], state);
    }
    return ManhattanF16.postprocessScore(result, state, point_id);
}

// --- U8 metrics ---

float _similarity_cosine_u8(uint point_id) {
    CosineU8.State state = CosineU8.initState();
    uint result = 0;
    uint index = (point_id / STORAGES_COUNT) * (DIM / PACK_SIZE) + WaveGetLaneIndex();
    uint si = point_id % STORAGES_COUNT;
    if (SINGLE_WAVE_PER_VECTOR) {
        result += CosineU8.scoreElement(loadElement_u8(si, index), target_cache_u8[0], state);
    } else {
        for (uint i = 0; i < WAVES_PER_VECTOR; i++, index += WAVE_SIZE)
            result += CosineU8.scoreElement(loadElement_u8(si, index), target_cache_u8[i], state);
    }
    return CosineU8.postprocessScore(result, state, point_id);
}

float _similarity_dot_u8(uint point_id) {
    DotU8.State state = DotU8.initState();
    uint result = 0;
    uint index = (point_id / STORAGES_COUNT) * (DIM / PACK_SIZE) + WaveGetLaneIndex();
    uint si = point_id % STORAGES_COUNT;
    if (SINGLE_WAVE_PER_VECTOR) {
        result += DotU8.scoreElement(loadElement_u8(si, index), target_cache_u8[0], state);
    } else {
        for (uint i = 0; i < WAVES_PER_VECTOR; i++, index += WAVE_SIZE)
            result += DotU8.scoreElement(loadElement_u8(si, index), target_cache_u8[i], state);
    }
    return DotU8.postprocessScore(result, state, point_id);
}

float _similarity_euclid_u8(uint point_id) {
    EuclidU8.State state = EuclidU8.initState();
    uint result = 0;
    uint index = (point_id / STORAGES_COUNT) * (DIM / PACK_SIZE) + WaveGetLaneIndex();
    uint si = point_id % STORAGES_COUNT;
    if (SINGLE_WAVE_PER_VECTOR) {
        result += EuclidU8.scoreElement(loadElement_u8(si, index), target_cache_u8[0], state);
    } else {
        for (uint i = 0; i < WAVES_PER_VECTOR; i++, index += WAVE_SIZE)
            result += EuclidU8.scoreElement(loadElement_u8(si, index), target_cache_u8[i], state);
    }
    return EuclidU8.postprocessScore(result, state, point_id);
}

float _similarity_manhattan_u8(uint point_id) {
    ManhattanU8.State state = ManhattanU8.initState();
    uint result = 0;
    uint index = (point_id / STORAGES_COUNT) * (DIM / PACK_SIZE) + WaveGetLaneIndex();
    uint si = point_id % STORAGES_COUNT;
    if (SINGLE_WAVE_PER_VECTOR) {
        result += ManhattanU8.scoreElement(loadElement_u8(si, index), target_cache_u8[0], state);
    } else {
        for (uint i = 0; i < WAVES_PER_VECTOR; i++, index += WAVE_SIZE)
            result += ManhattanU8.scoreElement(loadElement_u8(si, index), target_cache_u8[i], state);
    }
    return ManhattanU8.postprocessScore(result, state, point_id);
}

// --- BQ metric ---

float _similarity_bq(uint point_id) {
    BQMetric.State state = BQMetric.initState();
    uint result = 0;
    uint index = (point_id / STORAGES_COUNT) * (DIM / PACK_SIZE) + WaveGetLaneIndex();
    uint si = point_id % STORAGES_COUNT;
    if (SINGLE_WAVE_PER_VECTOR) {
        result += BQMetric.scoreElement(loadElement_bq(si, index), target_cache_bq[0], state);
    } else {
        for (uint i = 0; i < WAVES_PER_VECTOR; i++, index += WAVE_SIZE)
            result += BQMetric.scoreElement(loadElement_bq(si, index), target_cache_bq[i], state);
    }
    return BQMetric.postprocessScore(result, state, point_id);
}

// --- SQ metrics ---

float _similarity_sq_manhattan(uint point_id) {
    SQManhattan.State state = SQManhattan.initState();
    uint result = 0;
    uint index = (point_id / STORAGES_COUNT) * (DIM / PACK_SIZE) + WaveGetLaneIndex();
    uint si = point_id % STORAGES_COUNT;
    if (SINGLE_WAVE_PER_VECTOR) {
        result += SQManhattan.scoreElement(loadElement_u8(si, index), target_cache_u8[0], state);
    } else {
        for (uint i = 0; i < WAVES_PER_VECTOR; i++, index += WAVE_SIZE)
            result += SQManhattan.scoreElement(loadElement_u8(si, index), target_cache_u8[i], state);
    }
    return SQManhattan.postprocessScore(result, state, point_id);
}

float _similarity_sq_dot(uint point_id) {
    SQDot.State state = SQDot.initState();
    uint result = 0;
    uint index = (point_id / STORAGES_COUNT) * (DIM / PACK_SIZE) + WaveGetLaneIndex();
    uint si = point_id % STORAGES_COUNT;
    if (SINGLE_WAVE_PER_VECTOR) {
        result += SQDot.scoreElement(loadElement_u8(si, index), target_cache_u8[0], state);
    } else {
        for (uint i = 0; i < WAVES_PER_VECTOR; i++, index += WAVE_SIZE)
            result += SQDot.scoreElement(loadElement_u8(si, index), target_cache_u8[i], state);
    }
    return SQDot.postprocessScore(result, state, point_id);
}

// --- PQ metric ---

float _similarity_pq(uint point_id) {
    PQMetric.State state = PQMetric.initState();
    float result = 0;
    uint index = (point_id / STORAGES_COUNT) * (DIM / PACK_SIZE) + WaveGetLaneIndex();
    uint si = point_id % STORAGES_COUNT;
    if (SINGLE_WAVE_PER_VECTOR) {
        result += PQMetric.scoreElement(loadElement_u8(si, index), target_cache_u8[0], state);
    } else {
        for (uint i = 0; i < WAVES_PER_VECTOR; i++, index += WAVE_SIZE)
            result += PQMetric.scoreElement(loadElement_u8(si, index), target_cache_u8[i], state);
    }
    return PQMetric.postprocessScore(result, state, point_id);
}

// ============================================================
// Dispatch tables
// ============================================================

void set_target_dense(uint point_id) {
    if (VECTOR_STORAGE_ELEMENT_BQ) {
        _setTarget_bq(point_id);
    } else if (VECTOR_STORAGE_ELEMENT_SQ) {
        _setTarget_u8(point_id);
        if (MANHATTAN_DISTANCE) SQManhattan.onSetTarget(point_id);
        else                    SQDot.onSetTarget(point_id);
    } else if (VECTOR_STORAGE_ELEMENT_PQ) {
        _setTarget_u8(point_id);
    } else if (VECTOR_STORAGE_ELEMENT_FLOAT16) {
        _setTarget_f16(point_id);
    } else if (VECTOR_STORAGE_ELEMENT_UINT8) {
        _setTarget_u8(point_id);
    } else {
        _setTarget_f32(point_id);
    }
}

float similarity_dense(uint point_id) {
    if (VECTOR_STORAGE_ELEMENT_BQ) {
        return _similarity_bq(point_id);
    } else if (VECTOR_STORAGE_ELEMENT_SQ) {
        if (MANHATTAN_DISTANCE) return _similarity_sq_manhattan(point_id);
        else                    return _similarity_sq_dot(point_id);
    } else if (VECTOR_STORAGE_ELEMENT_PQ) {
        return _similarity_pq(point_id);
    } else if (VECTOR_STORAGE_ELEMENT_FLOAT16) {
        if (COSINE_DISTANCE)      return _similarity_cosine_f16(point_id);
        else if (DOT_DISTANCE)    return _similarity_dot_f16(point_id);
        else if (EUCLID_DISTANCE) return _similarity_euclid_f16(point_id);
        else                      return _similarity_manhattan_f16(point_id);
    } else if (VECTOR_STORAGE_ELEMENT_UINT8) {
        if (COSINE_DISTANCE)      return _similarity_cosine_u8(point_id);
        else if (DOT_DISTANCE)    return _similarity_dot_u8(point_id);
        else if (EUCLID_DISTANCE) return _similarity_euclid_u8(point_id);
        else                      return _similarity_manhattan_u8(point_id);
    } else {
        if (COSINE_DISTANCE)      return _similarity_cosine_f32(point_id);
        else if (DOT_DISTANCE)    return _similarity_dot_f32(point_id);
        else if (EUCLID_DISTANCE) return _similarity_euclid_f32(point_id);
        else                      return _similarity_manhattan_f32(point_id);
    }
}
