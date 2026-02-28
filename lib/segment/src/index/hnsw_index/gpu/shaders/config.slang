// Default config for IDE support. Overridden by Rust at compile time.

// Hardware
static const uint WAVE_SIZE = 32;

// Dimensions
static const uint DIM = 128;
static const uint STORAGES_COUNT = 4;
static const uint PACK_SIZE = 4;
static const uint WAVES_PER_VECTOR = 1;
static const bool SINGLE_WAVE_PER_VECTOR = true;

// Search
static const uint EF = 128;
static const uint SHARED_BUFFER_CAPACITY = 256;
static const uint NEAREST_HEAP_OFFSET = 0;
static const uint CANDIDATES_HEAP_OFFSET = 128;

// Binding layout set indices
static const uint VECTOR_STORAGE_LAYOUT_SET = 1;
static const uint LINKS_LAYOUT_SET = 2;
static const uint VISITED_FLAGS_LAYOUT_SET = 3;

// Feature flags
static const bool EXACT = false;
static const bool BHEAP_LINEAR = false;

// Distance metric flags
static const bool COSINE_DISTANCE = true;
static const bool DOT_DISTANCE = false;
static const bool EUCLID_DISTANCE = false;
static const bool MANHATTAN_DISTANCE = false;

// Element type flags
static const bool VECTOR_STORAGE_ELEMENT_FLOAT32 = true;
static const bool VECTOR_STORAGE_ELEMENT_FLOAT16 = false;
static const bool VECTOR_STORAGE_ELEMENT_UINT8 = false;
static const bool VECTOR_STORAGE_ELEMENT_BQ = false;
static const bool VECTOR_STORAGE_ELEMENT_SQ = false;
static const bool VECTOR_STORAGE_ELEMENT_PQ = false;
static const bool VECTOR_STORAGE_QUANTIZATION = false;

// Links
static const uint LINKS_CAPACITY = 32;

// Visited flags
static const uint VISITED_FLAGS_CAPACITY = 1024;
static const bool VISITED_FLAGS_REMAP = false;

// Multivectors
static const bool MULTIVECTORS = false;
static const uint MULTIVECTOR_OFFSETS_BINDING = 6;

// Binary quantization
static const uint BQ_SKIP_COUNT = 0;

// Scalar quantization
static const float SQ_MULTIPLIER = 1.0;
static const float SQ_DIFF = 0.0;
static const uint VECTOR_STORAGE_SQ_OFFSETS_BINDING = 4;

// Product quantization
static const uint VECTOR_STORAGE_PQ_CENTROIDS_BINDING = 4;
static const uint VECTOR_STORAGE_PQ_DIVISIONS_BINDING = 5;
static const uint PQ_DIVISIONS_COUNT = 0;
static const uint PQ_CENTROIDS_DIM = 0;
