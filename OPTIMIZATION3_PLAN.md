# Implementation Plan: Exa's Dot-Product Optimization for Binary Quantization

## Overview

This plan implements **Optimization 3: Dot-product optimization** from Exa's blog post (https://exa.ai/blog/building-web-scale-vector-db). The optimization uses uncompressed floating-point query vectors with binary-quantized document vectors and leverages lookup tables for subvectors to speed up dot product computation.

## Key Concept

**Current Approach:**
- Binary document vectors (values are -1 or 1, stored as bits)
- Binary or scalar-quantized query vectors
- Uses XOR-based calculations (approximates dot product)

**Exa's Optimization:**
- Binary document vectors (unchanged)
- **Uncompressed floating-point query vectors** (new)
- **True dot product computation** (not XOR approximation)
- **Lookup tables for subvectors** (e.g., length 4 or 8) to reduce computations by 1/4 or 1/8

**The Trick:**
1. Divide query and document vectors into subvectors of length `m` (e.g., 4 or 8)
2. For each query subvector, precompute dot products with all 2^m possible binary subvectors
3. Store these in a lookup table (16 entries for m=4, 256 for m=8)
4. During scoring: extract each document subvector (which can only be one of 2^m values), look up its dot product, and sum

## Implementation Plan

### Phase 1: Add Uncompressed Query Encoding Option

#### 1.1 Update `QueryEncoding` enum
**File:** `lib/quantization/src/encoded_vectors_binary.rs`

- Add `Uncompressed` variant to `QueryEncoding` enum (line ~45)
- Update `is_same_as_storage()` method if needed

#### 1.2 Update `EncodedQueryBQ` enum
**File:** `lib/quantization/src/encoded_vectors_binary.rs`

- Add `Uncompressed(Vec<f32>)` variant to `EncodedQueryBQ` enum (line ~58)
- This stores the raw floating-point query vector

#### 1.3 Update `BinaryQuantizationQueryEncoding` enum
**File:** `lib/segment/src/types.rs`

- Add `Uncompressed` or `Float` variant to `BinaryQuantizationQueryEncoding` enum (line ~886)
- This is the public API enum

#### 1.4 Update Proto definitions
**File:** `lib/api/src/grpc/proto/collections.proto`

- Add `Uncompressed = 4` to `BinaryQuantizationQueryEncoding.Setting` enum (line ~340)

#### 1.5 Update conversion functions
**Files:**
- `lib/segment/src/vector_storage/quantized/quantized_vectors.rs` (line ~1798)
- `lib/api/src/grpc/conversions.rs` (line ~1388, ~1422)

- Add conversion from `BinaryQuantizationQueryEncoding::Uncompressed` to `QueryEncoding::Uncompressed`

### Phase 2: Implement Lookup Table Generation

#### 2.1 Create lookup table structure
**File:** `lib/quantization/src/encoded_vectors_binary.rs`

- Create a new struct `DotProductLookupTable` that stores:
  - Subvector length `m` (typically 4 or 8)
  - Lookup table: `Vec<f32>` of size 2^m
  - Query subvectors: `Vec<[f32; m]>` (or similar structure)

#### 2.2 Implement lookup table generation
**File:** `lib/quantization/src/encoded_vectors_binary.rs`

- Create function `build_dot_product_lookup_table(query: &[f32], subvector_len: usize) -> DotProductLookupTable`
- For each query subvector of length `m`:
  - Precompute dot products with all 2^m possible binary subvectors
  - Binary subvector `[b0, b1, b2, b3]` where each `bi` is -1 or 1
  - Store: `lut[pattern] = query_subvec[0]*b0 + query_subvec[1]*b1 + ...`
  - Pattern is encoded as: `(b0==1?1:0) | (b1==1?2:0) | (b2==1?4:0) | (b3==1?8:0)`

**Example for m=4:**
```rust
// Query subvector: [0.5, -0.3, 0.8, 0.1]
// Generate all 16 possible binary subvectors and compute dot products:
// [1, 1, 1, 1] -> 0.5 + (-0.3) + 0.8 + 0.1 = 1.1
// [1, 1, 1, -1] -> 0.5 + (-0.3) + 0.8 + (-0.1) = 0.9
// ... (all 16 combinations)
```

#### 2.3 Store lookup table in encoded query
**File:** `lib/quantization/src/encoded_vectors_binary.rs`

- Update `EncodedQueryBQ::Uncompressed` to include:
  ```rust
  Uncompressed {
      query_vector: Vec<f32>,
      lookup_table: DotProductLookupTable,
  }
  ```

### Phase 3: Implement Dot Product Calculation with Lookup Tables

#### 3.1 Create dot product calculation function
**File:** `lib/quantization/src/encoded_vectors_binary.rs`

- Create function `calculate_dot_product_with_lut(
    binary_vector: &[TBitsStoreType],
    lookup_table: &DotProductLookupTable,
    dim: usize
) -> f32`
- Algorithm:
  1. Divide binary vector into subvectors of length `m`
  2. For each subvector:
     - Extract the bit pattern (convert bits to index 0..2^m)
     - Look up dot product from lookup table
     - Add to running sum
  3. Return total dot product

#### 3.2 Handle vector dimension alignment
- Handle cases where `dim % subvector_len != 0`
- Process remainder elements separately (direct dot product, no lookup)

#### 3.3 Update `encode_query` method
**File:** `lib/quantization/src/encoded_vectors_binary.rs` (line ~839)

- Add case for `QueryEncoding::Uncompressed`:
  - Keep query as `Vec<f32>`
  - Build lookup table
  - Return `EncodedQueryBQ::Uncompressed { query_vector, lookup_table }`

### Phase 4: Integrate with Scoring Infrastructure

#### 4.1 Update `score_bytes` method
**File:** `lib/quantization/src/encoded_vectors_binary.rs` (line ~937)

- Add case for `EncodedQueryBQ::Uncompressed`:
  ```rust
  EncodedQueryBQ::Uncompressed { query_vector, lookup_table } => {
      self.calculate_dot_product_with_lut(
          vector_data_usize,
          lookup_table,
          self.metadata.vector_parameters.dim
      )
  }
  ```

#### 4.2 Update `calculate_metric` or create new method
- Either extend `calculate_metric` to handle uncompressed queries, or
- Create separate `calculate_dot_product_metric` method
- Ensure it respects `distance_type` (Dot, L1, L2) and `invert` flag

### Phase 5: Optimize Lookup Table Access

#### 5.1 Consider CPU register optimization
- As mentioned in Exa's blog, they load lookup table into CPU registers
- For Rust, we can:
  - Use `#[inline]` and `#[target_feature]` for hot paths
  - Consider using `const` arrays for small lookup tables (m=4: 16 entries)
  - Use SIMD instructions if beneficial

#### 5.2 Choose optimal subvector length
- Benchmark m=4 vs m=8
- m=4: 16 entries (fits in cache better)
- m=8: 256 entries (fewer lookups, but larger table)
- Make it configurable or auto-select based on vector dimension

### Phase 6: Handle Different Binary Encodings

#### 6.1 Support OneBit encoding
- Current implementation focuses on OneBit (values are -1 or 1)
- Ensure lookup table works correctly with OneBit encoding

#### 6.2 Consider TwoBits and OneAndHalfBits
- May need separate lookup table logic or conversion
- For initial implementation, focus on OneBit only

### Phase 7: Testing

#### 7.1 Unit tests
**File:** `lib/quantization/tests/integration/test_binary.rs` or new test file

- Test lookup table generation
- Test dot product calculation with known vectors
- Compare results with direct dot product computation
- Test edge cases (dimension not divisible by subvector length)

#### 7.2 Integration tests
- Test end-to-end: encode query, score points, verify correctness
- Compare performance with existing binary/scalar encodings
- Verify distance metrics (Dot, L1, L2) work correctly

#### 7.3 Benchmarks
**File:** `lib/quantization/benches/binary.rs`

- Add benchmarks comparing:
  - Uncompressed query vs Binary query
  - Uncompressed query vs Scalar4bits/8bits
  - Different subvector lengths (m=4 vs m=8)

### Phase 8: Documentation and Configuration

#### 8.1 Update API documentation
- Document new `Uncompressed` query encoding option
- Explain when to use it (accuracy vs performance tradeoff)

#### 8.2 Add configuration option for subvector length
- Optional: Make subvector length configurable
- Default to 4 or 8 based on benchmarks

## Files to Modify

1. **lib/quantization/src/encoded_vectors_binary.rs**
   - Add `Uncompressed` to `QueryEncoding` and `EncodedQueryBQ`
   - Implement lookup table generation
   - Implement dot product with lookup tables
   - Update `encode_query` and `score_bytes`

2. **lib/segment/src/types.rs**
   - Add `Uncompressed` to `BinaryQuantizationQueryEncoding`

3. **lib/api/src/grpc/proto/collections.proto**
   - Add `Uncompressed` to proto enum

4. **lib/api/src/grpc/conversions.rs**
   - Add conversion logic

5. **lib/segment/src/vector_storage/quantized/quantized_vectors.rs**
   - Update `convert_binary_query_encoding`

6. **lib/edge/python/src/config/quantization.rs**
   - Add Python binding for new option

7. **Tests and benchmarks**
   - Add comprehensive tests
   - Add performance benchmarks

## Key Implementation Details

### Lookup Table Index Calculation

For a binary subvector `[b0, b1, b2, b3]` where each `bi` is -1 or 1:
- Convert to bit pattern: `bit_i = (bi == 1) ? 1 : 0`
- Index = `bit0 | (bit1 << 1) | (bit2 << 2) | (bit3 << 3)`

### Dot Product Formula

For query subvector `[q0, q1, q2, q3]` and binary subvector `[b0, b1, b2, b3]`:
```
dot_product = q0*b0 + q1*b1 + q2*b2 + q3*b3
```

Since `bi` is either -1 or 1:
```
dot_product = q0*(2*bit0 - 1) + q1*(2*bit1 - 1) + q2*(2*bit2 - 1) + q3*(2*bit3 - 1)
```

### Performance Considerations

- **Memory**: Lookup table for m=4 is 16 floats = 64 bytes (fits in cache)
- **Computation**: Reduces operations from O(dim) to O(dim/m) lookups
- **Accuracy**: True dot product (no approximation like XOR)

## Success Criteria

1. ✅ New `Uncompressed` query encoding option available
2. ✅ Lookup table-based dot product implementation
3. ✅ Correctness: Results match direct dot product computation
4. ✅ Performance: Faster than scalar encodings for appropriate use cases
5. ✅ Integration: Works with existing binary quantization infrastructure
6. ✅ Tests: Comprehensive test coverage
7. ✅ Documentation: API and usage documented

## PR Breakdown (150-250 LoC per PR)

This implementation will be split into 3 PRs, each between 150-250 lines of code.

### PR 1: Uncompressed Query Types and Direct Dot Product
**Estimated LoC: ~200-250 lines**

**Purpose:** Add uncompressed query encoding with direct dot product computation (no lookup table optimization yet).

**Changes:**
- Add `Uncompressed` variant to `QueryEncoding` enum in `encoded_vectors_binary.rs`
- Add `Uncompressed` variant to `EncodedQueryBQ` enum storing `Vec<f32>` query vector
- Add `Uncompressed` variant to `BinaryQuantizationQueryEncoding` in `types.rs`
- Update `encode_query_vector()` to handle `QueryEncoding::Uncompressed`
- Implement direct dot product computation in `score_bytes()` for uncompressed queries
- Handle distance types (Dot, L1, L2) and invert flag
- Add unit tests for direct dot product computation
- Add integration tests for end-to-end query encoding and scoring

**Files Modified:**
- `lib/quantization/src/encoded_vectors_binary.rs` (~150 lines)
- `lib/segment/src/types.rs` (~5 lines)
- `lib/quantization/tests/integration/test_binary.rs` (~80 lines)

**Testing:** Unit and integration tests verify dot product results match direct computation.

**Note:** This PR provides immediate accuracy benefits without the lookup table optimization. The lookup table will be added in PR 3 for performance optimization.

---

### PR 2: Proto Definitions, API Conversions, and Comprehensive Tests
**Estimated LoC: ~200-250 lines**

**Purpose:** Expose the new option through public APIs and add comprehensive test coverage.

**Changes:**
- Add `Uncompressed = 4` to `BinaryQuantizationQueryEncoding.Setting` in proto
- Update conversion functions in `conversions.rs`
- Update `convert_binary_query_encoding()` in `quantized_vectors.rs`
- Add Python binding in `edge/python/src/config/quantization.rs`
- Regenerate proto files
- Add unit tests for API conversions
- Test with different binary encodings (OneBit, TwoBits)
- Test distance metrics (Dot, L1, L2)
- Test invert flag behavior
- Compare accuracy with existing encodings
- Integration tests for full query/score workflow
- Add API documentation comments

**Files Modified:**
- `lib/api/src/grpc/proto/collections.proto` (~5 lines)
- `lib/api/src/grpc/conversions.rs` (~30 lines)
- `lib/segment/src/vector_storage/quantized/quantized_vectors.rs` (~10 lines)
- `lib/edge/python/src/config/quantization.rs` (~30 lines)
- Generated proto files (~20 lines)
- `lib/quantization/tests/integration/test_binary.rs` (~100 lines)
- `lib/quantization/src/encoded_vectors_binary.rs` (doc comments, ~20 lines)

**Testing:** Verify API conversions work correctly and comprehensive test coverage.

---

### PR 3: Lookup Table Optimization
**Estimated LoC: ~200-250 lines**

**Purpose:** Add lookup table optimization for improved performance.

**Changes:**
- Add `DotProductLookupTable` struct definition:
  ```rust
  pub struct DotProductLookupTable {
      subvector_len: usize,
      lookup_tables: Vec<Vec<f32>>, // One table per query subvector
  }
  ```
- Implement `build_dot_product_lookup_table(query: &[f32], subvector_len: usize) -> DotProductLookupTable`
- Implement `calculate_dot_product_with_lut()` function
- Handle bit extraction from binary vectors
- Handle dimension alignment (remainder elements)
- Update `EncodedQueryBQ::Uncompressed` to store lookup table
- Replace direct dot product with lookup table-based computation in `score_bytes()`
- Add unit tests for lookup table generation
- Add unit tests comparing lookup table vs direct dot product
- Add benchmarks comparing uncompressed with/without lookup tables
- Benchmark different subvector lengths (m=4 vs m=8)
- Update documentation with performance characteristics

**Files Modified:**
- `lib/quantization/src/encoded_vectors_binary.rs` (~150 lines)
- `lib/quantization/tests/integration/test_binary.rs` (~50 lines)
- `lib/quantization/benches/binary.rs` (~50 lines)
- Documentation files (~20 lines)

**Testing:** Unit tests verify lookup table correctness and performance benchmarks demonstrate improvement.

---

## PR Dependency Graph

```
PR 1 (Uncompressed Types + Direct Dot Product)
  ↓
PR 2 (Proto/API + Comprehensive Tests)
  ↓
PR 3 (Lookup Table Optimization)
```

## PR Review Checklist

Each PR should:
- [ ] Compile without errors
- [ ] Pass existing tests
- [ ] Add new tests for new functionality
- [ ] Keep changes focused and reviewable
- [ ] Include brief description of changes
- [ ] Update relevant documentation

## Notes

- This optimization is most beneficial when:
  - Query accuracy is critical (uncompressed queries preserve full precision)
  - Document vectors are binary-quantized (memory efficient)
  - Vector dimensions are large (lookup table overhead amortized)

- Consider making subvector length configurable or auto-tuned based on:
  - Vector dimension
  - CPU cache size
  - Benchmark results

- **Subvector Length Selection:**
  - Start with m=4 (16 entries) for better cache locality
  - Can optimize to m=8 (256 entries) later if benchmarks show benefit
  - Consider making it configurable in a follow-up PR if needed

