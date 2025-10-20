- storage/collections/benchmark/2/segments/d85c3bcf-4825-41a3-a342-e633ca00a9b2/vector_storage-0_sparse/store
  - @ 0x208280 (412 bytes)
  - 85 bytes too short
- sparse vector
- bincode deserialize from gridstore fails
- mutable segment
- pid 171915
- offset 4671

## Pointing to block_offset 2738
External ID: 17346
Internal ID: 790
Version: Some(1563893)
Payload: Payload({"a": String("keyword_9"), "timestamp": String("2025-10-15T18:01:46.271102733+00:00")})

External ID: 65881
Internal ID: 5399
Version: Some(1563902)
Payload: Payload({"a": String("keyword_3"), "timestamp": String("2025-10-15T18:01:46.269006604+00:00")})

## Pointing to block_offset 16645
External ID: 149007
Internal ID: 609
Version: Some(1563893)
Payload: Payload({"timestamp": String("2025-10-15T18:01:46.269820089+00:00"), "a": String("keyword_9")})

External ID: 25545
Internal ID: 5401
Version: Some(1563902)
Payload: Payload({"a": String("keyword_8"), "timestamp": String("2025-10-15T18:01:46.268537086+00:00")})

## Pointing to block_offset 16645 (this is the one creating the panic)
External ID: 107981
Internal ID: 4671
Version: Some(1563893)
Payload: Payload({"timestamp": String("2025-10-15T18:01:46.270296927+00:00"), "a": String("keyword_6")})
Length: 412 <- tries to load this length, but is too short

External ID: 191364
Internal ID: 5402
Version: Some(1563902)
Payload: Payload({"timestamp": String("2025-10-15T18:01:46.269516211+00:00"), "a": String("keyword_0")})
Length: 497 <- this should be the correct length, according to the value
