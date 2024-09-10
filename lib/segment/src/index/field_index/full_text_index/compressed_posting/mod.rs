pub mod compressed_chunks_reader;
pub mod compressed_common;
pub mod compressed_posting_iterator;
pub mod compressed_posting_list;
pub mod compressed_posting_visitor;

//
//                               CompressedPostingIterator
//                               Holds Current offset
//                            ┌──────────────────────────────────────┐
//                            │   CompressedPostingVisitor           │
//                            │   Holds Uncompressed Context         │
//                            │ ┌──────────────────────────────────┐ │
//          PostingList       │ │                                  │ │
//          Holds Data        │ │   ChunkReader                    │ │
//         ┌──────────────┐   │ │  ┌───────────────┐               │ │
//         │              │   │ │  │               │               │ │
//         │ Data ◄───────┼───│─┼──┼── Ref         │               │ │
//         │              │   │ │  │               │               │ │
//   ┌─────┤ ChunksIndex ◄┼───│─┼──┼── Ref         │               │ │
//   │     │              │   │ │  │               │               │ │
//   │     │ Remainder ◄──┼───│─┼──┼── Ref         │               │ │
//   │     │              │   │ │  │               │               │ │
//   │     └──────────────┘   │ │  └───────────────┘               │ │
//   │                        │ │                                  │ │
//   │       Replace-able     │ │    * Decompressed Chunk          │ │
//   │          With          │ │    * Various Ids to know what's  │ │
//   │                        │ │       currently decompressed     │ │
//   │      PostingListMmap   │ └──────────────────────────────────┘ │
//   │      Holds Data        │      * Offset                        │
//   │     ┌──────────────┐   └──────────────────────────────────────┘
//   │     │              │
//   │     │ Data ◄───────┼─
//   │     │              │
//   └───► │ ChunksIndex ◄┼─
//         │              │
//         │ Remainder ◄──┼─
//         │              │
//         └──────────────┘
//
