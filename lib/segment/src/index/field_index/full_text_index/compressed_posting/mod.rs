mod compressed_chunks_reader;
pub mod compressed_common;
mod compressed_posting_iterator;
pub mod compressed_posting_list;
pub mod compressed_posting_visitor;

//
//                               Posting Iterator
//                               Holds Current offset
//                            ┌──────────────────────────────────────┐
//                            │   Chunk Visitor                      │
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
//   │     │ Reminder ◄───┼───│─┼──┼── Ref         │               │ │
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
//         │ Reminder ◄───┼─
//         │              │
//         └──────────────┘
//
