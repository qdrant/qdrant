pub mod compressed_posting_list;
mod compressed_chunks_reader;
pub mod compressed_common;
pub mod compressed_posting_visitor;
mod compressed_posting_iterator;

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