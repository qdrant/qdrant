# blobstore

Storage for variable-sized values, provided by the outer `Blobstore` type.

Operates in one of two modes, specified when creating a storage and selected
automatically when opening one, based on the persisted config:

- **dynamic** (default, the `Gridstore` variant): read-write storage with
  in-place space reuse, backed by memory mapped files.
- **append-only** (the `Arenastore` variant): append-only storage for
  serverless deployments, reading and writing files directly.

Concepts shared by both modes:

- IDs (point offsets) are sequential integers, starting at 0.
- Value data is stored in page files.
- Values are compressed with lz4 (configurable).
- A tracker maps each point offset to page + offset within the page + value
  length. The offset counts blocks in dynamic mode and bytes in append-only
  mode. The tracker is updated in-memory, and only persisted on flush.
- Supports multiple threads reading and single thread writing.

## Dynamic mode (`Gridstore`)

![Storage concepts](./storage-concepts.svg)

- The storage is divided into file pages of fixed size (32MB by default),
  mapped into memory using mmap and preallocated.
- Data units are blocks of fixed size (128 bytes by default); values span an
  integer number of contiguous blocks.
- Data can be written and read across multiple pages.
- Each block is mapped to a bit in the bitmask.
- A region is a fixed number of contiguous blocks.
- Gaps of free blocks in each region are tracked in a file.
- Deletes mark the block as deleted (in-memory) & updates their region
- Updates:
  - not done in place, always a new value is inserted
  - calculation of the new regions gaps is done on the fly
- One file per page, one file for tracker, one file for bitmask, one file for
  gaps, and one config file:

| file          | content                                                     |
|---------------|-------------------------------------------------------------|
| `config.json` | storage config, `"mode": "dynamic"`                         |
| `tracker.dat` | header with mapping count + mapping slots, preallocated     |
| `page_{n}.dat`| value data, preallocated to the page size                   |
| `bitmask.dat` | one bit per block: used or free                             |
| `gaps.dat`    | per-region free block gap summaries                         |

## Append-only mode (`Arenastore`)

Designed for serverless environments, which restrict IO: files can only be
appended to, existing bytes can never be rewritten (preallocated zero padding
cannot be filled in later), and IO is expensive so as few files as possible
are used. Value data goes through the configured universal IO backend; the
tracker file is read and written directly on the local filesystem.

- Values cannot be updated or deleted, and must be put at monotonically
  increasing point offsets. Violating puts and deletes are rejected.
- No blocks, bitmask, gaps or regions: space is never reused.
- Value data is packed back to back in page files, without alignment: each
  value starts right after the previous one. There is no preallocation and no
  padding, so a file's length always matches the end of its last appended
  value.
- Once appending a value would grow the current page beyond the configured
  page size, a new page is started, bounding the size of and the number of
  appends to each file (object stores limit appends per object). A value
  larger than the page size gets a page of its own; values never span pages.
- The tracker file is a plain array of 16 byte mapping entries without any
  header: the entry index is the point offset, and the mapping count is
  defined by the exact file length. Skipped point offsets are backfilled as
  zeroed entries, which decode as `None`.
- Puts buffer both the value data and the mapping in memory; only a page
  rollover touches disk between flushes, by creating the new, empty page
  file. Reads transparently serve buffered values.
- Flushing appends all buffered value data to the page files with a single
  write per page and syncs them, then does the same for the pending mappings
  in the tracker file. A mapping on disk therefore never points at value data
  that is not durable. A flush with a stale target is a no-op, appended bytes
  are never written twice.
- A write may be torn. If the tracker file length is not a multiple of the
  entry size, the trailing partial entry is ignored when reading, and
  truncated away when opening writable.
- One file per page, one tracker file and one config file, with names
  distinct from the dynamic mode so that one mode never attempts to load the
  incompatible file format of the other:

| file                       | content                                        |
|----------------------------|------------------------------------------------|
| `config.json`              | storage config, `"mode": "append_only"`        |
| `append_only_tracker.dat`  | mapping entries, exact length = count * 16     |
| `append_only_page_{n}.dat` | value data, exact length = end of last value   |

## TODOs

- [ ] dictionary compression to optimize payload key repetition
- [ ] validate the usage with a block storage via HTTP range requests
