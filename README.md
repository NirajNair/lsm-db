# lsm-db

A log-structured merge-tree key-value store written in Go. Writes go to an in-memory skiplist (MemTable) backed by a write-ahead log, which flushes to sorted SSTables on disk. A background compaction process merges SSTables across levels to bound read amplification.

## Architecture

```
Write Path                          Read Path
──────────                          ─────────
                                   
key=value                          
   │                               
   ▼                               
 ┌──────┐    flush     ┌─────────┐  Get(key)
 │MemTable├───────────►│  L0 SST ├─►│
 │(skiplist)│          │(overlapping)│ │
 └──────┘             └────┬─────┘  │
   │                        │        │  search order:
   ▼                        ▼        │  MemTable → rotated MemTable
 ┌──────┐             ┌─────────┐   │  → L0 (newest first)
 │  WAL  │             │  L1 SST ├─►│  → L1, L2 (binary search)
 └──────┘             │(sorted,  │   │
                       │non-overlap)│ 
                       └────┬─────┘  │
                            │        │
                            ▼        │
                       ┌─────────┐   │
                       │  L2 SST ├─►─┘
                       │(last level,
                       │ tombstones
                       │ dropped)
                       └─────────┘
```

**Components:**

| Component | Role |
|---|---|
| **MemTable** | In-memory skiplist (`huandu/skiplist`). All writes go here first. |
| **WAL** | Append-only write-ahead log. Survives crashes. Rotated on flush. |
| **SSTable** | Immutable sorted files on disk. Written via tmp+rename for atomicity. |
| **Manifest** | Single source of truth for which SSTables exist and which level they belong to. Updated atomically via deep-copy + write. |
| **Compaction** | Hybrid leveled: L0→L1 is a full merge (L0 files overlap), L1+ picks one file via round-robin + overlapping targets. Cascades until no triggers fire. |

## Design Constants

This implementation makes specific tradeoff choices. The rationale matters more than the numbers — changing one constant shifts the amplification profile.

**3 levels (L0–L2).** Enough to demonstrate full leveled compaction with cascade. Adding levels is a constant change (`NumLevels`); the algorithm generalizes.

**L0 triggers at ≥4 files, not by size.** L0 SSTables can overlap in key range, so the number of files is the meaningful metric — more files means more work per read.

**Each level is 4× the size of the one above it (SizeRatio = 4).** L0 caps at 4 × MaxFileSize, L1 at SizeRatio × L0, L2 at SizeRatio × L1. This exponential sizing means data gets rewritten at most SizeRatio times per level transition, bounding write amplification.

**Max file size = 1 MB.** Compaction splits merged output into files no larger than this. Smaller files reduce the overlap window for L1+ compaction (less data rewritten per compaction), at the cost of more files per level.

**Round-robin file picking for L1+ compaction.** Distributes compaction work evenly across files in a level. The cursor (`NextCompactionIdx`) is persisted in the manifest so position survives restarts.

**Compaction cascades up to 10 levels deep per flush.** A single L0→L1 merge can push L1 over its size limit, triggering L1→L2. The cascade depth is a safety valve, not a normal case.

## Amplification

Every LSM-tree design trades off three amplifications. Here's how this implementation sits:

**Write amplification.** Each key gets rewritten once per level transition. With SizeRatio = 4 and 3 levels, worst-case write amp is roughly (1 + SizeRatio) per level merge — L0→L1 rewrites all L1 data in a full merge, L1→L2 rewrites the picked file + its overlapping targets. The round-robin picker at L1+ bounds the per-compaction write amp to the SizeRatio, but L0→L1 is intentionally a full merge (L0 files overlap, so there's no smaller scope).

**Read amplification.** A `Get(key)` checks the MemTable, then the rotated MemTable, then L0 files in reverse order (newest first — L0 files overlap, so all may need checking), then one file per level below via binary search on sorted, non-overlapping key ranges. Worst case: 2 MemTable lookups + all L0 files + 1 binary-searched file per level. Bounding L0 to 4 files and compacting aggressively keeps this manageable.

**Space amplification.** Temporary: during compaction both old and new files coexist on disk until the manifest write succeeds and old files are deleted. Level sizing: at steady state, total space is roughly SizeRatio × the data in the level above — L1 holds ~4× L0's worth, L2 holds ~4× L1's worth. Tombstones are dropped at the last level (L2), so space waste from deletes is reclaimed only when data compacts into L2.

## Quick Start

```bash
go build .
./lsm-db
```

```go
db, _ := db.NewDB(2 << 20) // 2 MB memtable threshold
defer db.Close()

db.Put([]byte("name"), []byte("lsm-db"))
val, _ := db.Get([]byte("name"))  // → "lsm-db"

db.Delete([]byte("name"))
_, err := db.Get([]byte("name"))   // → ErrKeyNotFound
```

## Crash Recovery

- **WAL replay**: on startup, the active WAL and any rotated WALs are replayed to reconstruct the MemTable.
- **Manifest reconciliation**: any SSTable file on disk not referenced by the manifest is treated as garbage and deleted. This covers crashes at any point in the flush or compaction pipeline.
- **Atomic manifest writes**: every manifest update uses a deep-copy snapshot + tmp+rename pattern. If the write fails, the in-memory state is rolled back from the snapshot and orphan SSTable files are cleaned up.

## License

MIT