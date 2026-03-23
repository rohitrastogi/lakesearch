//! Compaction service for LakeSearch: merges small segments into larger ones,
//! atomically updates metadata, and sweeps orphaned files.

pub mod merge;
pub mod server;
