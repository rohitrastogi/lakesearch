//! Core domain types for LakeSearch.
//!
//! These types represent the fundamental data structures used across the
//! segment file format: document identifiers, term metadata, the doc table,
//! file table, corpus statistics, and the segment footer.

use thiserror::Error;

/// A dense, segment-local page identifier (0..N-1).
pub type DocId = u32;

/// An ordinal into the segment's term dictionary (FST value).
pub type TermOrdinal = u64;

/// Errors that can occur when reading or writing segment data.
#[derive(Debug, Error)]
pub enum SegmentError {
    #[error("invalid magic bytes")]
    InvalidMagic,

    #[error("unsupported segment version: {0}")]
    UnsupportedVersion(u16),

    #[error("segment data too short: need {need} bytes, got {got}")]
    TooShort { need: usize, got: usize },

    #[error("checksum mismatch: expected {expected:#010x}, got {actual:#010x}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("FST error: {0}")]
    Fst(#[from] fst::Error),

    #[error("posting list decode error: {0}")]
    PostingDecode(String),

    #[error("invalid term ordinal: {0}")]
    InvalidTermOrdinal(TermOrdinal),

    #[error("LZ4 decompression error: {0}")]
    Lz4(String),
}

pub type Result<T> = std::result::Result<T, SegmentError>;

/// Segment file magic bytes: "LKSR".
pub const SEGMENT_MAGIC: [u8; 4] = *b"LKSR";

/// Current segment format version.
pub const SEGMENT_VERSION: u16 = 1;

/// Maximum number of doc_ids per posting list block.
pub const POSTING_BLOCK_SIZE: usize = 128;

/// Fixed size of the segment footer in bytes.
pub const FOOTER_SIZE: usize = 56;

/// Fixed size of each posting block header in bytes.
/// num_docs(u16) + min_doc_id(u32) + bit_width(u8) + flags(u8) + compressed_size(u32) + uncompressed_size(u16)
pub const BLOCK_HEADER_SIZE: usize = 14;

/// Fixed size of each doc table entry in bytes.
pub const DOC_TABLE_ENTRY_SIZE: usize = 20;

/// Fixed size of each term info entry in bytes.
pub const TERM_INFO_SIZE: usize = 16;

/// An entry in the segment's file table, mapping a file_ordinal to its
/// Parquet file path and row group count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileTableEntry {
    pub path: String,
    pub row_group_count: u16,
}

/// An entry in the segment's doc table, mapping a dense doc_id to its
/// Parquet page location.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DocTableEntry {
    pub file_ordinal: u32,
    pub row_group: u16,
    pub page_index: u16,
    pub first_row_index: u64,
    pub row_count: u32,
}

/// Metadata for a single term in the segment's term info table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TermInfo {
    /// Byte offset into the posting blocks section.
    pub posting_offset: u64,
    /// Byte length of the encoded posting list.
    pub posting_length: u32,
    /// Number of rows (not pages) containing this term.
    pub doc_frequency: u32,
}

/// Corpus-level statistics stored in each segment, used for BM25 scoring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CorpusStats {
    /// Total number of rows across all indexed files in this segment.
    pub total_rows: u64,
    /// Total token count across all rows, for computing average document length.
    pub total_tokens: u64,
}

/// The segment file footer, containing byte offsets to each section.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentFooter {
    pub file_table_offset: u64,
    pub doc_table_offset: u64,
    pub forward_fst_offset: u64,
    pub reverse_fst_offset: u64,
    pub posting_offset: u64,
    pub corpus_stats_offset: u64,
    pub checksum: u32,
}

/// Posting block header flags.
pub mod flags {
    /// Bit 0: block data is LZ4-compressed.
    pub const LZ4_COMPRESSED: u8 = 0x01;
}
