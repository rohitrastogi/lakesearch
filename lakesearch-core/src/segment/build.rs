use std::collections::BTreeMap;

use fst::MapBuilder;

use crate::posting;
use crate::types::{
    CorpusStats, DocId, DocTableEntry, FileTableEntry, FOOTER_SIZE, SEGMENT_MAGIC, SEGMENT_VERSION,
};

/// Builds a segment file from terms, posting lists, doc table entries, and
/// file table entries.
///
/// Terms must be added in lexicographic order (required by the FST builder).
/// The builder writes the complete binary segment format to an in-memory buffer.
pub struct SegmentBuilder {
    /// term → (sorted doc_id list, doc_frequency)
    terms: BTreeMap<String, (Vec<DocId>, u32)>,
    doc_table: Vec<DocTableEntry>,
    file_table: Vec<FileTableEntry>,
    corpus_stats: CorpusStats,
}

impl SegmentBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            terms: BTreeMap::new(),
            doc_table: Vec::new(),
            file_table: Vec::new(),
            corpus_stats: CorpusStats {
                total_rows: 0,
                total_tokens: 0,
            },
        }
    }

    /// Adds a term with its posting list (sorted doc_ids) and doc_frequency.
    ///
    /// `doc_ids` are page-level identifiers. `doc_frequency` is the number of
    /// rows (not pages) containing the term.
    pub fn add_term(&mut self, term: &str, doc_ids: Vec<DocId>, doc_frequency: u32) {
        self.terms.insert(term.to_owned(), (doc_ids, doc_frequency));
    }

    /// Sets the doc table entries. Index in the vec corresponds to doc_id.
    pub fn set_doc_table(&mut self, entries: Vec<DocTableEntry>) {
        self.doc_table = entries;
    }

    /// Sets the file table entries. Index corresponds to file_ordinal.
    pub fn set_file_table(&mut self, entries: Vec<FileTableEntry>) {
        self.file_table = entries;
    }

    /// Sets the corpus statistics for BM25 scoring.
    pub fn set_corpus_stats(&mut self, stats: CorpusStats) {
        self.corpus_stats = stats;
    }

    /// Builds the segment file and returns the bytes.
    pub fn build(self) -> crate::types::Result<Vec<u8>> {
        let mut buf = Vec::new();

        // --- Header (8 bytes) ---
        buf.extend_from_slice(&SEGMENT_MAGIC);
        buf.extend_from_slice(&SEGMENT_VERSION.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes()); // flags

        // --- File Table Section ---
        let file_table_offset = buf.len() as u64;
        write_file_table(&self.file_table, &mut buf);

        // --- Posting Blocks ---
        let posting_offset = buf.len() as u64;

        // Encode each term's posting list and track offsets.
        // We consume self.terms via into_iter to avoid cloning term strings.
        let mut term_postings: Vec<(String, u64, u32, u32)> = Vec::new(); // (term, offset, length, df)
        for (term, (doc_ids, doc_frequency)) in self.terms {
            let offset = (buf.len() as u64) - posting_offset;
            let encoded = posting::encode(&doc_ids);
            let length = encoded.len() as u32;
            buf.extend_from_slice(&encoded);
            term_postings.push((term, offset, length, doc_frequency));
        }

        // --- Forward FST ---
        let forward_fst_offset = buf.len() as u64;
        let fst_bytes = {
            let mut fst_builder = MapBuilder::memory();
            for (ordinal, (term, _, _, _)) in term_postings.iter().enumerate() {
                fst_builder.insert(term.as_bytes(), ordinal as u64)?;
            }
            fst_builder.into_inner()?
        };
        buf.extend_from_slice(&(fst_bytes.len() as u64).to_le_bytes());
        buf.extend_from_slice(&fst_bytes);

        // --- Reverse FST ---
        let reverse_fst_offset = buf.len() as u64;
        let rev_fst_bytes = {
            // Build reversed terms sorted lexicographically.
            // Reverse by chars (not bytes) to maintain valid UTF-8 keys.
            let mut reversed: Vec<(String, u64)> = term_postings
                .iter()
                .enumerate()
                .map(|(ordinal, (term, _, _, _))| {
                    let rev: String = term.chars().rev().collect();
                    (rev, ordinal as u64)
                })
                .collect();
            reversed.sort();

            let mut fst_builder = MapBuilder::memory();
            for (rev_term, ordinal) in &reversed {
                fst_builder.insert(rev_term.as_bytes(), *ordinal)?;
            }
            fst_builder.into_inner()?
        };
        buf.extend_from_slice(&(rev_fst_bytes.len() as u64).to_le_bytes());
        buf.extend_from_slice(&rev_fst_bytes);

        // --- Term Info Table ---
        buf.extend_from_slice(&(term_postings.len() as u32).to_le_bytes());
        for (_term, offset, length, doc_frequency) in &term_postings {
            buf.extend_from_slice(&offset.to_le_bytes()); // posting_offset: u64
            buf.extend_from_slice(&length.to_le_bytes()); // posting_length: u32
            buf.extend_from_slice(&doc_frequency.to_le_bytes()); // doc_frequency: u32
        }

        // --- Doc Table ---
        let doc_table_offset = buf.len() as u64;
        buf.extend_from_slice(&(self.doc_table.len() as u32).to_le_bytes());
        for entry in &self.doc_table {
            buf.extend_from_slice(&entry.file_ordinal.to_le_bytes());
            buf.extend_from_slice(&entry.row_group.to_le_bytes());
            buf.extend_from_slice(&entry.page_index.to_le_bytes());
            buf.extend_from_slice(&entry.first_row_index.to_le_bytes());
            buf.extend_from_slice(&entry.row_count.to_le_bytes());
        }

        // --- Corpus Stats (16 bytes) ---
        let corpus_stats_offset = buf.len() as u64;
        buf.extend_from_slice(&self.corpus_stats.total_rows.to_le_bytes());
        buf.extend_from_slice(&self.corpus_stats.total_tokens.to_le_bytes());

        // --- Footer (56 bytes) ---
        // Write offset fields first so they are covered by the checksum.
        // The CRC covers everything up to (but not including) the checksum
        // and trailing magic (last 8 bytes of the footer).
        buf.extend_from_slice(&file_table_offset.to_le_bytes());
        buf.extend_from_slice(&doc_table_offset.to_le_bytes());
        buf.extend_from_slice(&forward_fst_offset.to_le_bytes());
        buf.extend_from_slice(&reverse_fst_offset.to_le_bytes());
        buf.extend_from_slice(&posting_offset.to_le_bytes());
        buf.extend_from_slice(&corpus_stats_offset.to_le_bytes());

        // CRC covers header + all sections + footer offsets
        let checksum = crc32fast::hash(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());
        buf.extend_from_slice(&SEGMENT_MAGIC);

        debug_assert_eq!(buf.len() - (buf.len() - FOOTER_SIZE), FOOTER_SIZE,);

        Ok(buf)
    }
}

impl Default for SegmentBuilder {
    fn default() -> Self {
        Self::new()
    }
}

fn write_file_table(entries: &[FileTableEntry], buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());

    // Build string pool
    let mut string_pool = Vec::new();
    let mut file_records: Vec<(u32, u16, u16)> = Vec::new(); // (path_offset, path_length, rg_count)
    for entry in entries {
        let path_offset = string_pool.len() as u32;
        let path_bytes = entry.path.as_bytes();
        let path_length = path_bytes.len() as u16;
        string_pool.extend_from_slice(path_bytes);
        file_records.push((path_offset, path_length, entry.row_group_count));
    }

    // Write per-file records
    for &(path_offset, path_length, rg_count) in &file_records {
        buf.extend_from_slice(&path_offset.to_le_bytes());
        buf.extend_from_slice(&path_length.to_le_bytes());
        buf.extend_from_slice(&rg_count.to_le_bytes());
    }

    // Write string pool
    buf.extend_from_slice(&string_pool);
}
