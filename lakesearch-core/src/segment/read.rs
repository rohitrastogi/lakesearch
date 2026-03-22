use fst::Map;

use crate::types::{
    CorpusStats, DocId, DocTableEntry, FileTableEntry, SegmentError, SegmentFooter, TermInfo,
    TermOrdinal, DOC_TABLE_ENTRY_SIZE, FOOTER_SIZE, SEGMENT_MAGIC, SEGMENT_VERSION, TERM_INFO_SIZE,
};

/// Reads and queries a segment file from a byte buffer.
///
/// Parses the footer on construction and lazily loads FST, doc table, and term
/// info table. The segment data must remain valid for the lifetime of the reader.
pub struct SegmentReader {
    data: Vec<u8>,
    footer: SegmentFooter,
    forward_fst: Map<Vec<u8>>,
    reverse_fst: Map<Vec<u8>>,
    term_infos: Vec<TermInfo>,
    doc_table: Vec<DocTableEntry>,
    file_table: Vec<FileTableEntry>,
    corpus_stats: CorpusStats,
}

impl std::fmt::Debug for SegmentReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentReader")
            .field("data_len", &self.data.len())
            .field("footer", &self.footer)
            .field("term_count", &self.term_infos.len())
            .field("doc_count", &self.doc_table.len())
            .field("file_count", &self.file_table.len())
            .field("corpus_stats", &self.corpus_stats)
            .finish()
    }
}

impl SegmentReader {
    /// Parses a segment from its complete byte representation.
    pub fn open(data: Vec<u8>) -> crate::types::Result<Self> {
        if data.len() < FOOTER_SIZE {
            return Err(SegmentError::TooShort {
                need: FOOTER_SIZE,
                got: data.len(),
            });
        }

        // Validate header magic
        if data[0..4] != SEGMENT_MAGIC {
            return Err(SegmentError::InvalidMagic);
        }

        let version = u16::from_le_bytes([data[4], data[5]]);
        if version != SEGMENT_VERSION {
            return Err(SegmentError::UnsupportedVersion(version));
        }

        // Parse footer
        let footer = parse_footer(&data)?;

        // Verify checksum — covers everything except the last 8 bytes
        // (checksum field + trailing magic)
        let body = &data[..data.len() - 8];
        let actual_checksum = crc32fast::hash(body);
        if actual_checksum != footer.checksum {
            return Err(SegmentError::ChecksumMismatch {
                expected: footer.checksum,
                actual: actual_checksum,
            });
        }

        // Parse sections
        let forward_fst = parse_fst(&data, footer.forward_fst_offset as usize)?;
        let reverse_fst = parse_fst(&data, footer.reverse_fst_offset as usize)?;
        let term_infos = parse_term_info_table(&data, footer)?;
        let doc_table = parse_doc_table(&data, footer.doc_table_offset as usize)?;
        let file_table = parse_file_table(&data, footer.file_table_offset as usize)?;
        let corpus_stats = parse_corpus_stats(&data, footer.corpus_stats_offset as usize)?;

        Ok(Self {
            data,
            footer,
            forward_fst,
            reverse_fst,
            term_infos,
            doc_table,
            file_table,
            corpus_stats,
        })
    }

    /// Looks up a term in the forward FST and returns its term ordinal.
    #[must_use]
    pub fn term_ordinal(&self, term: &str) -> Option<TermOrdinal> {
        self.forward_fst.get(term.as_bytes())
    }

    /// Returns the term info for a given term ordinal.
    pub fn term_info(&self, ordinal: TermOrdinal) -> crate::types::Result<&TermInfo> {
        self.term_infos
            .get(ordinal as usize)
            .ok_or(SegmentError::InvalidTermOrdinal(ordinal))
    }

    /// Decodes and returns the posting list for a given term ordinal.
    pub fn posting_list(&self, ordinal: TermOrdinal) -> crate::types::Result<Vec<DocId>> {
        let info = self.term_info(ordinal)?;
        let abs_offset = self.footer.posting_offset + info.posting_offset;
        let start = abs_offset as usize;
        let end = start + info.posting_length as usize;

        if end > self.data.len() {
            return Err(SegmentError::TooShort {
                need: end,
                got: self.data.len(),
            });
        }

        crate::posting::decode(&self.data[start..end])
    }

    /// Convenience: look up a term and return its posting list.
    pub fn search_term(&self, term: &str) -> crate::types::Result<Option<Vec<DocId>>> {
        match self.term_ordinal(term) {
            Some(ord) => self.posting_list(ord).map(Some),
            None => Ok(None),
        }
    }

    /// Returns all terms matching the given prefix via forward FST iteration.
    #[must_use]
    pub fn prefix_terms(&self, prefix: &str) -> Vec<(String, TermOrdinal)> {
        use fst::automaton::Str;
        use fst::{Automaton, IntoStreamer, Streamer};

        let auto = Str::new(prefix).starts_with();
        let mut stream = self.forward_fst.search(auto).into_stream();
        let mut results = Vec::new();
        while let Some((key, ordinal)) = stream.next() {
            if let Ok(term) = std::str::from_utf8(key) {
                results.push((term.to_owned(), ordinal));
            }
        }
        results
    }

    /// Returns all terms matching the given suffix via reverse FST iteration.
    ///
    /// Reverses the suffix by chars, performs a prefix search on the reverse
    /// FST, and returns the matching (term, term_ordinal) pairs.
    #[must_use]
    pub fn suffix_terms(&self, suffix: &str) -> Vec<(String, TermOrdinal)> {
        use fst::automaton::Str;
        use fst::{Automaton, IntoStreamer, Streamer};

        // Reverse by chars to match how the reverse FST was built
        let reversed: String = suffix.chars().rev().collect();
        let auto = Str::new(&reversed).starts_with();
        let mut stream = self.reverse_fst.search(auto).into_stream();
        let mut results = Vec::new();
        while let Some((key, ordinal)) = stream.next() {
            // Reverse the key chars back to get the original term
            if let Ok(rev_str) = std::str::from_utf8(key) {
                let term: String = rev_str.chars().rev().collect();
                results.push((term, ordinal));
            }
        }
        results
    }

    /// Returns the doc table entry for a given doc_id.
    #[must_use]
    pub fn doc_entry(&self, doc_id: DocId) -> Option<&DocTableEntry> {
        self.doc_table.get(doc_id as usize)
    }

    /// Returns the full doc table.
    #[must_use]
    pub fn doc_table(&self) -> &[DocTableEntry] {
        &self.doc_table
    }

    /// Returns the file table.
    #[must_use]
    pub fn file_table(&self) -> &[FileTableEntry] {
        &self.file_table
    }

    /// Returns corpus statistics for BM25 scoring.
    #[must_use]
    pub fn corpus_stats(&self) -> CorpusStats {
        self.corpus_stats
    }

    /// Returns the segment footer.
    #[must_use]
    pub fn footer(&self) -> &SegmentFooter {
        &self.footer
    }

    /// Returns the number of terms in this segment.
    #[must_use]
    pub fn term_count(&self) -> usize {
        self.term_infos.len()
    }

    /// Returns the number of docs (pages) in this segment.
    #[must_use]
    pub fn doc_count(&self) -> usize {
        self.doc_table.len()
    }
}

// --- Parsing helpers ---------------------------------------------------------

fn parse_footer(data: &[u8]) -> crate::types::Result<SegmentFooter> {
    let footer_start = data.len() - FOOTER_SIZE;
    let f = &data[footer_start..];

    // Validate trailing magic
    if f[52..56] != SEGMENT_MAGIC {
        return Err(SegmentError::InvalidMagic);
    }

    Ok(SegmentFooter {
        file_table_offset: u64::from_le_bytes(f[0..8].try_into().unwrap()),
        doc_table_offset: u64::from_le_bytes(f[8..16].try_into().unwrap()),
        forward_fst_offset: u64::from_le_bytes(f[16..24].try_into().unwrap()),
        reverse_fst_offset: u64::from_le_bytes(f[24..32].try_into().unwrap()),
        posting_offset: u64::from_le_bytes(f[32..40].try_into().unwrap()),
        corpus_stats_offset: u64::from_le_bytes(f[40..48].try_into().unwrap()),
        checksum: u32::from_le_bytes(f[48..52].try_into().unwrap()),
    })
}

fn parse_fst(data: &[u8], offset: usize) -> crate::types::Result<Map<Vec<u8>>> {
    if offset + 8 > data.len() {
        return Err(SegmentError::TooShort {
            need: offset + 8,
            got: data.len(),
        });
    }
    let fst_len = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
    let fst_start = offset + 8;
    let fst_end = fst_start + fst_len;
    if fst_end > data.len() {
        return Err(SegmentError::TooShort {
            need: fst_end,
            got: data.len(),
        });
    }
    let fst_data = data[fst_start..fst_end].to_vec();
    Map::new(fst_data).map_err(SegmentError::Fst)
}

fn parse_term_info_table(
    data: &[u8],
    footer: SegmentFooter,
) -> crate::types::Result<Vec<TermInfo>> {
    // The term info table immediately follows the reverse FST section.
    // Its offset is not stored in the footer — we derive it by reading
    // the reverse FST's length prefix and skipping past the FST data.
    let rev_fst_offset = footer.reverse_fst_offset as usize;
    if rev_fst_offset + 8 > data.len() {
        return Err(SegmentError::TooShort {
            need: rev_fst_offset + 8,
            got: data.len(),
        });
    }
    let rev_fst_len =
        u64::from_le_bytes(data[rev_fst_offset..rev_fst_offset + 8].try_into().unwrap()) as usize;
    let term_info_start = rev_fst_offset + 8 + rev_fst_len;

    if term_info_start + 4 > data.len() {
        return Err(SegmentError::TooShort {
            need: term_info_start + 4,
            got: data.len(),
        });
    }

    let num_terms = u32::from_le_bytes(
        data[term_info_start..term_info_start + 4]
            .try_into()
            .unwrap(),
    ) as usize;

    let entries_start = term_info_start + 4;
    let entries_end = entries_start + num_terms * TERM_INFO_SIZE;
    if entries_end > data.len() {
        return Err(SegmentError::TooShort {
            need: entries_end,
            got: data.len(),
        });
    }

    let mut infos = Vec::with_capacity(num_terms);
    for i in 0..num_terms {
        let base = entries_start + i * TERM_INFO_SIZE;
        infos.push(TermInfo {
            posting_offset: u64::from_le_bytes(data[base..base + 8].try_into().unwrap()),
            posting_length: u32::from_le_bytes(data[base + 8..base + 12].try_into().unwrap()),
            doc_frequency: u32::from_le_bytes(data[base + 12..base + 16].try_into().unwrap()),
        });
    }

    Ok(infos)
}

fn parse_doc_table(data: &[u8], offset: usize) -> crate::types::Result<Vec<DocTableEntry>> {
    if offset + 4 > data.len() {
        return Err(SegmentError::TooShort {
            need: offset + 4,
            got: data.len(),
        });
    }

    let num_docs = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
    let entries_start = offset + 4;
    let entries_end = entries_start + num_docs * DOC_TABLE_ENTRY_SIZE;
    if entries_end > data.len() {
        return Err(SegmentError::TooShort {
            need: entries_end,
            got: data.len(),
        });
    }

    let mut entries = Vec::with_capacity(num_docs);
    for i in 0..num_docs {
        let base = entries_start + i * DOC_TABLE_ENTRY_SIZE;
        entries.push(DocTableEntry {
            file_ordinal: u32::from_le_bytes(data[base..base + 4].try_into().unwrap()),
            row_group: u16::from_le_bytes(data[base + 4..base + 6].try_into().unwrap()),
            page_index: u16::from_le_bytes(data[base + 6..base + 8].try_into().unwrap()),
            first_row_index: u64::from_le_bytes(data[base + 8..base + 16].try_into().unwrap()),
            row_count: u32::from_le_bytes(data[base + 16..base + 20].try_into().unwrap()),
        });
    }

    Ok(entries)
}

fn parse_file_table(data: &[u8], offset: usize) -> crate::types::Result<Vec<FileTableEntry>> {
    if offset + 4 > data.len() {
        return Err(SegmentError::TooShort {
            need: offset + 4,
            got: data.len(),
        });
    }

    let num_files = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;

    if num_files == 0 {
        return Ok(Vec::new());
    }

    // Each file record: path_offset(u32) + path_length(u16) + row_group_count(u16) = 8 bytes
    let records_start = offset + 4;
    let records_end = records_start + num_files * 8;
    if records_end > data.len() {
        return Err(SegmentError::TooShort {
            need: records_end,
            got: data.len(),
        });
    }

    // String pool starts after all file records
    let pool_start = records_end;

    let mut entries = Vec::with_capacity(num_files);
    for i in 0..num_files {
        let base = records_start + i * 8;
        let path_offset = u32::from_le_bytes(data[base..base + 4].try_into().unwrap()) as usize;
        let path_length = u16::from_le_bytes(data[base + 4..base + 6].try_into().unwrap()) as usize;
        let row_group_count = u16::from_le_bytes(data[base + 6..base + 8].try_into().unwrap());

        let path_start = pool_start + path_offset;
        let path_end = path_start + path_length;
        if path_end > data.len() {
            return Err(SegmentError::TooShort {
                need: path_end,
                got: data.len(),
            });
        }

        let path = std::str::from_utf8(&data[path_start..path_end])
            .map_err(|e| SegmentError::PostingDecode(format!("invalid UTF-8 in file path: {e}")))?
            .to_owned();

        entries.push(FileTableEntry {
            path,
            row_group_count,
        });
    }

    Ok(entries)
}

fn parse_corpus_stats(data: &[u8], offset: usize) -> crate::types::Result<CorpusStats> {
    if offset + 16 > data.len() {
        return Err(SegmentError::TooShort {
            need: offset + 16,
            got: data.len(),
        });
    }
    Ok(CorpusStats {
        total_rows: u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()),
        total_tokens: u64::from_le_bytes(data[offset + 8..offset + 16].try_into().unwrap()),
    })
}
