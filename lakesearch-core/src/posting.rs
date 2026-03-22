//! Posting list codec: delta-encoded, bit-packed blocks with optional LZ4.
//!
//! Each posting list is a sequence of blocks of up to 128 sorted `DocId`s.
//! Within a block: delta-encode → determine bit_width → bit-pack → optionally
//! LZ4-compress. The block header stores enough metadata for skip-ahead and
//! decoding without external state.

use crate::types::{flags, DocId, SegmentError, BLOCK_HEADER_SIZE, POSTING_BLOCK_SIZE};

// --- Encoding ----------------------------------------------------------------

/// Encodes a sorted slice of `DocId`s into the posting list binary format.
///
/// The input must be sorted and deduplicated. Returns the encoded bytes.
#[must_use]
pub fn encode(doc_ids: &[DocId]) -> Vec<u8> {
    debug_assert!(
        doc_ids.windows(2).all(|w| w[0] < w[1]),
        "encode: doc_ids must be sorted and deduplicated"
    );
    let mut out = Vec::new();
    for chunk in doc_ids.chunks(POSTING_BLOCK_SIZE) {
        encode_block(chunk, &mut out);
    }
    out
}

fn encode_block(doc_ids: &[DocId], out: &mut Vec<u8>) {
    debug_assert!(!doc_ids.is_empty());
    debug_assert!(doc_ids.len() <= POSTING_BLOCK_SIZE);

    let min_doc_id = doc_ids[0];

    // Delta-encode
    let mut deltas = Vec::with_capacity(doc_ids.len());
    deltas.push(0u32); // first element's delta is 0 (min_doc_id stored in header)
    for i in 1..doc_ids.len() {
        deltas.push(doc_ids[i] - doc_ids[i - 1]);
    }

    // Determine bit width
    let max_delta = deltas.iter().copied().max().unwrap_or(0);
    let bit_width = if max_delta == 0 {
        0u8
    } else {
        (32 - max_delta.leading_zeros()) as u8
    };

    // Bit-pack
    let packed = bitpack(&deltas, bit_width);

    // Try LZ4 compression — only use if it actually saves space
    let (data, flags_byte, compressed_size, uncompressed_size) = if packed.len() >= 32 {
        let compressed = lz4_flex::compress_prepend_size(&packed);
        if compressed.len() < packed.len() {
            let csz = compressed.len() as u32;
            let usz = packed.len() as u16;
            (compressed, flags::LZ4_COMPRESSED, csz, usz)
        } else {
            (packed, 0u8, 0u32, 0u16)
        }
    } else {
        (packed, 0u8, 0u32, 0u16)
    };

    let actual_compressed_size = if flags_byte & flags::LZ4_COMPRESSED != 0 {
        compressed_size
    } else {
        data.len() as u32
    };

    // Write header (15 bytes)
    out.extend_from_slice(&(doc_ids.len() as u16).to_le_bytes());
    out.extend_from_slice(&min_doc_id.to_le_bytes());
    out.push(bit_width);
    out.push(flags_byte);
    out.extend_from_slice(&actual_compressed_size.to_le_bytes());
    out.extend_from_slice(&uncompressed_size.to_le_bytes());

    // Write data
    out.extend_from_slice(&data);
}

/// Bit-packs an array of values into the minimum number of bytes.
fn bitpack(values: &[u32], bit_width: u8) -> Vec<u8> {
    if bit_width == 0 {
        return Vec::new();
    }
    let total_bits = values.len() * bit_width as usize;
    let num_bytes = total_bits.div_ceil(8);
    let mut buf = vec![0u8; num_bytes];

    let bw = bit_width as usize;
    for (i, &val) in values.iter().enumerate() {
        let bit_offset = i * bw;
        let byte_offset = bit_offset / 8;
        let bit_shift = bit_offset % 8;

        // Write the value starting at the bit position (little-endian bit order)
        let mut v = (val as u64) << bit_shift;
        let bytes_needed = (bit_shift + bw).div_ceil(8);
        for j in 0..bytes_needed {
            if byte_offset + j < buf.len() {
                buf[byte_offset + j] |= (v & 0xFF) as u8;
                v >>= 8;
            }
        }
    }
    buf
}

// --- Decoding ----------------------------------------------------------------

/// Decodes a posting list from its binary representation.
///
/// Returns the sorted `DocId` array.
pub fn decode(data: &[u8]) -> crate::types::Result<Vec<DocId>> {
    let mut doc_ids = Vec::new();
    let mut offset = 0;
    while offset < data.len() {
        offset = decode_block(data, offset, &mut doc_ids)?;
    }
    Ok(doc_ids)
}

/// Reads the `min_doc_id` from a block header without fully decoding.
/// Useful for skip-ahead during intersection.
#[must_use]
pub fn block_min_doc_id(header: &[u8]) -> Option<DocId> {
    if header.len() < BLOCK_HEADER_SIZE {
        return None;
    }
    Some(u32::from_le_bytes([
        header[2], header[3], header[4], header[5],
    ]))
}

fn decode_block(
    data: &[u8],
    offset: usize,
    doc_ids: &mut Vec<DocId>,
) -> crate::types::Result<usize> {
    if offset + BLOCK_HEADER_SIZE > data.len() {
        return Err(SegmentError::PostingDecode(format!(
            "block header truncated at offset {offset}"
        )));
    }

    let num_docs = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
    let min_doc_id = u32::from_le_bytes([
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
    ]);
    let bit_width = data[offset + 6];
    let flags_byte = data[offset + 7];
    let compressed_size = u32::from_le_bytes([
        data[offset + 8],
        data[offset + 9],
        data[offset + 10],
        data[offset + 11],
    ]) as usize;
    let uncompressed_size = u16::from_le_bytes([data[offset + 12], data[offset + 13]]) as usize;

    let data_start = offset + BLOCK_HEADER_SIZE;
    if data_start + compressed_size > data.len() {
        return Err(SegmentError::PostingDecode(format!(
            "block data truncated at offset {data_start}, need {compressed_size} bytes"
        )));
    }

    let packed_data = &data[data_start..data_start + compressed_size];

    let unpacked = if flags_byte & flags::LZ4_COMPRESSED != 0 {
        lz4_flex::decompress_size_prepended(packed_data)
            .map_err(|e| SegmentError::Lz4(e.to_string()))?
    } else {
        packed_data.to_vec()
    };

    // Validate uncompressed size if LZ4 was used
    if flags_byte & flags::LZ4_COMPRESSED != 0 && unpacked.len() != uncompressed_size {
        return Err(SegmentError::PostingDecode(format!(
            "LZ4 uncompressed size mismatch: expected {uncompressed_size}, got {}",
            unpacked.len()
        )));
    }

    // Bit-unpack deltas
    let deltas = bitunpack(&unpacked, num_docs, bit_width);

    // Reconstruct doc_ids from deltas
    let mut current = min_doc_id;
    for (i, delta) in deltas.into_iter().enumerate() {
        if i == 0 {
            doc_ids.push(current);
        } else {
            current += delta;
            doc_ids.push(current);
        }
    }

    Ok(data_start + compressed_size)
}

/// Unpacks bit-packed values.
fn bitunpack(data: &[u8], count: usize, bit_width: u8) -> Vec<u32> {
    if bit_width == 0 {
        return vec![0; count];
    }

    let bw = bit_width as usize;
    let mask = if bw >= 32 { u32::MAX } else { (1u32 << bw) - 1 };
    let mut values = Vec::with_capacity(count);

    for i in 0..count {
        let bit_offset = i * bw;
        let byte_offset = bit_offset / 8;
        let bit_shift = bit_offset % 8;

        // Read up to 5 bytes to cover the value (max 32 bits + shift)
        let mut v: u64 = 0;
        let bytes_needed = (bit_shift + bw).div_ceil(8);
        for j in 0..bytes_needed {
            if byte_offset + j < data.len() {
                v |= (data[byte_offset + j] as u64) << (j * 8);
            }
        }
        values.push(((v >> bit_shift) as u32) & mask);
    }
    values
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_empty() {
        let encoded = encode(&[]);
        assert!(encoded.is_empty());
        let decoded = decode(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn round_trip_single() {
        let ids = vec![42];
        let encoded = encode(&ids);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, ids);
    }

    #[test]
    fn round_trip_dense_sequential() {
        let ids: Vec<DocId> = (0..256).collect();
        let encoded = encode(&ids);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, ids);
    }

    #[test]
    fn round_trip_sparse() {
        let ids: Vec<DocId> = (0..100).map(|i| i * 1000).collect();
        let encoded = encode(&ids);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, ids);
    }

    #[test]
    fn round_trip_large() {
        let ids: Vec<DocId> = (0..10_000).collect();
        let encoded = encode(&ids);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, ids);
    }

    #[test]
    fn block_min_doc_id_works() {
        let ids: Vec<DocId> = vec![100, 200, 300];
        let encoded = encode(&ids);
        assert_eq!(block_min_doc_id(&encoded), Some(100));
    }

    #[test]
    fn dense_compresses_well() {
        let ids: Vec<DocId> = (0..10_000).collect();
        let encoded = encode(&ids);
        let bits_per_id = (encoded.len() * 8) as f64 / ids.len() as f64;
        // Dense sequential should compress to <2 bits per doc_id (plus header overhead)
        // With block headers (15 bytes per 128 docs), overhead is ~1 bit/doc
        assert!(
            bits_per_id < 4.0,
            "dense encoding too large: {bits_per_id:.1} bits/doc"
        );
    }

    mod proptest_round_trip {
        use super::*;
        use proptest::prelude::*;
        use std::collections::BTreeSet;

        proptest! {
            #[test]
            fn round_trip_random(
                raw_ids in proptest::collection::vec(0u32..100_000, 0..500)
            ) {
                let mut ids: Vec<DocId> = raw_ids.into_iter().collect::<BTreeSet<_>>().into_iter().collect();
                ids.sort();
                let encoded = encode(&ids);
                let decoded = decode(&encoded).unwrap();
                prop_assert_eq!(decoded, ids);
            }

            #[test]
            fn round_trip_dense(count in 0usize..1000) {
                let ids: Vec<DocId> = (0..count as u32).collect();
                let encoded = encode(&ids);
                let decoded = decode(&encoded).unwrap();
                prop_assert_eq!(decoded, ids);
            }

            #[test]
            fn round_trip_sparse_large_range(
                raw_ids in proptest::collection::vec(0u32..1_000_000, 0..200)
            ) {
                let ids: Vec<DocId> = raw_ids.into_iter().collect::<BTreeSet<_>>().into_iter().collect();
                let encoded = encode(&ids);
                let decoded = decode(&encoded).unwrap();
                prop_assert_eq!(decoded, ids);
            }
        }
    }
}
