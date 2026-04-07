// Split Block Bloom Filter (SBBF) evaluation.
// Parquet-compatible: 256-bit blocks (8 × u32 LE), xxHash64 with seed 0.

/// Checks if a value might be present in an SBBF.
///
/// Returns `true` if the value might be present (no false negatives),
/// `false` if definitely absent (no false positives on `false`).
pub fn sbbf_contains(filter_data: &[u8], value: &[u8]) -> bool {
    if filter_data.len() < 32 {
        return false;
    }

    let hash = xxhash_rust::xxh64::xxh64(value, 0);
    let num_blocks = filter_data.len() / 32;
    if num_blocks == 0 {
        return false;
    }

    // Select block using upper 32 bits of hash.
    let block_index = ((hash >> 32) as usize) % num_blocks;
    let block_offset = block_index * 32;
    let block = &filter_data[block_offset..block_offset + 32];

    // Check 8 lanes using lower 32 bits with Parquet SBBF salt values.
    let key = hash as u32;
    const SALT: [u32; 8] = [
        0x47b6_137b,
        0x4497_4d91,
        0x8824_ad5b,
        0xa2b7_289d,
        0x7054_95c7,
        0x2df1_424b,
        0x9efc_4947,
        0x05c6_377d,
    ];

    for i in 0..8 {
        let lane_word = u32::from_le_bytes([
            block[i * 4],
            block[i * 4 + 1],
            block[i * 4 + 2],
            block[i * 4 + 3],
        ]);
        let bit_index = key.wrapping_mul(SALT[i]) >> 27;
        if lane_word & (1 << bit_index) == 0 {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xxhash64_known_values() {
        // Verify the xxhash crate produces expected values.
        assert_eq!(xxhash_rust::xxh64::xxh64(b"", 0), 0xef46_db37_51d8_e999);
    }

    #[test]
    fn sbbf_empty_filter() {
        assert!(!sbbf_contains(&[], b"test"));
    }

    #[test]
    fn sbbf_all_zeros() {
        assert!(!sbbf_contains(&[0u8; 32], b"test"));
    }

    #[test]
    fn sbbf_all_ones() {
        assert!(sbbf_contains(&[0xff; 32], b"test"));
        assert!(sbbf_contains(&[0xff; 32], b"anything"));
    }

    #[test]
    fn sbbf_insert_and_check() {
        // Build a filter with one value inserted.
        let mut filter = vec![0u8; 32];
        let value = b"user-12345";
        let hash = xxhash_rust::xxh64::xxh64(value, 0);

        let key = hash as u32;
        const SALT: [u32; 8] = [
            0x47b6_137b,
            0x4497_4d91,
            0x8824_ad5b,
            0xa2b7_289d,
            0x7054_95c7,
            0x2df1_424b,
            0x9efc_4947,
            0x05c6_377d,
        ];

        // Set bits for this value.
        for (i, salt) in SALT.iter().enumerate() {
            let bit_index = key.wrapping_mul(*salt) >> 27;
            let word_offset = i * 4;
            let mut word = u32::from_le_bytes([
                filter[word_offset],
                filter[word_offset + 1],
                filter[word_offset + 2],
                filter[word_offset + 3],
            ]);
            word |= 1 << bit_index;
            filter[word_offset..word_offset + 4].copy_from_slice(&word.to_le_bytes());
        }

        // Should find the inserted value.
        assert!(sbbf_contains(&filter, value));
        // Should NOT find a different value (with high probability).
        assert!(!sbbf_contains(&filter, b"user-99999"));
    }
}
