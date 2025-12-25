//! Compression module for the Async Disk Manager.
//!
//! This module provides the `CompressionEngine` which handles data compression and decompression
//! using various algorithms (LZ4, Zstd) or a custom RLE format. It abstracts the complexity
//! of different compression libraries and provides a unified interface for the disk manager
//! to optimize storage space and I/O performance.
//!
//! # Supported Algorithms
//!
//! - **LZ4**: Fast compression/decompression, suitable for high-throughput scenarios.
//! - **Zstd**: High compression ratio, suitable for archival or storage optimization.
//! - **None**: No compression.
//!
//! The module also handles magic bytes detection to automatically determine the compression
//! format during decompression.

use std::io::Cursor;

const MAGIC_LZ4: u8 = 0xF0;
const MAGIC_ZSTD: u8 = 0xF1;

/// Compression algorithms supported by the system
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    /// No compression
    None,
    /// LZ4 compression (fast)
    LZ4,
    /// Zstd compression (high ratio)
    Zstd,
}

/// Compression engine that provides various compression algorithms
#[derive(Debug)]
pub struct CompressionEngine {
    // Track compression statistics
    compression_ratio: f64,
    bytes_before_compression: u64,
    bytes_after_compression: u64,
}

impl Default for CompressionEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl CompressionEngine {
    /// Creates a new compression engine
    pub fn new() -> Self {
        Self {
            compression_ratio: 1.0,
            bytes_before_compression: 0,
            bytes_after_compression: 0,
        }
    }

    /// Compresses data using the specified algorithm
    pub fn compress_data(
        &mut self,
        data: &[u8],
        algorithm: CompressionAlgorithm,
        level: u32,
    ) -> Vec<u8> {
        let original_size = data.len() as u64;

        let compressed = match algorithm {
            CompressionAlgorithm::None => data.to_vec(),
            CompressionAlgorithm::LZ4 => self.compress_lz4(data),
            CompressionAlgorithm::Zstd => self.compress_zstd(data, level),
        };

        // Update compression statistics
        let compressed_size = compressed.len() as u64;
        self.bytes_before_compression += original_size;
        self.bytes_after_compression += compressed_size;

        if self.bytes_before_compression > 0 {
            self.compression_ratio =
                self.bytes_before_compression as f64 / self.bytes_after_compression as f64;
        }

        compressed
    }

    /// LZ4 compression for high-speed compression
    fn compress_lz4(&self, data: &[u8]) -> Vec<u8> {
        if data.is_empty() {
            return Vec::new();
        }

        // Use lz4_flex for compression (prepend size for easier decompression)
        let compressed = lz4_flex::compress_prepend_size(data);

        // Add magic header
        let mut result = Vec::with_capacity(1 + compressed.len());
        result.push(MAGIC_LZ4);
        result.extend_from_slice(&compressed);

        result
    }

    /// Zstd compression for high compression ratios
    fn compress_zstd(&self, data: &[u8], level: u32) -> Vec<u8> {
        if data.is_empty() {
            return Vec::new();
        }

        // Use zstd for compression
        let compressed = zstd::stream::encode_all(Cursor::new(data), level as i32)
            .expect("Zstd compression failed");

        let mut result = Vec::with_capacity(1 + compressed.len());
        result.push(MAGIC_ZSTD);
        result.extend_from_slice(&compressed);

        result
    }

    /// Decompresses data based on detected format
    pub fn decompress_data(&self, compressed: &[u8]) -> Vec<u8> {
        if compressed.is_empty() {
            return Vec::new();
        }

        // Check for LZ4 magic header
        if compressed[0] == MAGIC_LZ4 {
            return lz4_flex::decompress_size_prepended(&compressed[1..])
                .expect("LZ4 decompression failed");
        }

        // Check for Zstd magic header
        if compressed[0] == MAGIC_ZSTD {
            return zstd::stream::decode_all(Cursor::new(&compressed[1..]))
                .expect("Zstd decompression failed");
        }

        if compressed.len() < 2 {
            return compressed.to_vec();
        }

        // Check for zero page marker
        if compressed[0] == 0xFF && compressed[1] == 0xFE && compressed.len() == 6 {
            let length =
                u32::from_le_bytes([compressed[2], compressed[3], compressed[4], compressed[5]])
                    as usize;
            return vec![0u8; length];
        }

        // Decompress custom RLE format
        let mut decompressed = Vec::new();
        let mut i = 0;

        while i < compressed.len() {
            if compressed[i] == 0xFF {
                if i + 1 >= compressed.len() {
                    // Invalid format, just add the byte
                    decompressed.push(compressed[i]);
                    i += 1;
                    continue;
                }

                if compressed[i + 1] == 0x00 {
                    // Escaped 0xFF
                    decompressed.push(0xFF);
                    i += 2;
                } else if compressed[i + 1] == 0xFE {
                    // Zero page marker, should have been handled above
                    // Just skip it
                    i += 2;
                } else if i + 2 < compressed.len() {
                    // RLE sequence
                    let count = compressed[i + 1] as usize;
                    let byte = compressed[i + 2];
                    for _ in 0..count {
                        decompressed.push(byte);
                    }
                    i += 3;
                } else {
                    // Invalid format, just add the byte
                    decompressed.push(compressed[i]);
                    i += 1;
                }
            } else {
                // Regular byte
                decompressed.push(compressed[i]);
                i += 1;
            }
        }

        decompressed
    }

    /// Gets the current compression ratio
    pub fn get_compression_ratio(&self) -> f64 {
        self.compression_ratio
    }

    /// Gets compression statistics
    pub fn get_stats(&self) -> (u64, u64, f64) {
        (
            self.bytes_before_compression,
            self.bytes_after_compression,
            self.compression_ratio,
        )
    }
}

/// Result of a compression operation
#[derive(Debug)]
pub struct CompressionResult {
    /// Original data size
    pub original_size: usize,
    /// Compressed data size
    pub compressed_size: usize,
    /// Compression ratio (original/compressed)
    pub ratio: f64,
    /// Algorithm used
    pub algorithm: CompressionAlgorithm,
    /// Whether compression was effective
    pub effective: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lz4_compression_roundtrip() {
        let mut engine = CompressionEngine::new();
        let data = b"Hello, LZ4 compression! This is a test string that should be compressed.";

        let compressed = engine.compress_data(data, CompressionAlgorithm::LZ4, 0);
        assert!(!compressed.is_empty());
        assert_eq!(compressed[0], MAGIC_LZ4);

        let decompressed = engine.decompress_data(&compressed);
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_compression_roundtrip() {
        let mut engine = CompressionEngine::new();
        let data = b"Hello, Zstd compression! This is a test string that should be compressed with high ratio.";

        let compressed = engine.compress_data(data, CompressionAlgorithm::Zstd, 3);
        assert!(!compressed.is_empty());
        assert_eq!(compressed[0], MAGIC_ZSTD);

        let decompressed = engine.decompress_data(&compressed);
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_no_compression_roundtrip() {
        let mut engine = CompressionEngine::new();
        let data = b"Hello, No compression!";

        let compressed = engine.compress_data(data, CompressionAlgorithm::None, 0);
        assert_eq!(compressed, data);

        let decompressed = engine.decompress_data(&compressed);
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_empty_data() {
        let mut engine = CompressionEngine::new();
        let data = b"";

        let compressed_lz4 = engine.compress_data(data, CompressionAlgorithm::LZ4, 0);
        assert!(compressed_lz4.is_empty());
        let decompressed_lz4 = engine.decompress_data(&compressed_lz4);
        assert!(decompressed_lz4.is_empty());

        let compressed_zstd = engine.compress_data(data, CompressionAlgorithm::Zstd, 0);
        assert!(compressed_zstd.is_empty());
        let decompressed_zstd = engine.decompress_data(&compressed_zstd);
        assert!(decompressed_zstd.is_empty());
    }

    #[test]
    fn test_compression_stats() {
        let mut engine = CompressionEngine::new();
        let data = vec![0u8; 1000]; // Highly compressible data

        let compressed = engine.compress_data(&data, CompressionAlgorithm::LZ4, 0);

        let (original, comp, ratio) = engine.get_stats();
        assert_eq!(original, 1000);
        assert_eq!(comp, compressed.len() as u64);
        assert!(ratio > 1.0);
    }

    #[test]
    fn test_fallback_decompression() {
        let engine = CompressionEngine::new();
        // Test data without magic header (simulating old format or uncompressed data that falls through)
        let data = b"some random data";
        let decompressed = engine.decompress_data(data);
        assert_eq!(decompressed, data);
    }
}
