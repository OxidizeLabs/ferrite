//! SIMD-optimized data processing utilities
//! 
//! This module contains SIMD-optimized functions for data processing.

/// SIMD-optimized data processing utilities
#[allow(dead_code)] // Used in tests and advanced features
pub struct SimdProcessor;

impl SimdProcessor {
    /// Fast memory comparison using optimized byte processing
    pub fn fast_memcmp(a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() {
            return false;
        }

        let len = a.len();
        if len < 64 {
            return a == b; // Fallback for small data
        }

        // Process 8-byte chunks using u64 comparison (faster than byte-by-byte)
        let chunks = len / 8;
        for i in 0..chunks {
            let start = i * 8;
            let a_chunk = u64::from_le_bytes(a[start..start + 8].try_into().unwrap());
            let b_chunk = u64::from_le_bytes(b[start..start + 8].try_into().unwrap());

            if a_chunk != b_chunk {
                return false;
            }
        }

        // Handle remaining bytes
        let remainder = len % 8;
        if remainder > 0 {
            let start = chunks * 8;
            return a[start..] == b[start..];
        }

        true
    }

    /// Fast zero detection using optimized processing
    pub fn is_zero_page(data: &[u8]) -> bool {
        if data.len() < 8 {
            return data.iter().all(|&b| b == 0);
        }

        // Process 8-byte chunks
        let chunks = data.len() / 8;
        for i in 0..chunks {
            let start = i * 8;
            let chunk = u64::from_le_bytes(data[start..start + 8].try_into().unwrap());
            if chunk != 0 {
                return false;
            }
        }

        // Check remainder
        let remainder = data.len() % 8;
        if remainder > 0 {
            let start = chunks * 8;
            return data[start..].iter().all(|&b| b == 0);
        }

        true
    }

    /// Fast checksum calculation using optimized processing
    pub fn fast_checksum(data: &[u8]) -> u64 {
        let mut checksum = 0u64;

        // Process 8-byte chunks for better performance
        let chunks = data.len() / 8;
        for i in 0..chunks {
            let start = i * 8;
            let chunk = u64::from_le_bytes(data[start..start + 8].try_into().unwrap());
            checksum = checksum.wrapping_add(chunk);
        }

        // Handle remainder
        let remainder = data.len() % 8;
        if remainder > 0 {
            let start = chunks * 8;
            for &byte in &data[start..] {
                checksum = checksum.wrapping_add(byte as u64);
            }
        }

        checksum
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fast_memcmp() {
        let a = vec![1u8; 1024];
        let b = vec![1u8; 1024];
        let c = vec![2u8; 1024];
        
        assert!(SimdProcessor::fast_memcmp(&a, &b));
        assert!(!SimdProcessor::fast_memcmp(&a, &c));
    }

    #[test]
    fn test_is_zero_page() {
        let zeros = vec![0u8; 1024];
        let non_zeros = vec![0u8; 1023];
        let non_zeros = [non_zeros, vec![1u8]].concat();
        
        assert!(SimdProcessor::is_zero_page(&zeros));
        assert!(!SimdProcessor::is_zero_page(&non_zeros));
    }

    #[test]
    fn test_fast_checksum() {
        let data = vec![1u8; 1024];
        let checksum = SimdProcessor::fast_checksum(&data);
        
        assert_ne!(checksum, 0);
    }
}