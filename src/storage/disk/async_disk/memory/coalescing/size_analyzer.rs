//! Advanced size analysis for write coalescing
//! 
//! This module provides sophisticated size calculation and analysis capabilities
//! for determining the efficiency and memory footprint of coalescing operations.

use crate::common::config::PageId;
use std::collections::HashMap;
use std::io::{Result as IoResult, Error as IoError, ErrorKind};

/// Represents a contiguous range of pages for coalescing calculations
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRange {
    pub start: PageId,
    pub end: PageId,   // Inclusive
    pub size_bytes: usize,
}

/// Size calculation result with detailed breakdown
#[derive(Debug, Clone)]
pub struct CoalescedSizeInfo {
    /// Total size including all overhead
    pub total_size: usize,
    /// Raw data size without overhead
    pub data_size: usize,
    /// Metadata overhead (headers, alignment, etc.)
    pub metadata_overhead: usize,
    /// Number of page ranges (contiguous segments)
    pub num_ranges: usize,
    /// Compression savings estimate (if applicable)
    pub compression_savings: usize,
    /// Memory alignment padding
    pub alignment_padding: usize,
    /// Whether this coalescing is efficient (good ratio)
    pub is_efficient: bool,
}

/// Advanced size analyzer for coalescing operations
#[derive(Debug)]
pub struct SizeAnalyzer;

impl SizeAnalyzer {
    /// Creates a new size analyzer
    pub fn new() -> Self {
        Self
    }

    /// Calculates the total size of coalesced data with comprehensive analysis
    pub fn calculate_coalesced_size(
        &self,
        adjacent_pages: &[PageId],
        new_data: &[u8],
        pending_writes: &HashMap<PageId, Vec<u8>>,
    ) -> usize {
        let size_info = self.calculate_detailed_coalesced_size(adjacent_pages, new_data, pending_writes);
        size_info.total_size
    }

    /// Detailed coalescing size calculation with comprehensive analysis
    pub fn calculate_detailed_coalesced_size(
        &self,
        adjacent_pages: &[PageId],
        new_data: &[u8],
        pending_writes: &HashMap<PageId, Vec<u8>>,
    ) -> CoalescedSizeInfo {
        // Constants for production-grade calculations
        const ALIGNMENT_BOUNDARY: usize = 64;   // CPU cache line alignment
        const COMPRESSION_EFFICIENCY_THRESHOLD: f64 = 0.75; // 75% compression efficiency
        const MAX_EFFICIENT_GAP_RATIO: f64 = 0.20; // 20% gap tolerance

        if adjacent_pages.is_empty() {
            return CoalescedSizeInfo {
                total_size: self.calculate_aligned_size(new_data.len()),
                data_size: new_data.len(),
                metadata_overhead: 0,
                num_ranges: 1,
                compression_savings: 0,
                alignment_padding: self.calculate_padding(new_data.len(), ALIGNMENT_BOUNDARY),
                is_efficient: true,
            };
        }

        // Step 1: Build sorted page ranges for gap analysis
        let page_ranges = self.build_page_ranges(adjacent_pages, pending_writes);

        // Step 2: Calculate raw data sizes including current page data
        let mut total_data_size = new_data.len();
        for &page_id in adjacent_pages {
            if let Some(data) = pending_writes.get(&page_id) {
                total_data_size += data.len();
            }
        }

        // Step 3: Analyze gaps and calculate true memory requirements
        let gap_analysis = self.analyze_page_gaps(&page_ranges);
        let effective_span = self.calculate_effective_span(&page_ranges, &gap_analysis);

        // Step 4: Calculate metadata overhead
        let metadata_overhead = self.calculate_metadata_overhead(&page_ranges, &gap_analysis);

        // Step 5: Calculate alignment and padding requirements
        let alignment_padding = self.calculate_alignment_overhead(total_data_size, &page_ranges);

        // Step 6: Estimate compression impact
        let compression_savings = self.estimate_compression_savings(
            total_data_size,
            &page_ranges,
            COMPRESSION_EFFICIENCY_THRESHOLD,
        );

        // Step 7: Calculate final total size
        let base_size = effective_span.unwrap_or(total_data_size);
        let total_size = base_size + metadata_overhead + alignment_padding - compression_savings;

        // Step 8: Efficiency analysis
        let is_efficient = self.analyze_coalescing_efficiency(
            total_data_size,
            total_size,
            &gap_analysis,
            MAX_EFFICIENT_GAP_RATIO,
        );

        CoalescedSizeInfo {
            total_size,
            data_size: total_data_size,
            metadata_overhead,
            num_ranges: page_ranges.len(),
            compression_savings,
            alignment_padding,
            is_efficient,
        }
    }

    /// Builds contiguous page ranges from adjacent pages for efficient analysis
    pub fn build_page_ranges(
        &self,
        adjacent_pages: &[PageId],
        pending_writes: &HashMap<PageId, Vec<u8>>,
    ) -> Vec<PageRange> {
        if adjacent_pages.is_empty() {
            return Vec::new();
        }

        let mut sorted_pages = adjacent_pages.to_vec();
        sorted_pages.sort_unstable();

        let mut ranges = Vec::new();
        let mut current_start = sorted_pages[0];
        let mut current_end = sorted_pages[0];
        
        // Initialize with the first page's data size
        let mut current_size = pending_writes
            .get(&sorted_pages[0])
            .map(|data| data.len())
            .unwrap_or(4096);

        // Start from the second page since we've already processed the first
        for &page_id in &sorted_pages[1..] {
            let page_data_size = pending_writes
                .get(&page_id)
                .map(|data| data.len())
                .unwrap_or(4096); // Default page size

            if page_id == current_end + 1 {
                // Contiguous page, extend current range
                current_end = page_id;
                current_size += page_data_size;
            } else {
                // Gap detected, finalize current range and start new one
                ranges.push(PageRange {
                    start: current_start,
                    end: current_end,
                    size_bytes: current_size,
                });
                current_start = page_id;
                current_end = page_id;
                current_size = page_data_size;
            }
        }

        // Add the final range
        if current_start <= current_end {
            ranges.push(PageRange {
                start: current_start,
                end: current_end,
                size_bytes: current_size,
            });
        }

        ranges
    }

    /// Analyzes gaps between page ranges to optimize memory layout
    pub fn analyze_page_gaps(&self, page_ranges: &[PageRange]) -> Vec<(PageId, PageId, usize)> {
        let mut gaps = Vec::new();

        for i in 1..page_ranges.len() {
            let prev_range = &page_ranges[i - 1];
            let curr_range = &page_ranges[i];

            if curr_range.start > prev_range.end + 1 {
                let gap_start = prev_range.end + 1;
                let gap_end = curr_range.start - 1;
                let gap_size = ((gap_end - gap_start + 1) * 4096) as usize; // Assume 4KB pages
                gaps.push((gap_start, gap_end, gap_size));
            }
        }

        gaps
    }

    /// Calculates the effective memory span considering gaps and efficiency
    pub fn calculate_effective_span(
        &self,
        page_ranges: &[PageRange],
        gaps: &[(PageId, PageId, usize)],
    ) -> Option<usize> {
        if page_ranges.is_empty() {
            return None;
        }

        let total_data_size: usize = page_ranges.iter().map(|r| r.size_bytes).sum();
        let total_gap_size: usize = gaps.iter().map(|(_, _, size)| *size).sum();

        // If gaps are too large relative to data, don't coalesce as single span
        let gap_ratio = if total_data_size > 0 {
            total_gap_size as f64 / total_data_size as f64
        } else {
            0.0
        };

        if gap_ratio > 0.5 {
            // Too many gaps, calculate as separate ranges
            Some(total_data_size)
        } else {
            // Calculate full span including gaps
            let first_range = page_ranges.first()?;
            let last_range = page_ranges.last()?;
            let span_pages = last_range.end - first_range.start + 1;
            Some((span_pages * 4096) as usize) // Full span in bytes
        }
    }

    /// Calculates metadata overhead for coalesced ranges
    pub fn calculate_metadata_overhead(
        &self,
        page_ranges: &[PageRange],
        gaps: &[(PageId, PageId, usize)],
    ) -> usize {
        const RANGE_HEADER_SIZE: usize = 32;
        const PAGE_METADATA_SIZE: usize = 8;
        const GAP_METADATA_SIZE: usize = 16;

        let range_overhead = page_ranges.len() * RANGE_HEADER_SIZE;
        let page_overhead = page_ranges
            .iter()
            .map(|r| (r.end - r.start + 1) as usize * PAGE_METADATA_SIZE)
            .sum::<usize>();
        let gap_overhead = gaps.len() * GAP_METADATA_SIZE;

        range_overhead + page_overhead + gap_overhead
    }

    /// Calculates alignment and padding overhead for optimal memory access
    pub fn calculate_alignment_overhead(&self, data_size: usize, page_ranges: &[PageRange]) -> usize {
        const CACHE_LINE_SIZE: usize = 64;
        const PAGE_ALIGNMENT: usize = 4096;

        // Align each range to cache line boundaries
        let alignment_per_range = page_ranges.len() * CACHE_LINE_SIZE;

        // Additional page alignment if needed
        let page_alignment_overhead = if data_size % PAGE_ALIGNMENT != 0 {
            PAGE_ALIGNMENT - (data_size % PAGE_ALIGNMENT)
        } else {
            0
        };

        alignment_per_range + page_alignment_overhead
    }

    /// Estimates compression savings for coalesced data
    pub fn estimate_compression_savings(
        &self,
        data_size: usize,
        page_ranges: &[PageRange],
        efficiency_threshold: f64,
    ) -> usize {
        if page_ranges.len() < 2 {
            return 0; // No compression benefit for single pages
        }

        // Estimate based on data patterns and repetition potential
        let repetition_factor = (page_ranges.len() as f64).sqrt() / 10.0; // Heuristic
        let base_compression_ratio = 0.3 + repetition_factor.min(0.4); // 30-70% compression

        if base_compression_ratio >= efficiency_threshold {
            (data_size as f64 * base_compression_ratio) as usize
        } else {
            0
        }
    }

    /// Analyzes whether coalescing is memory-efficient
    pub fn analyze_coalescing_efficiency(
        &self,
        data_size: usize,
        total_size: usize,
        gaps: &[(PageId, PageId, usize)],
        max_gap_ratio: f64,
    ) -> bool {
        if data_size == 0 {
            return false;
        }

        // Check overhead ratio
        let overhead_ratio = if total_size > data_size {
            (total_size - data_size) as f64 / data_size as f64
        } else {
            0.0
        };

        // Check gap ratio
        let total_gap_size: usize = gaps.iter().map(|(_, _, size)| *size).sum();
        let gap_ratio = total_gap_size as f64 / data_size as f64;

        // Efficient if overhead is reasonable and gaps are acceptable
        overhead_ratio < 0.25 && gap_ratio <= max_gap_ratio
    }

    /// Calculates memory-aligned size for optimal performance
    pub fn calculate_aligned_size(&self, size: usize) -> usize {
        const ALIGNMENT: usize = 64; // Cache line alignment
        (size + ALIGNMENT - 1) & !(ALIGNMENT - 1)
    }

    /// Calculates padding needed for alignment
    pub fn calculate_padding(&self, size: usize, alignment: usize) -> usize {
        let aligned_size = (size + alignment - 1) & !(alignment - 1);
        aligned_size - size
    }

    /// Validates that page ranges are properly ordered and non-overlapping
    pub fn validate_page_ranges(&self, page_ranges: &[PageRange]) -> IoResult<()> {
        for i in 1..page_ranges.len() {
            let prev = &page_ranges[i - 1];
            let curr = &page_ranges[i];

            if curr.start <= prev.end {
                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    format!(
                        "Overlapping or out-of-order page ranges: [{}, {}] and [{}, {}]",
                        prev.start, prev.end, curr.start, curr.end
                    ),
                ));
            }
        }
        Ok(())
    }
}

impl Default for SizeAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_size_analyzer_creation() {
        let analyzer = SizeAnalyzer::new();
        // Just verify it can be created
        assert_eq!(analyzer.calculate_aligned_size(1), 64);
    }

    #[test]
    fn test_calculate_detailed_coalesced_size_empty() {
        let analyzer = SizeAnalyzer::new();
        let pending_writes = HashMap::new();
        let data = vec![1, 2, 3, 4];

        let size_info = analyzer.calculate_detailed_coalesced_size(&[], &data, &pending_writes);

        assert_eq!(size_info.data_size, 4);
        assert_eq!(size_info.num_ranges, 1);
        assert_eq!(size_info.compression_savings, 0);
        assert!(size_info.is_efficient);
        assert!(size_info.total_size >= size_info.data_size);
    }

    #[test]
    fn test_build_page_ranges_contiguous() {
        let analyzer = SizeAnalyzer::new();
        let mut pending_writes = HashMap::new();
        pending_writes.insert(1, vec![0; 100]);
        pending_writes.insert(2, vec![0; 200]);
        pending_writes.insert(3, vec![0; 150]);

        let adjacent_pages = vec![1, 2, 3];
        let ranges = analyzer.build_page_ranges(&adjacent_pages, &pending_writes);

        assert_eq!(ranges.len(), 1); // Should be one contiguous range
        assert_eq!(ranges[0].start, 1);
        assert_eq!(ranges[0].end, 3);
        assert_eq!(ranges[0].size_bytes, 100 + 200 + 150);
    }

    #[test]
    fn test_build_page_ranges_with_gaps() {
        let analyzer = SizeAnalyzer::new();
        let mut pending_writes = HashMap::new();
        pending_writes.insert(1, vec![0; 100]);
        pending_writes.insert(3, vec![0; 150]);
        pending_writes.insert(6, vec![0; 200]);

        let adjacent_pages = vec![1, 3, 6];
        let ranges = analyzer.build_page_ranges(&adjacent_pages, &pending_writes);

        assert_eq!(ranges.len(), 3); // Should be three separate ranges due to gaps
        assert_eq!(ranges[0].start, 1);
        assert_eq!(ranges[0].end, 1);
        assert_eq!(ranges[1].start, 3);
        assert_eq!(ranges[1].end, 3);
        assert_eq!(ranges[2].start, 6);
        assert_eq!(ranges[2].end, 6);
    }

    #[test]
    fn test_analyze_page_gaps() {
        let analyzer = SizeAnalyzer::new();
        let page_ranges = vec![
            PageRange {
                start: 1,
                end: 2,
                size_bytes: 300,
            },
            PageRange {
                start: 5,
                end: 6,
                size_bytes: 400,
            },
            PageRange {
                start: 10,
                end: 10,
                size_bytes: 100,
            },
        ];

        let gaps = analyzer.analyze_page_gaps(&page_ranges);

        assert_eq!(gaps.len(), 2);
        // Gap between ranges 1-2 and 5-6: pages 3-4
        assert_eq!(gaps[0], (3, 4, 2 * 4096));
        // Gap between ranges 5-6 and 10: pages 7-9
        assert_eq!(gaps[1], (7, 9, 3 * 4096));
    }

    #[test]
    fn test_calculate_effective_span() {
        let analyzer = SizeAnalyzer::new();

        // Test with small gaps (should use full span)
        let page_ranges = vec![
            PageRange {
                start: 1,
                end: 2,
                size_bytes: 8192,
            },
            PageRange {
                start: 4,
                end: 5,
                size_bytes: 8192,
            },
        ];
        let small_gaps = vec![(3, 3, 4096)]; // Small gap

        let span = analyzer.calculate_effective_span(&page_ranges, &small_gaps);
        assert_eq!(span, Some(5 * 4096)); // Full span from page 1 to 5

        // Test with large gaps (should use separate ranges)
        let large_gaps = vec![(3, 10, 8 * 4096)]; // Large gap
        let span_with_large_gaps = analyzer.calculate_effective_span(&page_ranges, &large_gaps);
        assert_eq!(span_with_large_gaps, Some(8192 + 8192)); // Just the data sizes
    }

    #[test]
    fn test_calculate_metadata_overhead() {
        let analyzer = SizeAnalyzer::new();
        let page_ranges = vec![
            PageRange {
                start: 1,
                end: 3,
                size_bytes: 300,
            },
            PageRange {
                start: 6,
                end: 8,
                size_bytes: 400,
            },
        ];
        let gaps = vec![(4, 5, 2 * 4096)];

        let overhead = analyzer.calculate_metadata_overhead(&page_ranges, &gaps);

        // Should include range headers, page metadata, and gap metadata
        assert!(overhead > 0);
        assert!(overhead > page_ranges.len() * 32); // At least range headers
    }

    #[test]
    fn test_calculate_alignment_overhead() {
        let analyzer = SizeAnalyzer::new();
        let page_ranges = vec![
            PageRange {
                start: 1,
                end: 2,
                size_bytes: 200,
            },
            PageRange {
                start: 4,
                end: 4,
                size_bytes: 100,
            },
        ];

        let overhead = analyzer.calculate_alignment_overhead(300, &page_ranges);

        // Should include cache line alignment for ranges plus page alignment
        assert!(overhead > 0);
        assert!(overhead >= page_ranges.len() * 64); // At least cache line per range
    }

    #[test]
    fn test_estimate_compression_savings() {
        let analyzer = SizeAnalyzer::new();

        // Single page - no compression benefit
        let single_range = vec![PageRange {
            start: 1,
            end: 1,
            size_bytes: 1000,
        }];
        let savings_single = analyzer.estimate_compression_savings(1000, &single_range, 0.75);
        assert_eq!(savings_single, 0);

        // Multiple pages - should have compression benefit
        let multi_ranges = vec![
            PageRange {
                start: 1,
                end: 3,
                size_bytes: 1000,
            },
            PageRange {
                start: 5,
                end: 7,
                size_bytes: 1000,
            },
        ];
        let savings_multi = analyzer.estimate_compression_savings(2000, &multi_ranges, 0.4);
        assert!(savings_multi > 0);
        assert!(savings_multi <= 2000); // Can't save more than original size
    }

    #[test]
    fn test_analyze_coalescing_efficiency() {
        let analyzer = SizeAnalyzer::new();

        // Efficient case - low overhead, small gaps
        let small_gaps = vec![(3, 3, 100)];
        let efficient = analyzer.analyze_coalescing_efficiency(1000, 1200, &small_gaps, 0.2);
        assert!(efficient);

        // Inefficient case - high overhead
        let inefficient_overhead = analyzer.analyze_coalescing_efficiency(1000, 1500, &[], 0.2);
        assert!(!inefficient_overhead);

        // Inefficient case - large gaps
        let large_gaps = vec![(3, 10, 500)];
        let inefficient_gaps = analyzer.analyze_coalescing_efficiency(1000, 1200, &large_gaps, 0.2);
        assert!(!inefficient_gaps);
    }

    #[test]
    fn test_calculate_aligned_size() {
        let analyzer = SizeAnalyzer::new();

        // Test various sizes
        assert_eq!(analyzer.calculate_aligned_size(1), 64); // Rounds up to 64
        assert_eq!(analyzer.calculate_aligned_size(64), 64); // Already aligned
        assert_eq!(analyzer.calculate_aligned_size(65), 128); // Rounds up to 128
        assert_eq!(analyzer.calculate_aligned_size(100), 128); // Rounds up to 128
    }

    #[test]
    fn test_calculate_padding() {
        let analyzer = SizeAnalyzer::new();

        assert_eq!(analyzer.calculate_padding(1, 64), 63); // 63 bytes padding
        assert_eq!(analyzer.calculate_padding(64, 64), 0); // No padding needed
        assert_eq!(analyzer.calculate_padding(65, 64), 63); // 63 bytes padding
        assert_eq!(analyzer.calculate_padding(100, 32), 28); // 28 bytes padding
    }

    #[test]
    fn test_validate_page_ranges_valid() {
        let analyzer = SizeAnalyzer::new();
        let valid_ranges = vec![
            PageRange {
                start: 1,
                end: 3,
                size_bytes: 300,
            },
            PageRange {
                start: 5,
                end: 7,
                size_bytes: 400,
            },
            PageRange {
                start: 10,
                end: 10,
                size_bytes: 100,
            },
        ];

        let result = analyzer.validate_page_ranges(&valid_ranges);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_page_ranges_invalid() {
        let analyzer = SizeAnalyzer::new();
        let invalid_ranges = vec![
            PageRange {
                start: 1,
                end: 5,
                size_bytes: 300,
            },
            PageRange {
                start: 3,
                end: 7,
                size_bytes: 400,
            }, // Overlaps with previous range
        ];

        let result = analyzer.validate_page_ranges(&invalid_ranges);
        assert!(result.is_err());
    }
} 