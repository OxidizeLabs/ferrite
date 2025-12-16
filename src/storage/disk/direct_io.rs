//! Platform-specific direct I/O implementation for bypassing OS page cache
//!
//! This module provides cross-platform direct I/O functionality to ensure
//! database operations go directly to disk rather than being buffered by the OS.

use log::{debug, warn};
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::fs::{File, OpenOptions};
use std::io::{Result as IoResult, Write};
use std::ops::{Deref, DerefMut};
use std::path::Path;

/// Configuration for direct I/O operations
#[derive(Debug, Clone)]
pub struct DirectIOConfig {
    pub enabled: bool,
    pub alignment: usize, // Buffer alignment requirement (typically 512 bytes)
}

impl Default for DirectIOConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            alignment: 512, // Standard sector size
        }
    }
}

/// Platform-specific direct I/O file opening
pub fn open_direct_io<P: AsRef<Path>>(
    path: P,
    read: bool,
    write: bool,
    create: bool,
    config: &DirectIOConfig,
) -> IoResult<File> {
    let mut options = OpenOptions::new();
    options.read(read).write(write).create(create);

    if config.enabled {
        debug!("Opening file with direct I/O: {}", path.as_ref().display());

        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_DIRECT);
        }

        #[cfg(target_os = "macos")]
        {
            // macOS doesn't have O_DIRECT, but we can use F_NOCACHE after opening
            // For now, just open normally and set F_NOCACHE later
            debug!("macOS: Direct I/O will be configured after file opening");
        }

        #[cfg(target_os = "windows")]
        {
            use std::os::windows::fs::OpenOptionsExt;
            // FILE_FLAG_NO_BUFFERING = 0x20000000
            options.custom_flags(0x20000000);
        }

        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        {
            warn!("Direct I/O not supported on this platform, falling back to buffered I/O");
        }
    } else {
        debug!(
            "Opening file with buffered I/O: {}",
            path.as_ref().display()
        );
    }

    let file = options.open(path.as_ref())?;

    // Platform-specific post-open configuration
    #[cfg(target_os = "macos")]
    {
        if config.enabled {
            // Set F_NOCACHE to bypass buffer cache
            use std::os::unix::io::AsRawFd;
            unsafe {
                let fd = file.as_raw_fd();
                let result = libc::fcntl(fd, libc::F_NOCACHE, 1);
                if result == -1 {
                    warn!("Failed to set F_NOCACHE on macOS, continuing with buffered I/O");
                } else {
                    debug!("Successfully enabled F_NOCACHE on macOS");
                }
            }
        }
    }

    Ok(file)
}

/// Check if a buffer is properly aligned for direct I/O
pub fn is_aligned(buffer: &[u8], alignment: usize) -> bool {
    (buffer.as_ptr() as usize).is_multiple_of(alignment)
}

/// An aligned buffer that maintains alignment throughout its lifetime.
/// Properly deallocates with the correct layout when dropped.
pub struct AlignedBuffer {
    ptr: *mut u8,
    size: usize,
    layout: Layout,
}

impl AlignedBuffer {
    /// Create a new aligned buffer with zeroed memory
    pub fn new(size: usize, alignment: usize) -> Self {
        let layout = Layout::from_size_align(size, alignment)
            .expect("Invalid layout for aligned buffer");

        let ptr = unsafe { alloc_zeroed(layout) };
        if ptr.is_null() {
            panic!("Failed to allocate aligned buffer");
        }

        debug!(
            "Created aligned buffer: size={}, alignment={}, ptr={:p}",
            size, alignment, ptr
        );

        Self { ptr, size, layout }
    }

    /// Get the buffer as a slice
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.size) }
    }

    /// Get the buffer as a mutable slice
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size) }
    }

    /// Convert to a Vec (copies the data, loses alignment guarantee)
    pub fn to_vec(&self) -> Vec<u8> {
        self.as_slice().to_vec()
    }

    /// Get the length of the buffer
    pub fn len(&self) -> usize {
        self.size
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
}

impl Deref for AlignedBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for AlignedBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr, self.layout);
        }
    }
}

// SAFETY: AlignedBuffer owns its memory exclusively
unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

impl AlignedBuffer {
    /// Create an aligned buffer from existing data, copying the data into aligned memory.
    /// If the data length is not a multiple of alignment, it will be padded with zeros.
    pub fn from_slice(data: &[u8], alignment: usize) -> Self {
        let padded_size = round_up_to_alignment(data.len(), alignment);
        let mut buffer = Self::new(padded_size, alignment);
        buffer.as_mut_slice()[..data.len()].copy_from_slice(data);
        buffer
    }

    /// Create an aligned buffer with a specific size, copying data into it.
    /// The buffer size will be the maximum of the data length (rounded up) and the specified size.
    pub fn from_slice_with_size(data: &[u8], size: usize, alignment: usize) -> Self {
        let padded_size = round_up_to_alignment(size.max(data.len()), alignment);
        let mut buffer = Self::new(padded_size, alignment);
        buffer.as_mut_slice()[..data.len()].copy_from_slice(data);
        buffer
    }
}

/// Round up a size to the nearest multiple of alignment
#[inline]
pub fn round_up_to_alignment(size: usize, alignment: usize) -> usize {
    if alignment == 0 {
        return size;
    }
    (size + alignment - 1) & !(alignment - 1)
}

/// Check if a size is aligned to the given alignment
#[inline]
pub fn is_size_aligned(size: usize, alignment: usize) -> bool {
    if alignment == 0 {
        return true;
    }
    size.is_multiple_of(alignment)
}

/// Create an aligned buffer for direct I/O operations
/// Returns a Vec<u8> for API compatibility - uses AlignedBuffer internally
/// and copies to Vec (which may lose alignment).
pub fn create_aligned_buffer(size: usize, alignment: usize) -> Vec<u8> {
    let aligned = AlignedBuffer::new(size, alignment);
    // For API compatibility, we return a Vec
    // The caller should use AlignedBuffer directly for guaranteed alignment
    aligned.to_vec()
}

/// Create an aligned buffer that maintains alignment (preferred for direct I/O)
pub fn create_aligned_buffer_raw(size: usize, alignment: usize) -> AlignedBuffer {
    AlignedBuffer::new(size, alignment)
}

/// Perform a direct I/O read operation with proper alignment
pub fn read_aligned(
    file: &mut File,
    offset: u64,
    size: usize,
    config: &DirectIOConfig,
) -> IoResult<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};

    file.seek(SeekFrom::Start(offset))?;

    // For direct I/O, we need to read in aligned chunks
    if config.enabled && size % config.alignment != 0 {
        warn!(
            "Direct I/O read size {} is not aligned to {}, this may cause issues",
            size, config.alignment
        );
    }

    if config.enabled {
        // Use properly aligned buffer for direct I/O
        let mut buffer = create_aligned_buffer_raw(size, config.alignment);
        file.read_exact(&mut *buffer)?;
        Ok(buffer.to_vec())
    } else {
        let mut buffer = vec![0u8; size];
        file.read_exact(&mut buffer)?;
        Ok(buffer)
    }
}

/// Perform a direct I/O write operation with proper alignment
pub fn write_aligned(
    file: &mut File,
    offset: u64,
    data: &[u8],
    config: &DirectIOConfig,
) -> IoResult<()> {
    use std::io::{Seek, SeekFrom, Write};

    file.seek(SeekFrom::Start(offset))?;

    if config.enabled {
        // Check alignment requirements
        if !is_aligned(data, config.alignment) {
            warn!("Data buffer is not aligned for direct I/O, performance may be degraded");
        }

        if data.len() % config.alignment != 0 {
            warn!(
                "Write size {} is not aligned to {}, this may cause issues with direct I/O",
                data.len(),
                config.alignment
            );
        }
    }

    file.write_all(data)?;

    Ok(())
}

/// Force synchronization to disk based on policy
pub fn sync_file(file: &mut File, force_sync: bool) -> IoResult<()> {
    if force_sync {
        debug!("Performing fsync to ensure data durability");
        file.sync_all()?;
    } else {
        debug!("Performing flush to OS buffers");
        file.flush()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_aligned_buffer_creation() {
        // Use AlignedBuffer directly for guaranteed alignment
        let buffer = create_aligned_buffer_raw(4096, 512);
        assert_eq!(buffer.len(), 4096);
        assert!(is_aligned(&buffer, 512));
    }

    #[test]
    fn test_aligned_buffer_from_slice() {
        let data = vec![1u8, 2, 3, 4, 5];
        let buffer = AlignedBuffer::from_slice(&data, 512);

        // Should be padded to 512 bytes
        assert_eq!(buffer.len(), 512);
        assert!(is_aligned(&buffer, 512));

        // Data should be preserved
        assert_eq!(&buffer.as_slice()[..5], &data[..]);

        // Rest should be zeros
        assert!(buffer.as_slice()[5..].iter().all(|&b| b == 0));
    }

    #[test]
    fn test_aligned_buffer_from_slice_page_size() {
        let data = vec![42u8; 4096];
        let buffer = AlignedBuffer::from_slice(&data, 4096);

        assert_eq!(buffer.len(), 4096);
        assert!(is_aligned(&buffer, 4096));
        assert_eq!(buffer.as_slice(), &data[..]);
    }

    #[test]
    fn test_round_up_to_alignment() {
        assert_eq!(round_up_to_alignment(0, 512), 0);
        assert_eq!(round_up_to_alignment(1, 512), 512);
        assert_eq!(round_up_to_alignment(512, 512), 512);
        assert_eq!(round_up_to_alignment(513, 512), 1024);
        assert_eq!(round_up_to_alignment(4096, 4096), 4096);
        assert_eq!(round_up_to_alignment(4097, 4096), 8192);
    }

    #[test]
    fn test_is_size_aligned() {
        assert!(is_size_aligned(0, 512));
        assert!(is_size_aligned(512, 512));
        assert!(is_size_aligned(1024, 512));
        assert!(!is_size_aligned(1, 512));
        assert!(!is_size_aligned(513, 512));
        assert!(is_size_aligned(4096, 4096));
    }

    #[test]
    fn test_file_opening() {
        let temp_file = NamedTempFile::new().unwrap();
        let config = DirectIOConfig::default();

        let file = open_direct_io(temp_file.path(), true, true, false, &config);
        assert!(file.is_ok());
    }

    #[test]
    fn test_direct_io_enabled() {
        let temp_file = NamedTempFile::new().unwrap();
        let config = DirectIOConfig {
            enabled: true,
            alignment: 512,
        };

        // This should work on most platforms (may fall back to buffered I/O on unsupported platforms)
        let file = open_direct_io(temp_file.path(), true, true, false, &config);
        assert!(file.is_ok());
    }
}
