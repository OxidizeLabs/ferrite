//! # Direct I/O
//!
//! This module provides cross-platform direct I/O functionality to bypass the OS page cache,
//! ensuring database operations go directly to disk. This is critical for database systems
//! to maintain control over buffer management and guarantee transaction durability.
//!
//! ## Architecture
//!
//! ```text
//!   Standard Buffered I/O                      Direct I/O
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   ┌─────────────────┐                    ┌─────────────────┐
//!   │   Application   │                    │   Application   │
//!   │  (BufferPool)   │                    │  (BufferPool)   │
//!   └────────┬────────┘                    └────────┬────────┘
//!            │                                      │
//!            ▼                                      │
//!   ┌─────────────────┐                             │
//!   │  OS Page Cache  │  ← double buffering!       │ bypassed!
//!   │  (wastes RAM)   │                             │
//!   └────────┬────────┘                             │
//!            │                                      │
//!            ▼                                      ▼
//!   ┌─────────────────┐                    ┌─────────────────┐
//!   │      Disk       │                    │      Disk       │
//!   └─────────────────┘                    └─────────────────┘
//!
//!   Problems with buffered I/O:            Benefits of direct I/O:
//!   • Double caching (app + OS)            • Single cache (app only)
//!   • Unpredictable eviction               • Controlled eviction (LRU-K)
//!   • fsync() may be delayed               • Immediate durability
//!   • Wasted memory                        • Predictable memory usage
//! ```
//!
//! ## Platform Support
//!
//! | Platform | Mechanism               | Alignment Required | Notes                    |
//! |----------|-------------------------|--------------------|--------------------------|
//! | Linux    | `O_DIRECT` flag         | **Yes** (strict)   | Buffer, offset, size     |
//! | macOS    | `fcntl(F_NOCACHE)`      | No (recommended)   | Best-effort cache bypass |
//! | Windows  | `FILE_FLAG_NO_BUFFERING`| **Yes** (strict)   | Buffer, offset, size     |
//! | Others   | Buffered I/O fallback   | No                 | Warning logged           |
//!
//! ## AlignedBuffer Memory Layout
//!
//! ```text
//!   AlignedBuffer (alignment = 512)
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   Memory Address:  0x...000  0x...200  0x...400  0x...600  0x...800
//!                    │         │         │         │         │
//!                    ▼         ▼         ▼         ▼         ▼
//!   ┌────────────────┬─────────┬─────────┬─────────┬─────────┐
//!   │  Sector 0      │ Sector 1│ Sector 2│ Sector 3│   ...   │
//!   │  (512 bytes)   │         │         │         │         │
//!   └────────────────┴─────────┴─────────┴─────────┴─────────┘
//!   ▲
//!   │
//!   ptr is aligned to 512-byte boundary (ptr % 512 == 0)
//!
//!   Layout: { ptr: *mut u8, size: usize, layout: Layout }
//!   - Allocated via std::alloc::alloc_zeroed with proper Layout
//!   - Deallocated in Drop with matching Layout
//!   - Send + Sync for cross-thread usage
//! ```
//!
//! ## Key Components
//!
//! | Component          | Description                                          |
//! |--------------------|------------------------------------------------------|
//! | `DirectIOConfig`   | Configuration: enabled flag and alignment (512/4096)|
//! | `AlignedBuffer`    | Heap-allocated buffer with guaranteed alignment      |
//! | `open_direct_io()` | Open file with platform-specific direct I/O flags    |
//! | `read_aligned()`   | Read into new AlignedBuffer with alignment handling  |
//! | `read_aligned_into()`| Read into user-provided buffer (bounce if needed) |
//! | `write_aligned()`  | Write with alignment validation (or bounce buffer)   |
//! | `sync_file()`      | fsync or flush based on durability requirements      |
//!
//! ## Core Operations
//!
//! | Function                  | Description                                    |
//! |---------------------------|------------------------------------------------|
//! | `create_aligned_buffer()` | Allocate zeroed buffer with alignment          |
//! | `round_up_to_alignment()` | Round size up to alignment boundary            |
//! | `is_aligned()`            | Check if buffer pointer is aligned             |
//! | `is_size_aligned()`       | Check if size is multiple of alignment         |
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::storage::disk::direct_io::{
//!     DirectIOConfig, AlignedBuffer, open_direct_io,
//!     read_aligned, write_aligned, sync_file,
//! };
//!
//! // Configure direct I/O (typically 512 or 4096 byte alignment)
//! let config = DirectIOConfig {
//!     enabled: true,
//!     alignment: 4096,
//! };
//!
//! // Open file with direct I/O
//! let mut file = open_direct_io("data.db", true, true, true, &config)?;
//!
//! // Create aligned buffer for a page
//! let mut buffer = AlignedBuffer::new(4096, config.alignment);
//! buffer[0..4].copy_from_slice(b"PAGE");
//!
//! // Write at aligned offset
//! write_aligned(&mut file, 0, &buffer, &config)?;
//!
//! // Ensure durability
//! sync_file(&mut file, true)?;
//!
//! // Read back
//! let read_buffer = read_aligned(&mut file, 0, 4096, &config)?;
//! assert_eq!(&read_buffer[0..4], b"PAGE");
//! ```
//!
//! ## Alignment Requirements
//!
//! For Linux and Windows direct I/O, **all three** must be aligned:
//!
//! ```text
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │  Alignment Checklist (Linux/Windows)                                    │
//!   │                                                                         │
//!   │  ✓ Buffer pointer:  buffer.as_ptr() % alignment == 0                    │
//!   │  ✓ File offset:     offset % alignment == 0                             │
//!   │  ✓ I/O size:        size % alignment == 0                               │
//!   │                                                                         │
//!   │  If any fails → EINVAL / ERROR_INVALID_PARAMETER                        │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Thread Safety
//!
//! - `AlignedBuffer` is `Send + Sync` (owns memory exclusively)
//! - `DirectIOConfig` is `Clone` (no shared state)
//! - File handles should be protected by caller (e.g., `Mutex<File>`)
//! - Multiple threads can use separate `AlignedBuffer` instances safely
//!
//! ## Error Handling
//!
//! | Error Condition             | Result                                      |
//! |-----------------------------|---------------------------------------------|
//! | Unaligned offset (strict)   | `Err(InvalidInput)` with message            |
//! | Unaligned size (strict)     | `Err(InvalidInput)` with message            |
//! | Unaligned buffer (strict)   | Automatic bounce buffer (transparent)       |
//! | Zero alignment config       | `Err(InvalidInput)` - invalid configuration |
//! | Non-power-of-two alignment  | `Err(InvalidInput)` - invalid configuration |

use log::{debug, warn};
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::fs::{File, OpenOptions};
use std::io::{Error as IoError, ErrorKind, Result as IoResult, Write};
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
    if alignment == 0 || buffer.is_empty() {
        return true;
    }
    (buffer.as_ptr() as usize).is_multiple_of(alignment)
}

#[inline]
fn is_offset_aligned(offset: u64, alignment: usize) -> bool {
    if alignment == 0 {
        return true;
    }
    offset % (alignment as u64) == 0
}

#[inline]
fn direct_io_requires_alignment(config: &DirectIOConfig) -> bool {
    // Linux O_DIRECT and Windows FILE_FLAG_NO_BUFFERING have strict alignment requirements.
    // macOS F_NOCACHE is "best-effort no-cache" and does not require aligned I/O.
    config.enabled && cfg!(any(target_os = "linux", target_os = "windows"))
}

#[inline]
fn invalid_input(msg: impl Into<String>) -> IoError {
    IoError::new(ErrorKind::InvalidInput, msg.into())
}

#[inline]
fn validate_alignment(alignment: usize) -> IoResult<()> {
    if alignment == 0 {
        return Err(invalid_input("direct I/O alignment must be non-zero"));
    }
    if !alignment.is_power_of_two() {
        return Err(invalid_input(format!(
            "direct I/O alignment must be a power of two, got {alignment}"
        )));
    }
    Ok(())
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
        if size == 0 {
            // Avoid relying on allocator behavior for zero-sized allocations.
            // For size=0, a dangling aligned pointer is valid to form slices of length 0.
            let layout = Layout::from_size_align(0, alignment)
                .expect("Invalid layout for aligned buffer");
            return Self {
                ptr: std::ptr::NonNull::<u8>::dangling().as_ptr(),
                size,
                layout,
            };
        }

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
        if self.size != 0 {
            unsafe {
                dealloc(self.ptr, self.layout);
            }
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
    // Works for any non-zero alignment (not just power-of-two).
    // Panics on overflow because it indicates a programming error.
    let add = alignment - 1;
    let adjusted = size
        .checked_add(add)
        .expect("round_up_to_alignment overflow");
    (adjusted / alignment) * alignment
}

/// Check if a size is aligned to the given alignment
#[inline]
pub fn is_size_aligned(size: usize, alignment: usize) -> bool {
    if alignment == 0 {
        return true;
    }
    size.is_multiple_of(alignment)
}

/// Create an aligned buffer that maintains alignment (preferred for direct I/O)
pub fn create_aligned_buffer(size: usize, alignment: usize) -> AlignedBuffer {
    AlignedBuffer::new(size, alignment)
}

// --- Platform-specific helper functions ---

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn read_exact_at(file: &mut File, offset: u64, buf: &mut [u8]) -> IoResult<()> {
    use std::os::unix::fs::FileExt;
    let mut done = 0usize;
    while done < buf.len() {
        let n = file.read_at(&mut buf[done..], offset + done as u64)?;
        if n == 0 {
            return Err(IoError::new(ErrorKind::UnexpectedEof, "unexpected EOF"));
        }
        done += n;
    }
    Ok(())
}

#[cfg(target_os = "windows")]
fn read_exact_at(file: &mut File, offset: u64, buf: &mut [u8]) -> IoResult<()> {
    use std::os::windows::fs::FileExt;
    let mut done = 0usize;
    while done < buf.len() {
        let n = file.seek_read(&mut buf[done..], offset + done as u64)?;
        if n == 0 {
            return Err(IoError::new(ErrorKind::UnexpectedEof, "unexpected EOF"));
        }
        done += n;
    }
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn read_exact_at(file: &mut File, offset: u64, buf: &mut [u8]) -> IoResult<()> {
    use std::io::{Read, Seek, SeekFrom};
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(buf)?;
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn write_all_at(file: &mut File, offset: u64, buf: &[u8]) -> IoResult<()> {
    use std::os::unix::fs::FileExt;
    let mut done = 0usize;
    while done < buf.len() {
        let n = file.write_at(&buf[done..], offset + done as u64)?;
        if n == 0 {
            return Err(IoError::new(
                ErrorKind::WriteZero,
                "failed to write whole buffer",
            ));
        }
        done += n;
    }
    Ok(())
}

#[cfg(target_os = "windows")]
fn write_all_at(file: &mut File, offset: u64, buf: &[u8]) -> IoResult<()> {
    use std::os::windows::fs::FileExt;
    let mut done = 0usize;
    while done < buf.len() {
        let n = file.seek_write(&buf[done..], offset + done as u64)?;
        if n == 0 {
            return Err(IoError::new(
                ErrorKind::WriteZero,
                "failed to write whole buffer",
            ));
        }
        done += n;
    }
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn write_all_at(file: &mut File, offset: u64, buf: &[u8]) -> IoResult<()> {
    use std::io::{Seek, SeekFrom, Write};
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(buf)?;
    Ok(())
}

// --- End helper functions ---

/// Perform a direct I/O read operation with proper alignment
pub fn read_aligned(
    file: &mut File,
    offset: u64,
    size: usize,
    config: &DirectIOConfig,
) -> IoResult<AlignedBuffer> {
    if size == 0 {
        return Ok(AlignedBuffer::new(0, config.alignment.max(1)));
    }

    if direct_io_requires_alignment(config) {
        validate_alignment(config.alignment)?;
        if !is_offset_aligned(offset, config.alignment) {
            return Err(invalid_input(format!(
                "direct I/O read offset {offset} is not aligned to {}",
                config.alignment
            )));
        }
    }

    if config.enabled {
        // For Linux/Windows direct I/O, the kernel often requires I/O sizes to be aligned.
        let io_size = if direct_io_requires_alignment(config) {
            round_up_to_alignment(size, config.alignment)
        } else {
            size
        };

        let mut buffer = create_aligned_buffer(io_size, config.alignment.max(1));
        read_exact_at(file, offset, buffer.as_mut_slice())?;
        
        // Note: The buffer might be larger than requested 'size'. 
        // The caller is responsible for slicing only what they need.
        Ok(buffer)
    } else {
        let mut buffer = create_aligned_buffer(size, config.alignment.max(1));
        read_exact_at(file, offset, buffer.as_mut_slice())?;
        Ok(buffer)
    }
}

/// Perform a direct I/O read operation into a user-provided buffer
pub fn read_aligned_into(
    file: &mut File,
    offset: u64,
    buf: &mut [u8],
    config: &DirectIOConfig,
) -> IoResult<()> {
    if buf.is_empty() {
        return Ok(());
    }

    if direct_io_requires_alignment(config) {
        validate_alignment(config.alignment)?;
        
        let offset_aligned = is_offset_aligned(offset, config.alignment);
        let ptr_aligned = is_aligned(buf, config.alignment);
        let size_aligned = is_size_aligned(buf.len(), config.alignment);

        if !offset_aligned {
            return Err(invalid_input(format!(
                "direct I/O read offset {offset} is not aligned to {}",
                config.alignment
            )));
        }

        // If strict alignment is required and the user buffer doesn't satisfy it,
        // we must use a bounce buffer.
        if !ptr_aligned || !size_aligned {
            let aligned_size = round_up_to_alignment(buf.len(), config.alignment);
            let mut bounce_buffer = create_aligned_buffer(aligned_size, config.alignment);
            
            read_exact_at(file, offset, bounce_buffer.as_mut_slice())?;
            
            // Copy relevant part to user buffer
            buf.copy_from_slice(&bounce_buffer[..buf.len()]);
            return Ok(());
        }
    }

    // Direct path (aligned or buffered I/O)
    read_exact_at(file, offset, buf)
}

/// Perform a direct I/O write operation with proper alignment
pub fn write_aligned(
    file: &mut File,
    offset: u64,
    data: &[u8],
    config: &DirectIOConfig,
) -> IoResult<()> {
    if data.is_empty() {
        return Ok(());
    }

    if direct_io_requires_alignment(config) {
        validate_alignment(config.alignment)?;

        if !is_offset_aligned(offset, config.alignment) {
            return Err(invalid_input(format!(
                "direct I/O write offset {offset} is not aligned to {}",
                config.alignment
            )));
        }

        // IMPORTANT: padding a write changes file contents beyond `data.len()`.
        // So we enforce size alignment rather than silently padding.
        if !is_size_aligned(data.len(), config.alignment) {
            return Err(invalid_input(format!(
                "direct I/O write size {} is not aligned to {}",
                data.len(),
                config.alignment
            )));
        }
    }

    if config.enabled && direct_io_requires_alignment(config) && !is_aligned(data, config.alignment)
    {
        // Fix pointer alignment by copying into an aligned buffer (same size).
        let mut aligned = create_aligned_buffer(data.len(), config.alignment);
        aligned.as_mut_slice().copy_from_slice(data);
        write_all_at(file, offset, aligned.as_slice())
    } else {
        if config.enabled && !direct_io_requires_alignment(config) && !is_aligned(data, config.alignment) {
            warn!("Data buffer is not aligned; this is fine on this platform but may reduce performance");
        }
        write_all_at(file, offset, data)
    }
}

/// Force synchronization to disk based on policy
pub fn sync_file(file: &mut File, force_sync: bool) -> IoResult<()> {
    if force_sync {
        debug!("Performing fsync to ensure data durability");
        file.sync_all()?;
    } else {
        // For std::fs::File, flush() does not imply durability. Keep this as a best-effort
        // "flush user-space buffers" operation and make the log message explicit.
        debug!("Flushing user-space buffers (no durability guarantee)");
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
        let buffer = create_aligned_buffer(4096, 512);
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
    fn test_round_up_to_alignment_non_power_of_two() {
        // round_up_to_alignment is intentionally general, even though DirectIO alignments
        // are expected to be powers of two.
        assert_eq!(round_up_to_alignment(0, 6), 0);
        assert_eq!(round_up_to_alignment(1, 6), 6);
        assert_eq!(round_up_to_alignment(6, 6), 6);
        assert_eq!(round_up_to_alignment(7, 6), 12);
    }

    #[test]
    fn test_validate_alignment_rejects_invalid() {
        assert!(validate_alignment(0).is_err());
        assert!(validate_alignment(3).is_err());
        assert!(validate_alignment(512).is_ok());
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
