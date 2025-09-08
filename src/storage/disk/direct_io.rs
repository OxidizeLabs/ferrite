//! Platform-specific direct I/O implementation for bypassing OS page cache
//! 
//! This module provides cross-platform direct I/O functionality to ensure
//! database operations go directly to disk rather than being buffered by the OS.

use log::{debug, warn};
use std::fs::{File, OpenOptions};
use std::io::{Result as IoResult, Write};
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
        debug!("Opening file with buffered I/O: {}", path.as_ref().display());
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
    buffer.as_ptr() as usize % alignment == 0
}

/// Create an aligned buffer for direct I/O operations
pub fn create_aligned_buffer(size: usize, alignment: usize) -> Vec<u8> {
    // Allocate extra space to ensure we can align the buffer
    let total_size = size + alignment - 1;
    let mut buffer = vec![0; total_size];
    
    // Find the aligned offset
    let ptr = buffer.as_ptr() as usize;
    let aligned_ptr = (ptr + alignment - 1) & !(alignment - 1);
    let offset = aligned_ptr - ptr;
    
    // Truncate to the aligned portion
    buffer.drain(..offset);
    buffer.truncate(size);
    
    debug!("Created aligned buffer: size={}, alignment={}, ptr={:p}", 
           size, alignment, buffer.as_ptr());
    
    buffer
}

/// Perform a direct I/O read operation with proper alignment
pub fn read_aligned(
    file: &mut File, 
    offset: u64, 
    size: usize, 
    config: &DirectIOConfig
) -> IoResult<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};
    
    let buffer = if config.enabled {
        create_aligned_buffer(size, config.alignment)
    } else {
        vec![0u8; size]
    };
    
    file.seek(SeekFrom::Start(offset))?;
    
    // For direct I/O, we need to read in aligned chunks
    if config.enabled && size % config.alignment != 0 {
        warn!("Direct I/O read size {} is not aligned to {}, this may cause issues", 
              size, config.alignment);
    }
    
    let mut result = buffer;
    file.read_exact(&mut result)?;
    
    Ok(result)
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
            warn!("Write size {} is not aligned to {}, this may cause issues with direct I/O", 
                  data.len(), config.alignment);
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
        let buffer = create_aligned_buffer(4096, 512);
        assert_eq!(buffer.len(), 4096);
        assert!(is_aligned(&buffer, 512));
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