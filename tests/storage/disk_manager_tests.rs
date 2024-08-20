extern crate tkdb;

use crate::test_setup::initialize_logger;
use chrono::Utc;
use mockall::predicate::*;
use mockall::*;
use spin::{Barrier, RwLock};
use std::fs;
use std::future::Future;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::thread;
use tkdb::common::config::PageId;
use tkdb::common::config::DB_PAGE_SIZE;
use tkdb::storage::disk::disk_manager::{DiskIO as OtherDiskIO, FileDiskManager};
use std::io::{Seek, Write};

struct TestContext {
    disk_manager: Arc<RwLock<FileDiskManager>>,
    db_file: String,
    db_log: String,
}

impl TestContext {
    fn new(test_name: &str) -> Self {
        initialize_logger();
        let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
        let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
        let db_log_file = format!("tests/data/{}_{}.log", test_name, timestamp);
        let disk_manager = Arc::new(RwLock::new(FileDiskManager::new(db_file.clone(), db_log_file.clone(), 100)));
        Self {
            disk_manager,
            db_file,
            db_log: db_log_file,
        }
    }

    fn cleanup(&self) {
        // let _ = fs::remove_file(&self.db_file);
        // let _ = fs::remove_file(&self.db_log);
        self.disk_manager.write().shut_down();
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        self.cleanup();
    }
}

#[automock]
pub trait DiskIO {
    fn write_page(&self, page_id: PageId, page_data: &[u8; 4096]) -> Result<(), std::io::Error>;
    fn read_page(&self, page_id: PageId, page_data: &mut [u8; 4096]) -> Result<(), std::io::Error>;
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn file_disk_manager_initialization() {
        let ctx = TestContext::new("test_initialization");

        // Check that the files are created
        assert!(std::path::Path::new(&ctx.db_file).exists(), "Database file was not created.");
        assert!(std::path::Path::new(&ctx.db_log).exists(), "Log file was not created.");

        let disk_manager = ctx.disk_manager.read();

        // Check initial state of internal fields
        assert_eq!(disk_manager.get_num_flushes(), 0, "Initial number of flushes should be 0.");
        assert_eq!(disk_manager.get_num_writes(), 0, "Initial number of writes should be 0.");
        assert!(!disk_manager.get_flush_state(), "Flush state should initially be false.");
        assert!(!disk_manager.has_flush_log_future(), "There should be no flush log future initially.");
    }

    #[test]
    fn write_and_read_page() -> Result<(), std::io::Error> {
        let ctx = TestContext::new("test_write_and_read_page");
        let mut buf = [0u8; DB_PAGE_SIZE];
        let mut data1 = [0u8; DB_PAGE_SIZE];
        let mut data2 = [0u8; DB_PAGE_SIZE];
        data1[..16].copy_from_slice(b"First page data.");
        data2[..17].copy_from_slice(b"Second page data."); // Adjusted slice length to match the source

        // Write to page 0 and page 5
        ctx.disk_manager.write().write_page(0, &data1).expect("Failed to write data to page 0");
        ctx.disk_manager.write().write_page(5, &data2).expect("Failed to write data to page 5");

        // Read back and verify data for page 0
        ctx.disk_manager.read().read_page(0, &mut buf).expect("Failed to read data from page 0");
        assert_eq!(buf, data1, "Data read from page 0 does not match expected data.");

        // Read back and verify data for page 5
        buf.fill(0); // Clear buffer before reading the next page
        ctx.disk_manager.read().read_page(5, &mut buf).expect("Failed to read data from page 5");
        assert_eq!(buf, data2, "Data read from page 5 does not match expected data.");

        Ok(())
    }

    #[test]
    fn write_and_read_log_data() {
        let ctx = TestContext::new("test_write_and_read_log_data");
        let mut buf = [0u8; 32];
        let mut data = [0u8; 32];
        data[..20].copy_from_slice(b"Test log data entry.");

        // Write log data
        ctx.disk_manager.write().write_log(&data, ).expect("TODO: panic message");

        // Read back the log data from offset 0
        ctx.disk_manager.read().read_log(&mut buf, 0).expect("TODO: panic message");
        assert_eq!(buf, data, "Log data read does not match expected data.");

        // Test reading from an offset within the log
        let mut partial_buf = [0u8; 10];
        ctx.disk_manager.read().read_log(&mut partial_buf, 10).expect("TODO: panic message");
        assert_eq!(&partial_buf[..], &data[10..20], "Partial log data read does not match expected data.");
    }

    #[test]
    fn flush_log_future_handling() {
        let ctx = TestContext::new("test_flush_log_future_handling");
        let disk_manager = ctx.disk_manager.write();

        // Initially, there should be no flush log future
        assert!(!disk_manager.has_flush_log_future(), "There should be no flush log future initially.");

        // Set a flush log future
        let future = Box::new(async { () });
        disk_manager.set_flush_log_future(future);

        // Verify the future is now set
        assert!(disk_manager.has_flush_log_future(), "Flush log future should be set.");
    }

    #[test]
    fn number_of_writes_and_flushes() {
        let ctx = TestContext::new("test_number_of_writes_and_flushes");
        let mut data = [0u8; DB_PAGE_SIZE];
        data[..16].copy_from_slice(b"Test write data.");

        // Write data to a page
        ctx.disk_manager.write().write_page(0, &data).expect("TODO: panic message");
        ctx.disk_manager.write().write_page(1, &data).expect("TODO: panic message");

        // Write log data
        let log_data = b"Test log entry.";
        ctx.disk_manager.write().write_log(log_data).expect("TODO: panic message");

        // Verify the number of writes and flushes
        let disk_manager = ctx.disk_manager.read();
        assert_eq!(disk_manager.get_num_writes(), 2, "Number of page writes should be 2.");
        assert_eq!(disk_manager.get_num_flushes(), 1, "Number of flushes should be 1 after log write.");
    }
}

#[cfg(test)]
mod basic_behaviour {
    use super::*;

    #[test]
    fn shutdown_behavior_test() -> Result<(), std::io::Error> {
        let ctx = TestContext::new("test_shutdown_behavior");
        let mut data = [0u8; DB_PAGE_SIZE];
        data[..14].copy_from_slice(b"Shutdown test.");

        // Write data to a page
        ctx.disk_manager
            .write()
            .write_page(0, &data)
            .expect("Failed to write page data before shutdown.");

        // Call shut_down to flush the file
        ctx.disk_manager
            .write()
            .shut_down()
            .expect("Failed to flush and shut down the disk manager.");

        // Verify that the data was flushed and saved correctly by reading it back
        let mut buf = [0u8; DB_PAGE_SIZE];
        let reopened_manager = FileDiskManager::new(ctx.db_file.clone(), ctx.db_log.clone(), 100);
        reopened_manager
            .read_page(0, &mut buf)
            .expect("Failed to read page data after reopening the disk manager.");

        assert_eq!(buf, data, "Data was not correctly flushed during shutdown.");

        Ok(())
    }

    #[test]
    fn log_data_incomplete_read_test() -> Result<(), std::io::Error> {
        let ctx = TestContext::new("test_log_data_incomplete_read");
        const LOG_DATA: &[u8; 16] = b"Short log entry.";
        let mut buf = [0u8; 32];

        // Write log data
        ctx.disk_manager
            .write()
            .write_log(LOG_DATA)
            .expect("Failed to write log data.");

        // Manually check the log file content to ensure data is written correctly
        let log_content = std::fs::read(&ctx.db_log).expect("Failed to read log file");
        assert_eq!(
            &log_content[..LOG_DATA.len()],
            LOG_DATA,
            "Log file content should match the written log data."
        );

        // Read more data than was written to simulate reading past the end of the file
        ctx.disk_manager
            .read()
            .read_log(&mut buf, 0)
            .expect("Failed to read log data.");

        // Verify that the buffer is filled with zeros after the actual log data
        assert_eq!(
            &buf[..LOG_DATA.len()],
            LOG_DATA,
            "The initial portion of the buffer should match the written log data."
        );
        assert_eq!(
            &buf[LOG_DATA.len()..],
            &[0u8; 32 - LOG_DATA.len()][..],
            "The buffer should be filled with zeros after the log data."
        );

        Ok(())
    }

    #[test]
    fn file_size_test() -> Result<(), std::io::Error> {
        let ctx = TestContext::new("test_file_size");
        let mut data = [0u8; DB_PAGE_SIZE];
        data[..15].copy_from_slice(b"File size test.");

        // Write data to multiple pages to increase file size
        ctx.disk_manager.write().write_page(0, &data)?;
        ctx.disk_manager.write().write_page(1, &data)?;

        // Get the size of the database file
        let file_size = FileDiskManager::get_file_size(&ctx.db_file)?;

        // The expected file size should be 2 * DB_PAGE_SIZE
        let expected_size = 2 * DB_PAGE_SIZE as u64;
        assert_eq!(file_size, expected_size, "File size does not match the expected size after writing pages.");

        Ok(())
    }

    #[test]
    fn read_write_page() -> Result<(), std::io::Error> {
        let ctx = TestContext::new("test_read_write_page");
        let mut buf = [0u8; DB_PAGE_SIZE];
        let mut data = [0u8; DB_PAGE_SIZE];
        data[..14].copy_from_slice(b"A test string.");

        // Tolerate empty read - buffer should be filled with zeros if the page is unwritten.
        if let Err(e) = ctx.disk_manager.read().read_page(0, &mut buf) {
            assert_eq!(e.kind(), std::io::ErrorKind::UnexpectedEof, "Unexpected error during empty read: {:?}", e);
            buf.fill(0); // Simulate expected behavior of filling with zeros
        }

        // Write and read page 0
        ctx.disk_manager.write().write_page(0, &data)?;
        ctx.disk_manager.read().read_page(0, &mut buf)?;
        assert_eq!(buf, data);

        // Write and read page 5
        buf.fill(0);
        ctx.disk_manager.write().write_page(5, &data)?;
        ctx.disk_manager.read().read_page(5, &mut buf)?;
        assert_eq!(buf, data);

        Ok(())
    }

    #[test]
    fn read_write_log() -> Result<(), std::io::Error> {
        let ctx = TestContext::new("test_read_write_log");
        let mut buf = [0u8; 16];
        let mut data = [0u8; 16];
        data[..14].copy_from_slice(b"A test string.");

        // Tolerate empty read
        ctx.disk_manager
            .read()
            .read_log(&mut buf, 0)
            .expect("Failed to read from log file before writing.");

        // Write and read log
        ctx.disk_manager
            .write()
            .write_log(&data)
            .expect("Failed to write log data.");

        ctx.disk_manager
            .read()
            .read_log(&mut buf, 0)
            .expect("Failed to read log data after writing.");

        assert_eq!(buf, data, "Read log data does not match written data.");

        // Shut down and ensure data persistence
        ctx.disk_manager
            .write()
            .shut_down()
            .expect("Failed to shut down the disk manager.");

        Ok(())
    }
}

#[cfg(test)]
mod concurrency {
    use super::*;

    #[test]
    fn concurrent_write_and_read_page() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = TestContext::new("test_concurrent_write_and_read_page");
        let disk_manager = Arc::clone(&ctx.disk_manager);
        let num_threads = 5;

        let (tx, rx) = channel();
        let barrier = Arc::new(Barrier::new(num_threads));

        // Spawn multiple threads to write and read pages concurrently
        for i in 0..num_threads {
            let tx = tx.clone();
            let disk_manager = Arc::clone(&disk_manager);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                let mut data = [0u8; DB_PAGE_SIZE];
                let content = format!("Thread data {}", i);
                data[..content.len()].copy_from_slice(content.as_bytes());

                // Wait until all threads are ready before starting writes
                barrier.wait();

                // Write to a specific page
                if let Err(e) = disk_manager.write().write_page(i as PageId, &data) {
                    tx.send((i, false)).expect("Failed to send test result");
                    panic!("Failed to write page {}: {:?}", i, e);
                }

                // Wait until all writes are done
                barrier.wait();

                // Read the page back
                let mut buf = [0u8; DB_PAGE_SIZE];
                if let Err(e) = disk_manager.read().read_page(i as PageId, &mut buf) {
                    tx.send((i, false)).expect("Failed to send test result");
                    panic!("Failed to read page {}: {:?}", i, e);
                }

                // Send the result back
                let result = data == buf;
                tx.send((i, result)).expect("Failed to send test result");
            });
        }

        // Collect and verify results
        for _ in 0..num_threads {
            let (i, result) = rx.recv()?;
            assert!(result, "Data integrity check failed for thread {}", i);
        }

        Ok(())
    }

    #[test]
    #[ignore]
    fn concurrent_log_writing_and_reading() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = TestContext::new("test_concurrent_log_writing_and_reading");
        let disk_manager = Arc::clone(&ctx.disk_manager);
        let num_threads = 5;

        let (tx, rx) = channel();
        let barrier = Arc::new(Barrier::new(num_threads));

        // Spawn multiple threads to write and read logs concurrently
        for i in 0..num_threads {
            let tx = tx.clone();
            let disk_manager = Arc::clone(&disk_manager);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                let log_data = format!("Log entry from thread {}\n", i).into_bytes();

                // Wait for all threads to be ready
                barrier.wait();

                // Write to the log
                disk_manager
                    .write()
                    .write_log(&log_data)
                    .expect(&format!("Failed to write log for thread {}", i));

                // Wait for all threads to finish writing before reading
                barrier.wait();

                // Since logs are written sequentially and appended, we can read them back sequentially
                let mut buf = vec![0u8; log_data.len()];
                let read_offset = i as u64 * log_data.len() as u64;
                disk_manager
                    .read()
                    .read_log(&mut buf, read_offset)
                    .expect(&format!("Failed to read log for thread {}", i));

                // Send the result back
                tx.send((i, log_data == buf)).expect("Failed to send test result");
            });
        }

        // Collect and verify results
        for _ in 0..num_threads {
            let (i, result) = rx.recv().expect("Failed to receive test result");
            assert!(
                result,
                "Log data integrity check failed for thread {}",
                i
            );
        }

        Ok(())
    }

    #[test]
    fn concurrent_flush_log_future_handling() {
        use std::sync::{Arc, Barrier, mpsc::channel};
        use std::thread;
        use futures::future::ready;

        let ctx = TestContext::new("test_concurrent_flush_log_future_handling");
        let disk_manager = Arc::clone(&ctx.disk_manager);
        let num_threads = 10;

        let (tx, rx) = channel();
        let barrier = Arc::new(Barrier::new(num_threads));

        // Spawn multiple threads to set and check flush log futures concurrently
        for i in 0..num_threads {
            let tx = tx.clone();
            let disk_manager = Arc::clone(&disk_manager);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                // Wait for all threads to reach this point
                barrier.wait();

                let future = Box::new(ready(())) as Box<dyn Future<Output = ()> + Send>;
                disk_manager.write().set_flush_log_future(future);

                // Verify that the flush log future is set
                let has_future = disk_manager.read().has_flush_log_future();

                // Send the result back
                tx.send((i, has_future)).expect("Failed to send test result");

                // Additional barrier wait to ensure all threads have finished setting/checking before exiting
                barrier.wait();
            });
        }

        // Collect and verify results
        for _ in 0..num_threads {
            let (i, result) = rx.recv().expect("Failed to receive test result");
            assert!(result, "Flush log future was not set correctly in thread {}", i);
        }
    }
}

#[cfg(test)]
mod edge_cases {
    use super::*;

    #[test]
    fn reading_non_existent_page() -> Result<(), std::io::Error> {
        let ctx = TestContext::new("test_reading_non_existent_page");
        let mut buf = [0u8; DB_PAGE_SIZE];

        // Attempt to read a page that hasn't been written to (should return zero-filled buffer)
        ctx.disk_manager.read().read_page(10, &mut buf)?;
        assert_eq!(
            buf,
            [0u8; DB_PAGE_SIZE],
            "Buffer should be filled with zeros when reading a non-existent page."
        );

        // Attempt to read from a very high page ID, which is beyond the current file size
        ctx.disk_manager.read().read_page(10000, &mut buf)?;
        assert_eq!(
            buf,
            [0u8; DB_PAGE_SIZE],
            "Buffer should be filled with zeros when reading past the end of the file."
        );

        Ok(())
    }

    #[test]
    fn writing_beyond_file_capacity() -> Result<(), std::io::Error> {
        let ctx = TestContext::new("test_writing_beyond_file_capacity");
        let mut data = [0u8; DB_PAGE_SIZE];
        data[..16].copy_from_slice(b"Beyond capacity.");

        // Attempt to write to a very large page ID
        let large_page_id = 1000; // Arbitrary large number
        ctx.disk_manager.write().write_page(large_page_id, &data)?;

        // Verify that the file size has increased if the write was allowed
        let file_size = FileDiskManager::get_file_size(&ctx.db_file)?;

        assert!(
            file_size >= (large_page_id as u64 + 1) * DB_PAGE_SIZE as u64,
            "File size should reflect the large page write if supported."
        );

        Ok(())
    }

    #[test]
    fn reading_with_corrupted_data() -> Result<(), std::io::Error> {
        let ctx = TestContext::new("test_reading_with_corrupted_data");
        let mut data = [0u8; DB_PAGE_SIZE];
        data[..14].copy_from_slice(b"Original data.");

        // Write data to a page
        ctx.disk_manager.write().write_page(0, &data)?;

        // Corrupt the data by writing random bytes directly to the file
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&ctx.db_file)
            .expect("Failed to open file for corruption");
        file.seek(std::io::SeekFrom::Start(0))?;
        // Corrupt a larger portion of the page to simulate severe corruption
        file.write_all(&[0xFF; 512])?; // Corrupt the first 512 bytes

        // Attempt to read the corrupted page
        let mut buf = [0u8; DB_PAGE_SIZE];
        if let Err(e) = ctx.disk_manager.read().read_page(0, &mut buf) {
            eprintln!("Error reading corrupted page: {}", e);
        }

        // Verify that the read does not panic and the result is either corrupted or handled gracefully
        assert_ne!(buf, data, "The read data should not match the original data after corruption.");

        // Optionally check for specific corrupted content (like the 0xFF bytes)
        assert!(
            buf[..512].iter().all(|&byte| byte == 0xFF),
            "Expected the first 512 bytes to be corrupted."
        );

        Ok(())
    }

    #[test]
    fn handling_io_errors_with_mock() {
        let mut mock_disk_io = MockDiskIO::new();

        // Simulate a "disk full" error when trying to write a page
        mock_disk_io
            .expect_write_page()
            .with(predicate::eq(0), predicate::always())
            .returning(|_, _| Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "Disk full")));

        // Attempt to write a page and verify the error is returned
        let result = mock_disk_io.write_page(0, &[0u8; 4096]);

        // Ensure the result is an error
        assert!(result.is_err(), "Expected a disk full error but got a successful result.");

        // Ensure the error kind is correct
        if let Err(e) = result {
            assert_eq!(
                e.kind(),
                std::io::ErrorKind::WriteZero,
                "Expected WriteZero error kind but got {:?}.",
                e.kind()
            );
            assert_eq!(e.to_string(), "Disk full", "Unexpected error message: {}", e);
        }
    }
}
