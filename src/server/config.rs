//! # Server Configuration
//!
//! This module provides `ServerConfig` for loading and managing database server
//! configuration from TOML files.

use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Configuration settings for the Ferrite database server.
///
/// Supports loading from a TOML configuration file or using default values.
///
/// # Example TOML File
///
/// ```toml
/// host = "127.0.0.1"
/// port = 5432
/// max_connections = 100
/// buffer_pool_size = 8192
/// log_level = "info"
/// ```
///
/// # Example Usage
///
/// ```rust,ignore
/// use std::path::Path;
/// use crate::server::config::ServerConfig;
///
/// // Load from file
/// let config = ServerConfig::load(Path::new("ferrite.toml"))?;
///
/// // Or use defaults
/// let config = ServerConfig::default();
/// ```
#[derive(Serialize, Deserialize)]
pub struct ServerConfig {
    /// The host address to bind the server to.
    ///
    /// Default: `"127.0.0.1"` (localhost only for security).
    pub host: String,

    /// The TCP port to listen on.
    ///
    /// Default: `5432` (PostgreSQL-compatible port).
    pub port: u16,

    /// Maximum number of concurrent client connections.
    ///
    /// Default: `100`.
    pub max_connections: u32,

    /// Size of the buffer pool in pages.
    ///
    /// Larger values improve cache hit rates but consume more memory.
    /// Default: `8192` pages.
    pub buffer_pool_size: usize,

    /// Logging verbosity level.
    ///
    /// Valid values: `"trace"`, `"debug"`, `"info"`, `"warn"`, `"error"`.
    /// Default: `"info"`.
    pub log_level: String,
}

impl ServerConfig {
    /// Loads configuration from a TOML file.
    ///
    /// # Parameters
    /// - `path`: Path to the TOML configuration file.
    ///
    /// # Returns
    /// - `Ok(ServerConfig)`: Successfully loaded configuration.
    /// - `Err(...)`: File not found, read error, or invalid TOML syntax.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let config = ServerConfig::load(Path::new("config/server.toml"))?;
    /// println!("Binding to {}:{}", config.host, config.port);
    /// ```
    pub fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = fs::read_to_string(path)?;
        let config: ServerConfig = toml::from_str(&contents)?;
        Ok(config)
    }
}

impl Default for ServerConfig {
    /// Creates a configuration with default values.
    ///
    /// # Default Values
    ///
    /// | Field | Value |
    /// |-------|-------|
    /// | `host` | `"127.0.0.1"` |
    /// | `port` | `5432` |
    /// | `max_connections` | `100` |
    /// | `buffer_pool_size` | `8192` |
    /// | `log_level` | `"info"` |
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 5432,
            max_connections: 100,
            buffer_pool_size: 8192,
            log_level: "info".to_string(),
        }
    }
}
