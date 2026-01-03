//! # Distinct Executor Module
//!
//! This module implements the executor for SQL `DISTINCT` clauses, which
//! removes duplicate tuples from query results.
//!
//! ## SQL Syntax
//!
//! ```sql
//! SELECT DISTINCT <column_list> FROM <table> ...
//! ```
//!
//! ## Execution Strategy
//!
//! Uses **hash-based duplicate elimination**:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                   DistinctExecutor                          │
//! │                                                             │
//! │  ┌─────────────────────────────────────────────────────┐    │
//! │  │ Child Executor                                      │    │
//! │  │  └─ Produces tuples (may contain duplicates)        │    │
//! │  └─────────────────────────────────────────────────────┘    │
//! │                        │                                    │
//! │                        ▼                                    │
//! │  ┌─────────────────────────────────────────────────────┐    │
//! │  │ HashSet<Vec<u8>>                                    │    │
//! │  │  └─ Stores serialized tuple bytes                   │    │
//! │  │  └─ O(1) duplicate detection                        │    │
//! │  └─────────────────────────────────────────────────────┘    │
//! │                        │                                    │
//! │                        ▼                                    │
//! │        Only first occurrence of each tuple                  │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Tuple Comparison
//!
//! Tuples are compared by serializing all column values to bytes:
//!
//! | Type      | Serialization                        |
//! |-----------|--------------------------------------|
//! | INTEGER   | Little-endian 4-byte representation  |
//! | BIGINT    | Little-endian 8-byte representation  |
//! | VARCHAR   | Length prefix + UTF-8 bytes          |
//! | BOOLEAN   | Single byte (0 or 1)                 |
//! | DECIMAL   | Little-endian 8-byte representation  |
//! | NULL      | Special marker byte (0xFF)           |
//!
//! ## Memory Usage
//!
//! The executor stores one serialized copy of each unique tuple in memory.
//! For large result sets with high cardinality, this can consume significant
//! memory.

use std::collections::HashSet;
use std::sync::Arc;

use log::{debug, trace};
use parking_lot::RwLock;

use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Val;

/// Executor for removing duplicate tuples from query results.
///
/// `DistinctExecutor` implements hash-based duplicate elimination by
/// maintaining a set of serialized tuple representations. Only the first
/// occurrence of each unique tuple is returned.
///
/// # Algorithm
///
/// 1. Pull tuple from child executor
/// 2. Serialize tuple values to byte vector
/// 3. Check if bytes exist in hash set
/// 4. If new: insert bytes, return tuple
/// 5. If duplicate: skip, pull next tuple
///
/// # Pipelining
///
/// Unlike aggregation, this executor is **not a pipeline breaker**. It
/// produces tuples incrementally as it receives them from the child,
/// filtering out duplicates along the way.
///
/// # Example
///
/// ```ignore
/// // SELECT DISTINCT name FROM users
/// let scan = SeqScanPlanNode::new(schema, table_oid, "users");
/// let distinct = DistinctExecutor::new(output_schema, Box::new(scan_executor));
/// ```
///
/// # NULL Handling
///
/// NULL values are treated as equal to each other for duplicate detection.
/// Two tuples `(1, NULL)` and `(1, NULL)` are considered duplicates.
pub struct DistinctExecutor {
    /// Child executor providing input tuples (potentially with duplicates).
    child_executor: Box<dyn AbstractExecutor>,
    /// Output schema (same as input schema).
    schema: Schema,
    /// Set of serialized tuple bytes seen so far.
    seen_tuples: HashSet<Vec<u8>>,
    /// Flag indicating whether `init()` has been called.
    initialized: bool,
}

impl DistinctExecutor {
    /// Creates a new `DistinctExecutor` with the given schema and child.
    ///
    /// # Arguments
    ///
    /// * `schema` - Output schema (should match child executor's output).
    /// * `child_executor` - Child executor that may produce duplicate tuples.
    ///
    /// # Returns
    ///
    /// A new executor ready for initialization.
    pub fn new(schema: Schema, child_executor: Box<dyn AbstractExecutor>) -> Self {
        Self {
            child_executor,
            schema,
            seen_tuples: HashSet::new(),
            initialized: false,
        }
    }

    /// Serializes tuple values to a byte vector for duplicate detection.
    ///
    /// Creates a deterministic byte representation of all tuple values that
    /// can be used as a hash set key. The serialization is designed to
    /// ensure that equal tuples produce identical byte sequences.
    ///
    /// # Arguments
    ///
    /// * `tuple` - The tuple to serialize.
    ///
    /// # Returns
    ///
    /// A `Vec<u8>` containing the serialized tuple data.
    ///
    /// # Serialization Format
    ///
    /// Each value is serialized according to its type:
    /// - **INTEGER**: 4 bytes, little-endian
    /// - **BIGINT**: 8 bytes, little-endian
    /// - **VARCHAR**: 4-byte length prefix + UTF-8 bytes
    /// - **BOOLEAN**: 1 byte (0 = false, 1 = true)
    /// - **DECIMAL**: 8 bytes, little-endian (f64 representation)
    /// - **NULL**: 1 byte marker (0xFF)
    /// - **Other**: Debug string with length prefix (fallback)
    fn serialize_tuple(&self, tuple: &Tuple) -> Vec<u8> {
        let mut bytes = Vec::new();

        for value in tuple.get_values() {
            // Serialize each value to bytes for comparison
            match value.get_val() {
                Val::Integer(i) => bytes.extend_from_slice(&i.to_le_bytes()),
                Val::BigInt(i) => bytes.extend_from_slice(&i.to_le_bytes()),
                Val::VarLen(s) => {
                    bytes.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(s.as_bytes());
                },
                Val::Boolean(b) => bytes.push(if *b { 1 } else { 0 }),
                Val::Decimal(d) => bytes.extend_from_slice(&d.to_le_bytes()),
                Val::Null => bytes.push(0xFF), // Special marker for NULL
                _ => {
                    // For other types, convert to string and serialize
                    let s = format!("{:?}", value);
                    bytes.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(s.as_bytes());
                },
            }
        }

        bytes
    }
}

impl AbstractExecutor for DistinctExecutor {
    /// Initializes the distinct executor and its child.
    ///
    /// Clears the seen tuples set and initializes the child executor.
    /// This method is idempotent; subsequent calls have no effect.
    fn init(&mut self) {
        if self.initialized {
            trace!("DistinctExecutor already initialized");
            return;
        }

        trace!("Initializing DistinctExecutor");

        // Initialize the child executor
        self.child_executor.init();

        // Clear seen tuples set
        self.seen_tuples.clear();

        self.initialized = true;
        trace!("DistinctExecutor initialized successfully");
    }

    /// Returns the next unique tuple from the child executor.
    ///
    /// Pulls tuples from the child and filters out duplicates by checking
    /// against the hash set of previously seen tuples. May consume multiple
    /// child tuples before returning if duplicates are encountered.
    ///
    /// # Returns
    ///
    /// * `Ok(Some((tuple, rid)))` - Next unique tuple with its RID
    /// * `Ok(None)` - No more tuples from child executor
    /// * `Err(DBError)` - Error from child executor
    ///
    /// # Performance
    ///
    /// - **Time**: O(1) average per unique tuple (hash lookup)
    /// - **Space**: O(n) where n is the number of unique tuples
    ///
    /// # Example Flow
    ///
    /// ```text
    /// Child produces: (1, "A"), (2, "B"), (1, "A"), (3, "C"), (2, "B")
    /// DISTINCT returns: (1, "A"), (2, "B"), (3, "C")
    /// ```
    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if !self.initialized {
            trace!("DistinctExecutor not initialized, initializing now");
            self.init();
        }

        loop {
            match self.child_executor.next()? {
                Some((tuple, rid)) => {
                    debug!("DistinctExecutor received tuple with RID {:?}", rid);

                    // Serialize the tuple to bytes for comparison
                    let tuple_bytes = self.serialize_tuple(&tuple);

                    // Check if we've seen this tuple before
                    if !self.seen_tuples.contains(&tuple_bytes) {
                        // Insert the tuple bytes into our set
                        self.seen_tuples.insert(tuple_bytes);
                        debug!("DistinctExecutor returning unique tuple with RID {:?}", rid);
                        return Ok(Some((tuple, rid)));
                    } else {
                        debug!(
                            "DistinctExecutor skipping duplicate tuple with RID {:?}",
                            rid
                        );
                        // Continue to the next tuple if this one is a duplicate
                    }
                },
                None => {
                    trace!("DistinctExecutor reached end of child executor");
                    return Ok(None);
                },
            }
        }
    }

    /// Returns the output schema for distinct results.
    ///
    /// The output schema is identical to the input schema since DISTINCT
    /// only filters rows, not columns.
    fn get_output_schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns the execution context from the child executor.
    ///
    /// Delegates to the child since `DistinctExecutor` doesn't maintain
    /// its own context.
    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.child_executor.get_executor_context()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::RwLock;
    use tempfile::TempDir;

    use super::*;
    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::execution_context::ExecutionContext;
    use crate::sql::execution::executors::mock_executor::MockExecutor;
    use crate::sql::execution::plans::mock_scan_plan::MockScanNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<TransactionManager>,
        transaction_context: Arc<TransactionContext>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 100;
            const K: usize = 2;

            // Create temporary directory
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir
                .path()
                .join(format!("{name}.db"))
                .to_str()
                .unwrap()
                .to_string();
            let log_path = temp_dir
                .path()
                .join(format!("{name}.log"))
                .to_str()
                .unwrap()
                .to_string();

            // Create disk components
            let disk_manager = AsyncDiskManager::new(
                db_path.clone(),
                log_path.clone(),
                DiskManagerConfig::default(),
            )
            .await;
            let disk_manager_arc = Arc::new(disk_manager.unwrap());
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(
                BufferPoolManager::new(
                    BUFFER_POOL_SIZE,
                    disk_manager_arc.clone(),
                    replacer.clone(),
                )
                .unwrap(),
            );

            // Create transaction manager and lock manager
            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());

            let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));
            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            Self {
                bpm,
                transaction_manager,
                transaction_context,
                _temp_dir: temp_dir,
            }
        }

        pub fn bpm(&self) -> Arc<BufferPoolManager> {
            self.bpm.clone()
        }
    }

    fn create_catalog(ctx: &TestContext) -> Catalog {
        Catalog::new(ctx.bpm(), ctx.transaction_manager.clone())
    }

    fn create_execution_context(
        ctx: &TestContext,
        catalog: Arc<RwLock<Catalog>>,
    ) -> Arc<RwLock<ExecutionContext>> {
        Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm(),
            catalog,
            ctx.transaction_context.clone(),
        )))
    }

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ])
    }

    fn create_test_tuples() -> Vec<(Vec<Value>, RID)> {
        vec![
            // Some duplicates for testing
            (
                vec![
                    Value::new(1),
                    Value::new("Alice".to_string()),
                    Value::new(25),
                ],
                RID::new(0, 0),
            ),
            (
                vec![Value::new(2), Value::new("Bob".to_string()), Value::new(30)],
                RID::new(0, 1),
            ),
            (
                vec![
                    Value::new(1),
                    Value::new("Alice".to_string()),
                    Value::new(25),
                ],
                RID::new(0, 2),
            ), // Duplicate
            (
                vec![
                    Value::new(3),
                    Value::new("Charlie".to_string()),
                    Value::new(35),
                ],
                RID::new(0, 3),
            ),
            (
                vec![Value::new(2), Value::new("Bob".to_string()), Value::new(30)],
                RID::new(0, 4),
            ), // Duplicate
            (
                vec![
                    Value::new(4),
                    Value::new("Diana".to_string()),
                    Value::new(28),
                ],
                RID::new(0, 5),
            ),
        ]
    }

    #[tokio::test]
    async fn test_distinct_removes_duplicates() {
        let ctx = TestContext::new("test_distinct_removes_duplicates").await;
        let catalog = create_catalog(&ctx);
        let exec_context = create_execution_context(&ctx, Arc::new(RwLock::new(catalog)));
        let schema = create_test_schema();
        let test_tuples = create_test_tuples();

        // Create mock executor with test data
        let mock_node = MockScanNode::new(schema.clone(), "test_table".to_string(), vec![])
            .with_tuples(test_tuples.clone());
        let mock_executor = MockExecutor::new(
            exec_context.clone(),
            Arc::new(mock_node),
            0,
            test_tuples,
            schema.clone(),
        );

        // Create distinct executor
        let mut distinct_executor = DistinctExecutor::new(schema, Box::new(mock_executor));
        distinct_executor.init();

        // Collect all results
        let mut results = Vec::new();
        while let Ok(Some((tuple, _rid))) = distinct_executor.next() {
            results.push(tuple);
        }

        // Should have 4 unique tuples (1,Alice,25), (2,Bob,30), (3,Charlie,35), (4,Diana,28)
        assert_eq!(
            results.len(),
            4,
            "Should have 4 unique tuples after DISTINCT"
        );

        // Verify the unique tuples
        let mut found_alice = false;
        let mut found_bob = false;
        let mut found_charlie = false;
        let mut found_diana = false;

        for tuple in &results {
            let name = match tuple.get_value(1).get_val() {
                Val::VarLen(s) => s.clone(),
                _ => panic!("Expected VarLen value for name"),
            };
            match name.as_str() {
                "Alice" => {
                    assert!(!found_alice, "Alice should appear only once");
                    found_alice = true;
                    assert_eq!(
                        match tuple.get_value(0).get_val() {
                            Val::Integer(i) => *i,
                            _ => panic!("Expected integer"),
                        },
                        1
                    );
                    assert_eq!(
                        match tuple.get_value(2).get_val() {
                            Val::Integer(i) => *i,
                            _ => panic!("Expected integer"),
                        },
                        25
                    );
                },
                "Bob" => {
                    assert!(!found_bob, "Bob should appear only once");
                    found_bob = true;
                    assert_eq!(
                        match tuple.get_value(0).get_val() {
                            Val::Integer(i) => *i,
                            _ => panic!("Expected integer"),
                        },
                        2
                    );
                    assert_eq!(
                        match tuple.get_value(2).get_val() {
                            Val::Integer(i) => *i,
                            _ => panic!("Expected integer"),
                        },
                        30
                    );
                },
                "Charlie" => {
                    assert!(!found_charlie, "Charlie should appear only once");
                    found_charlie = true;
                    assert_eq!(
                        match tuple.get_value(0).get_val() {
                            Val::Integer(i) => *i,
                            _ => panic!("Expected integer"),
                        },
                        3
                    );
                    assert_eq!(
                        match tuple.get_value(2).get_val() {
                            Val::Integer(i) => *i,
                            _ => panic!("Expected integer"),
                        },
                        35
                    );
                },
                "Diana" => {
                    assert!(!found_diana, "Diana should appear only once");
                    found_diana = true;
                    assert_eq!(
                        match tuple.get_value(0).get_val() {
                            Val::Integer(i) => *i,
                            _ => panic!("Expected integer"),
                        },
                        4
                    );
                    assert_eq!(
                        match tuple.get_value(2).get_val() {
                            Val::Integer(i) => *i,
                            _ => panic!("Expected integer"),
                        },
                        28
                    );
                },
                _ => panic!("Unexpected name: {}", name),
            }
        }

        assert!(
            found_alice && found_bob && found_charlie && found_diana,
            "All unique names should be found"
        );
    }

    #[tokio::test]
    async fn test_distinct_empty_input() {
        let ctx = TestContext::new("test_distinct_empty_input").await;
        let catalog = create_catalog(&ctx);
        let exec_context = create_execution_context(&ctx, Arc::new(RwLock::new(catalog)));
        let schema = create_test_schema();

        // Create mock executor with no data
        let mock_node =
            MockScanNode::new(schema.clone(), "test_table".to_string(), vec![]).with_tuples(vec![]);
        let mock_executor = MockExecutor::new(
            exec_context.clone(),
            Arc::new(mock_node),
            0,
            vec![],
            schema.clone(),
        );

        // Create distinct executor
        let mut distinct_executor = DistinctExecutor::new(schema, Box::new(mock_executor));
        distinct_executor.init();

        // Should return None immediately
        assert!(distinct_executor.next().unwrap().is_none());
    }

    #[tokio::test]
    async fn test_distinct_no_duplicates() {
        let ctx = TestContext::new("test_distinct_no_duplicates").await;
        let catalog = create_catalog(&ctx);
        let exec_context = create_execution_context(&ctx, Arc::new(RwLock::new(catalog)));
        let schema = create_test_schema();

        // Create tuples with no duplicates
        let test_tuples = vec![
            (
                vec![
                    Value::new(1),
                    Value::new("Alice".to_string()),
                    Value::new(25),
                ],
                RID::new(0, 0),
            ),
            (
                vec![Value::new(2), Value::new("Bob".to_string()), Value::new(30)],
                RID::new(0, 1),
            ),
            (
                vec![
                    Value::new(3),
                    Value::new("Charlie".to_string()),
                    Value::new(35),
                ],
                RID::new(0, 2),
            ),
        ];

        // Create mock executor with test data
        let mock_node = MockScanNode::new(schema.clone(), "test_table".to_string(), vec![])
            .with_tuples(test_tuples.clone());
        let mock_executor = MockExecutor::new(
            exec_context.clone(),
            Arc::new(mock_node),
            0,
            test_tuples,
            schema.clone(),
        );

        // Create distinct executor
        let mut distinct_executor = DistinctExecutor::new(schema, Box::new(mock_executor));
        distinct_executor.init();

        // Collect all results
        let mut results = Vec::new();
        while let Ok(Some((tuple, _rid))) = distinct_executor.next() {
            results.push(tuple);
        }

        // Should have all 3 tuples since there are no duplicates
        assert_eq!(
            results.len(),
            3,
            "Should have all 3 tuples when no duplicates exist"
        );
    }

    #[tokio::test]
    async fn test_distinct_all_duplicates() {
        let ctx = TestContext::new("test_distinct_all_duplicates").await;
        let catalog = create_catalog(&ctx);
        let exec_context = create_execution_context(&ctx, Arc::new(RwLock::new(catalog)));
        let schema = create_test_schema();

        // Create tuples where all are duplicates
        let test_tuples = vec![
            (
                vec![
                    Value::new(1),
                    Value::new("Alice".to_string()),
                    Value::new(25),
                ],
                RID::new(0, 0),
            ),
            (
                vec![
                    Value::new(1),
                    Value::new("Alice".to_string()),
                    Value::new(25),
                ],
                RID::new(0, 1),
            ),
            (
                vec![
                    Value::new(1),
                    Value::new("Alice".to_string()),
                    Value::new(25),
                ],
                RID::new(0, 2),
            ),
            (
                vec![
                    Value::new(1),
                    Value::new("Alice".to_string()),
                    Value::new(25),
                ],
                RID::new(0, 3),
            ),
        ];

        // Create mock executor with test data
        let mock_node = MockScanNode::new(schema.clone(), "test_table".to_string(), vec![])
            .with_tuples(test_tuples.clone());
        let mock_executor = MockExecutor::new(
            exec_context.clone(),
            Arc::new(mock_node),
            0,
            test_tuples,
            schema.clone(),
        );

        // Create distinct executor
        let mut distinct_executor = DistinctExecutor::new(schema, Box::new(mock_executor));
        distinct_executor.init();

        // Collect all results
        let mut results = Vec::new();
        while let Ok(Some((tuple, _rid))) = distinct_executor.next() {
            results.push(tuple);
        }

        // Should have only 1 tuple since all are duplicates
        assert_eq!(
            results.len(),
            1,
            "Should have only 1 tuple when all are duplicates"
        );

        let tuple = &results[0];
        assert_eq!(
            match tuple.get_value(0).get_val() {
                Val::Integer(i) => *i,
                _ => panic!("Expected integer"),
            },
            1
        );
        assert_eq!(
            match tuple.get_value(1).get_val() {
                Val::VarLen(s) => s.as_str(),
                _ => panic!("Expected string"),
            },
            "Alice"
        );
        assert_eq!(
            match tuple.get_value(2).get_val() {
                Val::Integer(i) => *i,
                _ => panic!("Expected integer"),
            },
            25
        );
    }

    #[tokio::test]
    async fn test_distinct_with_nulls() {
        let ctx = TestContext::new("test_distinct_with_nulls").await;
        let catalog = create_catalog(&ctx);
        let exec_context = create_execution_context(&ctx, Arc::new(RwLock::new(catalog)));
        let schema = create_test_schema();

        // Create tuples with NULL values
        let test_tuples = vec![
            (
                vec![
                    Value::new(1),
                    Value::new("Alice".to_string()),
                    Value::new(Val::Null),
                ],
                RID::new(0, 0),
            ),
            (
                vec![Value::new(2), Value::new(Val::Null), Value::new(30)],
                RID::new(0, 1),
            ),
            (
                vec![
                    Value::new(1),
                    Value::new("Alice".to_string()),
                    Value::new(Val::Null),
                ],
                RID::new(0, 2),
            ), // Duplicate with NULL
            (
                vec![
                    Value::new(Val::Null),
                    Value::new(Val::Null),
                    Value::new(Val::Null),
                ],
                RID::new(0, 3),
            ),
            (
                vec![
                    Value::new(Val::Null),
                    Value::new(Val::Null),
                    Value::new(Val::Null),
                ],
                RID::new(0, 4),
            ), // Duplicate NULLs
        ];

        // Create mock executor with test data
        let mock_node = MockScanNode::new(schema.clone(), "test_table".to_string(), vec![])
            .with_tuples(test_tuples.clone());
        let mock_executor = MockExecutor::new(
            exec_context.clone(),
            Arc::new(mock_node),
            0,
            test_tuples,
            schema.clone(),
        );

        // Create distinct executor
        let mut distinct_executor = DistinctExecutor::new(schema, Box::new(mock_executor));
        distinct_executor.init();

        // Collect all results
        let mut results = Vec::new();
        while let Ok(Some((tuple, _rid))) = distinct_executor.next() {
            results.push(tuple);
        }

        // Should have 3 unique tuples
        assert_eq!(
            results.len(),
            3,
            "Should have 3 unique tuples including those with NULLs"
        );
    }
}
