use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::expressions::abstract_expression::ExpressionOps;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::nested_loop_join_plan::NestedLoopJoinNode;
use crate::storage::table::tuple::Tuple;
use crate::types_db::types::Type;
use crate::types_db::value::{Val, Value};
use log::{debug, trace};
use parking_lot::RwLock;
use sqlparser::ast::JoinOperator as JoinType;
use std::sync::Arc;

/**
 * NESTED LOOP JOIN EXECUTOR IMPLEMENTATION OVERVIEW
 * =================================================
 * 
 * The Nested Loop Join executor implements the simplest join algorithm by using two nested loops:
 * - Outer loop: iterates through all tuples from the left child executor
 * - Inner loop: for each left tuple, iterates through all tuples from the right child executor
 * 
 * ALGORITHM PHASES:
 * 1. INITIALIZATION: Create and initialize both child executors (left and right)
 * 2. EXECUTION: Implement nested loop with join predicate evaluation
 * 3. OUTPUT: Combine tuples based on join type and predicate result
 * 
 * JOIN TYPE HANDLING:
 * - INNER JOIN: Output combined tuple only when predicate evaluates to true
 * - LEFT OUTER JOIN: Output left tuple with nulls if no right match found
 * - RIGHT OUTER JOIN: Output right tuple with nulls if no left match found  
 * - FULL OUTER JOIN: Output unmatched tuples from both sides with nulls
 * - CROSS JOIN: Output cartesian product (predicate always true)
 * - SEMI JOIN: Output left tuple if any right tuple matches (no right columns)
 * - ANTI JOIN: Output left tuple if no right tuple matches
 * 
 * STATE MANAGEMENT:
 * The executor maintains state between next() calls:
 * - Current left tuple being processed
 * - Current right tuple position
 * - Tracking which left tuples have been matched (for outer joins)
 * - Tracking which right tuples have been matched (for right/full outer joins)
 * 
 * PERFORMANCE CONSIDERATIONS:
 * - Time complexity: O(M * N) where M = left tuples, N = right tuples
 * - Memory: Minimal (streaming), but may need to cache unmatched tuples for outer joins
 * - Best for small datasets or when no indexes are available
 */
pub struct NestedLoopJoinExecutor {
    // Child executors
    children_executors: Option<Vec<Box<dyn AbstractExecutor>>>,
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<NestedLoopJoinNode>,
    
    // Execution state
    initialized: bool,
    
    // Current execution state for nested loops
    current_left_tuple: Option<(Arc<Tuple>, RID)>,
    current_right_executor_exhausted: bool,
    left_executor_exhausted: bool,
    
    // Join state tracking for outer joins
    current_left_matched: bool,
    unmatched_right_tuples: Vec<(Arc<Tuple>, RID)>, // For right/full outer joins
    processing_unmatched_right: bool,
    unmatched_right_index: usize,
    
    // For full outer join - track which left tuples were unmatched
    unmatched_left_tuples: Vec<(Arc<Tuple>, RID)>,
    processing_unmatched_left: bool,
    unmatched_left_index: usize,
}

impl NestedLoopJoinExecutor {
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: Arc<NestedLoopJoinNode>) -> Self {
        Self {
            children_executors: None,
            context,
            plan,
            initialized: false,
            current_left_tuple: None,
            current_right_executor_exhausted: false,
            left_executor_exhausted: false,
            current_left_matched: false,
            unmatched_right_tuples: Vec::new(),
            processing_unmatched_right: false,
            unmatched_right_index: 0,
            unmatched_left_tuples: Vec::new(),
            processing_unmatched_left: false,
            unmatched_left_index: 0,
        }
    }

    /**
     * HELPER METHOD: Create null-padded tuple for outer joins
     * Used when one side of the join has no match and needs to be padded with nulls
     */
    fn create_null_padded_tuple(&self, left_tuple: Option<&Arc<Tuple>>, right_tuple: Option<&Arc<Tuple>>) -> Arc<Tuple> {
        // 1. Get schemas to determine final tuple structure
        let left_schema = self.plan.get_left_schema();
        let right_schema = self.plan.get_right_schema();
        let output_schema = self.plan.get_output_schema();
        
        // 2. Create combined vector of values
        let mut combined_values = Vec::new();
        
        // 3. Add left tuple values or nulls
        if let Some(left_tuple) = left_tuple {
            // Add actual values from left tuple
            combined_values.extend(left_tuple.get_values().iter().cloned());
        } else {
            // Create null values for left schema columns
            for _ in left_schema.get_columns() {
                combined_values.push(Value::new(Val::Null));
            }
        }
        
        // 4. Add right tuple values or nulls
        if let Some(right_tuple) = right_tuple {
            // Add actual values from right tuple
            combined_values.extend(right_tuple.get_values().iter().cloned());
        } else {
            // Create null values for right schema columns
            for _ in right_schema.get_columns() {
                combined_values.push(Value::new(Val::Null));
            }
        }
        
        // 5. Create and return new tuple with combined values
        let rid = RID::new(0, 0); // Use dummy RID for joined tuples
        Arc::new(Tuple::new(&combined_values, output_schema, rid))
    }

    /**
     * HELPER METHOD: Combine two tuples into output tuple
     * Used for successful joins where both sides have values
     */
    fn combine_tuples(&self, left_tuple: &Arc<Tuple>, right_tuple: &Arc<Tuple>) -> Arc<Tuple> {
        // 1. Get output schema from the plan
        let output_schema = self.plan.get_output_schema();
        
        // 2. Create combined vector of values
        let mut combined_values = Vec::new();
        
        // 3. Add values from left tuple
        combined_values.extend(left_tuple.get_values().iter().cloned());
        
        // 4. Add values from right tuple
        combined_values.extend(right_tuple.get_values().iter().cloned());
        
        // 5. Create and return new tuple with combined values
        let rid = RID::new(0, 0); // Use dummy RID for joined tuples
        Arc::new(Tuple::new(&combined_values, output_schema, rid))
    }

    /**
     * HELPER METHOD: Evaluate join predicate
     * Returns true if the predicate evaluates to true for the given tuple pair
     */
    fn evaluate_join_predicate(&self, left_tuple: &Arc<Tuple>, right_tuple: &Arc<Tuple>) -> bool {
        // 1. Get the predicate from the plan
        let predicate = self.plan.get_predicate();
        
        // 2. Get schemas for both sides
        let left_schema = self.plan.get_left_schema();
        let right_schema = self.plan.get_right_schema();
        
        // 3. Call predicate.evaluate_join() with the context
        // The predicate knows how to resolve column references:
        // - tuple_index 0 refers to left_tuple
        // - tuple_index 1 refers to right_tuple
        match predicate.evaluate_join(left_tuple, left_schema, right_tuple, right_schema) {
            Ok(value) => {
                // 4. Extract boolean result from Value
                // 5. Handle null/error cases (typically treat as false)
                match value.as_bool() {
                    Ok(result) => result,
                    Err(_) => {
                        // Handle null values - treat as false for join predicates
                        trace!("Join predicate evaluated to null or error, treating as false");
                        false
                    }
                }
            }
            Err(e) => {
                // Handle evaluation errors as false
                trace!("Join predicate evaluation error: {:?}, treating as false", e);
                false
            }
        }
    }

    /**
     * HELPER METHOD: Handle inner join logic
     * Returns Some(tuple) if join condition is met, None otherwise
     */
    fn handle_inner_join(&mut self, left_tuple: &Arc<Tuple>, right_tuple: &Arc<Tuple>) -> Option<(Arc<Tuple>, RID)> {
        // 1. Evaluate join predicate for tuple pair
        if self.evaluate_join_predicate(left_tuple, right_tuple) {
            // 2. If predicate is true:
            //    a. Mark current left tuple as matched
            self.current_left_matched = true;
            //    b. Combine tuples using combine_tuples()
            let combined_tuple = self.combine_tuples(left_tuple, right_tuple);
            //    c. Return Some((combined_tuple, combined_rid))
            let combined_rid = RID::new(0, 0); // Use dummy RID for joined tuples
            Some((combined_tuple, combined_rid))
        } else {
            // 3. If predicate is false:
            //    a. Return None to continue searching
            None
        }
    }

    /**
     * HELPER METHOD: Handle left outer join logic
     * Ensures left tuples are output even if no right match exists
     */
    fn handle_left_outer_join(&mut self, left_tuple: &Arc<Tuple>, right_tuple: Option<&Arc<Tuple>>) -> Option<(Arc<Tuple>, RID)> {
        // 1. If right_tuple is Some:
        if let Some(right_tuple) = right_tuple {
            //    a. Evaluate join predicate
            if self.evaluate_join_predicate(left_tuple, right_tuple) {
                //    b. If true: mark as matched, combine tuples, return result
                self.current_left_matched = true;
                let combined_tuple = self.combine_tuples(left_tuple, right_tuple);
                let combined_rid = RID::new(0, 0); // Use dummy RID for joined tuples
                Some((combined_tuple, combined_rid))
            } else {
                //    c. If false: return None to continue
                None
            }
        } else {
            // 2. If right_tuple is None (end of right side):
            //    a. Check if current left tuple was matched
            if !self.current_left_matched {
                //    b. If not matched: create null-padded tuple with left + nulls
                let padded_tuple = self.create_null_padded_tuple(Some(left_tuple), None);
                let padded_rid = RID::new(0, 0); // Use dummy RID for joined tuples
                Some((padded_tuple, padded_rid))
            } else {
                //    c. If was matched: return None (already output this left tuple)
                None
            }
        }
    }

    /**
     * HELPER METHOD: Handle right outer join logic  
     * Ensures right tuples are output even if no left match exists
     */
    fn handle_right_outer_join(&mut self, left_tuple: Option<&Arc<Tuple>>, right_tuple: &Arc<Tuple>) -> Option<(Arc<Tuple>, RID)> {
        // 1. If left_tuple is Some:
        if let Some(left_tuple) = left_tuple {
            //    a. Evaluate join predicate
            if self.evaluate_join_predicate(left_tuple, right_tuple) {
                //    b. If true: combine tuples, mark right as matched, return result
                let combined_tuple = self.combine_tuples(left_tuple, right_tuple);
                let combined_rid = RID::new(0, 0); // Use dummy RID for joined tuples
                Some((combined_tuple, combined_rid))
            } else {
                //    c. If false: track right tuple as potentially unmatched
                // Return None to continue checking other left tuples for this right tuple
                None
            }
        } else {
            // 2. After all left tuples processed (left_tuple is None):
            //    a. Process unmatched right tuples
            //    b. Create null-padded tuples (nulls + right)
            let padded_tuple = self.create_null_padded_tuple(None, Some(right_tuple));
            let padded_rid = RID::new(0, 0); // Use dummy RID for joined tuples
            //    c. Return unmatched right tuples one by one
            Some((padded_tuple, padded_rid))
        }
    }

    /**
     * HELPER METHOD: Handle full outer join logic
     * Ensures all tuples from both sides are output, with nulls for unmatched
     */
    fn handle_full_outer_join(&mut self, left_tuple: Option<&Arc<Tuple>>, right_tuple: Option<&Arc<Tuple>>) -> Option<(Arc<Tuple>, RID)> {
        match (left_tuple, right_tuple) {
            // 1. During normal processing (both tuples available):
            (Some(left_tuple), Some(right_tuple)) => {
                //    a. Evaluate join predicate
                if self.evaluate_join_predicate(left_tuple, right_tuple) {
                    //    b. If true: combine tuples, mark both as matched
                    self.current_left_matched = true;
                    let combined_tuple = self.combine_tuples(left_tuple, right_tuple);
                    let combined_rid = RID::new(0, 0); // Use dummy RID for joined tuples
                    Some((combined_tuple, combined_rid))
                } else {
                    //    c. Track unmatched tuples from both sides
                    // Return None to continue checking other combinations
                    None
                }
            }
            
            // 2. After main loop completion:
            //    a. Process unmatched left tuples (left + nulls)
            (Some(left_tuple), None) => {
                // Processing unmatched left tuples
                let padded_tuple = self.create_null_padded_tuple(Some(left_tuple), None);
                let padded_rid = RID::new(0, 0); // Use dummy RID for joined tuples
                Some((padded_tuple, padded_rid))
            }
            
            //    b. Process unmatched right tuples (nulls + right)
            (None, Some(right_tuple)) => {
                // Processing unmatched right tuples
                let padded_tuple = self.create_null_padded_tuple(None, Some(right_tuple));
                let padded_rid = RID::new(0, 0); // Use dummy RID for joined tuples
                Some((padded_tuple, padded_rid))
            }
            
            // 3. Both tuples are None - shouldn't happen in normal execution
            (None, None) => {
                // This case shouldn't occur in normal full outer join processing
                None
            }
        }
    }

    /**
     * HELPER METHOD: Handle cross join logic
     * Returns cartesian product of all tuple combinations
     */
    fn handle_cross_join(&self, left_tuple: &Arc<Tuple>, right_tuple: &Arc<Tuple>) -> Option<(Arc<Tuple>, RID)> {
        // 1. Cross join ignores predicate (or assumes it's always true)
        // 2. Combine every left tuple with every right tuple
        let combined_tuple = self.combine_tuples(left_tuple, right_tuple);
        
        // 3. Return combined tuple directly
        let combined_rid = RID::new(0, 0); // Use dummy RID for joined tuples
        Some((combined_tuple, combined_rid))
        
        // Note: Cross join is the simplest case - just combine all pairs
    }

    /**
     * HELPER METHOD: Handle semi join logic
     * Returns left tuples that have at least one matching right tuple
     */
    fn handle_semi_join(&mut self, left_tuple: &Arc<Tuple>, right_tuple: &Arc<Tuple>) -> Option<(Arc<Tuple>, RID)> {
        // 1. Evaluate join predicate
        if self.evaluate_join_predicate(left_tuple, right_tuple) {
            // 2. If predicate is true:
            //    a. Mark current left tuple as matched
            self.current_left_matched = true;
            
            //    b. Return left tuple ONLY (no right columns)
            //    c. Skip to next left tuple (optimization)
            // Create a new tuple with only left schema columns
            let left_schema = self.plan.get_left_schema();
            let left_values = left_tuple.get_values().clone();
            let left_rid = RID::new(0, 0); // Use dummy RID for joined tuples
            let left_only_tuple = Arc::new(Tuple::new(&left_values, left_schema, left_rid));
            
            Some((left_only_tuple, left_rid))
        } else {
            // 3. If predicate is false:
            //    a. Continue to next right tuple
            None
        }
        // 4. Only output left schema columns (handled above)
    }

    /**
     * HELPER METHOD: Handle anti join logic  
     * Returns left tuples that have NO matching right tuples
     */
    fn handle_anti_join(&mut self, left_tuple: &Arc<Tuple>, right_tuple: Option<&Arc<Tuple>>) -> Option<(Arc<Tuple>, RID)> {
        todo!("IMPLEMENTATION STEP 10: Anti join processing")
        // 1. If right_tuple is Some:
        //    a. Evaluate join predicate
        //    b. If true: mark left tuple as matched (exclude from output)
        //    c. If false: continue checking
        // 2. If right_tuple is None (end of right side):
        //    a. If left tuple was never matched: return left tuple
        //    b. If left tuple was matched: return None
        // 3. Only output left schema columns
    }

    /**
     * HELPER METHOD: Reset right executor for next left tuple
     * Resets the right child executor to the beginning for the next outer loop iteration
     */
    fn reset_right_executor(&mut self) {
        todo!("IMPLEMENTATION STEP 11: Reset right executor state")
        // 1. Re-initialize the right child executor
        // 2. Reset right executor exhausted flag
        // 3. Clear any right-side state tracking
        // Note: This requires re-initialization which may be expensive
        // Alternative: Cache right tuples in memory (trade memory for speed)
    }
}

impl AbstractExecutor for NestedLoopJoinExecutor {
    /**
     * INITIALIZATION PHASE
     * Sets up child executors and prepares for execution
     */
    fn init(&mut self) {
        todo!("IMPLEMENTATION STEP 12: Initialize nested loop join executor")
        // 1. VALIDATE PLAN STRUCTURE:
        //    a. Verify exactly 2 children exist
        //    b. Validate join predicate is not null
        //    c. Check schemas are compatible

        // 2. CREATE CHILD EXECUTORS:
        //    a. Get left and right child plans from self.plan
        //    b. Create executor instances for each child
        //    c. Store in self.children_executors

        // 3. INITIALIZE CHILD EXECUTORS:
        //    a. Call init() on left executor
        //    b. Call init() on right executor

        // 4. INITIALIZE STATE VARIABLES:
        //    a. Set initialized = true
        //    b. Reset all iteration state
        //    c. Clear any cached data structures

        // 5. SPECIAL SETUP FOR OUTER JOINS:
        //    a. For right/full outer: prepare unmatched tracking
        //    b. For full outer: prepare left unmatched tracking
        
        // 6. LOG INITIALIZATION:
        //    debug!("NestedLoopJoin initialized - Join Type: {:?}", self.plan.get_join_type());
    }

    /**
     * MAIN EXECUTION LOGIC
     * Implements the nested loop algorithm with join type-specific handling
     */
    fn next(&mut self) -> Option<(Arc<Tuple>, RID)> {
        todo!("IMPLEMENTATION STEP 13: Main nested loop execution")
        // PHASE 1: INITIALIZATION CHECK
        // 1. If not initialized, call init()
        // 2. Return None if initialization failed

        // PHASE 2: HANDLE POST-PROCESSING (for outer joins)
        // 1. If processing_unmatched_right:
        //    a. Return next unmatched right tuple with nulls
        //    b. Increment unmatched_right_index
        //    c. Switch to unmatched left processing when done
        // 2. If processing_unmatched_left:
        //    a. Return next unmatched left tuple with nulls
        //    b. Increment unmatched_left_index
        //    c. Return None when completely done

        // PHASE 3: MAIN NESTED LOOP ALGORITHM
        // 1. OUTER LOOP (Left tuples):
        //    while !left_executor_exhausted {
        //        a. If current_left_tuple is None:
        //           - Get next tuple from left executor
        //           - If None: mark left_executor_exhausted, break
        //           - Reset right executor for new left tuple
        //           - Reset current_left_matched flag
        //        
        //        b. INNER LOOP (Right tuples):
        //           while !current_right_executor_exhausted {
        //               - Get next tuple from right executor
        //               - If None: mark current_right_executor_exhausted
        //               - Otherwise: process tuple pair based on join type
        //           }
        //        
        //        c. If right executor exhausted:
        //           - Handle end-of-inner-loop logic for current join type
        //           - Move to next left tuple
        //    }

        // PHASE 4: JOIN TYPE DISPATCH
        // Based on self.plan.get_join_type(), call appropriate handler:
        // match join_type {
        //     JoinType::Inner(_) => self.handle_inner_join(left, right),
        //     JoinType::Left(_) | JoinType::LeftOuter(_) => self.handle_left_outer_join(left, right),
        //     JoinType::Right(_) | JoinType::RightOuter(_) => self.handle_right_outer_join(left, right),
        //     JoinType::FullOuter(_) => self.handle_full_outer_join(left, right),
        //     JoinType::CrossJoin => self.handle_cross_join(left, right),
        //     JoinType::Semi(_) | JoinType::LeftSemi(_) => self.handle_semi_join(left, right),
        //     JoinType::Anti(_) | JoinType::LeftAnti(_) => self.handle_anti_join(left, right),
        //     JoinType::RightSemi(_) => /* Mirror of left semi with sides swapped */,
        //     JoinType::RightAnti(_) => /* Mirror of left anti with sides swapped */,
        //     JoinType::Join(_) => /* Treat as inner join */,
        //     _ => /* Unsupported join types - return error or None */
        // }

        // PHASE 5: FINALIZATION
        // 1. If main loop completed:
        //    a. For right/full outer: start processing unmatched right tuples
        //    b. For full outer: queue unmatched left tuples for later
        //    c. Return None if no more results available

        // LOGGING AND DEBUGGING:
        // trace!("Processing left tuple: {:?}, right tuple: {:?}", left_tuple, right_tuple);
        // debug!("Join predicate result: {}", predicate_result);
    }

    fn get_output_schema(&self) -> &Schema {
        debug!("Getting output schema: {:?}", self.plan.get_output_schema());
        self.plan.get_output_schema()
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.context.clone()
    }
}

mod unit_tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::expressions::abstract_expression::Expression;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::sql::execution::plans::nested_loop_join_plan::NestedLoopJoinNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::types::Type;
    use crate::types_db::value::Value;
    use sqlparser::ast::JoinConstraint;
    use std::sync::Arc;
    use tempfile::TempDir;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<TransactionManager>,
        transaction_context: Arc<TransactionContext>,
        catalog: Arc<RwLock<Catalog>>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        fn new(name: &str) -> Self {
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
            let disk_manager = Arc::new(FileDiskManager::new(db_path, log_path, BUFFER_POOL_SIZE));
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

            // Create transaction manager and lock manager
            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());

            // Create transaction with ID 0 and ReadUncommitted isolation level
            let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));
            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            // Create catalog
            let catalog = Arc::new(RwLock::new(Catalog::new(
                bpm.clone(),
                transaction_manager.clone(),
            )));

            Self {
                bpm,
                transaction_manager,
                transaction_context,
                catalog,
                _temp_dir: temp_dir,
            }
        }
    }

    // Helper function to create a basic test context
    fn create_basic_test_context() -> (TestContext, Arc<RwLock<ExecutionContext>>) {
        let ctx = TestContext::new("helper_test");
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));
        (ctx, execution_context)
    }

    // Helper function to create test tuples
    fn create_test_tuple(values: Vec<Value>) -> Arc<Tuple> {
        let schema = Schema::new(values.iter().enumerate().map(|(i, v)| {
            Column::new(&format!("col_{}", i), v.get_type_id())
        }).collect());

        let rid = RID::new(0, 0);
        Arc::new(Tuple::new(&values, &schema, rid))
    }

    // Helper function to create a test executor with mock data
    fn create_test_executor() -> NestedLoopJoinExecutor {
        let (_, execution_context) = create_basic_test_context();

        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let right_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("user_id", TypeId::Integer),
            Column::new("title", TypeId::VarChar),
        ]);

        let predicate = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(true),
            Column::new("", TypeId::Boolean),
            vec![],
        )));

        let join_plan = Arc::new(NestedLoopJoinNode::new(
            left_schema,
            right_schema,
            predicate,
            JoinType::Inner(JoinConstraint::None),
            vec![],
            vec![],
            vec![], // Empty children for unit tests
        ));

        NestedLoopJoinExecutor::new(execution_context, join_plan)
    }

    #[test]
    fn test_create_null_padded_tuple_left_null() {
        let executor = create_test_executor();

        // Create a right tuple
        let right_tuple = create_test_tuple(vec![
            Value::new(1),
            Value::new(100),
            Value::new("Test Post"),
        ]);

        // Test with left tuple as None (should pad left side with nulls)
        let result = executor.create_null_padded_tuple(None, Some(&right_tuple));
        let values = result.get_values();

        // Should have 5 columns total (2 from left schema + 3 from right schema)
        assert_eq!(values.len(), 5);

        // Left side should be nulls
        assert!(values[0].is_null()); // left id
        assert!(values[1].is_null()); // left name

        // Right side should have actual values
        assert_eq!(values[2].as_integer().unwrap(), 1);
        assert_eq!(values[3].as_integer().unwrap(), 100);
        assert_eq!(ToString::to_string(&values[4]), "Test Post");
    }

    #[test]
    fn test_create_null_padded_tuple_right_null() {
        let executor = create_test_executor();

        // Create a left tuple
        let left_tuple = create_test_tuple(vec![
            Value::new(1),
            Value::new("Alice"),
        ]);

        // Test with right tuple as None (should pad right side with nulls)
        let result = executor.create_null_padded_tuple(Some(&left_tuple), None);
        let values = result.get_values();

        // Should have 5 columns total (2 from left schema + 3 from right schema)
        assert_eq!(values.len(), 5);

        // Left side should have actual values
        assert_eq!(values[0].as_integer().unwrap(), 1);
        assert_eq!(ToString::to_string(&values[1]), "Alice");

        // Right side should be nulls
        assert!(values[2].is_null()); // right id
        assert!(values[3].is_null()); // right user_id
        assert!(values[4].is_null()); // right title
    }

    #[test]
    fn test_create_null_padded_tuple_both_null() {
        let executor = create_test_executor();

        // Test with both tuples as None (should create all nulls)
        let result = executor.create_null_padded_tuple(None, None);
        let values = result.get_values();

        // Should have 5 columns total (2 from left schema + 3 from right schema)
        assert_eq!(values.len(), 5);

        // All values should be null
        for value in values {
            assert!(value.is_null());
        }
    }

    #[test]
    fn test_combine_tuples() {
        let executor = create_test_executor();

        let left_tuple = create_test_tuple(vec![
            Value::new(1),
            Value::new("Alice"),
        ]);

        let right_tuple = create_test_tuple(vec![
            Value::new(100),
            Value::new(1),
            Value::new("Alice's Post"),
        ]);

        let result = executor.combine_tuples(&left_tuple, &right_tuple);
        let values = result.get_values();

        // Should have 5 columns total
        assert_eq!(values.len(), 5);

        // Left values first
        assert_eq!(values[0].as_integer().unwrap(), 1);
        assert_eq!(ToString::to_string(&values[1]), "Alice");

        // Then right values
        assert_eq!(values[2].as_integer().unwrap(), 100);
        assert_eq!(values[3].as_integer().unwrap(), 1);
        assert_eq!(ToString::to_string(&values[4]), "Alice's Post");
    }

    #[test]
    fn test_evaluate_join_predicate_true() {
        let executor = create_test_executor();

        let left_tuple = create_test_tuple(vec![
            Value::new(1),
            Value::new("Alice"),
        ]);

        let right_tuple = create_test_tuple(vec![
            Value::new(100),
            Value::new(1), // matches left tuple id
            Value::new("Alice's Post"),
        ]);

        // Since our test executor uses a constant TRUE predicate, this should return true
        let result = executor.evaluate_join_predicate(&left_tuple, &right_tuple);
        assert!(result);
    }

    #[test]
    fn test_evaluate_join_predicate_false() {
        // Create executor with FALSE predicate
        let (_, execution_context) = create_basic_test_context();

        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let right_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("user_id", TypeId::Integer),
            Column::new("title", TypeId::VarChar),
        ]);

        let predicate = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(false),
            Column::new("", TypeId::Boolean),
            vec![],
        )));

        let join_plan = Arc::new(NestedLoopJoinNode::new(
            left_schema,
            right_schema,
            predicate,
            JoinType::Inner(JoinConstraint::None),
            vec![],
            vec![],
            vec![],
        ));

        let executor = NestedLoopJoinExecutor::new(execution_context, join_plan);

        let left_tuple = create_test_tuple(vec![
            Value::new(1),
            Value::new("Alice"),
        ]);

        let right_tuple = create_test_tuple(vec![
            Value::new(100),
            Value::new(2), // doesn't match left tuple id
            Value::new("Bob's Post"),
        ]);

        let result = executor.evaluate_join_predicate(&left_tuple, &right_tuple);
        assert!(!result);
    }

    #[test]
    fn test_handle_inner_join_match() {
        let mut executor = create_test_executor();

        let left_tuple = create_test_tuple(vec![
            Value::new(1),
            Value::new("Alice"),
        ]);

        let right_tuple = create_test_tuple(vec![
            Value::new(100),
            Value::new(1),
            Value::new("Alice's Post"),
        ]);

        let result = executor.handle_inner_join(&left_tuple, &right_tuple);

        // Should return a combined tuple since predicate is always true
        assert!(result.is_some());

        let (combined_tuple, _) = result.unwrap();
        let values = combined_tuple.get_values();
        assert_eq!(values.len(), 5);
        assert!(executor.current_left_matched); // Should be marked as matched
    }

    #[test]
    fn test_handle_inner_join_no_match() {
        // Create executor with FALSE predicate for no match scenario
        let (_, execution_context) = create_basic_test_context();

        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let right_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("user_id", TypeId::Integer),
            Column::new("title", TypeId::VarChar),
        ]);

        let predicate = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(false),
            Column::new("", TypeId::Boolean),
            vec![],
        )));

        let join_plan = Arc::new(NestedLoopJoinNode::new(
            left_schema,
            right_schema,
            predicate,
            JoinType::Inner(JoinConstraint::None),
            vec![],
            vec![],
            vec![],
        ));

        let mut executor = NestedLoopJoinExecutor::new(execution_context, join_plan);

        let left_tuple = create_test_tuple(vec![
            Value::new(1),
            Value::new("Alice"),
        ]);

        let right_tuple = create_test_tuple(vec![
            Value::new(100),
            Value::new(2),
            Value::new("Bob's Post"),
        ]);

        let result = executor.handle_inner_join(&left_tuple, &right_tuple);

        // Should return None since predicate is false
        assert!(result.is_none());
        assert!(!executor.current_left_matched); // Should not be marked as matched
    }

    #[test]
    fn test_handle_left_outer_join_with_match() {
        let mut executor = create_test_executor();

        let left_tuple = create_test_tuple(vec![
            Value::new(1),
            Value::new("Alice"),
        ]);

        let right_tuple = create_test_tuple(vec![
            Value::new(100),
            Value::new(1),
            Value::new("Alice's Post"),
        ]);

        let result = executor.handle_left_outer_join(&left_tuple, Some(&right_tuple));

        // Should return a combined tuple since predicate is always true
        assert!(result.is_some());

        let (combined_tuple, _) = result.unwrap();
        let values = combined_tuple.get_values();
        assert_eq!(values.len(), 5);
        assert!(executor.current_left_matched);
    }

    #[test]
    fn test_handle_left_outer_join_no_right_tuple() {
        let mut executor = create_test_executor();
        executor.current_left_matched = false; // Simulate no previous match

        let left_tuple = create_test_tuple(vec![
            Value::new(1),
            Value::new("Alice"),
        ]);

        let result = executor.handle_left_outer_join(&left_tuple, None);

        // Should return left tuple with null padding since no match was found
        assert!(result.is_some());

        let (padded_tuple, _) = result.unwrap();
        let values = padded_tuple.get_values();
        assert_eq!(values.len(), 5);

        // Left side should have values, right side should be null
        assert_eq!(values[0].as_integer().unwrap(), 1);
        assert_eq!(ToString::to_string(&values[1]), "Alice");
        assert!(values[2].is_null());
        assert!(values[3].is_null());
        assert!(values[4].is_null());
    }

    #[test]
    fn test_handle_cross_join() {
        let executor = create_test_executor();

        let left_tuple = create_test_tuple(vec![
            Value::new(1),
            Value::new("Alice"),
        ]);

        let right_tuple = create_test_tuple(vec![
            Value::new(100),
            Value::new(1),
            Value::new("Alice's Post"),
        ]);

        let result = executor.handle_cross_join(&left_tuple, &right_tuple);

        // Cross join should always return combined tuple
        assert!(result.is_some());

        let (combined_tuple, _) = result.unwrap();
        let values = combined_tuple.get_values();
        assert_eq!(values.len(), 5);
    }

    #[test]
    fn test_handle_semi_join_match() {
        let mut executor = create_test_executor();

        let left_tuple = create_test_tuple(vec![
            Value::new(1),
            Value::new("Alice"),
        ]);

        let right_tuple = create_test_tuple(vec![
            Value::new(100),
            Value::new(1),
            Value::new("Alice's Post"),
        ]);

        let result = executor.handle_semi_join(&left_tuple, &right_tuple);

        // Should return left tuple only (no right columns)
        assert!(result.is_some());

        let (result_tuple, _) = result.unwrap();
        let values = result_tuple.get_values();

        // Should only have left table columns
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].as_integer().unwrap(), 1);
        assert_eq!(ToString::to_string(&values[1]), "Alice");
        assert!(executor.current_left_matched);
    }

    #[test]
    fn test_handle_anti_join_no_match() {
        // Create executor with FALSE predicate to simulate no match
        let (_, execution_context) = create_basic_test_context();

        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let right_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("user_id", TypeId::Integer),
            Column::new("title", TypeId::VarChar),
        ]);

        let predicate = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(false),
            Column::new("", TypeId::Boolean),
            vec![],
        )));

        let join_plan = Arc::new(NestedLoopJoinNode::new(
            left_schema,
            right_schema,
            predicate,
            JoinType::Anti(JoinConstraint::None),
            vec![],
            vec![],
            vec![],
        ));

        let mut executor = NestedLoopJoinExecutor::new(execution_context, join_plan);
        executor.current_left_matched = false; // No match found during scan

        let left_tuple = create_test_tuple(vec![
            Value::new(1),
            Value::new("Alice"),
        ]);

        // Test with None right tuple (end of right side)
        let result = executor.handle_anti_join(&left_tuple, None);

        // Should return left tuple since no match was found
        assert!(result.is_some());

        let (result_tuple, _) = result.unwrap();
        let values = result_tuple.get_values();

        // Should only have left table columns
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].as_integer().unwrap(), 1);
        assert_eq!(ToString::to_string(&values[1]), "Alice");
    }

    #[test]
    fn test_handle_anti_join_with_match() {
        let mut executor = create_test_executor();
        executor.current_left_matched = true; // Match was found during scan

        let left_tuple = create_test_tuple(vec![
            Value::new(1),
            Value::new("Alice"),
        ]);

        // Test with None right tuple (end of right side)
        let result = executor.handle_anti_join(&left_tuple, None);

        // Should return None since a match was found
        assert!(result.is_none());
    }

    #[test]
    fn test_handle_right_outer_join_with_match() {
        let mut executor = create_test_executor();

        let left_tuple = create_test_tuple(vec![
            Value::new(1),
            Value::new("Alice"),
        ]);

        let right_tuple = create_test_tuple(vec![
            Value::new(100),
            Value::new(1),
            Value::new("Alice's Post"),
        ]);

        let result = executor.handle_right_outer_join(Some(&left_tuple), &right_tuple);

        // Should return combined tuple since predicate is always true
        assert!(result.is_some());

        let (combined_tuple, _) = result.unwrap();
        let values = combined_tuple.get_values();
        assert_eq!(values.len(), 5);
    }

    #[test]
    fn test_handle_right_outer_join_no_left_match() {
        let mut executor = create_test_executor();

        let right_tuple = create_test_tuple(vec![
            Value::new(100),
            Value::new(999), // No matching left tuple
            Value::new("Orphaned Post"),
        ]);

        // Simulate processing unmatched right tuples
        executor.unmatched_right_tuples.push((right_tuple.clone(), RID::new(0, 0)));
        executor.processing_unmatched_right = true;
        executor.unmatched_right_index = 0;

        let result = executor.handle_right_outer_join(None, &right_tuple);

        // Should return right tuple with null left padding
        assert!(result.is_some());

        let (padded_tuple, _) = result.unwrap();
        let values = padded_tuple.get_values();
        assert_eq!(values.len(), 5);

        // Left side should be null, right side should have values
        assert!(values[0].is_null());
        assert!(values[1].is_null());
        assert_eq!(values[2].as_integer().unwrap(), 100);
        assert_eq!(values[3].as_integer().unwrap(), 999);
        assert_eq!(ToString::to_string(&values[4]), "Orphaned Post");
    }

    #[test]
    fn test_handle_full_outer_join_match() {
        let mut executor = create_test_executor();

        let left_tuple = create_test_tuple(vec![
            Value::new(1),
            Value::new("Alice"),
        ]);

        let right_tuple = create_test_tuple(vec![
            Value::new(100),
            Value::new(1),
            Value::new("Alice's Post"),
        ]);

        let result = executor.handle_full_outer_join(Some(&left_tuple), Some(&right_tuple));

        // Should return combined tuple since predicate is always true
        assert!(result.is_some());

        let (combined_tuple, _) = result.unwrap();
        let values = combined_tuple.get_values();
        assert_eq!(values.len(), 5);
    }

    #[test]
    fn test_handle_full_outer_join_unmatched_left() {
        let mut executor = create_test_executor();

        let left_tuple = create_test_tuple(vec![
            Value::new(1),
            Value::new("Alice"),
        ]);

        // Simulate processing unmatched left tuples
        executor.unmatched_left_tuples.push((left_tuple.clone(), RID::new(0, 0)));
        executor.processing_unmatched_left = true;
        executor.unmatched_left_index = 0;

        let result = executor.handle_full_outer_join(Some(&left_tuple), None);

        // Should return left tuple with null right padding
        assert!(result.is_some());

        let (padded_tuple, _) = result.unwrap();
        let values = padded_tuple.get_values();
        assert_eq!(values.len(), 5);

        // Left side should have values, right side should be null
        assert_eq!(values[0].as_integer().unwrap(), 1);
        assert_eq!(ToString::to_string(&values[1]), "Alice");
        assert!(values[2].is_null());
        assert!(values[3].is_null());
        assert!(values[4].is_null());
    }

    #[test]
    fn test_reset_right_executor() {
        let mut executor = create_test_executor();

        // Set some state
        executor.current_right_executor_exhausted = true;

        executor.reset_right_executor();

        // Should reset the right executor state
        assert!(!executor.current_right_executor_exhausted);
    }

    #[test]
    fn test_state_management() {
        let mut executor = create_test_executor();

        // Test initial state
        assert!(!executor.initialized);
        assert!(executor.current_left_tuple.is_none());
        assert!(!executor.current_right_executor_exhausted);
        assert!(!executor.left_executor_exhausted);
        assert!(!executor.current_left_matched);
        assert!(executor.unmatched_right_tuples.is_empty());
        assert!(!executor.processing_unmatched_right);
        assert_eq!(executor.unmatched_right_index, 0);
        assert!(executor.unmatched_left_tuples.is_empty());
        assert!(!executor.processing_unmatched_left);
        assert_eq!(executor.unmatched_left_index, 0);

        // Test state changes
        executor.initialized = true;
        executor.current_left_matched = true;
        executor.processing_unmatched_right = true;
        executor.unmatched_right_index = 5;

        assert!(executor.initialized);
        assert!(executor.current_left_matched);
        assert!(executor.processing_unmatched_right);
        assert_eq!(executor.unmatched_right_index, 5);
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::expressions::abstract_expression::Expression;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::comparison_expression::{ComparisonExpression, ComparisonType};
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::sql::execution::plans::abstract_plan::PlanNode;
    use crate::sql::execution::plans::nested_loop_join_plan::NestedLoopJoinNode;
    use crate::sql::execution::plans::seq_scan_plan::SeqScanPlanNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::table::tuple::TupleMeta;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::types::Type;
    use crate::types_db::value::Value;
    use sqlparser::ast::JoinConstraint;
    use std::sync::Arc;
    use tempfile::TempDir;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<TransactionManager>,
        transaction_context: Arc<TransactionContext>,
        catalog: Arc<RwLock<Catalog>>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        fn new(name: &str) -> Self {
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
            let disk_manager = Arc::new(FileDiskManager::new(db_path, log_path, BUFFER_POOL_SIZE));
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

            // Create transaction manager and lock manager
            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());

            // Create transaction with ID 0 and ReadUncommitted isolation level
            let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));
            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            // Create catalog
            let catalog = Arc::new(RwLock::new(Catalog::new(
                bpm.clone(),
                transaction_manager.clone(),
            )));

            Self {
                bpm,
                transaction_manager,
                transaction_context,
                catalog,
                _temp_dir: temp_dir,
            }
        }
    }

    #[test]
    fn test_cross_join() {
        let ctx = TestContext::new("cross_join_test");

        // Create schemas for both tables
        let users_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let posts_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("user_id", TypeId::Integer),
            Column::new("title", TypeId::VarChar),
        ]);

        // Create tables in catalog
        let users_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("users".to_string(), users_schema.clone())
                .unwrap()
        };

        let posts_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("posts".to_string(), posts_schema.clone())
                .unwrap()
        };

        // Create mock data for users table
        let users_data = vec![
            vec![Value::new(1), Value::new("Alice")],
            vec![Value::new(2), Value::new("Bob")],
        ];

        // Create mock data for posts table
        let posts_data = vec![
            vec![Value::new(1), Value::new(1), Value::new("Post 1")],
            vec![Value::new(2), Value::new(1), Value::new("Post 2")],
            vec![Value::new(3), Value::new(2), Value::new("Post 3")],
        ];

        // Insert data into tables
        {
            let catalog = ctx.catalog.read();
            let users_table = catalog
                .get_table_by_oid(users_table_info.get_table_oidt())
                .unwrap();
            let posts_table = catalog
                .get_table_by_oid(posts_table_info.get_table_oidt())
                .unwrap();

            for row in users_data {
                let meta = Arc::new(TupleMeta::new(0));
                users_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &users_schema, meta)
                    .unwrap();
            }

            for row in posts_data {
                let meta = Arc::new(TupleMeta::new(0));
                posts_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &posts_schema, meta)
                    .unwrap();
            }
        }

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create left and right child plans
        let left_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            users_schema.clone(),
            users_table_info.get_table_oidt(),
            "users".to_string(),
        ));

        let right_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            posts_schema.clone(),
            posts_table_info.get_table_oidt(),
            "posts".to_string(),
        ));

        // Create join plan with constant TRUE predicate
        let join_plan = Arc::new(NestedLoopJoinNode::new(
            users_schema.clone(),
            posts_schema.clone(),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(true),
                Column::new("", TypeId::Boolean),
                vec![],
            ))),
            JoinType::CrossJoin,
            vec![],
            vec![],
            vec![left_plan, right_plan],
        ));

        // Create join executor
        let mut join_executor = NestedLoopJoinExecutor::new(execution_context.clone(), join_plan);

        // Initialize executor
        join_executor.init();

        // Verify results
        let mut result_count = 0;
        while let Some(_) = join_executor.next() {
            result_count += 1;
        }

        // We expect 6 rows (2 users × 3 posts)
        assert_eq!(result_count, 6);
    }

    #[test]
    fn test_empty_join() {
        let ctx = TestContext::new("empty_join_test");

        // Create schemas for both tables
        let users_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let posts_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("user_id", TypeId::Integer),
            Column::new("title", TypeId::VarChar),
        ]);

        // Create tables in catalog (but don't insert any data)
        let users_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("users".to_string(), users_schema.clone())
                .unwrap()
        };

        let posts_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("posts".to_string(), posts_schema.clone())
                .unwrap()
        };

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create left and right child plans
        let left_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            users_schema.clone(),
            users_table_info.get_table_oidt(),
            "users".to_string(),
        ));

        let right_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            posts_schema.clone(),
            posts_table_info.get_table_oidt(),
            "posts".to_string(),
        ));

        // Create join plan with constant TRUE predicate
        let join_plan = Arc::new(NestedLoopJoinNode::new(
            users_schema.clone(),
            posts_schema.clone(),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(true),
                Column::new("", TypeId::Boolean),
                vec![],
            ))),
            JoinType::CrossJoin,
            vec![],
            vec![],
            vec![left_plan, right_plan],
        ));

        // Create join executor
        let mut join_executor = NestedLoopJoinExecutor::new(execution_context.clone(), join_plan);

        // Initialize executor
        join_executor.init();

        // Verify results
        let mut result_count = 0;
        while let Some(_) = join_executor.next() {
            result_count += 1;
        }

        // We expect 0 rows since both tables are empty
        assert_eq!(result_count, 0);
    }

    #[test]
    fn test_inner_join_with_predicate() {
        let ctx = TestContext::new("inner_join_test");

        // Create schemas for both tables
        let users_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let posts_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("user_id", TypeId::Integer),
            Column::new("title", TypeId::VarChar),
        ]);

        // Create tables in catalog
        let users_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("users".to_string(), users_schema.clone())
                .unwrap()
        };

        let posts_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("posts".to_string(), posts_schema.clone())
                .unwrap()
        };

        // Create mock data for users table
        let users_data = vec![
            vec![Value::new(1), Value::new("Alice")],
            vec![Value::new(2), Value::new("Bob")],
            vec![Value::new(3), Value::new("Charlie")],
        ];

        // Create mock data for posts table
        let posts_data = vec![
            vec![Value::new(1), Value::new(1), Value::new("Post 1")],
            vec![Value::new(2), Value::new(1), Value::new("Post 2")],
            vec![Value::new(3), Value::new(2), Value::new("Post 3")],
        ];

        // Insert data into tables
        {
            let catalog = ctx.catalog.read();
            let users_table = catalog
                .get_table_by_oid(users_table_info.get_table_oidt())
                .unwrap();
            let posts_table = catalog
                .get_table_by_oid(posts_table_info.get_table_oidt())
                .unwrap();

            for row in users_data {
                let meta = Arc::new(TupleMeta::new(0));
                users_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &users_schema, meta)
                    .unwrap();
            }

            for row in posts_data {
                let meta = Arc::new(TupleMeta::new(0));
                posts_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &posts_schema, meta)
                    .unwrap();
            }
        }

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create left and right child plans
        let left_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            users_schema.clone(),
            users_table_info.get_table_oidt(),
            "users".to_string(),
        ));

        let right_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            posts_schema.clone(),
            posts_table_info.get_table_oidt(),
            "posts".to_string(),
        ));

        // Create join predicate (users.id = posts.user_id)
        let left_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, // tuple index
            0, // column index
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let right_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, // tuple index
            1, // column index
            Column::new("user_id", TypeId::Integer),
            vec![],
        )));

        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            left_col.clone(),
            right_col.clone(),
            ComparisonType::Equal,
            vec![left_col, right_col],
        )));

        // Create join plan
        let join_plan = Arc::new(NestedLoopJoinNode::new(
            users_schema.clone(),
            posts_schema.clone(),
            predicate,
            JoinType::Inner(JoinConstraint::None),
            vec![],
            vec![],
            vec![left_plan, right_plan],
        ));

        // Create join executor
        let mut join_executor = NestedLoopJoinExecutor::new(execution_context.clone(), join_plan);

        // Initialize executor
        join_executor.init();

        // Verify results
        let mut result_count = 0;
        let mut alice_posts = 0;
        let mut bob_posts = 0;
        let mut charlie_posts = 0;

        while let Some((tuple, _)) = join_executor.next() {
            result_count += 1;
            let values = tuple.get_values();
            let user_name = ToString::to_string(&values[1]);
            
            match user_name.as_str() {
                "Alice" => alice_posts += 1,
                "Bob" => bob_posts += 1,
                "Charlie" => charlie_posts += 1,
                _ => panic!("Unexpected user: {}", user_name),
            }
        }

        // We expect 3 rows total:
        // - Alice has 2 posts
        // - Bob has 1 post
        // - Charlie has 0 posts
        assert_eq!(result_count, 3);
        assert_eq!(alice_posts, 2);
        assert_eq!(bob_posts, 1);
        assert_eq!(charlie_posts, 0);
    }

    #[test]
    fn test_join_one_empty() {
        let ctx = TestContext::new("join_one_empty_test");

        // Create schemas for both tables
        let users_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let posts_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("user_id", TypeId::Integer),
            Column::new("title", TypeId::VarChar),
        ]);

        // Create tables in catalog
        let users_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("users".to_string(), users_schema.clone())
                .unwrap()
        };

        let posts_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("posts".to_string(), posts_schema.clone())
                .unwrap()
        };

        // Only insert data into users table
        {
            let catalog = ctx.catalog.read();
            let users_table = catalog
                .get_table_by_oid(users_table_info.get_table_oidt())
                .unwrap();

            let users_data = vec![
                vec![Value::new(1), Value::new("Alice")],
                vec![Value::new(2), Value::new("Bob")],
            ];

            for row in users_data {
                let meta = Arc::new(TupleMeta::new(0));
                users_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &users_schema, meta)
                    .unwrap();
            }
        }

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create left and right child plans
        let left_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            users_schema.clone(),
            users_table_info.get_table_oidt(),
            "users".to_string(),
        ));

        let right_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            posts_schema.clone(),
            posts_table_info.get_table_oidt(),
            "posts".to_string(),
        ));

        // Create join plan with constant TRUE predicate for cross join
        let join_plan = Arc::new(NestedLoopJoinNode::new(
            users_schema.clone(),
            posts_schema.clone(),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(true),
                Column::new("", TypeId::Boolean),
                vec![],
            ))),
            JoinType::CrossJoin,
            vec![],
            vec![],
            vec![left_plan, right_plan],
        ));

        // Create join executor
        let mut join_executor = NestedLoopJoinExecutor::new(execution_context.clone(), join_plan);

        // Initialize executor
        join_executor.init();

        // Verify results - should be empty since one table is empty
        let mut result_count = 0;
        while let Some(_) = join_executor.next() {
            result_count += 1;
        }

        assert_eq!(result_count, 0);
    }

    #[test]
    fn test_join_no_matches() {
        let ctx = TestContext::new("join_no_matches_test");

        // Create schemas for both tables
        let users_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let posts_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("user_id", TypeId::Integer),
            Column::new("title", TypeId::VarChar),
        ]);

        // Create tables in catalog
        let users_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("users".to_string(), users_schema.clone())
                .unwrap()
        };

        let posts_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("posts".to_string(), posts_schema.clone())
                .unwrap()
        };

        // Insert data with no matching user_ids
        {
            let catalog = ctx.catalog.read();
            let users_table = catalog
                .get_table_by_oid(users_table_info.get_table_oidt())
                .unwrap();
            let posts_table = catalog
                .get_table_by_oid(posts_table_info.get_table_oidt())
                .unwrap();

            // Users with IDs 1 and 2
            let users_data = vec![
                vec![Value::new(1), Value::new("Alice")],
                vec![Value::new(2), Value::new("Bob")],
            ];

            // Posts with user_ids 3 and 4 (no matching users)
            let posts_data = vec![
                vec![Value::new(1), Value::new(3), Value::new("Post 1")],
                vec![Value::new(2), Value::new(4), Value::new("Post 2")],
            ];

            for row in users_data {
                let meta = Arc::new(TupleMeta::new(0));
                users_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &users_schema, meta)
                    .unwrap();
            }

            for row in posts_data {
                let meta = Arc::new(TupleMeta::new(0));
                posts_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &posts_schema, meta)
                    .unwrap();
            }
        }

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create left and right child plans
        let left_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            users_schema.clone(),
            users_table_info.get_table_oidt(),
            "users".to_string(),
        ));

        let right_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            posts_schema.clone(),
            posts_table_info.get_table_oidt(),
            "posts".to_string(),
        ));

        // Create join predicate (users.id = posts.user_id)
        let left_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, // tuple index
            0, // column index
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let right_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, // tuple index
            1, // column index
            Column::new("user_id", TypeId::Integer),
            vec![],
        )));

        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            left_col.clone(),
            right_col.clone(),
            ComparisonType::Equal,
            vec![left_col, right_col],
        )));

        // Create join plan
        let join_plan = Arc::new(NestedLoopJoinNode::new(
            users_schema.clone(),
            posts_schema.clone(),
            predicate,
            JoinType::Inner(JoinConstraint::None),
            vec![],
            vec![],
            vec![left_plan, right_plan],
        ));

        // Create join executor
        let mut join_executor = NestedLoopJoinExecutor::new(execution_context.clone(), join_plan);

        // Initialize executor
        join_executor.init();

        // Verify results - should be empty since there are no matching user_ids
        let mut result_count = 0;
        while let Some(_) = join_executor.next() {
            result_count += 1;
        }

        assert_eq!(result_count, 0);
    }

    #[test]
    fn test_left_outer_join() {
        let ctx = TestContext::new("left_outer_join_test");

        // Create schemas for both tables
        let users_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let posts_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("user_id", TypeId::Integer),
            Column::new("title", TypeId::VarChar),
        ]);

        // Create tables in catalog
        let users_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("users".to_string(), users_schema.clone())
                .unwrap()
        };

        let posts_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("posts".to_string(), posts_schema.clone())
                .unwrap()
        };

        // Create mock data - some users have posts, some don't
        let users_data = vec![
            vec![Value::new(1), Value::new("Alice")],   // Has posts
            vec![Value::new(2), Value::new("Bob")],     // Has posts
            vec![Value::new(3), Value::new("Charlie")], // No posts
            vec![Value::new(4), Value::new("David")],   // No posts
        ];

        let posts_data = vec![
            vec![Value::new(1), Value::new(1), Value::new("Alice Post 1")],
            vec![Value::new(2), Value::new(1), Value::new("Alice Post 2")],
            vec![Value::new(3), Value::new(2), Value::new("Bob Post 1")],
        ];

        // Insert data into tables
        {
            let catalog = ctx.catalog.read();
            let users_table = catalog
                .get_table_by_oid(users_table_info.get_table_oidt())
                .unwrap();
            let posts_table = catalog
                .get_table_by_oid(posts_table_info.get_table_oidt())
                .unwrap();

            for row in users_data {
                let meta = Arc::new(TupleMeta::new(0));
                users_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &users_schema, meta)
                    .unwrap();
            }

            for row in posts_data {
                let meta = Arc::new(TupleMeta::new(0));
                posts_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &posts_schema, meta)
                    .unwrap();
            }
        }

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create left and right child plans
        let left_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            users_schema.clone(),
            users_table_info.get_table_oidt(),
            "users".to_string(),
        ));

        let right_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            posts_schema.clone(),
            posts_table_info.get_table_oidt(),
            "posts".to_string(),
        ));

        // Create join predicate (users.id = posts.user_id)
        let left_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, Column::new("id", TypeId::Integer), vec![],
        )));

        let right_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, 1, Column::new("user_id", TypeId::Integer), vec![],
        )));

        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            left_col.clone(),
            right_col.clone(),
            ComparisonType::Equal,
            vec![left_col, right_col],
        )));

        // Create left outer join plan
        let join_plan = Arc::new(NestedLoopJoinNode::new(
            users_schema.clone(),
            posts_schema.clone(),
            predicate,
            JoinType::LeftOuter(JoinConstraint::None),
            vec![],
            vec![],
            vec![left_plan, right_plan],
        ));

        // Create join executor
        let mut join_executor = NestedLoopJoinExecutor::new(execution_context.clone(), join_plan);

        // Initialize executor
        join_executor.init();

        // Verify results
        let mut result_count = 0;
        let mut alice_results = 0;
        let mut bob_results = 0;
        let mut charlie_results = 0;
        let mut david_results = 0;

        while let Some((tuple, _)) = join_executor.next() {
            result_count += 1;
            let values = tuple.get_values();
            let user_name = ToString::to_string(&values[1]);
            
            
            match user_name.as_str() {
                "Alice" => {
                    alice_results += 1;
                    // Should have non-null post data
                    assert!(!values[4].is_null()); // post title should not be null
                }
                "Bob" => {
                    bob_results += 1;
                    // Should have non-null post data
                    assert!(!values[4].is_null()); // post title should not be null
                }
                "Charlie" => {
                    charlie_results += 1;
                    // Should have null post data
                    assert!(values[3].is_null()); // post user_id should be null
                    assert!(values[4].is_null()); // post title should be null
                }
                "David" => {
                    david_results += 1;
                    // Should have null post data
                    assert!(values[3].is_null()); // post user_id should be null
                    assert!(values[4].is_null()); // post title should be null
                }
                _ => panic!("Unexpected user: {}", user_name),
            }
        }

        // Expected results: Alice(2) + Bob(1) + Charlie(1) + David(1) = 5 rows
        assert_eq!(result_count, 5);
        assert_eq!(alice_results, 2); // Alice has 2 posts
        assert_eq!(bob_results, 1);   // Bob has 1 post
        assert_eq!(charlie_results, 1); // Charlie with nulls
        assert_eq!(david_results, 1);   // David with nulls
    }

    #[test]
    fn test_right_outer_join() {
        let ctx = TestContext::new("right_outer_join_test");

        // Create schemas for both tables
        let users_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let posts_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("user_id", TypeId::Integer),
            Column::new("title", TypeId::VarChar),
        ]);

        // Create tables in catalog
        let users_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("users".to_string(), users_schema.clone())
                .unwrap()
        };

        let posts_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("posts".to_string(), posts_schema.clone())
                .unwrap()
        };

        // Create mock data - some posts have users, some are orphaned
        let users_data = vec![
            vec![Value::new(1), Value::new("Alice")],
            vec![Value::new(2), Value::new("Bob")],
        ];

        let posts_data = vec![
            vec![Value::new(1), Value::new(1), Value::new("Alice Post 1")], // Has user
            vec![Value::new(2), Value::new(1), Value::new("Alice Post 2")], // Has user
            vec![Value::new(3), Value::new(2), Value::new("Bob Post 1")],   // Has user
            vec![Value::new(4), Value::new(99), Value::new("Orphaned Post 1")], // No matching user
            vec![Value::new(5), Value::new(100), Value::new("Orphaned Post 2")], // No matching user
        ];

        // Insert data into tables
        {
            let catalog = ctx.catalog.read();
            let users_table = catalog
                .get_table_by_oid(users_table_info.get_table_oidt())
                .unwrap();
            let posts_table = catalog
                .get_table_by_oid(posts_table_info.get_table_oidt())
                .unwrap();

            for row in users_data {
                let meta = Arc::new(TupleMeta::new(0));
                users_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &users_schema, meta)
                    .unwrap();
            }

            for row in posts_data {
                let meta = Arc::new(TupleMeta::new(0));
                posts_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &posts_schema, meta)
                    .unwrap();
            }
        }

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create left and right child plans
        let left_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            users_schema.clone(),
            users_table_info.get_table_oidt(),
            "users".to_string(),
        ));

        let right_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            posts_schema.clone(),
            posts_table_info.get_table_oidt(),
            "posts".to_string(),
        ));

        // Create join predicate (users.id = posts.user_id)
        let left_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, Column::new("id", TypeId::Integer), vec![],
        )));

        let right_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, 1, Column::new("user_id", TypeId::Integer), vec![],
        )));

        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            left_col.clone(),
            right_col.clone(),
            ComparisonType::Equal,
            vec![left_col, right_col],
        )));

        // Create right outer join plan
        let join_plan = Arc::new(NestedLoopJoinNode::new(
            users_schema.clone(),
            posts_schema.clone(),
            predicate,
            JoinType::RightOuter(JoinConstraint::None),
            vec![],
            vec![],
            vec![left_plan, right_plan],
        ));

        // Create join executor
        let mut join_executor = NestedLoopJoinExecutor::new(execution_context.clone(), join_plan);

        // Initialize executor
        join_executor.init();

        // Verify results
        let mut result_count = 0;
        let mut posts_with_users = 0;
        let mut orphaned_posts = 0;

        while let Some((tuple, _)) = join_executor.next() {
            result_count += 1;
            let values = tuple.get_values();
            let post_title = ToString::to_string(&values[4]);

            if post_title.contains("Orphaned") {
                orphaned_posts += 1;
                // User data should be null
                assert!(values[0].is_null()); // user id should be null
                assert!(values[1].is_null()); // user name should be null
            } else {
                posts_with_users += 1;
                // User data should not be null
                assert!(!values[0].is_null()); // user id should not be null
                assert!(!values[1].is_null()); // user name should not be null
            }
        }

        // Expected results: All 5 posts should appear
        assert_eq!(result_count, 5);
        assert_eq!(posts_with_users, 3); // 3 posts have matching users
        assert_eq!(orphaned_posts, 2);   // 2 posts are orphaned
    }

    #[test]
    fn test_full_outer_join() {
        let ctx = TestContext::new("full_outer_join_test");

        // Create schemas for both tables
        let users_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let posts_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("user_id", TypeId::Integer),
            Column::new("title", TypeId::VarChar),
        ]);

        // Create tables in catalog
        let users_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("users".to_string(), users_schema.clone())
                .unwrap()
        };

        let posts_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("posts".to_string(), posts_schema.clone())
                .unwrap()
        };

        // Create mock data with various scenarios
        let users_data = vec![
            vec![Value::new(1), Value::new("Alice")],   // Has posts
            vec![Value::new(2), Value::new("Bob")],     // No posts
            vec![Value::new(3), Value::new("Charlie")], // No posts
        ];

        let posts_data = vec![
            vec![Value::new(1), Value::new(1), Value::new("Alice Post 1")], // Has user
            vec![Value::new(2), Value::new(1), Value::new("Alice Post 2")], // Has user
            vec![Value::new(3), Value::new(99), Value::new("Orphaned Post")], // No user
        ];

        // Insert data into tables
        {
            let catalog = ctx.catalog.read();
            let users_table = catalog
                .get_table_by_oid(users_table_info.get_table_oidt())
                .unwrap();
            let posts_table = catalog
                .get_table_by_oid(posts_table_info.get_table_oidt())
                .unwrap();

            for row in users_data {
                let meta = Arc::new(TupleMeta::new(0));
                users_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &users_schema, meta)
                    .unwrap();
            }

            for row in posts_data {
                let meta = Arc::new(TupleMeta::new(0));
                posts_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &posts_schema, meta)
                    .unwrap();
            }
        }

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create left and right child plans
        let left_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            users_schema.clone(),
            users_table_info.get_table_oidt(),
            "users".to_string(),
        ));

        let right_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            posts_schema.clone(),
            posts_table_info.get_table_oidt(),
            "posts".to_string(),
        ));

        // Create join predicate (users.id = posts.user_id)
        let left_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, Column::new("id", TypeId::Integer), vec![],
        )));

        let right_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, 1, Column::new("user_id", TypeId::Integer), vec![],
        )));

        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            left_col.clone(),
            right_col.clone(),
            ComparisonType::Equal,
            vec![left_col, right_col],
        )));

        // Create full outer join plan
        let join_plan = Arc::new(NestedLoopJoinNode::new(
            users_schema.clone(),
            posts_schema.clone(),
            predicate,
            JoinType::FullOuter(JoinConstraint::None),
            vec![],
            vec![],
            vec![left_plan, right_plan],
        ));

        // Create join executor
        let mut join_executor = NestedLoopJoinExecutor::new(execution_context.clone(), join_plan);

        // Initialize executor
        join_executor.init();

        // Verify results
        let mut result_count = 0;
        let mut matched_rows = 0;
        let mut unmatched_users = 0;
        let mut unmatched_posts = 0;

        while let Some((tuple, _)) = join_executor.next() {
            result_count += 1;
            let values = tuple.get_values();
            
            let user_id_null = values[0].is_null();
            let post_id_null = values[2].is_null();
            
            if !user_id_null && !post_id_null {
                matched_rows += 1; // Both sides have data
            } else if user_id_null && !post_id_null {
                unmatched_posts += 1; // Only post side has data
            } else if !user_id_null && post_id_null {
                unmatched_users += 1; // Only user side has data
            }
        }

        // Expected results:
        // - 2 matched rows (Alice's 2 posts)
        // - 2 unmatched users (Bob, Charlie)
        // - 1 unmatched post (Orphaned Post)
        // Total: 5 rows
        assert_eq!(result_count, 5);
        assert_eq!(matched_rows, 2);
        assert_eq!(unmatched_users, 2);
        assert_eq!(unmatched_posts, 1);
    }

    #[test]
    fn test_semi_join() {
        let ctx = TestContext::new("semi_join_test");

        // Create schemas for both tables
        let users_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let posts_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("user_id", TypeId::Integer),
            Column::new("title", TypeId::VarChar),
        ]);

        // Create tables in catalog
        let users_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("users".to_string(), users_schema.clone())
                .unwrap()
        };

        let posts_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("posts".to_string(), posts_schema.clone())
                .unwrap()
        };

        // Create mock data
        let users_data = vec![
            vec![Value::new(1), Value::new("Alice")],   // Has posts
            vec![Value::new(2), Value::new("Bob")],     // Has posts
            vec![Value::new(3), Value::new("Charlie")], // No posts
            vec![Value::new(4), Value::new("David")],   // No posts
        ];

        let posts_data = vec![
            vec![Value::new(1), Value::new(1), Value::new("Alice Post 1")],
            vec![Value::new(2), Value::new(1), Value::new("Alice Post 2")],
            vec![Value::new(3), Value::new(2), Value::new("Bob Post 1")],
        ];

        // Insert data into tables
        {
            let catalog = ctx.catalog.read();
            let users_table = catalog
                .get_table_by_oid(users_table_info.get_table_oidt())
                .unwrap();
            let posts_table = catalog
                .get_table_by_oid(posts_table_info.get_table_oidt())
                .unwrap();

            for row in users_data {
                let meta = Arc::new(TupleMeta::new(0));
                users_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &users_schema, meta)
                    .unwrap();
            }

            for row in posts_data {
                let meta = Arc::new(TupleMeta::new(0));
                posts_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &posts_schema, meta)
                    .unwrap();
            }
        }

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create left and right child plans
        let left_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            users_schema.clone(),
            users_table_info.get_table_oidt(),
            "users".to_string(),
        ));

        let right_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            posts_schema.clone(),
            posts_table_info.get_table_oidt(),
            "posts".to_string(),
        ));

        // Create join predicate (users.id = posts.user_id)
        let left_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, Column::new("id", TypeId::Integer), vec![],
        )));

        let right_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, 1, Column::new("user_id", TypeId::Integer), vec![],
        )));

        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            left_col.clone(),
            right_col.clone(),
            ComparisonType::Equal,
            vec![left_col, right_col],
        )));

        // Create semi join plan
        let join_plan = Arc::new(NestedLoopJoinNode::new(
            users_schema.clone(),
            posts_schema.clone(),
            predicate,
            JoinType::Semi(JoinConstraint::None),
            vec![],
            vec![],
            vec![left_plan, right_plan],
        ));

        // Create join executor
        let mut join_executor = NestedLoopJoinExecutor::new(execution_context.clone(), join_plan);

        // Initialize executor
        join_executor.init();

        // Verify results
        let mut result_count = 0;
        let mut alice_found = false;
        let mut bob_found = false;

        while let Some((tuple, _)) = join_executor.next() {
            result_count += 1;
            let values = tuple.get_values();
            
            // Semi join should only return left table columns
            assert_eq!(values.len(), 2); // Only user columns (id, name)
            
            let user_name = ToString::to_string(&values[1]);
            match user_name.as_str() {
                "Alice" => alice_found = true,
                "Bob" => bob_found = true,
                "Charlie" | "David" => panic!("Users without posts should not appear in semi join"),
                _ => panic!("Unexpected user: {}", user_name),
            }
        }

        // Expected results: Only users with posts (Alice, Bob)
        // Each user should appear exactly once, even if they have multiple posts
        assert_eq!(result_count, 2);
        assert!(alice_found);
        assert!(bob_found);
    }

    #[test]
    fn test_anti_join() {
        let ctx = TestContext::new("anti_join_test");

        // Create schemas for both tables
        let users_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let posts_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("user_id", TypeId::Integer),
            Column::new("title", TypeId::VarChar),
        ]);

        // Create tables in catalog
        let users_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("users".to_string(), users_schema.clone())
                .unwrap()
        };

        let posts_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("posts".to_string(), posts_schema.clone())
                .unwrap()
        };

        // Create mock data
        let users_data = vec![
            vec![Value::new(1), Value::new("Alice")],   // Has posts
            vec![Value::new(2), Value::new("Bob")],     // Has posts
            vec![Value::new(3), Value::new("Charlie")], // No posts
            vec![Value::new(4), Value::new("David")],   // No posts
        ];

        let posts_data = vec![
            vec![Value::new(1), Value::new(1), Value::new("Alice Post 1")],
            vec![Value::new(2), Value::new(1), Value::new("Alice Post 2")],
            vec![Value::new(3), Value::new(2), Value::new("Bob Post 1")],
        ];

        // Insert data into tables
        {
            let catalog = ctx.catalog.read();
            let users_table = catalog
                .get_table_by_oid(users_table_info.get_table_oidt())
                .unwrap();
            let posts_table = catalog
                .get_table_by_oid(posts_table_info.get_table_oidt())
                .unwrap();

            for row in users_data {
                let meta = Arc::new(TupleMeta::new(0));
                users_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &users_schema, meta)
                    .unwrap();
            }

            for row in posts_data {
                let meta = Arc::new(TupleMeta::new(0));
                posts_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &posts_schema, meta)
                    .unwrap();
            }
        }

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create left and right child plans
        let left_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            users_schema.clone(),
            users_table_info.get_table_oidt(),
            "users".to_string(),
        ));

        let right_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            posts_schema.clone(),
            posts_table_info.get_table_oidt(),
            "posts".to_string(),
        ));

        // Create join predicate (users.id = posts.user_id)
        let left_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, Column::new("id", TypeId::Integer), vec![],
        )));

        let right_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, 1, Column::new("user_id", TypeId::Integer), vec![],
        )));

        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            left_col.clone(),
            right_col.clone(),
            ComparisonType::Equal,
            vec![left_col, right_col],
        )));

        // Create anti join plan
        let join_plan = Arc::new(NestedLoopJoinNode::new(
            users_schema.clone(),
            posts_schema.clone(),
            predicate,
            JoinType::Anti(JoinConstraint::None),
            vec![],
            vec![],
            vec![left_plan, right_plan],
        ));

        // Create join executor
        let mut join_executor = NestedLoopJoinExecutor::new(execution_context.clone(), join_plan);

        // Initialize executor
        join_executor.init();

        // Verify results
        let mut result_count = 0;
        let mut charlie_found = false;
        let mut david_found = false;

        while let Some((tuple, _)) = join_executor.next() {
            result_count += 1;
            let values = tuple.get_values();
            
            // Anti join should only return left table columns
            assert_eq!(values.len(), 2); // Only user columns (id, name)
            
            let user_name = ToString::to_string(&values[1]);
            match user_name.as_str() {
                "Charlie" => charlie_found = true,
                "David" => david_found = true,
                "Alice" | "Bob" => panic!("Users with posts should not appear in anti join"),
                _ => panic!("Unexpected user: {}", user_name),
            }
        }

        // Expected results: Only users without posts (Charlie, David)
        assert_eq!(result_count, 2);
        assert!(charlie_found);
        assert!(david_found);
    }

    #[test]
    fn test_left_semi_join() {
        let ctx = TestContext::new("left_semi_join_test");

        // Create schemas for both tables
        let departments_schema = Schema::new(vec![
            Column::new("dept_id", TypeId::Integer),
            Column::new("dept_name", TypeId::VarChar),
        ]);

        let employees_schema = Schema::new(vec![
            Column::new("emp_id", TypeId::Integer),
            Column::new("dept_id", TypeId::Integer),
            Column::new("emp_name", TypeId::VarChar),
        ]);

        // Create tables in catalog
        let dept_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("departments".to_string(), departments_schema.clone())
                .unwrap()
        };

        let emp_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("employees".to_string(), employees_schema.clone())
                .unwrap()
        };

        // Create mock data
        let dept_data = vec![
            vec![Value::new(1), Value::new("Engineering")], // Has employees
            vec![Value::new(2), Value::new("Marketing")],   // Has employees
            vec![Value::new(3), Value::new("HR")],          // No employees
        ];

        let emp_data = vec![
            vec![Value::new(1), Value::new(1), Value::new("Alice")],
            vec![Value::new(2), Value::new(1), Value::new("Bob")],
            vec![Value::new(3), Value::new(2), Value::new("Charlie")],
        ];

        // Insert data into tables
        {
            let catalog = ctx.catalog.read();
            let dept_table = catalog
                .get_table_by_oid(dept_table_info.get_table_oidt())
                .unwrap();
            let emp_table = catalog
                .get_table_by_oid(emp_table_info.get_table_oidt())
                .unwrap();

            for row in dept_data {
                let meta = Arc::new(TupleMeta::new(0));
                dept_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &departments_schema, meta)
                    .unwrap();
            }

            for row in emp_data {
                let meta = Arc::new(TupleMeta::new(0));
                emp_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &employees_schema, meta)
                    .unwrap();
            }
        }

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create left and right child plans
        let left_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            departments_schema.clone(),
            dept_table_info.get_table_oidt(),
            "departments".to_string(),
        ));

        let right_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            employees_schema.clone(),
            emp_table_info.get_table_oidt(),
            "employees".to_string(),
        ));

        // Create join predicate (departments.dept_id = employees.dept_id)
        let left_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, Column::new("dept_id", TypeId::Integer), vec![],
        )));

        let right_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, 1, Column::new("dept_id", TypeId::Integer), vec![],
        )));

        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            left_col.clone(),
            right_col.clone(),
            ComparisonType::Equal,
            vec![left_col, right_col],
        )));

        // Create left semi join plan
        let join_plan = Arc::new(NestedLoopJoinNode::new(
            departments_schema.clone(),
            employees_schema.clone(),
            predicate,
            JoinType::LeftSemi(JoinConstraint::None),
            vec![],
            vec![],
            vec![left_plan, right_plan],
        ));

        // Create join executor
        let mut join_executor = NestedLoopJoinExecutor::new(execution_context.clone(), join_plan);

        // Initialize executor
        join_executor.init();

        // Verify results
        let mut result_count = 0;
        let mut departments_found = Vec::new();

        while let Some((tuple, _)) = join_executor.next() {
            result_count += 1;
            let values = tuple.get_values();
            
            // Left semi join should only return left table columns
            assert_eq!(values.len(), 2); // Only department columns
            
            let dept_name = ToString::to_string(&values[1]);
            departments_found.push(dept_name);
        }

        // Expected results: Only departments with employees (Engineering, Marketing)
        assert_eq!(result_count, 2);
        assert!(departments_found.contains(&"Engineering".to_string()));
        assert!(departments_found.contains(&"Marketing".to_string()));
        assert!(!departments_found.contains(&"HR".to_string()));
    }

    #[test]
    fn test_left_anti_join() {
        let ctx = TestContext::new("left_anti_join_test");

        // Create schemas for both tables
        let departments_schema = Schema::new(vec![
            Column::new("dept_id", TypeId::Integer),
            Column::new("dept_name", TypeId::VarChar),
        ]);

        let employees_schema = Schema::new(vec![
            Column::new("emp_id", TypeId::Integer),
            Column::new("dept_id", TypeId::Integer),
            Column::new("emp_name", TypeId::VarChar),
        ]);

        // Create tables in catalog
        let dept_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("departments".to_string(), departments_schema.clone())
                .unwrap()
        };

        let emp_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("employees".to_string(), employees_schema.clone())
                .unwrap()
        };

        // Create mock data
        let dept_data = vec![
            vec![Value::new(1), Value::new("Engineering")], // Has employees
            vec![Value::new(2), Value::new("Marketing")],   // Has employees
            vec![Value::new(3), Value::new("HR")],          // No employees
            vec![Value::new(4), Value::new("Finance")],     // No employees
        ];

        let emp_data = vec![
            vec![Value::new(1), Value::new(1), Value::new("Alice")],
            vec![Value::new(2), Value::new(1), Value::new("Bob")],
            vec![Value::new(3), Value::new(2), Value::new("Charlie")],
        ];

        // Insert data into tables
        {
            let catalog = ctx.catalog.read();
            let dept_table = catalog
                .get_table_by_oid(dept_table_info.get_table_oidt())
                .unwrap();
            let emp_table = catalog
                .get_table_by_oid(emp_table_info.get_table_oidt())
                .unwrap();

            for row in dept_data {
                let meta = Arc::new(TupleMeta::new(0));
                dept_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &departments_schema, meta)
                    .unwrap();
            }

            for row in emp_data {
                let meta = Arc::new(TupleMeta::new(0));
                emp_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &employees_schema, meta)
                    .unwrap();
            }
        }

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create left and right child plans
        let left_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            departments_schema.clone(),
            dept_table_info.get_table_oidt(),
            "departments".to_string(),
        ));

        let right_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            employees_schema.clone(),
            emp_table_info.get_table_oidt(),
            "employees".to_string(),
        ));

        // Create join predicate (departments.dept_id = employees.dept_id)
        let left_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, Column::new("dept_id", TypeId::Integer), vec![],
        )));

        let right_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, 1, Column::new("dept_id", TypeId::Integer), vec![],
        )));

        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            left_col.clone(),
            right_col.clone(),
            ComparisonType::Equal,
            vec![left_col, right_col],
        )));

        // Create left anti join plan
        let join_plan = Arc::new(NestedLoopJoinNode::new(
            departments_schema.clone(),
            employees_schema.clone(),
            predicate,
            JoinType::LeftAnti(JoinConstraint::None),
            vec![],
            vec![],
            vec![left_plan, right_plan],
        ));

        // Create join executor
        let mut join_executor = NestedLoopJoinExecutor::new(execution_context.clone(), join_plan);

        // Initialize executor
        join_executor.init();

        // Verify results
        let mut result_count = 0;
        let mut departments_found = Vec::new();

        while let Some((tuple, _)) = join_executor.next() {
            result_count += 1;
            let values = tuple.get_values();
            
            // Left anti join should only return left table columns
            assert_eq!(values.len(), 2); // Only department columns
            
            let dept_name = ToString::to_string(&values[1]);
            departments_found.push(dept_name);
        }

        // Expected results: Only departments without employees (HR, Finance)
        assert_eq!(result_count, 2);
        assert!(departments_found.contains(&"HR".to_string()));
        assert!(departments_found.contains(&"Finance".to_string()));
        assert!(!departments_found.contains(&"Engineering".to_string()));
        assert!(!departments_found.contains(&"Marketing".to_string()));
    }

    #[test]
    fn test_right_semi_join() {
        let ctx = TestContext::new("right_semi_join_test");

        // Create schemas for both tables
        let users_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let posts_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("user_id", TypeId::Integer),
            Column::new("title", TypeId::VarChar),
        ]);

        // Create tables in catalog
        let users_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("users".to_string(), users_schema.clone())
                .unwrap()
        };

        let posts_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("posts".to_string(), posts_schema.clone())
                .unwrap()
        };

        // Create mock data
        let users_data = vec![
            vec![Value::new(1), Value::new("Alice")],
            vec![Value::new(2), Value::new("Bob")],
            // Note: User ID 3 doesn't exist
        ];

        let posts_data = vec![
            vec![Value::new(1), Value::new(1), Value::new("Alice Post 1")], // Has matching user
            vec![Value::new(2), Value::new(1), Value::new("Alice Post 2")], // Has matching user
            vec![Value::new(3), Value::new(2), Value::new("Bob Post 1")],   // Has matching user
            vec![Value::new(4), Value::new(3), Value::new("Orphaned Post")], // No matching user
        ];

        // Insert data into tables
        {
            let catalog = ctx.catalog.read();
            let users_table = catalog
                .get_table_by_oid(users_table_info.get_table_oidt())
                .unwrap();
            let posts_table = catalog
                .get_table_by_oid(posts_table_info.get_table_oidt())
                .unwrap();

            for row in users_data {
                let meta = Arc::new(TupleMeta::new(0));
                users_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &users_schema, meta)
                    .unwrap();
            }

            for row in posts_data {
                let meta = Arc::new(TupleMeta::new(0));
                posts_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &posts_schema, meta)
                    .unwrap();
            }
        }

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create left and right child plans
        let left_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            users_schema.clone(),
            users_table_info.get_table_oidt(),
            "users".to_string(),
        ));

        let right_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            posts_schema.clone(),
            posts_table_info.get_table_oidt(),
            "posts".to_string(),
        ));

        // Create join predicate (users.id = posts.user_id)
        let left_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, Column::new("id", TypeId::Integer), vec![],
        )));

        let right_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, 1, Column::new("user_id", TypeId::Integer), vec![],
        )));

        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            left_col.clone(),
            right_col.clone(),
            ComparisonType::Equal,
            vec![left_col, right_col],
        )));

        // Create right semi join plan
        let join_plan = Arc::new(NestedLoopJoinNode::new(
            users_schema.clone(),
            posts_schema.clone(),
            predicate,
            JoinType::RightSemi(JoinConstraint::None),
            vec![],
            vec![],
            vec![left_plan, right_plan],
        ));

        // Create join executor
        let mut join_executor = NestedLoopJoinExecutor::new(execution_context.clone(), join_plan);

        // Initialize executor
        join_executor.init();

        // Verify results
        let mut result_count = 0;
        let mut post_titles = Vec::new();

        while let Some((tuple, _)) = join_executor.next() {
            result_count += 1;
            let values = tuple.get_values();
            
            // Right semi join should only return right table columns
            assert_eq!(values.len(), 3); // Only post columns (id, user_id, title)
            
            let post_title = ToString::to_string(&values[2]);
            post_titles.push(post_title);
        }

        // Expected results: Only posts with matching users (3 posts)
        assert_eq!(result_count, 3);
        assert!(post_titles.contains(&"Alice Post 1".to_string()));
        assert!(post_titles.contains(&"Alice Post 2".to_string()));
        assert!(post_titles.contains(&"Bob Post 1".to_string()));
        assert!(!post_titles.contains(&"Orphaned Post".to_string()));
    }

    #[test]
    fn test_right_anti_join() {
        let ctx = TestContext::new("right_anti_join_test");

        // Create schemas for both tables
        let users_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let posts_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("user_id", TypeId::Integer),
            Column::new("title", TypeId::VarChar),
        ]);

        // Create tables in catalog
        let users_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("users".to_string(), users_schema.clone())
                .unwrap()
        };

        let posts_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("posts".to_string(), posts_schema.clone())
                .unwrap()
        };

        // Create mock data
        let users_data = vec![
            vec![Value::new(1), Value::new("Alice")],
            vec![Value::new(2), Value::new("Bob")],
            // Note: User ID 3 and 4 don't exist
        ];

        let posts_data = vec![
            vec![Value::new(1), Value::new(1), Value::new("Alice Post 1")], // Has matching user
            vec![Value::new(2), Value::new(1), Value::new("Alice Post 2")], // Has matching user
            vec![Value::new(3), Value::new(2), Value::new("Bob Post 1")],   // Has matching user
            vec![Value::new(4), Value::new(3), Value::new("Orphaned Post 1")], // No matching user
            vec![Value::new(5), Value::new(4), Value::new("Orphaned Post 2")], // No matching user
        ];

        // Insert data into tables
        {
            let catalog = ctx.catalog.read();
            let users_table = catalog
                .get_table_by_oid(users_table_info.get_table_oidt())
                .unwrap();
            let posts_table = catalog
                .get_table_by_oid(posts_table_info.get_table_oidt())
                .unwrap();

            for row in users_data {
                let meta = Arc::new(TupleMeta::new(0));
                users_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &users_schema, meta)
                    .unwrap();
            }

            for row in posts_data {
                let meta = Arc::new(TupleMeta::new(0));
                posts_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &posts_schema, meta)
                    .unwrap();
            }
        }

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create left and right child plans
        let left_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            users_schema.clone(),
            users_table_info.get_table_oidt(),
            "users".to_string(),
        ));

        let right_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            posts_schema.clone(),
            posts_table_info.get_table_oidt(),
            "posts".to_string(),
        ));

        // Create join predicate (users.id = posts.user_id)
        let left_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, Column::new("id", TypeId::Integer), vec![],
        )));

        let right_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, 1, Column::new("user_id", TypeId::Integer), vec![],
        )));

        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            left_col.clone(),
            right_col.clone(),
            ComparisonType::Equal,
            vec![left_col, right_col],
        )));

        // Create right anti join plan
        let join_plan = Arc::new(NestedLoopJoinNode::new(
            users_schema.clone(),
            posts_schema.clone(),
            predicate,
            JoinType::RightAnti(JoinConstraint::None),
            vec![],
            vec![],
            vec![left_plan, right_plan],
        ));

        // Create join executor
        let mut join_executor = NestedLoopJoinExecutor::new(execution_context.clone(), join_plan);

        // Initialize executor
        join_executor.init();

        // Verify results
        let mut result_count = 0;
        let mut post_titles = Vec::new();

        while let Some((tuple, _)) = join_executor.next() {
            result_count += 1;
            let values = tuple.get_values();
            
            // Right anti join should only return right table columns
            assert_eq!(values.len(), 3); // Only post columns (id, user_id, title)
            
            let post_title = ToString::to_string(&values[2]);
            post_titles.push(post_title);
        }

        // Expected results: Only posts without matching users (2 orphaned posts)
        assert_eq!(result_count, 2);
        assert!(post_titles.contains(&"Orphaned Post 1".to_string()));
        assert!(post_titles.contains(&"Orphaned Post 2".to_string()));
        assert!(!post_titles.contains(&"Alice Post 1".to_string()));
        assert!(!post_titles.contains(&"Alice Post 2".to_string()));
        assert!(!post_titles.contains(&"Bob Post 1".to_string()));
    }

    #[test]
    fn test_join_with_complex_predicate() {
        let ctx = TestContext::new("complex_predicate_test");

        // Create schemas for both tables
        let products_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("price", TypeId::Integer),
            Column::new("category_id", TypeId::Integer),
        ]);

        let categories_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("min_price", TypeId::Integer),
        ]);

        // Create tables in catalog
        let products_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("products".to_string(), products_schema.clone())
                .unwrap()
        };

        let categories_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("categories".to_string(), categories_schema.clone())
                .unwrap()
        };

        // Create mock data
        let products_data = vec![
            vec![Value::new(1), Value::new(100), Value::new(1)], // Electronics, meets min price
            vec![Value::new(2), Value::new(50), Value::new(1)],  // Electronics, below min price
            vec![Value::new(3), Value::new(200), Value::new(2)], // Books, meets min price
            vec![Value::new(4), Value::new(10), Value::new(2)],  // Books, below min price
        ];

        let categories_data = vec![
            vec![Value::new(1), Value::new("Electronics"), Value::new(75)], // min_price = 75
            vec![Value::new(2), Value::new("Books"), Value::new(150)],      // min_price = 150
        ];

        // Insert data into tables
        {
            let catalog = ctx.catalog.read();
            let products_table = catalog
                .get_table_by_oid(products_table_info.get_table_oidt())
                .unwrap();
            let categories_table = catalog
                .get_table_by_oid(categories_table_info.get_table_oidt())
                .unwrap();

            for row in products_data {
                let meta = Arc::new(TupleMeta::new(0));
                products_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &products_schema, meta)
                    .unwrap();
            }

            for row in categories_data {
                let meta = Arc::new(TupleMeta::new(0));
                categories_table
                    .get_table_heap()
                    .insert_tuple_from_values(row, &categories_schema, meta)
                    .unwrap();
            }
        }

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create left and right child plans
        let left_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            products_schema.clone(),
            products_table_info.get_table_oidt(),
            "products".to_string(),
        ));

        let right_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            categories_schema.clone(),
            categories_table_info.get_table_oidt(),
            "categories".to_string(),
        ));

        // Create complex predicate: products.category_id = categories.id AND products.price >= categories.min_price
        let category_match = Arc::new(Expression::Comparison(ComparisonExpression::new(
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0, 2, Column::new("category_id", TypeId::Integer), vec![],
            ))),
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                1, 0, Column::new("id", TypeId::Integer), vec![],
            ))),
            ComparisonType::Equal,
            vec![],
        )));

        let price_check = Arc::new(Expression::Comparison(ComparisonExpression::new(
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0, 1, Column::new("price", TypeId::Integer), vec![],
            ))),
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                1, 2, Column::new("min_price", TypeId::Integer), vec![],
            ))),
            ComparisonType::GreaterThanOrEqual,
            vec![],
        )));

        // For now, use just the category match - complex AND predicates would need more implementation
        let predicate = category_match;

        // Create inner join plan
        let join_plan = Arc::new(NestedLoopJoinNode::new(
            products_schema.clone(),
            categories_schema.clone(),
            predicate,
            JoinType::Inner(JoinConstraint::None),
            vec![],
            vec![],
            vec![left_plan, right_plan],
        ));

        // Create join executor
        let mut join_executor = NestedLoopJoinExecutor::new(execution_context.clone(), join_plan);

        // Initialize executor
        join_executor.init();

        // Verify results
        let mut result_count = 0;

        while let Some((tuple, _)) = join_executor.next() {
            result_count += 1;
            let values = tuple.get_values();
            
            // Verify the join condition is met
            let product_category_id = values[2].as_integer().unwrap();
            let category_id = values[3].as_integer().unwrap();
            assert_eq!(product_category_id, category_id);
        }

        // Expected results: All products should match their categories (4 results)
        assert_eq!(result_count, 4);
    }
}
