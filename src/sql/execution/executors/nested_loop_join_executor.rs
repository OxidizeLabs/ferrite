use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::common::exception::DBError;
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
    fn create_null_padded_tuple(
        &self,
        left_tuple: Option<&Arc<Tuple>>,
        right_tuple: Option<&Arc<Tuple>>,
    ) -> Arc<Tuple> {
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
                trace!(
                    "Join predicate evaluation error: {:?}, treating as false",
                    e
                );
                false
            }
        }
    }

    /**
     * HELPER METHOD: Handle inner join logic
     * Returns Some(tuple) if join condition is met, None otherwise
     */
    fn handle_inner_join(
        &mut self,
        left_tuple: &Arc<Tuple>,
        right_tuple: &Arc<Tuple>,
    ) -> Option<(Arc<Tuple>, RID)> {
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
    fn handle_left_outer_join(
        &mut self,
        left_tuple: &Arc<Tuple>,
        right_tuple: Option<&Arc<Tuple>>,
    ) -> Option<(Arc<Tuple>, RID)> {
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
    fn handle_right_outer_join(
        &mut self,
        left_tuple: Option<&Arc<Tuple>>,
        right_tuple: &Arc<Tuple>,
    ) -> Option<(Arc<Tuple>, RID)> {
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
    fn handle_full_outer_join(
        &mut self,
        left_tuple: Option<&Arc<Tuple>>,
        right_tuple: Option<&Arc<Tuple>>,
    ) -> Option<(Arc<Tuple>, RID)> {
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
    fn handle_cross_join(
        &self,
        left_tuple: &Arc<Tuple>,
        right_tuple: &Arc<Tuple>,
    ) -> Option<(Arc<Tuple>, RID)> {
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
    fn handle_semi_join(
        &mut self,
        left_tuple: &Arc<Tuple>,
        right_tuple: &Arc<Tuple>,
    ) -> Option<(Arc<Tuple>, RID)> {
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
    fn handle_anti_join(
        &mut self,
        left_tuple: &Arc<Tuple>,
        right_tuple: Option<&Arc<Tuple>>,
    ) -> Option<(Arc<Tuple>, RID)> {
        // 1. If right_tuple is Some:
        if let Some(right_tuple) = right_tuple {
            //    a. Evaluate join predicate
            if self.evaluate_join_predicate(left_tuple, right_tuple) {
                //    b. If true: mark left tuple as matched (exclude from output)
                self.current_left_matched = true;
            }
            //    c. If false: continue checking
            None
        } else {
            // 2. If right_tuple is None (end of right side):
            //    a. If left tuple was never matched: return left tuple
            if !self.current_left_matched {
                //    b. Return left tuple ONLY (no right columns)
                // Create a new tuple with only left schema columns
                let left_schema = self.plan.get_left_schema();
                let left_values = left_tuple.get_values().clone();
                let left_rid = RID::new(0, 0); // Use dummy RID for joined tuples
                let left_only_tuple = Arc::new(Tuple::new(&left_values, left_schema, left_rid));

                Some((left_only_tuple, left_rid))
            } else {
                //    c. If left tuple was matched: return None
                None
            }
        }
        // 3. Only output left schema columns (handled above)
    }
}

impl AbstractExecutor for NestedLoopJoinExecutor {
    /**
     * INITIALIZATION PHASE
     * Creates and initializes child executors for both left and right sides
     */
    fn init(&mut self) {
        if self.initialized {
            trace!("NestedLoopJoinExecutor already initialized");
            return;
        }

        debug!("Initializing NestedLoopJoinExecutor");

        // 1. Create child executors from plan children
        let children_plans = self.plan.get_children();
        if children_plans.len() != 2 {
            panic!("NestedLoopJoin must have exactly 2 children");
        }

        // 2. Create left executor (index 0)
        let left_executor = children_plans[0]
            .create_executor(self.context.clone())
            .expect("Failed to create left child executor");

        // 3. Create right executor (index 1)
        let right_executor = children_plans[1]
            .create_executor(self.context.clone())
            .expect("Failed to create right child executor");

        // 4. Store in children_executors vector
        self.children_executors = Some(vec![left_executor, right_executor]);

        // 5. Initialize both child executors
        if let Some(ref mut children) = self.children_executors {
            children[0].init(); // Left executor
            children[1].init(); // Right executor
        }

        // 6. Initialize state variables for join processing
        self.current_left_tuple = None;
        self.current_right_executor_exhausted = false;
        self.left_executor_exhausted = false;
        self.current_left_matched = false;
        self.unmatched_right_tuples.clear();
        self.processing_unmatched_right = false;
        self.unmatched_right_index = 0;
        self.unmatched_left_tuples.clear();
        self.processing_unmatched_left = false;
        self.unmatched_left_index = 0;

        debug!("NestedLoopJoinExecutor initialization complete");
        self.initialized = true;
    }

    /**
     * MAIN EXECUTION METHOD - NESTED LOOP ALGORITHM
     * 
     * This method implements the core nested loop join algorithm:
     * 1. For each tuple from the left child (outer loop)
     * 2. For each tuple from the right child (inner loop)
     * 3. Evaluate join predicate and produce output based on join type
     * 
     * The method maintains state between calls to handle the nested iteration
     * and support all SQL join types including outer joins.
     */
    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if !self.initialized {
            self.init();
        }

        let children = match self.children_executors.as_mut() {
            Some(children) => children,
            None => return Err(DBError::Execution("Child executors not initialized".to_string())),
        };

        // Use split_at_mut to avoid multiple mutable borrows
        let (left_slice, right_slice) = children.split_at_mut(1);
        let left_executor = &mut left_slice[0];
        let right_executor = &mut right_slice[0];

        // Extract predicate and schemas to avoid borrowing issues
        let predicate = self.plan.get_predicate().clone();
        let left_schema = self.plan.get_left_schema().clone();
        let right_schema = self.plan.get_right_schema().clone();

        // Handle different phases of join processing
        loop {
            // PHASE 1: Process main join (matched pairs)
            if !self.processing_unmatched_right && !self.processing_unmatched_left {
                // Get current left tuple if we don't have one
                if self.current_left_tuple.is_none() && !self.left_executor_exhausted {
                    match left_executor.next()? {
                        Some((tuple, rid)) => {
                            self.current_left_tuple = Some((tuple, rid));
                            self.current_left_matched = false;
                            // Reset right executor for new left tuple
                            right_executor.init();
                            self.current_right_executor_exhausted = false;
                        }
                        None => {
                            self.left_executor_exhausted = true;
                        }
                    }
                }

                // If we have a left tuple, try to find matching right tuples
                if let Some((left_tuple, left_rid)) = self.current_left_tuple.clone() {
                    if !self.current_right_executor_exhausted {
                        // Try to get next right tuple
                        match right_executor.next()? {
                            Some((right_tuple, _right_rid)) => {
                                // Get join type before calling evaluation
                                let join_type = self.plan.get_join_type().clone();
                                
                                // Evaluate predicate without borrowing self
                                let predicate_result = match predicate.evaluate_join(&left_tuple, &left_schema, &right_tuple, &right_schema) {
                                    Ok(value) => {
                                        match value.as_bool() {
                                            Ok(result) => result,
                                            Err(_) => false, // Handle null values - treat as false
                                        }
                                    }
                                    Err(_) => false, // Handle evaluation errors as false
                                };

                                // Handle based on join type
                                match join_type {
                                    JoinType::Inner(_) => {
                                        if predicate_result {
                                            self.current_left_matched = true;
                                            let combined_tuple = self.combine_tuples(&left_tuple, &right_tuple);
                                            return Ok(Some((combined_tuple, left_rid)));
                                        }
                                    }
                                    JoinType::LeftOuter(_) => {
                                        if predicate_result {
                                            self.current_left_matched = true;
                                            let combined_tuple = self.combine_tuples(&left_tuple, &right_tuple);
                                            return Ok(Some((combined_tuple, left_rid)));
                                        }
                                        // For right outer and full outer, collect unmatched right tuples
                                        if !predicate_result {
                                            self.unmatched_right_tuples.push((right_tuple, _right_rid));
                                        }
                                    }
                                    JoinType::RightOuter(_) => {
                                        if predicate_result {
                                            self.current_left_matched = true;
                                            let combined_tuple = self.combine_tuples(&left_tuple, &right_tuple);
                                            return Ok(Some((combined_tuple, left_rid)));
                                        } else {
                                            // Collect unmatched right tuple
                                            self.unmatched_right_tuples.push((right_tuple, _right_rid));
                                        }
                                    }
                                    JoinType::FullOuter(_) => {
                                        if predicate_result {
                                            self.current_left_matched = true;
                                            let combined_tuple = self.combine_tuples(&left_tuple, &right_tuple);
                                            return Ok(Some((combined_tuple, left_rid)));
                                        } else {
                                            // Collect unmatched right tuple for later processing
                                            self.unmatched_right_tuples.push((right_tuple, _right_rid));
                                        }
                                    }
                                    JoinType::CrossJoin => {
                                        let combined_tuple = self.combine_tuples(&left_tuple, &right_tuple);
                                        return Ok(Some((combined_tuple, left_rid)));
                                    }
                                    _ => {
                                        return Err(DBError::Execution(format!(
                                            "Unsupported join type: {:?}",
                                            join_type
                                        )));
                                    }
                                }
                            }
                            None => {
                                // Right executor exhausted for current left tuple
                                self.current_right_executor_exhausted = true;
                            }
                        }
                    } else {
                        // Right executor exhausted - handle unmatched left tuple for outer joins
                        let join_type = self.plan.get_join_type().clone();
                        match join_type {
                            JoinType::LeftOuter(_) => {
                                if !self.current_left_matched {
                                    let padded_tuple = self.create_null_padded_tuple(Some(&left_tuple), None);
                                    self.current_left_tuple = None; // Move to next left tuple
                                    self.current_right_executor_exhausted = false;
                                    return Ok(Some((padded_tuple, left_rid)));
                                }
                            }
                            JoinType::FullOuter(_) => {
                                if !self.current_left_matched {
                                    // Store unmatched left tuple for later processing
                                    self.unmatched_left_tuples.push((left_tuple.clone(), left_rid));
                                }
                            }
                            _ => {}
                        }

                        // Move to next left tuple
                        self.current_left_tuple = None;
                        self.current_right_executor_exhausted = false;
                    }
                } else if self.left_executor_exhausted {
                    // No more left tuples - move to processing unmatched right tuples
                    let join_type = self.plan.get_join_type().clone();
                    match join_type {
                        JoinType::RightOuter(_) | JoinType::FullOuter(_) => {
                            self.processing_unmatched_right = true;
                        }
                        _ => {
                            // For other join types, we're done
                            return Ok(None);
                        }
                    }
                }
            }

            // PHASE 2: Process unmatched right tuples (for RIGHT/FULL OUTER joins)
            if self.processing_unmatched_right && !self.processing_unmatched_left {
                if self.unmatched_right_index < self.unmatched_right_tuples.len() {
                    let (right_tuple, right_rid) = self.unmatched_right_tuples[self.unmatched_right_index].clone();
                    self.unmatched_right_index += 1;

                    let join_type = self.plan.get_join_type().clone();
                    match join_type {
                        JoinType::RightOuter(_) => {
                            let padded_tuple = self.create_null_padded_tuple(None, Some(&right_tuple));
                            return Ok(Some((padded_tuple, right_rid)));
                        }
                        JoinType::FullOuter(_) => {
                            let padded_tuple = self.create_null_padded_tuple(None, Some(&right_tuple));
                            return Ok(Some((padded_tuple, right_rid)));
                        }
                        _ => {}
                    }
                } else {
                    // Done with unmatched right tuples
                    let join_type = self.plan.get_join_type().clone();
                    match join_type {
                        JoinType::FullOuter(_) => {
                            // Move to processing unmatched left tuples
                            self.processing_unmatched_left = true;
                            self.processing_unmatched_right = false;
                        }
                        _ => {
                            // Done with join
                            return Ok(None);
                        }
                    }
                }
            }

            // PHASE 3: Process unmatched left tuples (for FULL OUTER joins only)
            if self.processing_unmatched_left {
                if self.unmatched_left_index < self.unmatched_left_tuples.len() {
                    let (left_tuple, left_rid) = self.unmatched_left_tuples[self.unmatched_left_index].clone();
                    self.unmatched_left_index += 1;

                    let padded_tuple = self.create_null_padded_tuple(Some(&left_tuple), None);
                    return Ok(Some((padded_tuple, left_rid)));
                } else {
                    // Done with all join processing
                    return Ok(None);
                }
            }
        }
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.context.clone()
    }
}

#[cfg(test)]
mod unit_tests {
    // Add your test cases here
}
