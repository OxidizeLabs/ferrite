/**
 * REFACTORED NESTED LOOP JOIN EXECUTOR
 * ====================================
 *
 * This refactored version follows the Single Responsibility Principle by breaking
 * the monolithic join executor into composable parts:
 *
 * 1. JoinState - Manages execution state and progress tracking
 * 2. TupleCombiner - Handles tuple combination and null padding
 * 3. JoinPredicateEvaluator - Evaluates join predicates
 * 4. JoinTypeHandler - Implements join type-specific logic
 * 5. ExecutorManager - Manages child executors and reset logic
 * 6. NestedLoopJoinExecutor - Orchestrates all components
 */
use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::expressions::abstract_expression::ExpressionOps;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::nested_loop_join_plan::NestedLoopJoinNode;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::{Val, Value};
use log::{debug, info};
use parking_lot::RwLock;
use sqlparser::ast::JoinOperator as JoinType;
use std::sync::Arc;

// =============================================================================
// 1. JOIN STATE MANAGEMENT
// =============================================================================

/**
 * Represents the current state of join execution
 * Responsibility: Track progress through left/right tuples and join phases
 */
#[derive(Debug, Clone)]
pub enum JoinPhase {
    /// Processing main join (matching pairs)
    MainJoin,
    /// Processing unmatched right tuples for outer joins
    UnmatchedRight,
    /// Processing unmatched left tuples for full outer joins
    UnmatchedLeft,
    /// Join execution completed
    Completed,
}

#[derive(Debug)]
pub struct JoinState {
    /// Current execution phase
    pub phase: JoinPhase,

    /// Current left tuple being processed
    pub current_left_tuple: Option<(Arc<Tuple>, RID)>,

    /// Whether current left tuple has been matched
    pub current_left_matched: bool,

    /// Whether left executor is exhausted
    pub left_executor_exhausted: bool,

    /// Whether right executor is exhausted for current left tuple
    pub right_executor_exhausted: bool,

    /// Unmatched right tuples (for right/full outer joins)
    pub unmatched_right_tuples: Vec<(Arc<Tuple>, RID)>,
    pub unmatched_right_index: usize,

    /// Unmatched left tuples (for full outer joins)
    pub unmatched_left_tuples: Vec<(Arc<Tuple>, RID)>,
    pub unmatched_left_index: usize,

    /// All right tuples seen during main join (for full outer joins)
    pub all_right_tuples: Vec<(Arc<Tuple>, RID)>,

    /// Right tuples that have been matched (for full outer joins)
    pub matched_right_tuples: std::collections::HashSet<usize>,
}

impl Default for JoinState {
    fn default() -> Self {
        Self::new()
    }
}

impl JoinState {
    /// Create new join state
    pub fn new() -> Self {
        Self {
            phase: JoinPhase::MainJoin,
            current_left_tuple: None,
            current_left_matched: false,
            left_executor_exhausted: false,
            right_executor_exhausted: false,
            unmatched_right_tuples: Vec::new(),
            unmatched_right_index: 0,
            unmatched_left_tuples: Vec::new(),
            unmatched_left_index: 0,
            all_right_tuples: Vec::new(),
            matched_right_tuples: std::collections::HashSet::new(),
        }
    }

    /// Reset for new left tuple
    pub fn reset_for_new_left_tuple(&mut self, left_tuple: (Arc<Tuple>, RID)) {
        self.current_left_tuple = Some(left_tuple);
        self.current_left_matched = false;
        self.right_executor_exhausted = false;
    }

    /// Mark current left tuple as matched
    pub fn mark_left_matched(&mut self) {
        self.current_left_matched = true;
    }

    /// Move to next phase of join processing
    pub fn advance_phase(&mut self, join_type: &JoinType) {
        match (&self.phase, join_type) {
            (JoinPhase::MainJoin, JoinType::Right(_))
            | (JoinPhase::MainJoin, JoinType::RightOuter(_)) => {
                // For right joins, first collect unmatched right tuples
                self.collect_unmatched_right_tuples();
                self.phase = JoinPhase::UnmatchedRight;
            }
            (JoinPhase::MainJoin, JoinType::FullOuter(_)) => {
                // For full outer joins, first collect unmatched right tuples
                self.collect_unmatched_right_tuples();
                self.phase = JoinPhase::UnmatchedRight;
            }
            (JoinPhase::UnmatchedRight, JoinType::FullOuter(_)) => {
                self.phase = JoinPhase::UnmatchedLeft;
            }
            _ => {
                self.phase = JoinPhase::Completed;
            }
        }
    }

    /// Check if join execution is complete
    pub fn is_complete(&self) -> bool {
        matches!(self.phase, JoinPhase::Completed)
    }

    /// Clear current left tuple and reset right executor state
    pub fn clear_current_left_tuple(&mut self) {
        self.current_left_tuple = None;
        self.current_left_matched = false;
        self.right_executor_exhausted = false;
    }

    /// Add unmatched right tuple
    pub fn add_unmatched_right_tuple(&mut self, tuple: (Arc<Tuple>, RID)) {
        self.unmatched_right_tuples.push(tuple);
    }

    /// Add unmatched left tuple
    pub fn add_unmatched_left_tuple(&mut self, tuple: (Arc<Tuple>, RID)) {
        self.unmatched_left_tuples.push(tuple);
    }

    /// Get next unmatched right tuple
    pub fn next_unmatched_right_tuple(&mut self) -> Option<(Arc<Tuple>, RID)> {
        if self.unmatched_right_index < self.unmatched_right_tuples.len() {
            let tuple = self.unmatched_right_tuples[self.unmatched_right_index].clone();
            self.unmatched_right_index += 1;
            Some(tuple)
        } else {
            None
        }
    }

    /// Get next unmatched left tuple
    pub fn next_unmatched_left_tuple(&mut self) -> Option<(Arc<Tuple>, RID)> {
        if self.unmatched_left_index < self.unmatched_left_tuples.len() {
            let tuple = self.unmatched_left_tuples[self.unmatched_left_index].clone();
            self.unmatched_left_index += 1;
            Some(tuple)
        } else {
            None
        }
    }

    /// Add a right tuple to the collection (for full outer joins)
    pub fn add_right_tuple(&mut self, tuple: (Arc<Tuple>, RID)) {
        self.all_right_tuples.push(tuple);
    }

    /// Mark a right tuple as matched (for full outer joins)
    pub fn mark_right_tuple_matched(&mut self, index: usize) {
        self.matched_right_tuples.insert(index);
    }

    /// Collect unmatched right tuples for processing in UnmatchedRight phase
    pub fn collect_unmatched_right_tuples(&mut self) {
        self.unmatched_right_tuples.clear();
        for (i, tuple) in self.all_right_tuples.iter().enumerate() {
            if !self.matched_right_tuples.contains(&i) {
                self.unmatched_right_tuples.push(tuple.clone());
            }
        }
        self.unmatched_right_index = 0;
    }
}

// =============================================================================
// 2. TUPLE COMBINATION
// =============================================================================

/**
 * Handles tuple combination logic
 * Responsibility: Combine tuples and create null-padded tuples for outer joins
 */
#[derive(Clone)]
pub struct TupleCombiner {
    pub left_schema: Schema,
    pub right_schema: Schema,
    pub output_schema: Schema,
}

impl TupleCombiner {
    /// Create new tuple combiner
    pub fn new(left_schema: Schema, right_schema: Schema, output_schema: Schema) -> Self {
        Self {
            left_schema,
            right_schema,
            output_schema,
        }
    }

    /// Combine left and right tuples
    pub fn combine_tuples(&self, left_tuple: &Arc<Tuple>, right_tuple: &Arc<Tuple>) -> Arc<Tuple> {
        let mut combined_values = Vec::new();
        combined_values.extend(left_tuple.get_values().iter().cloned());
        combined_values.extend(right_tuple.get_values().iter().cloned());

        let rid = RID::new(0, 0); // Use dummy RID for joined tuples
        Arc::new(Tuple::new(&combined_values, &self.output_schema, rid))
    }

    /// Create null-padded tuple for outer joins
    pub fn create_null_padded_tuple(
        &self,
        left_tuple: Option<&Arc<Tuple>>,
        right_tuple: Option<&Arc<Tuple>>,
    ) -> Arc<Tuple> {
        let mut combined_values = Vec::new();

        // Add left tuple values or nulls
        if let Some(left_tuple) = left_tuple {
            combined_values.extend(left_tuple.get_values().iter().cloned());
        } else {
            for _ in self.left_schema.get_columns() {
                combined_values.push(Value::new(Val::Null));
            }
        }

        // Add right tuple values or nulls
        if let Some(right_tuple) = right_tuple {
            combined_values.extend(right_tuple.get_values().iter().cloned());
        } else {
            for _ in self.right_schema.get_columns() {
                combined_values.push(Value::new(Val::Null));
            }
        }

        let rid = RID::new(0, 0);
        Arc::new(Tuple::new(&combined_values, &self.output_schema, rid))
    }

    /// Create left-only tuple for semi/anti joins
    pub fn create_left_only_tuple(&self, left_tuple: &Arc<Tuple>) -> Arc<Tuple> {
        let left_values = left_tuple.get_values().clone();
        let rid = RID::new(0, 0);
        Arc::new(Tuple::new(&left_values, &self.left_schema, rid))
    }
}

// =============================================================================
// 3. JOIN PREDICATE EVALUATION
// =============================================================================

/**
 * Handles join predicate evaluation
 * Responsibility: Evaluate join conditions between tuple pairs
 */
#[derive(Clone)]
pub struct JoinPredicateEvaluator {
    predicate: Arc<dyn ExpressionOps + Send + Sync>,
    left_schema: Schema,
    right_schema: Schema,
}

impl JoinPredicateEvaluator {
    /// Create new predicate evaluator
    pub fn new(
        predicate: Arc<dyn ExpressionOps + Send + Sync>,
        left_schema: Schema,
        right_schema: Schema,
    ) -> Self {
        Self {
            predicate,
            left_schema,
            right_schema,
        }
    }

    /// Evaluate predicate for tuple pair
    pub fn evaluate(
        &self,
        left_tuple: &Arc<Tuple>,
        right_tuple: &Arc<Tuple>,
    ) -> Result<bool, DBError> {
        debug!("JoinPredicateEvaluator::evaluate called");
        debug!("Left tuple values: {:?}", left_tuple.get_values());
        debug!("Right tuple values: {:?}", right_tuple.get_values());
        debug!("About to evaluate predicate");

        let start_time = std::time::Instant::now();
        let result = self.predicate.evaluate_join(
            left_tuple,
            &self.left_schema,
            right_tuple,
            &self.right_schema,
        );
        let duration = start_time.elapsed();
        debug!("Predicate evaluation took: {:?}", duration);

        match result {
            Ok(value) => {
                debug!("Predicate evaluation succeeded with value: {:?}", value);
                // Use the correct method to extract boolean value
                match value.get_val() {
                    Val::Boolean(result) => {
                        debug!("Predicate evaluation result: {}", result);
                        Ok(*result)
                    }
                    Val::Null => {
                        debug!("Predicate evaluation returned null, treating as false");
                        Ok(false)
                    } // Treat null as false
                    other => {
                        debug!(
                            "Predicate evaluation returned non-boolean: {:?}, treating as false",
                            other
                        );
                        Ok(false)
                    } // Treat non-boolean as false
                }
            }
            Err(e) => {
                debug!("Predicate evaluation error: {}", e);
                Err(DBError::Execution(format!(
                    "Predicate evaluation error: {}",
                    e
                )))
            }
        }
    }
}

// =============================================================================
// 4. JOIN TYPE HANDLING
// =============================================================================

/**
 * Handles join type-specific logic
 * Responsibility: Implement behavior for different SQL join types
 */
pub struct JoinTypeHandler {
    join_type: JoinType,
    tuple_combiner: TupleCombiner,
    predicate_evaluator: JoinPredicateEvaluator,
}

impl JoinTypeHandler {
    /// Create new join type handler
    pub fn new(
        join_type: JoinType,
        tuple_combiner: TupleCombiner,
        predicate_evaluator: JoinPredicateEvaluator,
    ) -> Self {
        Self {
            join_type,
            tuple_combiner,
            predicate_evaluator,
        }
    }

    /// Process tuple pair for current join type
    pub fn process_tuple_pair(
        &self,
        left_tuple: &Arc<Tuple>,
        right_tuple: &Arc<Tuple>,
        state: &mut JoinState,
    ) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        match &self.join_type {
            JoinType::Inner(_) | JoinType::Join(_) => {
                self.handle_inner_join(left_tuple, right_tuple, state)
            }
            JoinType::Left(_) | JoinType::LeftOuter(_) => {
                self.handle_left_outer_join(left_tuple, Some(right_tuple), state)
            }
            JoinType::Right(_) | JoinType::RightOuter(_) => {
                self.handle_right_outer_join(Some(left_tuple), right_tuple, state)
            }
            JoinType::FullOuter(_) => {
                self.handle_full_outer_join(Some(left_tuple), Some(right_tuple), state)
            }
            JoinType::CrossJoin => {
                state.mark_left_matched();
                Ok(Some(self.handle_cross_join(left_tuple, right_tuple)))
            }
            _ => Err(DBError::Execution(format!(
                "Unsupported join type: {:?}",
                self.join_type
            ))),
        }
    }

    /// Handle inner join logic
    fn handle_inner_join(
        &self,
        left_tuple: &Arc<Tuple>,
        right_tuple: &Arc<Tuple>,
        state: &mut JoinState,
    ) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if self.predicate_evaluator.evaluate(left_tuple, right_tuple)? {
            state.mark_left_matched();
            let combined_tuple = self.tuple_combiner.combine_tuples(left_tuple, right_tuple);
            Ok(Some((combined_tuple, RID::new(0, 0))))
        } else {
            Ok(None)
        }
    }

    /// Handle left outer join logic
    pub fn handle_left_outer_join(
        &self,
        left_tuple: &Arc<Tuple>,
        right_tuple: Option<&Arc<Tuple>>,
        state: &mut JoinState,
    ) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if let Some(right_tuple) = right_tuple {
            if self.predicate_evaluator.evaluate(left_tuple, right_tuple)? {
                state.mark_left_matched();
                let combined_tuple = self.tuple_combiner.combine_tuples(left_tuple, right_tuple);
                Ok(Some((combined_tuple, RID::new(0, 0))))
            } else {
                Ok(None)
            }
        } else if !state.current_left_matched {
            // Unmatched left tuple - pad with nulls
            let padded_tuple = self
                .tuple_combiner
                .create_null_padded_tuple(Some(left_tuple), None);
            Ok(Some((padded_tuple, RID::new(0, 0))))
        } else {
            Ok(None)
        }
    }

    /// Handle right outer join logic
    fn handle_right_outer_join(
        &self,
        left_tuple: Option<&Arc<Tuple>>,
        right_tuple: &Arc<Tuple>,
        state: &mut JoinState,
    ) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if let Some(left_tuple) = left_tuple {
            if self.predicate_evaluator.evaluate(left_tuple, right_tuple)? {
                state.mark_left_matched();
                let combined_tuple = self.tuple_combiner.combine_tuples(left_tuple, right_tuple);
                Ok(Some((combined_tuple, RID::new(0, 0))))
            } else {
                // Track unmatched right tuple
                state.add_unmatched_right_tuple((right_tuple.clone(), RID::new(0, 0)));
                Ok(None)
            }
        } else {
            // Processing unmatched right tuple
            let padded_tuple = self
                .tuple_combiner
                .create_null_padded_tuple(None, Some(right_tuple));
            Ok(Some((padded_tuple, RID::new(0, 0))))
        }
    }

    /// Handle full outer join logic
    pub fn handle_full_outer_join(
        &self,
        left_tuple: Option<&Arc<Tuple>>,
        right_tuple: Option<&Arc<Tuple>>,
        state: &mut JoinState,
    ) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        match (left_tuple, right_tuple) {
            (Some(left_tuple), Some(right_tuple)) => {
                if self.predicate_evaluator.evaluate(left_tuple, right_tuple)? {
                    state.mark_left_matched();
                    let combined_tuple =
                        self.tuple_combiner.combine_tuples(left_tuple, right_tuple);
                    Ok(Some((combined_tuple, RID::new(0, 0))))
                } else {
                    // Don't add to unmatched right tuples here - we'll determine
                    // truly unmatched right tuples after processing all combinations
                    Ok(None)
                }
            }
            (Some(left_tuple), None) => {
                // Unmatched left tuple
                let padded_tuple = self
                    .tuple_combiner
                    .create_null_padded_tuple(Some(left_tuple), None);
                Ok(Some((padded_tuple, RID::new(0, 0))))
            }
            (None, Some(right_tuple)) => {
                // Unmatched right tuple
                let padded_tuple = self
                    .tuple_combiner
                    .create_null_padded_tuple(None, Some(right_tuple));
                Ok(Some((padded_tuple, RID::new(0, 0))))
            }
            (None, None) => Ok(None),
        }
    }

    /// Handle cross join logic
    fn handle_cross_join(
        &self,
        left_tuple: &Arc<Tuple>,
        right_tuple: &Arc<Tuple>,
    ) -> (Arc<Tuple>, RID) {
        let combined_tuple = self.tuple_combiner.combine_tuples(left_tuple, right_tuple);
        (combined_tuple, RID::new(0, 0))
    }

    /// Get join type
    pub fn get_join_type(&self) -> &JoinType {
        &self.join_type
    }
}

// =============================================================================
// 5. EXECUTOR MANAGEMENT
// =============================================================================

/**
 * Manages child executors and their lifecycle
 * Responsibility: Handle executor creation, initialization, and reset
 */
pub struct ExecutorManager {
    children_executors: Option<Vec<Box<dyn AbstractExecutor>>>,
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<NestedLoopJoinNode>,
}

impl ExecutorManager {
    /// Create new executor manager
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: Arc<NestedLoopJoinNode>) -> Self {
        Self {
            children_executors: None,
            context,
            plan,
        }
    }

    /// Initialize child executors
    pub fn initialize_executors(&mut self) -> Result<(), DBError> {
        let children_plans = self.plan.get_children();
        if children_plans.len() != 2 {
            return Err(DBError::Execution(
                "NestedLoopJoin must have exactly 2 children".to_string(),
            ));
        }

        let left_executor = children_plans[0]
            .create_executor(self.context.clone())
            .map_err(|e| {
                DBError::Execution(format!("Failed to create left child executor: {}", e))
            })?;

        let right_executor = children_plans[1]
            .create_executor(self.context.clone())
            .map_err(|e| {
                DBError::Execution(format!("Failed to create right child executor: {}", e))
            })?;

        self.children_executors = Some(vec![left_executor, right_executor]);

        // Initialize both executors
        if let Some(ref mut children) = self.children_executors {
            children[0].init();
            children[1].init();
        }

        Ok(())
    }

    /// Get next tuple from left executor
    pub fn get_next_left_tuple(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if let Some(ref mut children) = self.children_executors {
            children[0].next()
        } else {
            Err(DBError::Execution("Executors not initialized".to_string()))
        }
    }

    /// Get next tuple from right executor
    pub fn get_next_right_tuple(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if let Some(ref mut children) = self.children_executors {
            children[1].next()
        } else {
            Err(DBError::Execution("Executors not initialized".to_string()))
        }
    }

    /// Reset right executor for new left tuple
    pub fn reset_right_executor(&mut self) -> Result<(), DBError> {
        if let Some(ref mut children) = self.children_executors {
            // Recreate right executor
            let right_child_plan = &self.plan.get_children()[1];
            let new_right_executor = right_child_plan
                .create_executor(self.context.clone())
                .map_err(|e| {
                    DBError::Execution(format!("Failed to recreate right executor: {}", e))
                })?;

            children[1] = new_right_executor;
            children[1].init();
            Ok(())
        } else {
            Err(DBError::Execution("Executors not initialized".to_string()))
        }
    }
}

// =============================================================================
// 6. MAIN EXECUTOR ORCHESTRATOR
// =============================================================================

/**
 * Main nested loop join executor that orchestrates all components
 * Responsibility: Coordinate between all components to execute the join
 */
pub struct NestedLoopJoinExecutor {
    /// Executor management
    executor_manager: ExecutorManager,

    /// Join state tracking
    join_state: JoinState,

    /// Join type handler
    join_handler: JoinTypeHandler,

    /// Execution context
    context: Arc<RwLock<ExecutionContext>>,

    /// Join plan
    plan: Arc<NestedLoopJoinNode>,

    /// Initialization flag
    initialized: bool,
}

impl NestedLoopJoinExecutor {
    /// Create new nested loop join executor
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: Arc<NestedLoopJoinNode>) -> Self {
        debug!("Creating NestedLoopJoinExecutor");
        let executor_manager = ExecutorManager::new(context.clone(), plan.clone());
        let join_state = JoinState::new();

        // Create components
        let tuple_combiner = TupleCombiner::new(
            plan.get_left_schema().clone(),
            plan.get_right_schema().clone(),
            plan.get_output_schema().clone(),
        );

        let predicate_evaluator = JoinPredicateEvaluator::new(
            plan.get_predicate().clone(),
            plan.get_left_schema().clone(),
            plan.get_right_schema().clone(),
        );

        let join_handler = JoinTypeHandler::new(
            plan.get_join_type().clone(),
            tuple_combiner,
            predicate_evaluator,
        );

        debug!("NestedLoopJoinExecutor created successfully");
        Self {
            executor_manager,
            join_state,
            join_handler,
            context,
            plan,
            initialized: false,
        }
    }

    /// Execute main join logic
    fn execute_main_join(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        loop {
            debug!(
                "JOIN: Loop iteration - current_left_tuple: {:?}, left_exhausted: {}, right_exhausted: {}",
                self.join_state.current_left_tuple.is_some(),
                self.join_state.left_executor_exhausted,
                self.join_state.right_executor_exhausted
            );

            // Get or fetch current left tuple
            if self.join_state.current_left_tuple.is_none()
                && !self.join_state.left_executor_exhausted
            {
                match self.executor_manager.get_next_left_tuple()? {
                    Some(left_tuple) => {
                        info!("JOIN: Processing new left tuple");
                        self.join_state.reset_for_new_left_tuple(left_tuple);
                        self.executor_manager.reset_right_executor()?;
                    }
                    None => {
                        info!("JOIN: Left executor exhausted");
                        self.join_state.left_executor_exhausted = true;
                    }
                }
            }

            // Process current left tuple with right tuples
            if let Some((left_tuple, left_rid)) = &self.join_state.current_left_tuple.clone() {
                if !self.join_state.right_executor_exhausted {
                    match self.executor_manager.get_next_right_tuple()? {
                        Some((right_tuple, right_rid)) => {
                            // For right and full outer joins, track all right tuples
                            if matches!(
                                self.join_handler.join_type,
                                JoinType::Right(_)
                                    | JoinType::RightOuter(_)
                                    | JoinType::FullOuter(_)
                            ) {
                                // Check if this right tuple is already in our collection
                                let tuple_exists = self
                                    .join_state
                                    .all_right_tuples
                                    .iter()
                                    .any(|(t, _)| t.get_values() == right_tuple.get_values());

                                if !tuple_exists {
                                    self.join_state
                                        .add_right_tuple((right_tuple.clone(), right_rid));
                                }
                            }

                            if let Some(result) = self.join_handler.process_tuple_pair(
                                left_tuple,
                                &right_tuple,
                                &mut self.join_state,
                            )? {
                                // For right and full outer joins, mark the right tuple as matched
                                if matches!(
                                    self.join_handler.join_type,
                                    JoinType::Right(_)
                                        | JoinType::RightOuter(_)
                                        | JoinType::FullOuter(_)
                                ) {
                                    // Find the index of this right tuple
                                    if let Some(index) =
                                        self.join_state.all_right_tuples.iter().position(
                                            |(t, _)| t.get_values() == right_tuple.get_values(),
                                        )
                                    {
                                        self.join_state.mark_right_tuple_matched(index);
                                    }
                                }
                                return Ok(Some(result));
                            }
                        }
                        None => {
                            info!("JOIN: Right executor exhausted for current left tuple");
                            self.join_state.right_executor_exhausted = true;
                        }
                    }
                } else {
                    // Handle unmatched left tuple based on join type
                    match &self.join_handler.join_type {
                        JoinType::Left(_) | JoinType::LeftOuter(_) => {
                            // For left outer joins, immediately output unmatched left tuple
                            if let Some(result) = self.join_handler.handle_left_outer_join(
                                left_tuple,
                                None,
                                &mut self.join_state,
                            )? {
                                self.join_state.clear_current_left_tuple();
                                return Ok(Some(result));
                            } else {
                                self.join_state.clear_current_left_tuple();
                            }
                        }
                        JoinType::FullOuter(_) => {
                            // For full outer joins, store unmatched left tuple for later processing
                            if !self.join_state.current_left_matched {
                                self.join_state
                                    .add_unmatched_left_tuple((left_tuple.clone(), *left_rid));
                            }
                            self.join_state.clear_current_left_tuple();
                        }
                        _ => {
                            // For inner joins and cross joins, simply discard unmatched left tuples
                            debug!("JOIN: Discarding unmatched left tuple for inner/cross join");
                            self.join_state.clear_current_left_tuple();
                        }
                    }
                }
            } else if self.join_state.left_executor_exhausted {
                // Move to next phase
                self.join_state.advance_phase(&self.join_handler.join_type);
                return Ok(None);
            }
        }
    }

    /// Process unmatched tuples for outer joins
    fn process_unmatched_tuples(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        match &self.join_state.phase {
            JoinPhase::UnmatchedRight => {
                if let Some((right_tuple, _right_rid)) =
                    self.join_state.next_unmatched_right_tuple()
                {
                    if let Some(result) = self.join_handler.handle_right_outer_join(
                        None,
                        &right_tuple,
                        &mut self.join_state,
                    )? {
                        return Ok(Some(result));
                    }
                } else {
                    self.join_state.advance_phase(&self.join_handler.join_type);
                }
            }
            JoinPhase::UnmatchedLeft => {
                if let Some((left_tuple, _left_rid)) = self.join_state.next_unmatched_left_tuple() {
                    if let Some(result) = self.join_handler.handle_full_outer_join(
                        Some(&left_tuple),
                        None,
                        &mut self.join_state,
                    )? {
                        return Ok(Some(result));
                    }
                } else {
                    self.join_state.advance_phase(&self.join_handler.join_type);
                }
            }
            _ => {
                self.join_state.phase = JoinPhase::Completed;
            }
        }
        Ok(None)
    }
}

impl AbstractExecutor for NestedLoopJoinExecutor {
    fn init(&mut self) {
        debug!("NestedLoopJoinExecutor::init() called");
        if !self.initialized {
            debug!("Initializing nested loop join executor");
            if let Err(e) = self.executor_manager.initialize_executors() {
                debug!("Failed to initialize join executor: {}", e);
            } else {
                debug!("Successfully initialized join executor");
                self.initialized = true;
            }
        } else {
            debug!("NestedLoopJoinExecutor already initialized");
        }
    }

    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        debug!("NestedLoopJoinExecutor::next() called");
        if !self.initialized {
            debug!("Executor not initialized, calling init()");
            self.init();
        }

        debug!("Starting main execution loop");
        loop {
            match &self.join_state.phase {
                JoinPhase::MainJoin => {
                    if let Some(result) = self.execute_main_join()? {
                        return Ok(Some(result));
                    }
                    // Continue to next phase
                }
                JoinPhase::UnmatchedRight | JoinPhase::UnmatchedLeft => {
                    if let Some(result) = self.process_unmatched_tuples()? {
                        return Ok(Some(result));
                    }
                    // Continue to next phase
                }
                JoinPhase::Completed => {
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

// =============================================================================
// REFACTORING BENEFITS ACHIEVED
// =============================================================================

/**
 * IMPLEMENTATION COMPLETE ✅
 *
 * Phase 1: Basic Structure ✅
 * - [✅] Implement JoinState with basic state management
 * - [✅] Implement TupleCombiner with tuple combination logic
 * - [✅] Implement JoinPredicateEvaluator with predicate evaluation
 *
 * Phase 2: Join Logic ✅
 * - [✅] Implement JoinTypeHandler with basic join types (Inner, Cross)
 * - [✅] Implement ExecutorManager with executor lifecycle
 * - [✅] Wire up NestedLoopJoinExecutor orchestration
 *
 * Phase 3: Advanced Join Types ✅
 * - [✅] Add support for Left/Right/Full Outer joins
 * - [✅] Add comprehensive error handling
 * - [ ] Semi/Anti joins can be added easily in JoinTypeHandler
 *
 * BENEFITS ACHIEVED:
 *
 * 1. ✅ Single Responsibility: Each struct has one clear purpose
 *    - JoinState: manages execution state only
 *    - TupleCombiner: handles tuple operations only
 *    - JoinPredicateEvaluator: evaluates predicates only
 *    - JoinTypeHandler: implements join logic only
 *    - ExecutorManager: manages executors only
 *    - NestedLoopJoinExecutor: orchestrates components only
 *
 * 2. ✅ Testability: Components can be unit tested independently
 *    - Each component has focused, testable responsibilities
 *    - Mock dependencies can be easily injected
 *
 * 3. ✅ Maintainability: Changes isolated to specific components
 *    - Adding new join types only requires changes to JoinTypeHandler
 *    - Tuple combination logic changes only affect TupleCombiner
 *
 * 4. ✅ Reusability: Components can be reused in other algorithms
 *    - TupleCombiner can be used in hash joins, sort-merge joins
 *    - JoinPredicateEvaluator can be used in any join implementation
 *
 * 5. ✅ Readability: Code organized by logical concern
 *    - Easy to find and understand specific functionality
 *    - Clear separation of concerns
 *
 * 6. ✅ Extensibility: Easy to add new features
 *    - New join types: extend JoinTypeHandler
 *    - New tuple operations: extend TupleCombiner
 *    - New state management: extend JoinState
 *
 * 7. ✅ Idiomatic Rust: Proper error handling, ownership, patterns
 *    - Uses Result<T, E> for error handling
 *    - Proper ownership with Arc and cloning where needed
 *    - Follows Rust naming conventions and patterns
 */
#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::{Val, Value};
    use std::sync::Arc;

    // =============================================================================
    // HELPER FUNCTIONS
    // =============================================================================

    fn create_test_schemas() -> (Schema, Schema, Schema) {
        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("age", TypeId::Integer),
        ]);
        let right_schema = Schema::new(vec![
            Column::new("name", TypeId::VarChar),
            Column::new("dept", TypeId::VarChar),
        ]);
        let output_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("age", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("dept", TypeId::VarChar),
        ]);
        (left_schema, right_schema, output_schema)
    }

    fn create_test_tuples() -> (Arc<Tuple>, Arc<Tuple>) {
        let (left_schema, right_schema, _) = create_test_schemas();

        let left_tuple = Arc::new(Tuple::new(
            &[Value::new(1), Value::new(25)],
            &left_schema,
            RID::new(0, 0),
        ));
        let right_tuple = Arc::new(Tuple::new(
            &[Value::new("Alice"), Value::new("Engineering")],
            &right_schema,
            RID::new(0, 0),
        ));

        (left_tuple, right_tuple)
    }

    fn create_boolean_predicate(value: bool) -> Arc<dyn ExpressionOps + Send + Sync> {
        let column = Column::new("temp", TypeId::Boolean);
        Arc::new(ConstantExpression::new(Value::new(value), column, vec![]))
    }

    // =============================================================================
    // JOIN STATE TESTS
    // =============================================================================

    #[test]
    fn test_join_state_creation() {
        let state = JoinState::new();
        assert!(matches!(state.phase, JoinPhase::MainJoin));
        assert!(!state.left_executor_exhausted);
        assert!(!state.right_executor_exhausted);
        assert!(!state.current_left_matched);
        assert!(state.current_left_tuple.is_none());
        assert_eq!(state.unmatched_right_tuples.len(), 0);
        assert_eq!(state.unmatched_left_tuples.len(), 0);
    }

    #[test]
    fn test_join_state_phase_transitions() {
        let mut state = JoinState::new();

        // Test transition to UnmatchedRight for RightOuter
        state.advance_phase(&JoinType::RightOuter(sqlparser::ast::JoinConstraint::None));
        assert!(matches!(state.phase, JoinPhase::UnmatchedRight));

        // Test transition to UnmatchedLeft for FullOuter
        state.advance_phase(&JoinType::FullOuter(sqlparser::ast::JoinConstraint::None));
        assert!(matches!(state.phase, JoinPhase::UnmatchedLeft));

        // Test transition to Completed for Inner
        state.advance_phase(&JoinType::Inner(sqlparser::ast::JoinConstraint::None));
        assert!(matches!(state.phase, JoinPhase::Completed));

        // Test transition to Completed from UnmatchedLeft
        state.phase = JoinPhase::UnmatchedLeft;
        state.advance_phase(&JoinType::Inner(sqlparser::ast::JoinConstraint::None));
        assert!(matches!(state.phase, JoinPhase::Completed));
    }

    #[test]
    fn test_join_state_left_tuple_management() {
        let mut state = JoinState::new();
        let (left_tuple, _) = create_test_tuples();

        // Test reset for new left tuple
        state.reset_for_new_left_tuple((left_tuple.clone(), RID::new(1, 1)));
        assert!(state.current_left_tuple.is_some());
        assert!(!state.current_left_matched);
        assert!(!state.right_executor_exhausted);

        // Test marking left matched
        state.mark_left_matched();
        assert!(state.current_left_matched);

        // Test clearing current left tuple
        state.clear_current_left_tuple();
        assert!(state.current_left_tuple.is_none());
    }

    #[test]
    fn test_join_state_unmatched_tuple_tracking() {
        let mut state = JoinState::new();
        let (left_tuple, right_tuple) = create_test_tuples();

        // Test adding unmatched tuples
        state.add_unmatched_right_tuple((right_tuple.clone(), RID::new(1, 1)));
        state.add_unmatched_left_tuple((left_tuple.clone(), RID::new(2, 2)));

        assert_eq!(state.unmatched_right_tuples.len(), 1);
        assert_eq!(state.unmatched_left_tuples.len(), 1);

        // Test retrieving unmatched right tuples
        let retrieved_right = state.next_unmatched_right_tuple();
        assert!(retrieved_right.is_some());
        let retrieved_right2 = state.next_unmatched_right_tuple();
        assert!(retrieved_right2.is_none());

        // Test retrieving unmatched left tuples
        let retrieved_left = state.next_unmatched_left_tuple();
        assert!(retrieved_left.is_some());
        let retrieved_left2 = state.next_unmatched_left_tuple();
        assert!(retrieved_left2.is_none());
    }

    #[test]
    fn test_join_state_completion_detection() {
        let mut state = JoinState::new();

        assert!(!state.is_complete());

        state.phase = JoinPhase::Completed;
        assert!(state.is_complete());
    }

    // =============================================================================
    // TUPLE COMBINER TESTS
    // =============================================================================

    #[test]
    fn test_tuple_combiner_basic_combination() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let combiner = TupleCombiner::new(left_schema, right_schema, output_schema);
        let (left_tuple, right_tuple) = create_test_tuples();

        let result = combiner.combine_tuples(&left_tuple, &right_tuple);
        let values = result.get_values();

        assert_eq!(values.len(), 4);
        assert_eq!(*values[0].get_val(), Val::Integer(1));
        assert_eq!(*values[1].get_val(), Val::Integer(25));
        assert_eq!(*values[2].get_val(), Val::VarLen("Alice".to_string()));
        assert_eq!(*values[3].get_val(), Val::VarLen("Engineering".to_string()));
    }

    #[test]
    fn test_tuple_combiner_null_padding_left_only() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let combiner = TupleCombiner::new(left_schema, right_schema, output_schema);
        let (left_tuple, _) = create_test_tuples();

        // Test left tuple with null right padding
        let result = combiner.create_null_padded_tuple(Some(&left_tuple), None);
        let values = result.get_values();

        assert_eq!(values.len(), 4);
        assert_eq!(*values[0].get_val(), Val::Integer(1));
        assert_eq!(*values[1].get_val(), Val::Integer(25));
        assert_eq!(*values[2].get_val(), Val::Null);
        assert_eq!(*values[3].get_val(), Val::Null);
    }

    #[test]
    fn test_tuple_combiner_null_padding_right_only() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let combiner = TupleCombiner::new(left_schema, right_schema, output_schema);
        let (_, right_tuple) = create_test_tuples();

        // Test right tuple with null left padding
        let result = combiner.create_null_padded_tuple(None, Some(&right_tuple));
        let values = result.get_values();

        assert_eq!(values.len(), 4);
        assert_eq!(*values[0].get_val(), Val::Null);
        assert_eq!(*values[1].get_val(), Val::Null);
        assert_eq!(*values[2].get_val(), Val::VarLen("Alice".to_string()));
        assert_eq!(*values[3].get_val(), Val::VarLen("Engineering".to_string()));
    }

    #[test]
    fn test_tuple_combiner_null_padding_both_none() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let combiner = TupleCombiner::new(left_schema, right_schema, output_schema);

        // Test null padding when both tuples are None
        let result = combiner.create_null_padded_tuple(None, None);
        let values = result.get_values();

        assert_eq!(values.len(), 4);
        for value in values {
            assert_eq!(*value.get_val(), Val::Null);
        }
    }

    #[test]
    fn test_tuple_combiner_left_only_tuple() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let combiner = TupleCombiner::new(left_schema, right_schema, output_schema);
        let (left_tuple, _) = create_test_tuples();

        let result = combiner.create_left_only_tuple(&left_tuple);
        let values = result.get_values();

        assert_eq!(values.len(), 2);
        assert_eq!(*values[0].get_val(), Val::Integer(1));
        assert_eq!(*values[1].get_val(), Val::Integer(25));
    }

    // =============================================================================
    // JOIN PREDICATE EVALUATOR TESTS
    // =============================================================================

    #[test]
    fn test_join_predicate_evaluator_true_predicate() {
        let (left_schema, right_schema, _) = create_test_schemas();
        let predicate = create_boolean_predicate(true);
        let evaluator = JoinPredicateEvaluator::new(predicate, left_schema, right_schema);
        let (left_tuple, right_tuple) = create_test_tuples();

        let result = evaluator.evaluate(&left_tuple, &right_tuple);
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_join_predicate_evaluator_false_predicate() {
        let (left_schema, right_schema, _) = create_test_schemas();
        let predicate = create_boolean_predicate(false);
        let evaluator = JoinPredicateEvaluator::new(predicate, left_schema, right_schema);
        let (left_tuple, right_tuple) = create_test_tuples();

        let result = evaluator.evaluate(&left_tuple, &right_tuple);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_join_predicate_evaluator_null_predicate() {
        let (left_schema, right_schema, _) = create_test_schemas();
        let column = Column::new("temp", TypeId::Boolean);
        let predicate: Arc<dyn ExpressionOps + Send + Sync> = Arc::new(ConstantExpression::new(
            Value::new(Val::Null),
            column,
            vec![],
        ));
        let evaluator = JoinPredicateEvaluator::new(predicate, left_schema, right_schema);
        let (left_tuple, right_tuple) = create_test_tuples();

        let result = evaluator.evaluate(&left_tuple, &right_tuple);
        assert!(result.is_ok());
        assert!(!result.unwrap()); // Null should be treated as false
    }

    #[test]
    fn test_join_predicate_evaluator_non_boolean_predicate() {
        let (left_schema, right_schema, _) = create_test_schemas();
        let column = Column::new("temp", TypeId::Integer);
        let predicate: Arc<dyn ExpressionOps + Send + Sync> =
            Arc::new(ConstantExpression::new(Value::new(42), column, vec![]));
        let evaluator = JoinPredicateEvaluator::new(predicate, left_schema, right_schema);
        let (left_tuple, right_tuple) = create_test_tuples();

        let result = evaluator.evaluate(&left_tuple, &right_tuple);
        assert!(result.is_ok());
        assert!(!result.unwrap()); // Non-boolean should be treated as false
    }

    // =============================================================================
    // JOIN TYPE HANDLER TESTS
    // =============================================================================

    #[test]
    fn test_join_type_handler_inner_join_match() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let tuple_combiner =
            TupleCombiner::new(left_schema.clone(), right_schema.clone(), output_schema);
        let predicate_evaluator =
            JoinPredicateEvaluator::new(create_boolean_predicate(true), left_schema, right_schema);
        let handler = JoinTypeHandler::new(
            JoinType::Inner(sqlparser::ast::JoinConstraint::None),
            tuple_combiner,
            predicate_evaluator,
        );

        let (left_tuple, right_tuple) = create_test_tuples();
        let mut state = JoinState::new();

        let result = handler.process_tuple_pair(&left_tuple, &right_tuple, &mut state);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
        assert!(state.current_left_matched);
    }

    #[test]
    fn test_join_type_handler_inner_join_no_match() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let tuple_combiner =
            TupleCombiner::new(left_schema.clone(), right_schema.clone(), output_schema);
        let predicate_evaluator =
            JoinPredicateEvaluator::new(create_boolean_predicate(false), left_schema, right_schema);
        let handler = JoinTypeHandler::new(
            JoinType::Inner(sqlparser::ast::JoinConstraint::None),
            tuple_combiner,
            predicate_evaluator,
        );

        let (left_tuple, right_tuple) = create_test_tuples();
        let mut state = JoinState::new();

        let result = handler.process_tuple_pair(&left_tuple, &right_tuple, &mut state);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
        assert!(!state.current_left_matched);
    }

    #[test]
    fn test_join_type_handler_cross_join() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let tuple_combiner =
            TupleCombiner::new(left_schema.clone(), right_schema.clone(), output_schema);
        let predicate_evaluator =
            JoinPredicateEvaluator::new(create_boolean_predicate(true), left_schema, right_schema);
        let handler =
            JoinTypeHandler::new(JoinType::CrossJoin, tuple_combiner, predicate_evaluator);

        let (left_tuple, right_tuple) = create_test_tuples();
        let mut state = JoinState::new();

        let result = handler.process_tuple_pair(&left_tuple, &right_tuple, &mut state);
        assert!(result.is_ok());
        let (combined_tuple, _) = result.unwrap().unwrap();

        let values = combined_tuple.get_values();
        assert_eq!(values.len(), 4);
    }

    #[test]
    fn test_join_type_handler_left_outer_join_with_match() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let tuple_combiner =
            TupleCombiner::new(left_schema.clone(), right_schema.clone(), output_schema);
        let predicate_evaluator =
            JoinPredicateEvaluator::new(create_boolean_predicate(true), left_schema, right_schema);
        let handler = JoinTypeHandler::new(
            JoinType::LeftOuter(sqlparser::ast::JoinConstraint::None),
            tuple_combiner,
            predicate_evaluator,
        );

        let (left_tuple, right_tuple) = create_test_tuples();
        let mut state = JoinState::new();

        let result = handler.handle_left_outer_join(&left_tuple, Some(&right_tuple), &mut state);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
        assert!(state.current_left_matched);
    }

    #[test]
    fn test_join_type_handler_left_outer_join_unmatched() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let tuple_combiner =
            TupleCombiner::new(left_schema.clone(), right_schema.clone(), output_schema);
        let predicate_evaluator =
            JoinPredicateEvaluator::new(create_boolean_predicate(false), left_schema, right_schema);
        let handler = JoinTypeHandler::new(
            JoinType::LeftOuter(sqlparser::ast::JoinConstraint::None),
            tuple_combiner,
            predicate_evaluator,
        );

        let (left_tuple, _) = create_test_tuples();
        let mut state = JoinState::new();
        state.current_left_matched = false;

        // Test unmatched left tuple (no right tuple provided)
        let result = handler.handle_left_outer_join(&left_tuple, None, &mut state);
        assert!(result.is_ok());
        let (padded_tuple, _) = result.unwrap().unwrap();

        let values = padded_tuple.get_values();
        assert_eq!(values.len(), 4);
        assert_eq!(*values[0].get_val(), Val::Integer(1));
        assert_eq!(*values[1].get_val(), Val::Integer(25));
        assert_eq!(*values[2].get_val(), Val::Null);
        assert_eq!(*values[3].get_val(), Val::Null);
    }

    #[test]
    fn test_join_type_handler_right_outer_join() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let tuple_combiner =
            TupleCombiner::new(left_schema.clone(), right_schema.clone(), output_schema);
        let predicate_evaluator =
            JoinPredicateEvaluator::new(create_boolean_predicate(false), left_schema, right_schema);
        let handler = JoinTypeHandler::new(
            JoinType::RightOuter(sqlparser::ast::JoinConstraint::None),
            tuple_combiner,
            predicate_evaluator,
        );

        let (_, right_tuple) = create_test_tuples();
        let mut state = JoinState::new();

        // Test unmatched right tuple (no left tuple provided)
        let result = handler.handle_right_outer_join(None, &right_tuple, &mut state);
        assert!(result.is_ok());
        let (padded_tuple, _) = result.unwrap().unwrap();

        let values = padded_tuple.get_values();
        assert_eq!(values.len(), 4);
        assert_eq!(*values[0].get_val(), Val::Null);
        assert_eq!(*values[1].get_val(), Val::Null);
        assert_eq!(*values[2].get_val(), Val::VarLen("Alice".to_string()));
        assert_eq!(*values[3].get_val(), Val::VarLen("Engineering".to_string()));
    }

    #[test]
    fn test_join_type_handler_full_outer_join_with_match() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let tuple_combiner =
            TupleCombiner::new(left_schema.clone(), right_schema.clone(), output_schema);
        let predicate_evaluator =
            JoinPredicateEvaluator::new(create_boolean_predicate(true), left_schema, right_schema);
        let handler = JoinTypeHandler::new(
            JoinType::FullOuter(sqlparser::ast::JoinConstraint::None),
            tuple_combiner,
            predicate_evaluator,
        );

        let (left_tuple, right_tuple) = create_test_tuples();
        let mut state = JoinState::new();

        let result =
            handler.handle_full_outer_join(Some(&left_tuple), Some(&right_tuple), &mut state);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
        assert!(state.current_left_matched);
    }

    #[test]
    fn test_join_type_handler_full_outer_join_unmatched_left() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let tuple_combiner =
            TupleCombiner::new(left_schema.clone(), right_schema.clone(), output_schema);
        let predicate_evaluator =
            JoinPredicateEvaluator::new(create_boolean_predicate(false), left_schema, right_schema);
        let handler = JoinTypeHandler::new(
            JoinType::FullOuter(sqlparser::ast::JoinConstraint::None),
            tuple_combiner,
            predicate_evaluator,
        );

        let (left_tuple, _) = create_test_tuples();
        let mut state = JoinState::new();

        let result = handler.handle_full_outer_join(Some(&left_tuple), None, &mut state);
        assert!(result.is_ok());
        let (padded_tuple, _) = result.unwrap().unwrap();

        let values = padded_tuple.get_values();
        assert_eq!(values.len(), 4);
        assert_eq!(*values[0].get_val(), Val::Integer(1));
        assert_eq!(*values[1].get_val(), Val::Integer(25));
        assert_eq!(*values[2].get_val(), Val::Null);
        assert_eq!(*values[3].get_val(), Val::Null);
    }

    #[test]
    fn test_join_type_handler_full_outer_join_unmatched_right() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let tuple_combiner =
            TupleCombiner::new(left_schema.clone(), right_schema.clone(), output_schema);
        let predicate_evaluator =
            JoinPredicateEvaluator::new(create_boolean_predicate(false), left_schema, right_schema);
        let handler = JoinTypeHandler::new(
            JoinType::FullOuter(sqlparser::ast::JoinConstraint::None),
            tuple_combiner,
            predicate_evaluator,
        );

        let (_, right_tuple) = create_test_tuples();
        let mut state = JoinState::new();

        let result = handler.handle_full_outer_join(None, Some(&right_tuple), &mut state);
        assert!(result.is_ok());
        let (padded_tuple, _) = result.unwrap().unwrap();

        let values = padded_tuple.get_values();
        assert_eq!(values.len(), 4);
        assert_eq!(*values[0].get_val(), Val::Null);
        assert_eq!(*values[1].get_val(), Val::Null);
        assert_eq!(*values[2].get_val(), Val::VarLen("Alice".to_string()));
        assert_eq!(*values[3].get_val(), Val::VarLen("Engineering".to_string()));
    }

    #[test]
    fn test_join_type_handler_get_join_type() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let tuple_combiner =
            TupleCombiner::new(left_schema.clone(), right_schema.clone(), output_schema);
        let predicate_evaluator =
            JoinPredicateEvaluator::new(create_boolean_predicate(true), left_schema, right_schema);
        let handler = JoinTypeHandler::new(
            JoinType::Inner(sqlparser::ast::JoinConstraint::None),
            tuple_combiner,
            predicate_evaluator,
        );

        assert!(matches!(handler.get_join_type(), JoinType::Inner(_)));
    }

    // =============================================================================
    // EDGE CASES AND ERROR HANDLING TESTS
    // =============================================================================

    #[test]
    fn test_join_state_full_outer_join_phase_progression() {
        let mut state = JoinState::new();

        // MainJoin -> UnmatchedRight for FullOuter
        state.advance_phase(&JoinType::FullOuter(sqlparser::ast::JoinConstraint::None));
        assert!(matches!(state.phase, JoinPhase::UnmatchedRight));

        // UnmatchedRight -> UnmatchedLeft for FullOuter
        state.advance_phase(&JoinType::FullOuter(sqlparser::ast::JoinConstraint::None));
        assert!(matches!(state.phase, JoinPhase::UnmatchedLeft));

        // UnmatchedLeft -> Completed
        state.advance_phase(&JoinType::FullOuter(sqlparser::ast::JoinConstraint::None));
        assert!(matches!(state.phase, JoinPhase::Completed));
    }

    #[test]
    fn test_tuple_combiner_empty_schemas() {
        let left_schema = Schema::new(vec![]);
        let right_schema = Schema::new(vec![]);
        let output_schema = Schema::new(vec![]);

        let combiner = TupleCombiner::new(left_schema, right_schema, output_schema);

        let result = combiner.create_null_padded_tuple(None, None);
        let values = result.get_values();
        assert_eq!(values.len(), 0);
    }

    #[test]
    fn test_join_state_multiple_unmatched_tuples() {
        let mut state = JoinState::new();
        let (left_tuple, right_tuple) = create_test_tuples();

        // Add multiple unmatched tuples
        for i in 0..3 {
            state.add_unmatched_right_tuple((right_tuple.clone(), RID::new(i as u64, i as u32)));
            state.add_unmatched_left_tuple((
                left_tuple.clone(),
                RID::new((i + 10) as u64, (i + 10) as u32),
            ));
        }

        assert_eq!(state.unmatched_right_tuples.len(), 3);
        assert_eq!(state.unmatched_left_tuples.len(), 3);

        // Retrieve all right tuples
        let mut count = 0;
        while state.next_unmatched_right_tuple().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);

        // Retrieve all left tuples
        count = 0;
        while state.next_unmatched_left_tuple().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn test_cross_join_always_produces_result() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let tuple_combiner =
            TupleCombiner::new(left_schema.clone(), right_schema.clone(), output_schema);
        let predicate_evaluator = JoinPredicateEvaluator::new(
            create_boolean_predicate(false), // Even with false predicate
            left_schema,
            right_schema,
        );
        let handler =
            JoinTypeHandler::new(JoinType::CrossJoin, tuple_combiner, predicate_evaluator);

        let (left_tuple, right_tuple) = create_test_tuples();
        let mut state = JoinState::new();

        let result = handler.process_tuple_pair(&left_tuple, &right_tuple, &mut state);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some()); // Cross join always produces result
    }

    #[test]
    fn test_complex_tuple_combinations() {
        let left_schema = Schema::new(vec![
            Column::new("a", TypeId::Integer),
            Column::new("b", TypeId::VarChar),
            Column::new("c", TypeId::Boolean),
        ]);
        let right_schema = Schema::new(vec![
            Column::new("x", TypeId::BigInt),
            Column::new("y", TypeId::Decimal),
        ]);
        let output_schema = Schema::new(vec![
            Column::new("a", TypeId::Integer),
            Column::new("b", TypeId::VarChar),
            Column::new("c", TypeId::Boolean),
            Column::new("x", TypeId::BigInt),
            Column::new("y", TypeId::Decimal),
        ]);

        let combiner = TupleCombiner::new(left_schema.clone(), right_schema.clone(), output_schema);

        let left_tuple = Arc::new(Tuple::new(
            &[Value::new(42), Value::new("test"), Value::new(true)],
            &left_schema,
            RID::new(0, 0),
        ));
        let right_tuple = Arc::new(Tuple::new(
            &[Value::new(1000i64), Value::new(std::f64::consts::PI)],
            &right_schema,
            RID::new(0, 0),
        ));

        let result = combiner.combine_tuples(&left_tuple, &right_tuple);
        let values = result.get_values();

        assert_eq!(values.len(), 5);
        assert_eq!(*values[0].get_val(), Val::Integer(42));
        assert_eq!(*values[1].get_val(), Val::VarLen("test".to_string()));
        assert_eq!(*values[2].get_val(), Val::Boolean(true));
        assert_eq!(*values[3].get_val(), Val::BigInt(1000));
        assert_eq!(*values[4].get_val(), Val::Decimal(std::f64::consts::PI));
    }

    // =============================================================================
    // EXECUTOR MANAGER TESTS (Basic structure tests only)
    // =============================================================================

    // Note: Full ExecutorManager tests require complex setup with real execution contexts
    // and plans, so we focus on component-level testing instead.

    // =============================================================================
    // COMPREHENSIVE INTEGRATION TEST CASES FOR JOIN BEHAVIOR
    // =============================================================================

    #[test]
    fn test_inner_join_discards_unmatched_tuples() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let tuple_combiner =
            TupleCombiner::new(left_schema.clone(), right_schema.clone(), output_schema);

        // Create predicate that will never match (always false)
        let predicate_evaluator =
            JoinPredicateEvaluator::new(create_boolean_predicate(false), left_schema, right_schema);

        let handler = JoinTypeHandler::new(
            JoinType::Inner(sqlparser::ast::JoinConstraint::None),
            tuple_combiner,
            predicate_evaluator,
        );

        let (left_tuple, right_tuple) = create_test_tuples();
        let mut state = JoinState::new();

        // Test that INNER JOIN with no matches returns None
        let result = handler.process_tuple_pair(&left_tuple, &right_tuple, &mut state);
        assert!(result.is_ok());
        assert!(
            result.unwrap().is_none(),
            "INNER JOIN should return None for unmatched tuples"
        );
        assert!(
            !state.current_left_matched,
            "Left tuple should not be marked as matched"
        );
    }

    #[test]
    fn test_left_outer_join_handles_unmatched_tuples() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let tuple_combiner =
            TupleCombiner::new(left_schema.clone(), right_schema.clone(), output_schema);

        // Create predicate that will never match (always false)
        let predicate_evaluator =
            JoinPredicateEvaluator::new(create_boolean_predicate(false), left_schema, right_schema);

        let handler = JoinTypeHandler::new(
            JoinType::LeftOuter(sqlparser::ast::JoinConstraint::None),
            tuple_combiner,
            predicate_evaluator,
        );

        let (left_tuple, _) = create_test_tuples();
        let mut state = JoinState::new();

        // Test that LEFT OUTER JOIN handles unmatched left tuple
        let result = handler.handle_left_outer_join(&left_tuple, None, &mut state);
        assert!(result.is_ok());
        let result_value = result.unwrap();
        assert!(
            result_value.is_some(),
            "LEFT OUTER JOIN should return tuple for unmatched left tuple"
        );

        // Verify the result is null-padded
        let (combined_tuple, _) = result_value.unwrap();
        let values = combined_tuple.get_values();
        assert_eq!(values.len(), 4);
        assert_eq!(*values[0].get_val(), Val::Integer(1)); // Left values preserved
        assert_eq!(*values[1].get_val(), Val::Integer(25));
        assert_eq!(*values[2].get_val(), Val::Null); // Right values null-padded
        assert_eq!(*values[3].get_val(), Val::Null);
    }

    #[test]
    fn test_cross_join_always_combines_tuples() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let tuple_combiner =
            TupleCombiner::new(left_schema.clone(), right_schema.clone(), output_schema);

        // Even with false predicate, CROSS JOIN should combine tuples
        let predicate_evaluator =
            JoinPredicateEvaluator::new(create_boolean_predicate(false), left_schema, right_schema);

        let handler =
            JoinTypeHandler::new(JoinType::CrossJoin, tuple_combiner, predicate_evaluator);

        let (left_tuple, right_tuple) = create_test_tuples();
        let mut state = JoinState::new();

        // CROSS JOIN should always produce result regardless of predicate
        let result = handler.process_tuple_pair(&left_tuple, &right_tuple, &mut state);
        assert!(result.is_ok());
        let result_value = result.unwrap();
        assert!(
            result_value.is_some(),
            "CROSS JOIN should always return combined tuple"
        );

        let (combined_tuple, _) = result_value.unwrap();
        let values = combined_tuple.get_values();
        assert_eq!(values.len(), 4);
        assert_eq!(*values[0].get_val(), Val::Integer(1));
        assert_eq!(*values[1].get_val(), Val::Integer(25));
        assert_eq!(*values[2].get_val(), Val::VarLen("Alice".to_string()));
        assert_eq!(*values[3].get_val(), Val::VarLen("Engineering".to_string()));
    }

    #[test]
    fn test_join_type_handler_consistency() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let tuple_combiner =
            TupleCombiner::new(left_schema.clone(), right_schema.clone(), output_schema);

        let predicate_evaluator = JoinPredicateEvaluator::new(
            create_boolean_predicate(true), // Matching predicate
            left_schema,
            right_schema,
        );

        let (left_tuple, right_tuple) = create_test_tuples();

        // Test different join types with matching predicate
        let join_types = vec![
            JoinType::Inner(sqlparser::ast::JoinConstraint::None),
            JoinType::LeftOuter(sqlparser::ast::JoinConstraint::None),
            JoinType::RightOuter(sqlparser::ast::JoinConstraint::None),
            JoinType::FullOuter(sqlparser::ast::JoinConstraint::None),
            JoinType::CrossJoin,
        ];

        for join_type in join_types {
            let handler = JoinTypeHandler::new(
                join_type.clone(),
                tuple_combiner.clone(),
                predicate_evaluator.clone(),
            );

            let mut state = JoinState::new();
            let result = handler.process_tuple_pair(&left_tuple, &right_tuple, &mut state);

            assert!(
                result.is_ok(),
                "Join type {:?} should execute successfully",
                join_type
            );
            assert!(
                result.unwrap().is_some(),
                "Join type {:?} should return result for matching tuples",
                join_type
            );
            assert!(
                state.current_left_matched,
                "Left tuple should be marked as matched for join type {:?}",
                join_type
            );
        }
    }

    #[test]
    fn test_executor_state_management() {
        let mut state = JoinState::new();
        let (left_tuple, right_tuple) = create_test_tuples();

        // Test complete state lifecycle
        assert!(matches!(state.phase, JoinPhase::MainJoin));
        assert!(!state.left_executor_exhausted);
        assert!(!state.right_executor_exhausted);

        // Set up left tuple
        state.reset_for_new_left_tuple((left_tuple.clone(), RID::new(1, 1)));
        assert!(state.current_left_tuple.is_some());
        assert!(!state.current_left_matched);
        assert!(!state.right_executor_exhausted);

        // Mark as matched
        state.mark_left_matched();
        assert!(state.current_left_matched);

        // Add unmatched tuples
        state.add_unmatched_right_tuple((right_tuple.clone(), RID::new(2, 2)));
        state.add_unmatched_left_tuple((left_tuple.clone(), RID::new(3, 3)));

        // Test phase transitions
        state.advance_phase(&JoinType::LeftOuter(sqlparser::ast::JoinConstraint::None));
        assert!(matches!(state.phase, JoinPhase::Completed)); // LeftOuter goes directly to Completed

        // Reset and test RightOuter transition
        state.phase = JoinPhase::MainJoin;
        state.advance_phase(&JoinType::RightOuter(sqlparser::ast::JoinConstraint::None));
        assert!(matches!(state.phase, JoinPhase::UnmatchedRight));

        state.advance_phase(&JoinType::RightOuter(sqlparser::ast::JoinConstraint::None));
        assert!(matches!(state.phase, JoinPhase::Completed));

        // Test FullOuter transition
        state.phase = JoinPhase::MainJoin;
        state.advance_phase(&JoinType::FullOuter(sqlparser::ast::JoinConstraint::None));
        assert!(matches!(state.phase, JoinPhase::UnmatchedRight));

        state.advance_phase(&JoinType::FullOuter(sqlparser::ast::JoinConstraint::None));
        assert!(matches!(state.phase, JoinPhase::UnmatchedLeft));

        state.advance_phase(&JoinType::FullOuter(sqlparser::ast::JoinConstraint::None));
        assert!(matches!(state.phase, JoinPhase::Completed));
    }

    #[test]
    fn test_tuple_combiner_edge_cases() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let combiner = TupleCombiner::new(left_schema, right_schema, output_schema);

        // Test with empty tuple (minimal values)
        let empty_left = Arc::new(Tuple::new(
            &[Value::new(0), Value::new(0)],
            &combiner.left_schema,
            RID::new(0, 0),
        ));
        let empty_right = Arc::new(Tuple::new(
            &[Value::new(""), Value::new("")],
            &combiner.right_schema,
            RID::new(0, 0),
        ));

        let result = combiner.combine_tuples(&empty_left, &empty_right);
        let values = result.get_values();
        assert_eq!(values.len(), 4);
        assert_eq!(*values[0].get_val(), Val::Integer(0));
        assert_eq!(*values[1].get_val(), Val::Integer(0));
        assert_eq!(*values[2].get_val(), Val::VarLen("".to_string()));
        assert_eq!(*values[3].get_val(), Val::VarLen("".to_string()));
    }

    #[test]
    fn test_predicate_evaluator_error_handling() {
        let (left_schema, right_schema, _) = create_test_schemas();

        // Test with different value types to ensure robustness
        let test_cases = vec![
            (Val::Boolean(true), true),
            (Val::Boolean(false), false),
            (Val::Null, false),
            (Val::Integer(1), false), // Non-boolean treated as false
            (Val::VarLen("true".to_string()), false), // Non-boolean treated as false
        ];

        for (predicate_val, expected) in test_cases {
            let column = Column::new("temp", TypeId::Boolean);
            let predicate: Arc<dyn ExpressionOps + Send + Sync> = Arc::new(
                ConstantExpression::new(Value::new(predicate_val), column, vec![]),
            );
            let evaluator =
                JoinPredicateEvaluator::new(predicate, left_schema.clone(), right_schema.clone());

            let (left_tuple, right_tuple) = create_test_tuples();
            let result = evaluator.evaluate(&left_tuple, &right_tuple);

            assert!(result.is_ok(), "Predicate evaluation should not fail");
            assert!(
                result.unwrap() == expected,
                "Predicate evaluation result mismatch"
            );
        }
    }

    #[test]
    fn test_comprehensive_join_scenarios() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();

        // Test scenario: Some matches, some non-matches
        let scenarios = vec![("Always match", true, true), ("Never match", false, false)];

        for (scenario_name, predicate_result, should_match) in scenarios {
            let tuple_combiner = TupleCombiner::new(
                left_schema.clone(),
                right_schema.clone(),
                output_schema.clone(),
            );
            let predicate_evaluator = JoinPredicateEvaluator::new(
                create_boolean_predicate(predicate_result),
                left_schema.clone(),
                right_schema.clone(),
            );

            // Test INNER JOIN
            let inner_handler = JoinTypeHandler::new(
                JoinType::Inner(sqlparser::ast::JoinConstraint::None),
                tuple_combiner.clone(),
                predicate_evaluator.clone(),
            );

            let (left_tuple, right_tuple) = create_test_tuples();
            let mut state = JoinState::new();

            let result = inner_handler.process_tuple_pair(&left_tuple, &right_tuple, &mut state);
            assert!(
                result.is_ok(),
                "INNER JOIN should execute successfully for {}",
                scenario_name
            );

            let result_value = result.unwrap();
            if should_match {
                assert!(
                    result_value.is_some(),
                    "INNER JOIN should return result for matching case: {}",
                    scenario_name
                );
                assert!(
                    state.current_left_matched,
                    "Left tuple should be marked as matched for: {}",
                    scenario_name
                );
            } else {
                assert!(
                    result_value.is_none(),
                    "INNER JOIN should return None for non-matching case: {}",
                    scenario_name
                );
                assert!(
                    !state.current_left_matched,
                    "Left tuple should not be marked as matched for: {}",
                    scenario_name
                );
            }
        }
    }

    // =============================================================================
    // COMPREHENSIVE JOIN TYPE VALIDATION TESTS
    // =============================================================================

    #[test]
    fn test_left_join_type_support() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let tuple_combiner =
            TupleCombiner::new(left_schema.clone(), right_schema.clone(), output_schema);
        let predicate_evaluator = JoinPredicateEvaluator::new(
            create_boolean_predicate(true),
            left_schema.clone(),
            right_schema.clone(),
        );

        // Test both JoinType::Left and JoinType::LeftOuter
        let join_types = vec![
            JoinType::Left(sqlparser::ast::JoinConstraint::None),
            JoinType::LeftOuter(sqlparser::ast::JoinConstraint::None),
        ];

        for join_type in join_types {
            let join_handler = JoinTypeHandler::new(
                join_type.clone(),
                tuple_combiner.clone(),
                predicate_evaluator.clone(),
            );

            let (left_tuple, right_tuple) = create_test_tuples();
            let mut state = JoinState::new();

            // Both should be handled the same way - as left outer joins
            let result = join_handler.process_tuple_pair(&left_tuple, &right_tuple, &mut state);
            assert!(
                result.is_ok(),
                "LEFT JOIN type {:?} should be supported",
                join_type
            );

            if let Ok(Some(_)) = result {
                assert!(
                    state.current_left_matched,
                    "Left tuple should be marked as matched"
                );
            }
        }
    }

    #[test]
    fn test_right_join_type_support() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let tuple_combiner =
            TupleCombiner::new(left_schema.clone(), right_schema.clone(), output_schema);
        let predicate_evaluator = JoinPredicateEvaluator::new(
            create_boolean_predicate(true),
            left_schema.clone(),
            right_schema.clone(),
        );

        // Test both JoinType::Right and JoinType::RightOuter
        let join_types = vec![
            JoinType::Right(sqlparser::ast::JoinConstraint::None),
            JoinType::RightOuter(sqlparser::ast::JoinConstraint::None),
        ];

        for join_type in join_types {
            let join_handler = JoinTypeHandler::new(
                join_type.clone(),
                tuple_combiner.clone(),
                predicate_evaluator.clone(),
            );

            let (left_tuple, right_tuple) = create_test_tuples();
            let mut state = JoinState::new();

            // Both should be handled the same way - as right outer joins
            let result = join_handler.process_tuple_pair(&left_tuple, &right_tuple, &mut state);
            assert!(
                result.is_ok(),
                "RIGHT JOIN type {:?} should be supported",
                join_type
            );
        }
    }

    #[test]
    fn test_join_type_phase_transitions() {
        let mut state = JoinState::new();

        // Test Left join phase transitions
        state.advance_phase(&JoinType::Left(sqlparser::ast::JoinConstraint::None));
        assert!(
            matches!(state.phase, JoinPhase::Completed),
            "Left join should go directly to Completed"
        );

        // Reset state
        state = JoinState::new();

        // Test Right join phase transitions
        state.advance_phase(&JoinType::Right(sqlparser::ast::JoinConstraint::None));
        assert!(
            matches!(state.phase, JoinPhase::UnmatchedRight),
            "Right join should go to UnmatchedRight phase"
        );

        // Advance again
        state.advance_phase(&JoinType::Right(sqlparser::ast::JoinConstraint::None));
        assert!(
            matches!(state.phase, JoinPhase::Completed),
            "Right join should complete after UnmatchedRight"
        );
    }

    #[test]
    fn test_comprehensive_join_type_coverage() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let tuple_combiner =
            TupleCombiner::new(left_schema.clone(), right_schema.clone(), output_schema);
        let predicate_evaluator = JoinPredicateEvaluator::new(
            create_boolean_predicate(true),
            left_schema.clone(),
            right_schema.clone(),
        );

        // Test all supported join types
        let supported_join_types = vec![
            JoinType::Inner(sqlparser::ast::JoinConstraint::None),
            JoinType::Join(sqlparser::ast::JoinConstraint::None),
            JoinType::Left(sqlparser::ast::JoinConstraint::None),
            JoinType::LeftOuter(sqlparser::ast::JoinConstraint::None),
            JoinType::Right(sqlparser::ast::JoinConstraint::None),
            JoinType::RightOuter(sqlparser::ast::JoinConstraint::None),
            JoinType::FullOuter(sqlparser::ast::JoinConstraint::None),
            JoinType::CrossJoin,
        ];

        for join_type in supported_join_types {
            let join_handler = JoinTypeHandler::new(
                join_type.clone(),
                tuple_combiner.clone(),
                predicate_evaluator.clone(),
            );

            let (left_tuple, right_tuple) = create_test_tuples();
            let mut state = JoinState::new();

            let result = join_handler.process_tuple_pair(&left_tuple, &right_tuple, &mut state);
            assert!(
                result.is_ok(),
                "Join type {:?} should be supported",
                join_type
            );
        }
    }

    #[test]
    fn test_left_join_unmatched_tuple_handling() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let tuple_combiner =
            TupleCombiner::new(left_schema.clone(), right_schema.clone(), output_schema);
        let predicate_evaluator = JoinPredicateEvaluator::new(
            create_boolean_predicate(false), // Predicate that never matches
            left_schema.clone(),
            right_schema.clone(),
        );

        let join_handler = JoinTypeHandler::new(
            JoinType::Left(sqlparser::ast::JoinConstraint::None),
            tuple_combiner,
            predicate_evaluator,
        );

        let (left_tuple, _right_tuple) = create_test_tuples();
        let mut state = JoinState::new();

        // First test with a matching right tuple that doesn't match predicate
        let result = join_handler.handle_left_outer_join(&left_tuple, None, &mut state);
        assert!(result.is_ok());
        let result_value = result.unwrap();
        assert!(
            result_value.is_some(),
            "LEFT JOIN should return null-padded tuple for unmatched left tuple"
        );

        // Verify the result has correct structure (left + null right)
        let (combined_tuple, _) = result_value.unwrap();
        let values = combined_tuple.get_values();

        // Should have values from left tuple + null values for right tuple
        assert_eq!(
            values.len(),
            4,
            "Combined tuple should have 4 values (2 left + 2 right)"
        );
        assert_eq!(
            *values[0].get_val(),
            Val::Integer(1),
            "First value should be from left tuple"
        );
        assert_eq!(
            *values[1].get_val(),
            Val::Integer(25),
            "Second value should be from left tuple"
        );
        assert_eq!(
            *values[2].get_val(),
            Val::Null,
            "Third value should be null (right tuple)"
        );
        assert_eq!(
            *values[3].get_val(),
            Val::Null,
            "Fourth value should be null (right tuple)"
        );
    }

    #[test]
    fn test_join_type_error_handling() {
        let (left_schema, right_schema, output_schema) = create_test_schemas();
        let tuple_combiner =
            TupleCombiner::new(left_schema.clone(), right_schema.clone(), output_schema);
        let predicate_evaluator = JoinPredicateEvaluator::new(
            create_boolean_predicate(true),
            left_schema.clone(),
            right_schema.clone(),
        );

        // Test unsupported join type (create a custom unsupported type)
        // Note: We can't easily test this with the current enum structure,
        // but we can test that our supported types work correctly

        let join_handler = JoinTypeHandler::new(
            JoinType::Left(sqlparser::ast::JoinConstraint::None),
            tuple_combiner,
            predicate_evaluator,
        );

        // Verify that the join type is correctly stored
        assert!(matches!(join_handler.get_join_type(), JoinType::Left(_)));
    }
}
