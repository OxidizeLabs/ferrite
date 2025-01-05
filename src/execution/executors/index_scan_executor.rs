use crate::catalogue::schema::Schema;
use crate::common::rid::RID;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::execution::expressions::comparison_expression::ComparisonType;
use crate::execution::expressions::logic_expression::LogicType;
use crate::execution::plans::abstract_plan::AbstractPlanNode;
use crate::execution::plans::index_scan_plan::IndexScanNode;
use crate::storage::index::index_iterator::IndexIterator;
use crate::storage::table::table_heap::TableHeap;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use log::{debug, error, info};
use parking_lot::RwLock;
use std::sync::Arc;
use crate::storage::index::index::IndexInfo;

pub struct IndexScanExecutor {
    context: Arc<RwLock<ExecutorContext>>,
    plan: Arc<IndexScanNode>,
    table_heap: Arc<TableHeap>,
    initialized: bool,
    iterator: Option<IndexIterator>,
}

impl IndexScanExecutor {
    pub fn new(context: Arc<RwLock<ExecutorContext>>, plan: Arc<IndexScanNode>) -> Self {
        let table_name = plan.get_table_name();
        debug!(
            "Creating IndexScanExecutor for table '{}' using index '{}'",
            table_name,
            plan.get_index_name()
        );

        // First get the context lock
        debug!("Attempting to acquire context read lock");
        let context_guard = context.read();

        // Get catalog within its own scope
        debug!("Attempting to acquire catalog read lock");
        let catalog = context_guard.get_catalog();
        let catalog_guard = catalog.read();

        // Get table info
        let table_info = match catalog_guard.get_table(table_name) {
            Some(info) => {
                debug!("Found table '{}' in catalog", table_name);
                info
            }
            None => {
                error!("Table '{}' not found in catalog", table_name);
                panic!("Table not found");
            }
        };

        // Clone the table heap while we have the lock
        let table_heap = table_info.get_table_heap().clone();

        // Verify index exists
        if catalog_guard.get_index_by_index_oid(plan.get_index_id()).is_none() {
            error!("Index '{}' not found in catalog", plan.get_index_name());
            panic!("Index not found");
        }

        // Drop locks implicitly by ending their scope
        drop(catalog_guard);
        drop(context_guard);

        debug!("Released all locks in IndexScanExecutor creation");

        Self {
            context,
            plan,
            table_heap,
            iterator: None,
            initialized: false,
        }
    }

    fn analyze_predicate_ranges(
        expr: &Expression,
    ) -> Vec<(Option<Value>, bool, Option<Value>, bool)> {
        match expr {
            Expression::Logic(logic_expr) => {
                let mut ranges = Vec::new();

                match logic_expr.get_logic_type() {
                    LogicType::And => {
                        // For AND, find the most restrictive range
                        let mut start_value = None;
                        let mut end_value = None;
                        let mut include_start = false;
                        let mut include_end = false;

                        // Process each child of AND
                        for child in logic_expr.get_children() {
                            let child_ranges = Self::analyze_predicate_ranges(child);
                            for (child_start, child_start_incl, child_end, child_end_incl) in
                                child_ranges
                            {
                                // Update start bound if more restrictive
                                if let Some(value) = child_start {
                                    match &start_value {
                                        None => {
                                            start_value = Some(value);
                                            include_start = child_start_incl;
                                        }
                                        Some(current) if value > *current => {
                                            start_value = Some(value);
                                            include_start = child_start_incl;
                                        }
                                        _ => {}
                                    }
                                }

                                // Update end bound if more restrictive
                                if let Some(value) = child_end {
                                    match &end_value {
                                        None => {
                                            end_value = Some(value);
                                            include_end = child_end_incl;
                                        }
                                        Some(current) if value < *current => {
                                            end_value = Some(value);
                                            include_end = child_end_incl;
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        ranges.push((start_value, include_start, end_value, include_end));
                    }
                    LogicType::Or => {
                        // For OR, collect all ranges and union them
                        for child in logic_expr.get_children() {
                            ranges.extend(Self::analyze_predicate_ranges(child));
                        }
                    }
                }
                ranges
            }
            Expression::Comparison(comp_expr) => {
                if let Ok(value) = comp_expr.get_right().evaluate(
                    &Tuple::new(&vec![], Schema::new(vec![]), RID::new(0, 0)),
                    &Schema::new(vec![]),
                ) {
                    match comp_expr.get_comp_type() {
                        ComparisonType::Equal => {
                            // For equality, use same value for both bounds
                            vec![(Some(value.clone()), true, Some(value), true)]
                        }
                        ComparisonType::GreaterThan => {
                            // x > value
                            vec![(Some(value), false, None, false)]
                        }
                        ComparisonType::GreaterThanOrEqual => {
                            // x >= value
                            vec![(Some(value), true, None, false)]
                        }
                        ComparisonType::LessThan => {
                            // x < value
                            vec![(None, false, Some(value), false)]
                        }
                        ComparisonType::LessThanOrEqual => {
                            // x <= value
                            vec![(None, false, Some(value), true)]
                        }
                        ComparisonType::NotEqual => {
                            vec![(Some(value.clone()), false, Some(value), false)]
                        }
                    }
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        }
    }
    fn analyze_bounds(&self, index_info: &IndexInfo) -> (Option<Tuple>, Option<Tuple>) {
            let predicate_keys = self.plan.get_predicate_keys();
            let mut all_ranges = Vec::new();

            for pred in predicate_keys {
                all_ranges.extend(Self::analyze_predicate_ranges(pred.as_ref()));
            }

            // If we have no ranges to analyze, return None for full scan
            if all_ranges.is_empty() {
                return (None, None);
            }

            // Find the widest range that covers all predicates
            let mut final_start = None;
            let mut final_end = None;

            for (start, start_incl, end, end_incl) in all_ranges {
                // Take minimum start value
                match (&final_start, start) {
                    (None, Some(value)) => final_start = Some(value),
                    (Some(current), Some(value)) if value < *current => {
                        final_start = Some(value)
                    }
                    _ => {}
                }

                // Take maximum end value
                match (&final_end, end) {
                    (None, Some(value)) => final_end = Some(value),
                    (Some(current), Some(value)) if value > *current => final_end = Some(value),
                    _ => {}
                }
            }

            // Create tuples using the index's key schema
            let key_schema = index_info.get_key_schema().clone();

            let start_tuple = final_start.clone().map(|v| {
                Tuple::new(
                    &vec![v],
                    key_schema.clone(),
                    RID::new(0, 0)
                )
            });

            let end_tuple = final_end.clone().map(|v| {
                Tuple::new(
                    &vec![v],
                    key_schema.clone(),
                    RID::new(0, 0)
                )
            });

            debug!(
            "Scan bounds after analyzing predicates: start={:?}, end={:?}",
            final_start, final_end
        );

            (start_tuple, end_tuple)
    }
}

impl AbstractExecutor for IndexScanExecutor {
    fn init(&mut self) {
        if self.initialized {
            return;
        }

        let context_guard = self.context.read();
        let catalog = context_guard.get_catalog();
        let catalog_guard = catalog.read();

        if let Some((index_info, btree)) = catalog_guard.get_index_by_index_oid(self.plan.get_index_id()) {
            // Get metadata from the B+ tree itself
            let tree_guard = btree.read();
            let metadata = tree_guard.get_metadata();

            // Analyze predicates and create bounds...
            let (start_key, end_key) = self.analyze_bounds(&metadata);

            // Create iterator
            self.iterator = Some(IndexIterator::new(btree.clone(), start_key, end_key));
            self.initialized = true;
        }
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if !self.initialized {
            debug!("IndexScanExecutor not initialized, initializing now");
            self.init();
        }

        // Get iterator reference
        let iter = self.iterator.as_mut()?;

        // Keep trying until we find a valid tuple or reach the end
        loop {
            match iter.next() {
                Some(rid) => {
                    debug!("Found RID {:?} in index", rid);

                    // Use RID to fetch tuple from table heap
                    match self.table_heap.get_tuple(rid) {
                        Ok((meta, tuple)) => {
                            // Skip deleted tuples
                            if meta.is_deleted() {
                                debug!("Skipping deleted tuple with RID {:?}", rid);
                                continue;
                            }
                            debug!("Successfully fetched tuple for RID {:?}", rid);
                            return Some((tuple, rid));
                        }
                        Err(e) => {
                            error!("Failed to fetch tuple for RID {:?}: {:?}", rid, e);
                            continue;
                        }
                    }
                }
                None => {
                    info!("Reached end of index scan");
                    return None;
                }
            }
        }
    }

    fn get_output_schema(&self) -> Schema {
        debug!("Getting output schema: {:?}", self.plan.get_output_schema());
        self.plan.get_output_schema().clone()
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutorContext>> {
        self.context.clone()
    }
}
