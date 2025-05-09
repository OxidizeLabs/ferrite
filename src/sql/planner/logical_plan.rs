use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::config::{IndexOidT, TableOidT};
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::aggregate_expression::AggregationType;
use crate::sql::execution::expressions::binary_op_expression::BinaryOpExpression;
use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::sql::execution::expressions::comparison_expression::ComparisonType;
use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
use crate::sql::execution::expressions::logic_expression::LogicType;
use crate::sql::execution::plans::abstract_plan::PlanNode;
use crate::sql::execution::plans::aggregation_plan::AggregationPlanNode;
use crate::sql::execution::plans::create_index_plan::CreateIndexPlanNode;
use crate::sql::execution::plans::create_table_plan::CreateTablePlanNode;
use crate::sql::execution::plans::delete_plan::DeleteNode;
use crate::sql::execution::plans::filter_plan::FilterNode;
use crate::sql::execution::plans::hash_join_plan::HashJoinNode;
use crate::sql::execution::plans::index_scan_plan::IndexScanNode;
use crate::sql::execution::plans::insert_plan::InsertNode;
use crate::sql::execution::plans::limit_plan::LimitNode;
use crate::sql::execution::plans::mock_scan_plan::MockScanNode;
use crate::sql::execution::plans::nested_index_join_plan::NestedIndexJoinNode;
use crate::sql::execution::plans::nested_loop_join_plan::NestedLoopJoinNode;
use crate::sql::execution::plans::projection_plan::ProjectionNode;
use crate::sql::execution::plans::seq_scan_plan::SeqScanPlanNode;
use crate::sql::execution::plans::sort_plan::SortNode;
use crate::sql::execution::plans::topn_per_group_plan::TopNPerGroupNode;
use crate::sql::execution::plans::topn_plan::TopNNode;
use crate::sql::execution::plans::update_plan::UpdateNode;
use crate::sql::execution::plans::values_plan::ValuesNode;
use crate::sql::execution::plans::window_plan::{WindowFunction, WindowFunctionType, WindowNode};
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use log::debug;
use sqlparser::ast::{BinaryOperator, JoinOperator};
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display, Formatter};
use std::ptr;
use std::sync::Arc;
use std::thread_local;

// Add thread-local variable for tracking recursion depth
thread_local! {
    static RECURSION_DEPTH: Cell<usize> = Cell::new(0);
}

#[derive(Debug, Clone)]
pub enum LogicalPlanType {
    CreateTable {
        schema: Schema,
        table_name: String,
        if_not_exists: bool,
    },
    CreateIndex {
        schema: Schema,
        table_name: String,
        index_name: String,
        key_attrs: Vec<usize>,
        if_not_exists: bool,
    },
    MockScan {
        table_name: String,
        schema: Schema,
        table_oid: TableOidT,
    },
    TableScan {
        table_name: String,
        schema: Schema,
        table_oid: TableOidT,
    },
    IndexScan {
        table_name: String,
        table_oid: TableOidT,
        index_name: String,
        index_oid: IndexOidT,
        schema: Schema,
        predicate_keys: Vec<Arc<Expression>>,
    },
    Filter {
        schema: Schema,
        table_oid: TableOidT,
        table_name: String,
        predicate: Arc<Expression>,
        output_schema: Schema,
    },
    Projection {
        expressions: Vec<Arc<Expression>>,
        schema: Schema,
        /// Maps output column positions to input column positions
        column_mappings: Vec<usize>,
    },
    Insert {
        table_name: String,
        schema: Schema,
        table_oid: TableOidT,
    },
    Delete {
        table_name: String,
        schema: Schema,
        table_oid: TableOidT,
    },
    Update {
        table_name: String,
        schema: Schema,
        table_oid: TableOidT,
        update_expressions: Vec<Arc<Expression>>,
    },
    Values {
        rows: Vec<Vec<Arc<Expression>>>,
        schema: Schema,
    },
    Aggregate {
        group_by: Vec<Arc<Expression>>,
        aggregates: Vec<Arc<Expression>>,
        schema: Schema,
    },
    NestedLoopJoin {
        left_schema: Schema,
        right_schema: Schema,
        predicate: Arc<Expression>,
        join_type: JoinOperator,
    },
    NestedIndexJoin {
        left_schema: Schema,
        right_schema: Schema,
        predicate: Arc<Expression>,
        join_type: JoinOperator,
    },
    HashJoin {
        left_schema: Schema,
        right_schema: Schema,
        predicate: Arc<Expression>,
        join_type: JoinOperator,
    },
    Sort {
        sort_expressions: Vec<Arc<Expression>>,
        schema: Schema,
    },
    Limit {
        limit: usize,
        schema: Schema,
    },
    TopN {
        k: usize,
        sort_expressions: Vec<Arc<Expression>>,
        schema: Schema,
    },
    TopNPerGroup {
        k: usize,
        sort_expressions: Vec<Arc<Expression>>,
        groups: Vec<Arc<Expression>>,
        schema: Schema,
    },
    Window {
        group_by: Vec<Arc<Expression>>,
        aggregates: Vec<Arc<Expression>>,
        partitions: Vec<Arc<Expression>>,
        schema: Schema,
    },
}

pub trait LogicalToPhysical {
    fn to_physical_plan(&self) -> Result<PlanNode, String>;
}

#[derive(Debug, Clone)]
pub struct LogicalPlan {
    pub plan_type: LogicalPlanType,
    pub children: Vec<Box<LogicalPlan>>,
}

impl LogicalPlan {
    pub fn new(plan_type: LogicalPlanType, children: Vec<Box<LogicalPlan>>) -> Self {
        Self {
            plan_type,
            children,
        }
    }

    /// Returns a string representation of the logical plan tree
    pub fn explain(&self, depth: usize) -> String {
        let indent_str = "  ".repeat(depth);
        let mut result = String::new();

        match &self.plan_type {
            LogicalPlanType::CreateTable {
                schema,
                table_name,
                if_not_exists,
            } => {
                result.push_str(&format!("{}→ CreateTable: {}\n", indent_str, table_name));
                if *if_not_exists {
                    result.push_str(&format!("{}   IF NOT EXISTS\n", indent_str));
                }
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::CreateIndex {
                schema,
                table_name,
                index_name,
                key_attrs,
                if_not_exists,
            } => {
                result.push_str(&format!(
                    "{}→ CreateIndex: {} on {}\n",
                    indent_str, index_name, table_name
                ));
                result.push_str(&format!("{}   Key Columns: {:?}\n", indent_str, key_attrs));
                if *if_not_exists {
                    result.push_str(&format!("{}   IF NOT EXISTS\n", indent_str));
                }
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::MockScan {
                table_name, schema, ..
            } => {
                result.push_str(&format!("{}→ MockScan: {}\n", indent_str, table_name));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::TableScan {
                table_name, schema, ..
            } => {
                result.push_str(&format!("{}→ TableScan: {}\n", indent_str, table_name));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::IndexScan {
                table_name,
                table_oid,
                index_name,
                index_oid,
                schema,
                predicate_keys,
            } => {
                result.push_str(&format!(
                    "{}→ IndexScan: {} using {}\n",
                    indent_str, table_name, index_name
                ));
                result.push_str(&format!("{}   Predicate Keys: [", indent_str));
                for (i, key) in predicate_keys.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&key.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::Filter {
                schema,
                table_oid,
                table_name,
                predicate,
                ..
            } => {
                result.push_str(&format!("{}→ Filter\n", indent_str));
                result.push_str(&format!("{}   Predicate: {}\n", indent_str, predicate));
                result.push_str(&format!("{}   Table: {}\n", indent_str, table_name));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::Projection {
                expressions,
                schema,
                column_mappings,
            } => {
                result.push_str(&format!("{}→ Projection\n", indent_str));
                result.push_str(&format!("{}   Expressions: [", indent_str));
                for (i, expr) in expressions.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&expr.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::Limit { limit, schema } => {
                result.push_str(&format!("{}→ Limit: {}\n", indent_str, limit));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::Insert {
                table_name, schema, ..
            } => {
                result.push_str(&format!("{}→ Insert\n", indent_str));
                result.push_str(&format!("{}   Table: {}\n", indent_str, table_name));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::Delete {
                table_name, schema, ..
            } => {
                result.push_str(&format!("{}→ Delete\n", indent_str));
                result.push_str(&format!("{}   Table: {}\n", indent_str, table_name));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::Update {
                table_name,
                schema,
                update_expressions,
                ..
            } => {
                result.push_str(&format!("{}→ Update\n", indent_str));
                result.push_str(&format!("{}   Table: {}\n", indent_str, table_name));
                result.push_str(&format!("{}   Target Expressions: [", indent_str));
                for (i, expr) in update_expressions.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&expr.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::Values { rows, schema } => {
                result.push_str(&format!("{}→ Values: {} rows\n", indent_str, rows.len()));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::Aggregate {
                group_by,
                aggregates,
                schema,
            } => {
                result.push_str(&format!("{}→ Aggregate\n", indent_str));
                if !group_by.is_empty() {
                    result.push_str(&format!("{}   Group By: [", indent_str));
                    for (i, expr) in group_by.iter().enumerate() {
                        if i > 0 {
                            result.push_str(", ");
                        }
                        result.push_str(&expr.to_string());
                    }
                    result.push_str("]\n");
                }
                result.push_str(&format!("{}   Aggregates: [", indent_str));
                for (i, expr) in aggregates.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&expr.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::NestedLoopJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                result.push_str(&format!("{}→ NestedLoopJoin\n", indent_str));
                result.push_str(&format!("{}   Join Type: {:?}\n", indent_str, join_type));
                result.push_str(&format!("{}   Predicate: {}\n", indent_str, predicate));
                result.push_str(&format!("{}   Left Schema: {}\n", indent_str, left_schema));
                result.push_str(&format!(
                    "{}   Right Schema: {}\n",
                    indent_str, right_schema
                ));
            }
            LogicalPlanType::NestedIndexJoin {
                left_schema,
                right_schema,
                ..
            } => {
                result.push_str(&format!("{}→ NestedIndexJoin\n", indent_str));
                result.push_str(&format!("{}   Left Schema: {}\n", indent_str, left_schema));
                result.push_str(&format!(
                    "{}   Right Schema: {}\n",
                    indent_str, right_schema
                ));
            }
            LogicalPlanType::HashJoin {
                left_schema,
                right_schema,
                ..
            } => {
                result.push_str(&format!("{}→ HashJoin\n", indent_str));
                result.push_str(&format!("{}   Left Schema: {}\n", indent_str, left_schema));
                result.push_str(&format!(
                    "{}   Right Schema: {}\n",
                    indent_str, right_schema
                ));
            }
            LogicalPlanType::Sort {
                sort_expressions,
                schema,
            } => {
                result.push_str(&format!("{}→ Sort\n", indent_str));
                result.push_str(&format!("{}   Order By: [", indent_str));
                for (i, expr) in sort_expressions.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&expr.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::TopN {
                k,
                sort_expressions,
                schema,
            } => {
                result.push_str(&format!("{}→ TopN: {}\n", indent_str, k));
                result.push_str(&format!("{}   Order By: [", indent_str));
                for (i, expr) in sort_expressions.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&expr.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::TopNPerGroup {
                k,
                sort_expressions,
                groups,
                schema,
            } => {
                result.push_str(&format!("{}→ TopNPerGroup: {}\n", indent_str, k));
                result.push_str(&format!("{}   Order By: [", indent_str));
                for (i, expr) in sort_expressions.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&expr.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Group By: [", indent_str));
                for (i, expr) in groups.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&expr.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::Window {
                group_by,
                aggregates,
                partitions,
                schema,
            } => {
                result.push_str(&format!("{}→ Window\n", indent_str));
                if !group_by.is_empty() {
                    result.push_str(&format!("{}   Group By: [", indent_str));
                    for (i, expr) in group_by.iter().enumerate() {
                        if i > 0 {
                            result.push_str(", ");
                        }
                        result.push_str(&expr.to_string());
                    }
                    result.push_str("]\n");
                }
                result.push_str(&format!("{}   Window Functions: [", indent_str));
                for (i, expr) in aggregates.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&expr.to_string());
                }
                result.push_str("]\n");
                if !partitions.is_empty() {
                    result.push_str(&format!("{}   Partition By: [", indent_str));
                    for (i, expr) in partitions.iter().enumerate() {
                        if i > 0 {
                            result.push_str(", ");
                        }
                        result.push_str(&expr.to_string());
                    }
                    result.push_str("]\n");
                }
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
        }

        // Add children recursively
        for child in &self.children {
            result.push_str(&child.explain(depth + 1));
        }

        result
    }

    pub fn create_table(schema: Schema, table_name: String, if_not_exists: bool) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::CreateTable {
                schema,
                table_name,
                if_not_exists,
            },
            vec![],
        ))
    }

    pub fn create_index(
        schema: Schema,
        table_name: String,
        index_name: String,
        key_attrs: Vec<usize>,
        if_not_exists: bool,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::CreateIndex {
                schema,
                table_name,
                index_name,
                key_attrs,
                if_not_exists,
            },
            vec![],
        ))
    }

    pub fn table_scan(table_name: String, schema: Schema, table_oid: u64) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::TableScan {
                table_name,
                schema,
                table_oid,
            },
            vec![],
        ))
    }

    pub fn filter(
        schema: Schema,
        table_name: String,
        table_oid: TableOidT,
        predicate: Arc<Expression>,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Filter {
                schema: schema.clone(),
                table_oid,
                table_name,
                predicate,
                output_schema: schema,
            },
            vec![input],
        ))
    }

    pub fn project(
        expressions: Vec<Arc<Expression>>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        // Get the input schema
        let input_schema = input.get_schema();

        // Calculate column mappings
        let mut column_mappings = Vec::with_capacity(expressions.len());

        for expr in &expressions {
            if let Expression::ColumnRef(col_ref) = expr.as_ref() {
                let col_name = col_ref.get_return_type().get_name();

                // Try to find the column in the input schema
                let input_idx = input_schema
                    .get_columns()
                    .iter()
                    .position(|c| c.get_name() == col_name)
                    .unwrap_or_else(|| {
                        // If not found directly, look for aggregate functions
                        input_schema
                            .get_columns()
                            .iter()
                            .position(|c| {
                                c.get_name().starts_with("SUM(")
                                    || c.get_name().starts_with("COUNT(")
                                    || c.get_name().starts_with("AVG(")
                                    || c.get_name().starts_with("MIN(")
                                    || c.get_name().starts_with("MAX(")
                            })
                            .unwrap_or(0) // Default to first column if not found
                    });

                column_mappings.push(input_idx);
            } else {
                // For expressions that aren't simple column references,
                // we'll just use a placeholder mapping
                column_mappings.push(0);
            }
        }

        Box::new(Self::new(
            LogicalPlanType::Projection {
                expressions,
                schema,
                column_mappings,
            },
            vec![input],
        ))
    }

    pub fn insert(
        table_name: String,
        schema: Schema,
        table_oid: TableOidT,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Insert {
                table_name,
                schema,
                table_oid,
            },
            vec![input],
        ))
    }

    pub fn values(rows: Vec<Vec<Arc<Expression>>>, schema: Schema) -> Box<Self> {
        Box::new(Self::new(LogicalPlanType::Values { rows, schema }, vec![]))
    }

    pub fn aggregate(
        group_by: Vec<Arc<Expression>>,
        aggregates: Vec<Arc<Expression>>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Aggregate {
                group_by,
                aggregates,
                schema,
            },
            vec![input],
        ))
    }

    pub fn index_scan(
        table_name: String,
        table_oid: TableOidT,
        index_name: String,
        index_oid: IndexOidT,
        schema: Schema,
        predicate_keys: Vec<Arc<Expression>>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::IndexScan {
                table_name,
                table_oid,
                index_name,
                index_oid,
                schema,
                predicate_keys,
            },
            vec![],
        ))
    }

    pub fn mock_scan(table_name: String, schema: Schema, table_oid: TableOidT) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::MockScan {
                table_name,
                schema,
                table_oid,
            },
            vec![],
        ))
    }

    pub fn delete(
        table_name: String,
        schema: Schema,
        table_oid: TableOidT,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Delete {
                table_name,
                schema,
                table_oid,
            },
            vec![input],
        ))
    }

    pub fn update(
        table_name: String,
        schema: Schema,
        table_oid: TableOidT,
        update_expressions: Vec<Arc<Expression>>,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Update {
                table_name,
                schema,
                table_oid,
                update_expressions,
            },
            vec![input],
        ))
    }

    pub fn nested_loop_join(
        left_schema: Schema,
        right_schema: Schema,
        predicate: Arc<Expression>,
        join_type: JoinOperator,
        left_child: Box<LogicalPlan>,
        right_child: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::NestedLoopJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            },
            vec![left_child, right_child],
        ))
    }

    pub fn nested_index_join(
        left_schema: Schema,
        right_schema: Schema,
        predicate: Arc<Expression>,
        join_type: JoinOperator,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::NestedIndexJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            },
            vec![left, right],
        ))
    }

    pub fn hash_join(
        left_schema: Schema,
        right_schema: Schema,
        predicate: Arc<Expression>,
        join_type: JoinOperator,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::HashJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            },
            vec![left, right],
        ))
    }

    pub fn sort(
        sort_expressions: Vec<Arc<Expression>>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Sort {
                sort_expressions,
                schema,
            },
            vec![input],
        ))
    }

    pub fn limit(limit: usize, schema: Schema, input: Box<LogicalPlan>) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Limit { limit, schema },
            vec![input],
        ))
    }

    pub fn top_n(
        k: usize,
        sort_expressions: Vec<Arc<Expression>>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::TopN {
                k,
                sort_expressions,
                schema,
            },
            vec![input],
        ))
    }

    pub fn get_schema(&self) -> Schema {
        match &self.plan_type {
            LogicalPlanType::CreateTable { schema, .. } => schema.clone(),
            LogicalPlanType::CreateIndex { .. } => self.children[0].get_schema(),
            LogicalPlanType::MockScan { schema, .. } => schema.clone(),
            LogicalPlanType::TableScan { schema, .. } => schema.clone(),
            LogicalPlanType::IndexScan { schema, .. } => schema.clone(),
            LogicalPlanType::Projection { schema, .. } => (*schema).clone(),
            LogicalPlanType::Insert { schema, .. } => schema.clone(),
            LogicalPlanType::Values { schema, .. } => schema.clone(),
            LogicalPlanType::Delete { schema, .. } => schema.clone(),
            LogicalPlanType::Update { schema, .. } => schema.clone(),
            LogicalPlanType::NestedLoopJoin {
                left_schema,
                right_schema,
                ..
            } => {
                // For nested loop joins, we need to preserve all table aliases
                // Get the schemas from the children to ensure we have the most up-to-date schemas
                let left_child_schema = if !self.children.is_empty() {
                    self.children[0].get_schema()
                } else {
                    left_schema.clone()
                };

                let right_child_schema = if self.children.len() > 1 {
                    self.children[1].get_schema()
                } else {
                    right_schema.clone()
                };

                // Extract table names/aliases from the schemas
                let left_alias = extract_table_alias_from_schema(&left_child_schema);
                let right_alias = extract_table_alias_from_schema(&right_child_schema);

                debug!(
                    "Merging schemas in get_schema with aliases: left={:?}, right={:?}",
                    left_alias, right_alias
                );

                // Create a new schema that preserves all column names with their original aliases
                let mut merged_columns = Vec::new();

                // Add all columns from the left schema, preserving their original names
                for col in left_child_schema.get_columns() {
                    merged_columns.push(col.clone());
                }

                // Add all columns from the right schema, preserving their original names
                for col in right_child_schema.get_columns() {
                    merged_columns.push(col.clone());
                }

                Schema::new(merged_columns)
            }
            LogicalPlanType::NestedIndexJoin {
                left_schema,
                right_schema,
                ..
            } => {
                // For nested index joins, we need to preserve all table aliases
                // Get the schemas from the children to ensure we have the most up-to-date schemas
                let left_child_schema = if !self.children.is_empty() {
                    self.children[0].get_schema()
                } else {
                    left_schema.clone()
                };

                let right_child_schema = if self.children.len() > 1 {
                    self.children[1].get_schema()
                } else {
                    right_schema.clone()
                };

                // Extract table names/aliases from the schemas
                let left_alias = extract_table_alias_from_schema(&left_child_schema);
                let right_alias = extract_table_alias_from_schema(&right_child_schema);

                debug!(
                    "Merging schemas in get_schema with aliases: left={:?}, right={:?}",
                    left_alias, right_alias
                );

                // Create a new schema that preserves all column names with their original aliases
                let mut merged_columns = Vec::new();

                // Add all columns from the left schema, preserving their original names
                for col in left_child_schema.get_columns() {
                    merged_columns.push(col.clone());
                }

                // Add all columns from the right schema, preserving their original names
                for col in right_child_schema.get_columns() {
                    merged_columns.push(col.clone());
                }

                Schema::new(merged_columns)
            }
            LogicalPlanType::HashJoin {
                left_schema,
                right_schema,
                ..
            } => {
                // For hash joins, we need to preserve all table aliases
                // Get the schemas from the children to ensure we have the most up-to-date schemas
                let left_child_schema = if !self.children.is_empty() {
                    self.children[0].get_schema()
                } else {
                    left_schema.clone()
                };

                let right_child_schema = if self.children.len() > 1 {
                    self.children[1].get_schema()
                } else {
                    right_schema.clone()
                };

                // Extract table names/aliases from the schemas
                let left_alias = extract_table_alias_from_schema(&left_child_schema);
                let right_alias = extract_table_alias_from_schema(&right_child_schema);

                debug!(
                    "Merging schemas in get_schema with aliases: left={:?}, right={:?}",
                    left_alias, right_alias
                );

                // Create a new schema that preserves all column names with their original aliases
                let mut merged_columns = Vec::new();

                // Add all columns from the left schema, preserving their original names
                for col in left_child_schema.get_columns() {
                    merged_columns.push(col.clone());
                }

                // Add all columns from the right schema, preserving their original names
                for col in right_child_schema.get_columns() {
                    merged_columns.push(col.clone());
                }

                Schema::new(merged_columns)
            }
            LogicalPlanType::Filter { output_schema, .. } => output_schema.clone(),
            LogicalPlanType::Sort { schema, .. } => schema.clone(),
            LogicalPlanType::Limit { schema, .. } => schema.clone(),
            LogicalPlanType::TopN { schema, .. } => schema.clone(),
            LogicalPlanType::Aggregate { schema, .. } => schema.clone(),
            LogicalPlanType::TopNPerGroup { schema, .. } => schema.clone(),
            LogicalPlanType::Window { schema, .. } => schema.clone(),
        }
    }
}

/// A struct to handle the conversion from logical plans to physical plans
/// using an iterative approach to avoid stack overflows
struct PlanConverter<'a> {
    stack: Vec<&'a LogicalPlan>,
    results: HashMap<usize, Result<PlanNode, String>>,
    visited: HashSet<usize>,
}

impl<'a> PlanConverter<'a> {
    /// Create a new PlanConverter with the root node
    fn new(root: &'a LogicalPlan) -> Self {
        let mut converter = Self {
            stack: Vec::new(),
            results: HashMap::new(),
            visited: HashSet::new(),
        };
        converter.stack.push(root);
        converter
    }

    /// Convert the logical plan to a physical plan using an iterative approach
    fn convert(&mut self) -> Result<PlanNode, String> {
        // Store the root node ID at the beginning
        if self.stack.is_empty() {
            return Err("Empty plan stack".to_string());
        }

        let root_node = self.stack[0];
        let root_id = ptr::addr_of!(*root_node) as usize;

        while let Some(node) = self.stack.pop() {
            let node_id = ptr::addr_of!(*node) as usize;

            // If we've already processed this node, skip it
            if self.results.contains_key(&node_id) {
                continue;
            }

            // Check if all children have been processed
            if self.are_all_children_processed(node) {
                // Process the current node
                let result = self.convert_node(node);
                self.results.insert(node_id, result);
            } else {
                // Mark this node as being processed
                self.visited.insert(node_id);

                // Push the node back onto the stack
                self.stack.push(node);

                // Push all unprocessed children onto the stack
                self.push_unprocessed_children(node);

                // Remove this node from visited since we're done with its children
                self.visited.remove(&node_id);
            }
        }

        // Get the result for the root node using the stored ID
        match self.results.get(&root_id) {
            Some(result) => result.clone(),
            None => Err("Root node not processed".to_string()),
        }
    }

    /// Check if all children of a node have been processed
    fn are_all_children_processed(&self, node: &'a LogicalPlan) -> bool {
        // If the node has no children, return true immediately
        if node.children.is_empty() {
            return true;
        }

        let mut all_children_processed = true;

        for child in &node.children {
            let child_id = ptr::addr_of!(**child) as usize;
            if !self.results.contains_key(&child_id) {
                all_children_processed = false;
                break;
            } else if let Some(Err(_)) = self.results.get(&child_id) {
                all_children_processed = false;
                break;
            }
        }

        all_children_processed
    }

    /// Push all unprocessed children of a node onto the stack
    fn push_unprocessed_children(&mut self, node: &'a LogicalPlan) {
        // If the node has no children, return immediately
        if node.children.is_empty() {
            return;
        }

        for child in node.children.iter().rev() {
            let child_ref = child.as_ref();
            let child_id = ptr::addr_of!(*child_ref) as usize;

            if !self.results.contains_key(&child_id) {
                // Check for cycles
                if self.visited.contains(&child_id) {
                    self.results.insert(
                        child_id,
                        Err(format!(
                            "Cycle detected in plan conversion at node: {:?}",
                            child_ref.plan_type
                        )),
                    );
                } else {
                    self.stack.push(child_ref);
                }
            }
        }
    }

    /// Get all processed child plans for a node
    fn get_child_plans(&self, node: &'a LogicalPlan) -> Vec<PlanNode> {
        // If the node has no children, return an empty vector immediately
        if node.children.is_empty() {
            return Vec::new();
        }

        let mut child_plans = Vec::new();

        for child in &node.children {
            let child_id = ptr::addr_of!(**child) as usize;
            if let Some(Ok(plan)) = self.results.get(&child_id) {
                child_plans.push(plan.clone());
            }
        }

        child_plans
    }

    /// Convert a single node to a physical plan
    fn convert_node(&self, node: &'a LogicalPlan) -> Result<PlanNode, String> {
        let child_plans = self.get_child_plans(node);

        match &node.plan_type {
            LogicalPlanType::CreateTable {
                schema,
                table_name,
                if_not_exists,
            } => Ok(PlanNode::CreateTable(CreateTablePlanNode::new(
                schema.clone(),
                table_name.clone(),
                *if_not_exists,
            ))),

            LogicalPlanType::CreateIndex {
                schema,
                table_name,
                index_name,
                key_attrs,
                if_not_exists,
            } => Ok(PlanNode::CreateIndex(CreateIndexPlanNode::new(
                schema.clone(),
                table_name.clone(),
                index_name.clone(),
                key_attrs.clone(),
                *if_not_exists,
            ))),

            LogicalPlanType::TableScan {
                table_name,
                schema,
                table_oid,
            } => Ok(PlanNode::SeqScan(SeqScanPlanNode::new(
                schema.clone(),
                *table_oid,
                table_name.clone(),
            ))),

            LogicalPlanType::IndexScan {
                table_name,
                table_oid,
                index_name,
                index_oid,
                schema,
                predicate_keys,
            } => Ok(PlanNode::IndexScan(IndexScanNode::new(
                schema.clone(),
                table_name.to_string(),
                *table_oid,
                index_name.to_string(),
                *index_oid,
                predicate_keys.clone(),
            ))),

            LogicalPlanType::Filter {
                schema,
                table_oid,
                table_name,
                predicate,
                ..
            } => Ok(PlanNode::Filter(FilterNode::new(
                schema.clone(),
                *table_oid,
                table_name.to_string(),
                predicate.clone(),
                child_plans,
            ))),

            LogicalPlanType::Projection {
                expressions,
                schema,
                column_mappings,
            } => {
                // Create output schema
                let output_schema = schema.clone();

                Ok(PlanNode::Projection(
                    ProjectionNode::new(
                        output_schema,
                        expressions.clone(),
                        column_mappings.clone(),
                    )
                    .with_children(child_plans),
                ))
            }

            LogicalPlanType::Insert {
                table_name,
                schema,
                table_oid,
            } => Ok(PlanNode::Insert(InsertNode::new(
                schema.clone(),
                *table_oid,
                table_name.to_string(),
                vec![],
                child_plans,
            ))),

            LogicalPlanType::Values { rows, schema } => Ok(PlanNode::Values(ValuesNode::new(
                schema.clone(),
                rows.clone(),
                child_plans,
            ))),

            LogicalPlanType::MockScan {
                table_name,
                schema,
                table_oid: _,
            } => Ok(PlanNode::MockScan(MockScanNode::new(
                schema.clone(),
                table_name.clone(),
                vec![],
            ))),

            LogicalPlanType::Delete {
                table_name,
                schema,
                table_oid,
            } => Ok(PlanNode::Delete(DeleteNode::new(
                schema.clone(),
                table_name.clone(),
                *table_oid,
                child_plans,
            ))),

            LogicalPlanType::Update {
                table_name,
                schema,
                table_oid,
                update_expressions,
            } => Ok(PlanNode::Update(UpdateNode::new(
                schema.clone(),
                table_name.clone(),
                *table_oid,
                update_expressions.clone(),
                child_plans,
            ))),

            LogicalPlanType::Aggregate {
                group_by,
                aggregates,
                schema,
            } => {
                // Filter out duplicate expressions
                let agg_exprs = aggregates
                    .iter()
                    .filter(|expr| match expr.as_ref() {
                        Expression::ColumnRef(col_ref) => {
                            !group_by.iter().any(|g| match g.as_ref() {
                                Expression::ColumnRef(g_ref) => {
                                    g_ref.get_column_index() == col_ref.get_column_index()
                                }
                                _ => false,
                            })
                        }
                        _ => true,
                    })
                    .cloned()
                    .collect::<Vec<_>>();

                Ok(PlanNode::Aggregation(AggregationPlanNode::new(
                    child_plans,
                    group_by.clone(),
                    agg_exprs,
                )))
            }

            LogicalPlanType::NestedLoopJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                if child_plans.len() < 2 {
                    Err("NestedLoopJoin requires two children".to_string())
                } else {
                    // Extract join key expressions from the predicate
                    let (left_keys, right_keys) = match extract_join_keys(&predicate) {
                        Ok((l, r)) => (l, r),
                        Err(e) => return Err(format!("Failed to extract join keys: {}", e)),
                    };

                    Ok(PlanNode::NestedLoopJoin(NestedLoopJoinNode::new(
                        left_schema.clone(),
                        right_schema.clone(),
                        predicate.clone(),
                        join_type.clone(),
                        left_keys,
                        right_keys,
                        vec![child_plans[0].clone(), child_plans[1].clone()],
                    )))
                }
            }

            LogicalPlanType::NestedIndexJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                if child_plans.len() < 2 {
                    Err("NestedIndexJoin requires two children".to_string())
                } else {
                    // Extract join key expressions from the predicate
                    let (left_keys, right_keys) = match extract_join_keys(&predicate) {
                        Ok((l, r)) => (l, r),
                        Err(e) => return Err(format!("Failed to extract join keys: {}", e)),
                    };

                    Ok(PlanNode::NestedIndexJoin(NestedIndexJoinNode::new(
                        left_schema.clone(),
                        right_schema.clone(),
                        predicate.clone(),
                        join_type.clone(),
                        left_keys,
                        right_keys,
                        vec![child_plans[0].clone(), child_plans[1].clone()],
                    )))
                }
            }

            LogicalPlanType::HashJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                if child_plans.len() < 2 {
                    Err("HashJoin requires two children".to_string())
                } else {
                    // Extract join key expressions from the predicate
                    let (left_keys, right_keys) = match extract_join_keys(&predicate) {
                        Ok((l, r)) => (l, r),
                        Err(e) => return Err(format!("Failed to extract join keys: {}", e)),
                    };

                    Ok(PlanNode::HashJoin(HashJoinNode::new(
                        left_schema.clone(),
                        right_schema.clone(),
                        predicate.clone(),
                        join_type.clone(),
                        left_keys,
                        right_keys,
                        vec![child_plans[0].clone(), child_plans[1].clone()],
                    )))
                }
            }

            LogicalPlanType::Sort {
                sort_expressions,
                schema,
            } => Ok(PlanNode::Sort(SortNode::new(
                schema.clone(),
                sort_expressions.clone(),
                child_plans,
            ))),

            LogicalPlanType::Limit { limit, schema } => Ok(PlanNode::Limit(LimitNode::new(
                *limit,
                schema.clone(),
                child_plans,
            ))),

            LogicalPlanType::TopN {
                k,
                sort_expressions,
                schema,
            } => Ok(PlanNode::TopN(TopNNode::new(
                schema.clone(),
                sort_expressions.clone(),
                *k,
                child_plans,
            ))),

            LogicalPlanType::TopNPerGroup {
                k,
                sort_expressions,
                groups,
                schema,
            } => Ok(PlanNode::TopNPerGroup(TopNPerGroupNode::new(
                *k,
                sort_expressions.clone(),
                groups.clone(),
                schema.clone(),
                child_plans,
            ))),

            LogicalPlanType::Window {
                group_by,
                aggregates,
                partitions,
                schema,
            } => {
                // Convert the logical window expressions into WindowFunction structs
                let mut window_functions = Vec::with_capacity(aggregates.len());
                for (i, agg_expr) in aggregates.iter().enumerate() {
                    // Determine the window function type based on the aggregate expression
                    let function_type = match agg_expr.as_ref() {
                        Expression::Aggregate(agg) => match agg.get_agg_type() {
                            AggregationType::Count => WindowFunctionType::Count,
                            AggregationType::Sum => WindowFunctionType::Sum,
                            AggregationType::Min => WindowFunctionType::Min,
                            AggregationType::Max => WindowFunctionType::Max,
                            AggregationType::Avg => WindowFunctionType::Average,
                            // Add other mappings as needed
                            _ => return Err("Unsupported window function type".to_string()),
                        },
                        Expression::Window(window_func) => {
                            // If it's already a window function, use its type directly
                            window_func.get_window_type()
                        }
                        _ => return Err("Invalid window function expression".to_string()),
                    };

                    // Create a new WindowFunction with the appropriate partitioning and ordering
                    window_functions.push(WindowFunction::new(
                        function_type,
                        Arc::clone(agg_expr),
                        if i < partitions.len() {
                            vec![Arc::clone(&partitions[i])]
                        } else {
                            vec![]
                        },
                        if i < group_by.len() {
                            vec![Arc::clone(&group_by[i])]
                        } else {
                            vec![]
                        },
                    ));
                }

                Ok(PlanNode::Window(WindowNode::new(
                    schema.clone(),
                    window_functions,
                    child_plans,
                )))
            }
        }
    }
}

impl LogicalToPhysical for LogicalPlan {
    fn to_physical_plan(&self) -> Result<PlanNode, String> {
        // Use the PlanConverter to convert the logical plan to a physical plan
        let mut converter = PlanConverter::new(self);
        converter.convert()
    }
}

impl Display for LogicalPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            _ => write!(f, "{:#?}", self),
        }
    }
}

fn extract_join_keys(
    predicate: &Arc<Expression>,
) -> Result<(Vec<Arc<Expression>>, Vec<Arc<Expression>>), String> {
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();

    match predicate.as_ref() {
        Expression::Comparison(comp_expr) => {
            // Handle simple equality comparison
            if let ComparisonType::Equal = comp_expr.get_comp_type() {
                let children = comp_expr.get_children();
                if children.len() == 2 {
                    // Check if the left key is a column reference from left table
                    // and right key is from right table, or visa versa
                    if let Expression::ColumnRef(left_col) = children[0].as_ref() {
                        if let Expression::ColumnRef(right_col) = children[1].as_ref() {
                            // For now just add both for simplicity
                            left_keys.push(Arc::clone(&children[0]));
                            right_keys.push(Arc::clone(&children[1]));
                        }
                    }
                }
            }
        }
        Expression::Logic(logic_expr) => {
            // Handle AND conditions for multiple join keys
            if let LogicType::And = logic_expr.get_logic_type() {
                for child in logic_expr.get_children() {
                    if let Expression::Comparison(comp_expr) = child.as_ref() {
                        if let ComparisonType::Equal = comp_expr.get_comp_type() {
                            let comp_children = comp_expr.get_children();
                            if comp_children.len() == 2 {
                                // Check column references here too
                                if let Expression::ColumnRef(_) = comp_children[0].as_ref() {
                                    if let Expression::ColumnRef(_) = comp_children[1].as_ref() {
                                        left_keys.push(Arc::clone(&comp_children[0]));
                                        right_keys.push(Arc::clone(&comp_children[1]));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        // Support more expression types for join keys
        _ => {
            // If we can't extract keys, just return a constant true predicate
            // This allows the join to still work, just without optimizations
            if left_keys.is_empty() || right_keys.is_empty() {
                left_keys = vec![Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(true),
                    Column::new("TRUE", TypeId::Boolean),
                    vec![],
                )))];
                right_keys = vec![Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(true),
                    Column::new("TRUE", TypeId::Boolean),
                    vec![],
                )))];
            }
        }
    }

    // Even if we didn't find keys, we'll still return something usable
    if left_keys.is_empty() || right_keys.is_empty() {
        left_keys = vec![Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(true),
            Column::new("TRUE", TypeId::Boolean),
            vec![],
        )))];
        right_keys = vec![Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(true),
            Column::new("TRUE", TypeId::Boolean),
            vec![],
        )))];
    }

    Ok((left_keys, right_keys))
}

// Helper function to extract table alias from schema
fn extract_table_alias_from_schema(schema: &Schema) -> Option<String> {
    // Create a map to count occurrences of each alias
    let mut alias_counts = HashMap::new();

    // Look at all columns to find table aliases
    for column in schema.get_columns() {
        let name = column.get_name();
        if let Some(dot_pos) = name.find('.') {
            let alias = name[..dot_pos].to_string();
            *alias_counts.entry(alias).or_insert(0) += 1;
        }
    }

    // If we have aliases, return the most common one
    if !alias_counts.is_empty() {
        return alias_counts
            .into_iter()
            .max_by_key(|(_, count)| *count)
            .map(|(alias, _)| alias);
    }

    None
}

// Add these helper methods for Expression
impl Expression {
    pub fn column_ref(name: &str, type_id: TypeId) -> Self {
        Self::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new(name, type_id),
            vec![],
        ))
    }

    pub fn binary_op(left: Self, op: BinaryOperator, right: Self) -> Self {
        let left_arc = Arc::new(left);
        let right_arc = Arc::new(right);
        let children = vec![left_arc.clone(), right_arc.clone()];
        Self::BinaryOp(BinaryOpExpression::new(left_arc, right_arc, op, children).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::sql::execution::expressions::aggregate_expression::{
        AggregateExpression, AggregationType,
    };
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::comparison_expression::{
        ComparisonExpression, ComparisonType,
    };
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::{Val, Value};
    use sqlparser::ast::{BinaryOperator, JoinConstraint, JoinOperator};

    #[test]
    fn test_create_table_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let plan = LogicalPlan::create_table(schema.clone(), "users".to_string(), false);

        match plan.plan_type {
            LogicalPlanType::CreateTable {
                schema: s,
                table_name,
                if_not_exists,
            } => {
                assert_eq!(schema, s);
                assert_eq!(table_name, "users");
                assert_eq!(if_not_exists, false);
            }
            _ => panic!("Expected CreateTable plan"),
        }
    }

    #[test]
    fn test_table_scan_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);

        match plan.plan_type {
            LogicalPlanType::TableScan {
                table_name,
                schema: s,
                table_oid,
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(schema, s);
                assert_eq!(table_oid, 1);
            }
            _ => panic!("Expected TableScan plan"),
        }
    }

    #[test]
    fn test_filter_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("age", TypeId::Integer),
        ]);

        let column_ref = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("age", TypeId::Integer),
            vec![],
        )));
        let constant = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Integer(18)),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            column_ref.clone(),
            constant.clone(),
            ComparisonType::GreaterThan,
            vec![column_ref.clone(), constant.clone()],
        )));

        let scan_plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);
        let filter_plan = LogicalPlan::filter(
            schema.clone(),
            "users".to_string(),
            1,
            predicate.clone(),
            scan_plan,
        );

        match filter_plan.plan_type {
            LogicalPlanType::Filter {
                schema: s,
                table_oid,
                table_name,
                predicate: p,
                output_schema: _,
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(schema, s);
                assert_eq!(table_oid, 1);
                assert_eq!(p, predicate);
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn test_projection_plan() {
        // Create a schema for the input
        let input_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ]);

        // Create a table scan as the input
        let table_scan = LogicalPlan::table_scan("users".to_string(), input_schema.clone(), 1);

        // Create projection expressions
        let id_expr = Arc::new(Expression::column_ref("id", TypeId::Integer));
        let name_expr = Arc::new(Expression::column_ref("name", TypeId::VarChar));
        let expressions = vec![id_expr.clone(), name_expr.clone()];

        // Create output schema
        let output_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // Create a projection plan
        let projection_plan =
            LogicalPlan::project(expressions.clone(), output_schema.clone(), table_scan);

        // Check the plan type
        match projection_plan.plan_type {
            LogicalPlanType::Projection {
                expressions: e,
                schema: s,
                column_mappings,
            } => {
                assert_eq!(expressions, e);
                assert_eq!(output_schema, s);
                // The column mappings should map to the correct input columns
                assert_eq!(column_mappings, vec![0, 1]);
            }
            _ => panic!("Expected Projection plan"),
        }
    }

    #[test]
    fn test_aggregate_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("age", TypeId::Integer),
        ]);

        let group_by = vec![Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )))];

        let age_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("age", TypeId::Integer),
            vec![],
        )));
        let aggregates = vec![Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Avg,
            vec![age_col.clone()],
            Column::new("avg_age", TypeId::Decimal),
            "AVG".to_string(),
        )))];

        let scan_plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);
        let aggregate_plan = LogicalPlan::aggregate(
            group_by.clone(),
            aggregates.clone(),
            schema.clone(),
            scan_plan,
        );

        match aggregate_plan.plan_type {
            LogicalPlanType::Aggregate {
                group_by: g,
                aggregates: a,
                schema: s,
            } => {
                assert_eq!(group_by, g);
                assert_eq!(aggregates, a);
                assert_eq!(schema, s);
            }
            _ => panic!("Expected Aggregate plan"),
        }
    }

    #[test]
    fn test_explain_output() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("age", TypeId::Integer),
        ]);

        let column_ref = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("age", TypeId::Integer),
            vec![],
        )));
        let constant = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Integer(18)),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            column_ref.clone(),
            constant.clone(),
            ComparisonType::GreaterThan,
            vec![column_ref.clone(), constant.clone()],
        )));

        let scan_plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);
        let filter_plan = LogicalPlan::filter(
            schema.clone(),
            "users".to_string(),
            1,
            predicate.clone(),
            scan_plan,
        );

        let explain_output = filter_plan.explain(0);
        assert!(explain_output.contains("Filter"));
        assert!(explain_output.contains("TableScan"));
    }

    #[test]
    fn test_get_schema() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);
        let result_schema = plan.get_schema();
        assert_eq!(schema, result_schema);
    }

    #[test]
    fn test_to_physical_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);
        let physical_plan = plan.to_physical_plan();
        assert!(physical_plan.is_ok());
    }

    #[test]
    fn test_create_index_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let key_attrs = vec![0]; // Index on the "id" column
        let plan = LogicalPlan::create_index(
            schema.clone(),
            "users".to_string(),
            "users_id_idx".to_string(),
            key_attrs.clone(),
            false,
        );

        match plan.plan_type {
            LogicalPlanType::CreateIndex {
                schema: s,
                table_name,
                index_name,
                key_attrs: k,
                if_not_exists,
            } => {
                assert_eq!(schema, s);
                assert_eq!(table_name, "users");
                assert_eq!(index_name, "users_id_idx");
                assert_eq!(key_attrs, k);
                assert_eq!(if_not_exists, false);
            }
            _ => panic!("Expected CreateIndex plan"),
        }
    }

    #[test]
    fn test_mock_scan_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let plan = LogicalPlan::mock_scan("users".to_string(), schema.clone(), 1);

        match plan.plan_type {
            LogicalPlanType::MockScan {
                table_name,
                schema: s,
                table_oid,
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(schema, s);
                assert_eq!(table_oid, 1);
            }
            _ => panic!("Expected MockScan plan"),
        }
    }

    #[test]
    fn test_index_scan_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let column_ref = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));
        let predicate_keys = vec![column_ref];

        let plan = LogicalPlan::index_scan(
            "users".to_string(),
            1,
            "users_id_idx".to_string(),
            2,
            schema.clone(),
            predicate_keys.clone(),
        );

        match plan.plan_type {
            LogicalPlanType::IndexScan {
                table_name,
                table_oid,
                index_name,
                index_oid,
                schema: s,
                predicate_keys: p,
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(table_oid, 1);
                assert_eq!(index_name, "users_id_idx");
                assert_eq!(index_oid, 2);
                assert_eq!(schema, s);
                assert_eq!(predicate_keys, p);
            }
            _ => panic!("Expected IndexScan plan"),
        }
    }

    #[test]
    fn test_insert_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // Create a values plan as the input to insert
        let id_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Integer(1)),
            Column::new("id", TypeId::Integer),
            vec![],
        )));
        let name_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::VarLen("John".to_string())),
            Column::new("name", TypeId::VarChar),
            vec![],
        )));

        let rows = vec![vec![id_val, name_val]];
        let values_plan = LogicalPlan::values(rows.clone(), schema.clone());

        let insert_plan = LogicalPlan::insert("users".to_string(), schema.clone(), 1, values_plan);

        match insert_plan.plan_type {
            LogicalPlanType::Insert {
                table_name,
                schema: s,
                table_oid,
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(schema, s);
                assert_eq!(table_oid, 1);
            }
            _ => panic!("Expected Insert plan"),
        }
    }

    #[test]
    fn test_delete_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let scan_plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);
        let delete_plan = LogicalPlan::delete("users".to_string(), schema.clone(), 1, scan_plan);

        match delete_plan.plan_type {
            LogicalPlanType::Delete {
                table_name,
                schema: s,
                table_oid,
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(schema, s);
                assert_eq!(table_oid, 1);
            }
            _ => panic!("Expected Delete plan"),
        }
    }

    #[test]
    fn test_update_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ]);

        // Create an update expression to set age = 30
        let age_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Integer(30)),
            Column::new("age", TypeId::Integer),
            vec![],
        )));

        let update_expressions = vec![age_val];
        let scan_plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);

        let update_plan = LogicalPlan::update(
            "users".to_string(),
            schema.clone(),
            1,
            update_expressions.clone(),
            scan_plan,
        );

        match update_plan.plan_type {
            LogicalPlanType::Update {
                table_name,
                schema: s,
                table_oid,
                update_expressions: u,
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(schema, s);
                assert_eq!(table_oid, 1);
                assert_eq!(update_expressions, u);
            }
            _ => panic!("Expected Update plan"),
        }
    }

    #[test]
    fn test_values_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let id_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Integer(1)),
            Column::new("id", TypeId::Integer),
            vec![],
        )));
        let name_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::VarLen("John".to_string())),
            Column::new("name", TypeId::VarChar),
            vec![],
        )));

        let rows = vec![vec![id_val.clone(), name_val.clone()]];
        let values_plan = LogicalPlan::values(rows.clone(), schema.clone());

        match values_plan.plan_type {
            LogicalPlanType::Values { rows: r, schema: s } => {
                assert_eq!(rows, r);
                assert_eq!(schema, s);
            }
            _ => panic!("Expected Values plan"),
        }
    }

    #[test]
    fn test_nested_loop_join_plan() {
        // Create two mock scan plans
        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let right_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("age", TypeId::Integer),
        ]);

        let left_plan = LogicalPlan::mock_scan("left_table".to_string(), left_schema.clone(), 1);
        let right_plan = LogicalPlan::mock_scan("right_table".to_string(), right_schema.clone(), 2);

        // Create a join condition
        let join_condition = Expression::binary_op(
            Expression::column_ref("left_table.id", TypeId::Integer),
            BinaryOperator::Eq,
            Expression::column_ref("right_table.id", TypeId::Integer),
        );

        // Create a nested loop join plan
        let join_plan = LogicalPlan::nested_loop_join(
            left_schema.clone(),
            right_schema.clone(),
            Arc::new(join_condition.clone()),
            JoinOperator::Inner(JoinConstraint::None),
            left_plan,
            right_plan,
        );

        // Check the plan type
        match join_plan.plan_type {
            LogicalPlanType::NestedLoopJoin {
                left_schema: ls,
                right_schema: rs,
                predicate,
                join_type,
            } => {
                assert_eq!(left_schema, ls);
                assert_eq!(right_schema, rs);
                assert_eq!(*predicate, join_condition);
                assert!(matches!(join_type, JoinOperator::Inner(_)));
            }
            _ => panic!("Expected NestedLoopJoin plan type"),
        }
    }

    #[test]
    fn test_hash_join_plan() {
        // Create two mock scan plans
        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let right_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("age", TypeId::Integer),
        ]);

        let left_plan = LogicalPlan::mock_scan("left_table".to_string(), left_schema.clone(), 1);
        let right_plan = LogicalPlan::mock_scan("right_table".to_string(), right_schema.clone(), 2);

        // Create a join condition
        let join_condition = Expression::binary_op(
            Expression::column_ref("left_table.id", TypeId::Integer),
            BinaryOperator::Eq,
            Expression::column_ref("right_table.id", TypeId::Integer),
        );

        // Create a hash join plan
        let join_plan = LogicalPlan::hash_join(
            left_schema.clone(),
            right_schema.clone(),
            Arc::new(join_condition.clone()),
            JoinOperator::Inner(JoinConstraint::None),
            left_plan,
            right_plan,
        );

        // Check the plan type
        match join_plan.plan_type {
            LogicalPlanType::HashJoin {
                left_schema: ls,
                right_schema: rs,
                predicate,
                join_type,
            } => {
                assert_eq!(left_schema, ls);
                assert_eq!(right_schema, rs);
                assert_eq!(*predicate, join_condition);
                assert!(matches!(join_type, JoinOperator::Inner(_)));
            }
            _ => panic!("Expected HashJoin plan type"),
        }
    }

    #[test]
    fn test_sort_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // Sort by id column
        let sort_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let sort_expressions = vec![sort_expr.clone()];
        let scan_plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);

        let sort_plan = LogicalPlan::sort(sort_expressions.clone(), schema.clone(), scan_plan);

        match sort_plan.plan_type {
            LogicalPlanType::Sort {
                sort_expressions: se,
                schema: s,
            } => {
                assert_eq!(sort_expressions, se);
                assert_eq!(schema, s);
            }
            _ => panic!("Expected Sort plan"),
        }
    }

    #[test]
    fn test_limit_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let scan_plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);
        let limit_plan = LogicalPlan::limit(10, schema.clone(), scan_plan);

        match limit_plan.plan_type {
            LogicalPlanType::Limit { limit, schema: s } => {
                assert_eq!(limit, 10);
                assert_eq!(schema, s);
            }
            _ => panic!("Expected Limit plan"),
        }
    }

    #[test]
    fn test_top_n_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // Sort by id column
        let sort_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let sort_expressions = vec![sort_expr.clone()];
        let scan_plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);

        let top_n_plan = LogicalPlan::top_n(5, sort_expressions.clone(), schema.clone(), scan_plan);

        match top_n_plan.plan_type {
            LogicalPlanType::TopN {
                k,
                sort_expressions: se,
                schema: s,
            } => {
                assert_eq!(k, 5);
                assert_eq!(sort_expressions, se);
                assert_eq!(schema, s);
            }
            _ => panic!("Expected TopN plan"),
        }
    }
}
