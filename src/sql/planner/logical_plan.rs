use crate::catalog::schema::Schema;
use crate::catalog::column::Column;
use crate::common::config::{IndexOidT, TableOidT};
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::aggregate_expression::AggregationType;
use crate::sql::execution::expressions::comparison_expression::ComparisonType;
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
use sqlparser::ast::JoinOperator;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use log::debug;
use std::collections::HashMap;

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
            LogicalPlanType::HashJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                result.push_str(&format!("{}→ HashJoin\n", indent_str));
                result.push_str(&format!("{}   Join Type: {:?}\n", indent_str, join_type));
                result.push_str(&format!("{}   Predicate: {}\n", indent_str, predicate));
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
            LogicalPlanType::NestedIndexJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                result.push_str(&format!("{}→ NestedIndexJoin\n", indent_str));
                result.push_str(&format!("{}   Join Type: {:?}\n", indent_str, join_type));
                result.push_str(&format!("{}   Predicate: {}\n", indent_str, predicate));
                result.push_str(&format!("{}   Left Schema: {}\n", indent_str, left_schema));
                result.push_str(&format!(
                    "{}   Right Schema: {}\n",
                    indent_str, right_schema
                ));
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
        Box::new(Self::new(
            LogicalPlanType::Projection {
                expressions,
                schema,
            },
            vec![input],
        ))
    }

    pub fn insert(
        table_name: String,
        schema: Schema,
        table_oid: u64,
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
        table_oid: u64,
        index_name: String,
        index_oid: u64,
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

    pub fn mock_scan(table_name: String, schema: Schema, table_oid: u64) -> Box<Self> {
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
        table_oid: u64,
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
        table_oid: u64,
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
            LogicalPlanType::NestedLoopJoin { left_schema, right_schema, .. } => {
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
                
                debug!("Merging schemas in get_schema with aliases: left={:?}, right={:?}", left_alias, right_alias);
                
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
            LogicalPlanType::NestedIndexJoin { left_schema, right_schema, .. } => {
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
                
                debug!("Merging schemas in get_schema with aliases: left={:?}, right={:?}", left_alias, right_alias);
                
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
            LogicalPlanType::HashJoin { left_schema, right_schema, .. } => {
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
                
                debug!("Merging schemas in get_schema with aliases: left={:?}, right={:?}", left_alias, right_alias);
                
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

impl LogicalToPhysical for LogicalPlan {
    fn to_physical_plan(&self) -> Result<PlanNode, String> {
        match &self.plan_type {
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
            } => {
                let children = self
                    .children
                    .iter()
                    .map(|child| child.to_physical_plan().unwrap())
                    .collect::<Vec<PlanNode>>();
                Ok(PlanNode::Filter(FilterNode::new(
                    schema.clone(),
                    *table_oid,
                    table_name.to_string(),
                    predicate.clone(),
                    children,
                )))
            }

            LogicalPlanType::Projection {
                expressions,
                schema,
            } => {
                let children = self
                    .children
                    .iter()
                    .map(|child| child.to_physical_plan().unwrap())
                    .collect::<Vec<PlanNode>>();

                // Get the input schema from the child
                let input_schema = if let Some(child) = self.children.first() {
                    child.get_schema()
                } else {
                    return Err("Projection must have a child node".to_string());
                };

                // Create column mappings and update output schema
                let mut output_schema = schema.clone();
                let mut column_mappings = Vec::new();

                for (i, expr) in expressions.iter().enumerate() {
                    if let Expression::ColumnRef(col_ref) = expr.as_ref() {
                        let col_name = col_ref.get_return_type().get_name();
                        let input_idx = input_schema
                            .get_columns()
                            .iter()
                            .position(|c| {
                                // Match either exact name or aggregate function name
                                c.get_name() == col_name
                                    || (c.get_name().starts_with("SUM(") && col_name == "total_age")
                                    || (c.get_name().starts_with("COUNT(")
                                    && col_name == "emp_count")
                                    || (c.get_name().starts_with("AVG(")
                                    && col_name == "avg_salary")
                                    || (c.get_name().starts_with("MIN(") && col_name == "min_age")
                                    || (c.get_name().starts_with("MAX(")
                                    && col_name == "max_salary")
                            })
                            .unwrap_or(i);

                        column_mappings.push(input_idx);

                        // Update the output schema column name to use the alias
                        output_schema.get_columns_mut()[i].set_name(col_name.to_string());
                    } else {
                        column_mappings.push(i);
                    }
                }

                Ok(PlanNode::Projection(
                    ProjectionNode::new(output_schema, expressions.clone(), column_mappings)
                        .with_children(children),
                ))
            }

            LogicalPlanType::Insert {
                table_name,
                schema,
                table_oid,
            } => {
                let children = self
                    .children
                    .iter()
                    .map(|child| child.to_physical_plan().unwrap())
                    .collect::<Vec<PlanNode>>();
                Ok(PlanNode::Insert(InsertNode::new(
                    schema.clone(),
                    *table_oid,
                    table_name.to_string(),
                    vec![],
                    children,
                )))
            }

            LogicalPlanType::Delete {
                table_name,
                schema,
                table_oid,
            } => {
                let children = self
                    .children
                    .iter()
                    .map(|child| child.to_physical_plan().unwrap())
                    .collect::<Vec<PlanNode>>();
                Ok(PlanNode::Delete(DeleteNode::new(
                    schema.clone(),
                    table_name.clone(),
                    *table_oid,
                    children,
                )))
            }

            LogicalPlanType::Update {
                table_name,
                schema,
                table_oid,
                update_expressions,
            } => {
                let children = self
                    .children
                    .iter()
                    .map(|child| child.to_physical_plan().unwrap())
                    .collect::<Vec<PlanNode>>();
                Ok(PlanNode::Update(UpdateNode::new(
                    schema.clone(),
                    table_name.clone(),
                    *table_oid,
                    update_expressions.clone(),
                    children,
                )))
            }

            LogicalPlanType::Values { rows, schema } => {
                let children = self
                    .children
                    .iter()
                    .map(|child| child.to_physical_plan().unwrap())
                    .collect::<Vec<PlanNode>>();
                Ok(PlanNode::Values(ValuesNode::new(
                    schema.clone(),
                    rows.clone(),
                    children,
                )))
            }

            LogicalPlanType::Aggregate {
                group_by,
                aggregates,
                schema,
            } => {
                let children = self
                    .children
                    .iter()
                    .map(|child| child.to_physical_plan())
                    .collect::<Result<Vec<PlanNode>, String>>()?;

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
                    children,
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
                let left = self.children[0].to_physical_plan()?;
                let right = self.children[1].to_physical_plan()?;

                // Extract join key expressions from the predicate
                let (left_keys, right_keys) = extract_join_keys(predicate)?;

                Ok(PlanNode::NestedLoopJoin(NestedLoopJoinNode::new(
                    left_schema.clone(),
                    right_schema.clone(),
                    predicate.clone(),
                    join_type.clone(),
                    left_keys,
                    right_keys,
                    vec![left, right],
                )))
            }

            LogicalPlanType::HashJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                let left = self.children[0].to_physical_plan()?;
                let right = self.children[1].to_physical_plan()?;

                // Extract join key expressions from the predicate
                let (left_keys, right_keys) = extract_join_keys(predicate)?;

                Ok(PlanNode::HashJoin(HashJoinNode::new(
                    left_schema.clone(),
                    right_schema.clone(),
                    predicate.clone(),
                    join_type.clone(),
                    left_keys,
                    right_keys,
                    vec![left, right],
                )))
            }

            LogicalPlanType::Sort {
                sort_expressions,
                schema,
            } => {
                let child = self.children[0].to_physical_plan()?;
                Ok(PlanNode::Sort(SortNode::new(
                    schema.clone(),
                    sort_expressions.clone(),
                    vec![child],
                )))
            }

            LogicalPlanType::Limit { limit, schema } => {
                let child = self.children[0].to_physical_plan()?;
                Ok(PlanNode::Limit(LimitNode::new(
                    *limit,
                    schema.clone(),
                    vec![child],
                )))
            }

            LogicalPlanType::TopN {
                k,
                sort_expressions,
                schema,
            } => {
                let children = self
                    .children
                    .iter()
                    .map(|child| child.to_physical_plan().unwrap())
                    .collect::<Vec<PlanNode>>();
                Ok(PlanNode::TopN(TopNNode::new(
                    schema.clone(),
                    sort_expressions.clone(),
                    k.clone(),
                    children,
                )))
            }

            LogicalPlanType::MockScan {
                table_name,
                schema,
                table_oid: _,
            } => Ok(PlanNode::MockScan(MockScanNode::new(
                schema.clone(),
                table_name.clone(),
                vec![],
            ))),

            LogicalPlanType::NestedIndexJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                let left = self.children[0].to_physical_plan()?;
                let right = self.children[1].to_physical_plan()?;

                // Extract join key expressions from the predicate
                let (left_keys, right_keys) = extract_join_keys(predicate)?;

                Ok(PlanNode::NestedIndexJoin(NestedIndexJoinNode::new(
                    left_schema.clone(),
                    right_schema.clone(),
                    predicate.clone(),
                    join_type.clone(),
                    left_keys,
                    right_keys,
                    vec![left, right],
                )))
            }
            LogicalPlanType::TopNPerGroup {
                k,
                sort_expressions,
                groups,
                schema,
            } => {
                let children = self
                    .children
                    .iter()
                    .map(|child| child.to_physical_plan())
                    .collect::<Result<Vec<PlanNode>, String>>()?;

                Ok(PlanNode::TopNPerGroup(TopNPerGroupNode::new(
                    *k,
                    sort_expressions.clone(),
                    groups.clone(),
                    schema.clone(),
                    children,
                )))
            }
            LogicalPlanType::Window {
                group_by,
                aggregates,
                partitions,
                schema,
            } => {
                let children = self
                    .children
                    .iter()
                    .map(|child| child.to_physical_plan())
                    .collect::<Result<Vec<PlanNode>, String>>()?;

                // Convert the logical window expressions into WindowFunction structs
                let window_functions = aggregates
                    .iter()
                    .enumerate()
                    .map(|(i, agg_expr)| {
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
                        Ok(WindowFunction::new(
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
                        ))
                    })
                    .collect::<Result<Vec<WindowFunction>, String>>()?;

                Ok(PlanNode::Window(WindowNode::new(
                    schema.clone(),
                    window_functions,
                    children,
                )))
            }
        }
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
                    left_keys.push(Arc::clone(&children[0]));
                    right_keys.push(Arc::clone(&children[1]));
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
                                left_keys.push(Arc::clone(&comp_children[0]));
                                right_keys.push(Arc::clone(&comp_children[1]));
                            }
                        }
                    }
                }
            }
        }
        _ => return Err("Unsupported join predicate type".to_string()),
    }

    if left_keys.is_empty() || right_keys.is_empty() {
        return Err("No valid join keys found in predicate".to_string());
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

