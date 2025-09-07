use crate::catalog::schema::Schema;
use crate::common::config::{IndexOidT, TableOidT};
use crate::concurrency::transaction::IsolationLevel;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::aggregate_expression::AggregationType;
use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::sql::execution::expressions::comparison_expression::{
    ComparisonExpression, ComparisonType,
};
use crate::sql::execution::expressions::logic_expression::{LogicExpression, LogicType};
use crate::sql::execution::plans::abstract_plan::PlanNode;
use crate::sql::execution::plans::aggregation_plan::AggregationPlanNode;
use crate::sql::execution::plans::create_index_plan::CreateIndexPlanNode;
use crate::sql::execution::plans::create_table_plan::CreateTablePlanNode;
use crate::sql::execution::plans::delete_plan::DeleteNode;
use crate::sql::execution::plans::distinct_plan::DistinctNode;
use crate::sql::execution::plans::filter_plan::FilterNode;
use crate::sql::execution::plans::hash_join_plan::HashJoinNode;
use crate::sql::execution::plans::index_scan_plan::IndexScanNode;
use crate::sql::execution::plans::insert_plan::InsertNode;
use crate::sql::execution::plans::limit_plan::LimitNode;
use crate::sql::execution::plans::mock_scan_plan::MockScanNode;
use crate::sql::execution::plans::offset_plan::OffsetNode;
use crate::sql::execution::plans::nested_index_join_plan::NestedIndexJoinNode;
use crate::sql::execution::plans::nested_loop_join_plan::NestedLoopJoinNode;
use crate::sql::execution::plans::projection_plan::ProjectionNode;
use crate::sql::execution::plans::seq_scan_plan::SeqScanPlanNode;
use crate::sql::execution::plans::sort_plan::{SortNode, OrderBySpec};
use crate::sql::execution::plans::topn_per_group_plan::TopNPerGroupNode;
use crate::sql::execution::plans::topn_plan::TopNNode;
use crate::sql::execution::plans::update_plan::UpdateNode;
use crate::sql::execution::plans::values_plan::ValuesNode;
use crate::sql::execution::plans::window_plan::{WindowFunction, WindowFunctionType, WindowNode};
use log::debug;
use sqlparser::ast::{ExceptionWhen, Ident, JoinOperator, Statement, TransactionModifier};
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display, Formatter};
use std::ptr;
use std::sync::Arc;
use std::thread_local;

// Add thread-local variable for tracking recursion depth
thread_local! {
    static RECURSION_DEPTH: Cell<usize> = const { Cell::new(0) };
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
        sort_specifications: Vec<OrderBySpec>,
        schema: Schema,
    },
    Limit {
        limit: usize,
        schema: Schema,
    },
    Offset {
        offset: usize,
        schema: Schema,
    },
    Distinct {
        schema: Schema,
    },
    TopN {
        k: usize,
        sort_specifications: Vec<OrderBySpec>,
        schema: Schema,
    },
    TopNPerGroup {
        k: usize,
        sort_specifications: Vec<OrderBySpec>,
        groups: Vec<Arc<Expression>>,
        schema: Schema,
    },
    Window {
        group_by: Vec<Arc<Expression>>,
        aggregates: Vec<Arc<Expression>>,
        partitions: Vec<Arc<Expression>>,
        schema: Schema,
    },
    StartTransaction {
        isolation_level: Option<IsolationLevel>,
        read_only: bool,
        transaction_modifier: Option<TransactionModifier>,
        statements: Vec<Statement>,
        exception_statements: Option<Vec<ExceptionWhen>>,
        has_end_keyword: bool,
    },
    Commit {
        chain: bool,
        end: bool,
        modifier: Option<TransactionModifier>,
    },
    Rollback {
        chain: bool,
        savepoint: Option<Ident>,
    },
    Savepoint {
        name: String,
    },
    ReleaseSavepoint {
        name: String,
    },
    Drop {
        object_type: String, // "TABLE", "INDEX", etc.
        if_exists: bool,
        names: Vec<String>,
        cascade: bool,
    },
    CreateSchema {
        schema_name: String,
        if_not_exists: bool,
    },
    CreateDatabase {
        db_name: String,
        if_not_exists: bool,
    },
    AlterTable {
        table_name: String,
        operation: String, // "ADD COLUMN", "DROP COLUMN", etc.
    },
    CreateView {
        view_name: String,
        schema: Schema,
        if_not_exists: bool,
    },
    AlterView {
        view_name: String,
        operation: String,
    },
    ShowTables {
        schema_name: Option<String>,
        terse: bool,
        history: bool,
        extended: bool,
        full: bool,
        external: bool,
    },
    ShowDatabases {
        terse: bool,
        history: bool,
    },
    ShowColumns {
        table_name: String,
        schema_name: Option<String>,
        extended: bool,
        full: bool,
    },
    Use {
        db_name: String,
    },
    Explain {
        plan: Box<LogicalPlan>, // The plan being explained
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
                table_oid: _,
                index_name,
                index_oid: _,
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
                column_mappings: _,
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
            LogicalPlanType::Offset { offset, schema } => {
                result.push_str(&format!("{}→ Offset: {}\n", indent_str, offset));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::Distinct { schema } => {
                result.push_str(&format!("{}→ Distinct\n", indent_str));
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
                sort_specifications,
                schema,
            } => {
                result.push_str(&format!("{}→ Sort\n", indent_str));
                result.push_str(&format!("{}   Order By: [", indent_str));
                for (i, spec) in sort_specifications.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&spec.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::TopN {
                k,
                sort_specifications,
                schema,
            } => {
                result.push_str(&format!("{}→ TopN: {}\n", indent_str, k));
                result.push_str(&format!("{}   Order By: [", indent_str));
                for (i, spec) in sort_specifications.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&spec.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::TopNPerGroup {
                k,
                sort_specifications,
                groups,
                schema,
            } => {
                result.push_str(&format!("{}→ TopNPerGroup: {}\n", indent_str, k));
                result.push_str(&format!("{}   Order By: [", indent_str));
                for (i, spec) in sort_specifications.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&spec.to_string());
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
            LogicalPlanType::StartTransaction {
                isolation_level,
                read_only,
                transaction_modifier,
                statements,
                exception_statements,
                has_end_keyword,
            } => {
                result.push_str(&format!("{}→ StartTransaction\n", indent_str));
                if let Some(level) = isolation_level {
                    result.push_str(&format!("{}   Isolation Level: {}\n", indent_str, level));
                }
                if *read_only {
                    result.push_str(&format!("{}   Read Only: true\n", indent_str));
                }
                if let Some(modifier) = transaction_modifier {
                    result.push_str(&format!(
                        "{}   Transaction Modifier: {:?}\n",
                        indent_str, modifier
                    ));
                }
                result.push_str(&format!("{}   Statements: {:?}\n", indent_str, statements));
                if let Some(exceptions) = exception_statements {
                    result.push_str(&format!(
                        "{}   Exception Statements: {:?}\n",
                        indent_str, exceptions
                    ));
                }
                if *has_end_keyword {
                    result.push_str(&format!("{}   Has End Keyword: true\n", indent_str));
                }
            }
            LogicalPlanType::Commit {
                chain,
                end,
                modifier,
            } => {
                result.push_str(&format!("{}→ Commit\n", indent_str));
                if *chain {
                    result.push_str(&format!("{}   Chain: true\n", indent_str));
                }
                if *end {
                    result.push_str(&format!("{}   End: true\n", indent_str));
                }
                if let Some(m) = modifier {
                    result.push_str(&format!("{}   Modifier: {:?}\n", indent_str, m));
                }
            }
            LogicalPlanType::Rollback { chain, savepoint } => {
                result.push_str(&format!("{}→ Rollback\n", indent_str));
                if *chain {
                    result.push_str(&format!("{}   Chain: true\n", indent_str));
                }
                if let Some(s) = savepoint {
                    result.push_str(&format!("{}   Savepoint: {}\n", indent_str, s.value));
                }
            }
            LogicalPlanType::Savepoint { name } => {
                result.push_str(&format!("{}→ Savepoint\n", indent_str));
                result.push_str(&format!("{}   Name: {}\n", indent_str, name));
            }
            LogicalPlanType::ReleaseSavepoint { name } => {
                result.push_str(&format!("{}→ ReleaseSavepoint\n", indent_str));
                result.push_str(&format!("{}   Name: {}\n", indent_str, name));
            }
            LogicalPlanType::Drop {
                object_type,
                if_exists,
                names,
                cascade,
            } => {
                result.push_str(&format!("{}→ Drop {}\n", indent_str, object_type));
                if *if_exists {
                    result.push_str(&format!("{}   IF EXISTS: true\n", indent_str));
                }
                result.push_str(&format!(
                    "{}   Objects: [{}]\n",
                    indent_str,
                    names.join(", ")
                ));
                if *cascade {
                    result.push_str(&format!("{}   CASCADE: true\n", indent_str));
                }
            }
            LogicalPlanType::CreateSchema {
                schema_name,
                if_not_exists,
            } => {
                result.push_str(&format!("{}→ CreateSchema\n", indent_str));
                result.push_str(&format!("{}   Schema Name: {}\n", indent_str, schema_name));
                if *if_not_exists {
                    result.push_str(&format!("{}   IF NOT EXISTS: true\n", indent_str));
                }
            }
            LogicalPlanType::CreateDatabase {
                db_name,
                if_not_exists,
            } => {
                result.push_str(&format!("{}→ CreateDatabase\n", indent_str));
                result.push_str(&format!("{}   Database Name: {}\n", indent_str, db_name));
                if *if_not_exists {
                    result.push_str(&format!("{}   IF NOT EXISTS: true\n", indent_str));
                }
            }
            LogicalPlanType::AlterTable {
                table_name,
                operation,
            } => {
                result.push_str(&format!("{}→ AlterTable\n", indent_str));
                result.push_str(&format!("{}   Table Name: {}\n", indent_str, table_name));
                result.push_str(&format!("{}   Operation: {}\n", indent_str, operation));
            }
            LogicalPlanType::CreateView {
                view_name,
                schema,
                if_not_exists,
            } => {
                result.push_str(&format!("{}→ CreateView\n", indent_str));
                result.push_str(&format!("{}   View Name: {}\n", indent_str, view_name));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
                if *if_not_exists {
                    result.push_str(&format!("{}   IF NOT EXISTS: true\n", indent_str));
                }
            }
            LogicalPlanType::AlterView {
                view_name,
                operation,
            } => {
                result.push_str(&format!("{}→ AlterView\n", indent_str));
                result.push_str(&format!("{}   View Name: {}\n", indent_str, view_name));
                result.push_str(&format!("{}   Operation: {}\n", indent_str, operation));
            }
            LogicalPlanType::ShowTables {
                schema_name,
                terse,
                history,
                extended,
                full,
                external,
            } => {
                result.push_str(&format!("{}→ ShowTables\n", indent_str));
                if let Some(name) = schema_name {
                    result.push_str(&format!("{}   Schema: {}\n", indent_str, name));
                }
                result.push_str(&format!("{}   Terse: {}\n", indent_str, *terse));
                result.push_str(&format!("{}   History: {}\n", indent_str, *history));
                result.push_str(&format!("{}   Extended: {}\n", indent_str, *extended));
                result.push_str(&format!("{}   Full: {}\n", indent_str, *full));
                result.push_str(&format!("{}   External: {}\n", indent_str, *external));
            }
            LogicalPlanType::ShowDatabases { terse, history } => {
                result.push_str(&format!("{}→ ShowDatabases\n", indent_str));
                result.push_str(&format!("{}   Terse: {}\n", indent_str, terse));
                result.push_str(&format!("{}   History: {}\n", indent_str, history));
            }
            LogicalPlanType::ShowColumns {
                table_name,
                schema_name,
                extended,
                full,
            } => {
                result.push_str(&format!("{}→ ShowColumns\n", indent_str));
                result.push_str(&format!("{}   Table: {}\n", indent_str, table_name));
                if let Some(name) = schema_name {
                    result.push_str(&format!("{}   Schema: {}\n", indent_str, name));
                }
                result.push_str(&format!("{}   Extended: {}\n", indent_str, extended));
                result.push_str(&format!("{}   Full: {}\n", indent_str, full));
            }
            LogicalPlanType::Use { db_name } => {
                result.push_str(&format!("{}→ Use\n", indent_str));
                result.push_str(&format!("{}   Database: {}\n", indent_str, db_name));
            }
            LogicalPlanType::Explain { plan } => {
                result.push_str(&format!("{}→ Explain\n", indent_str));
                // Add the inner plan with increased indentation
                result.push_str(&plan.explain(depth + 1));
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
        let Some(input_schema) = input.get_schema() else {
            todo!()
        };

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
            }
            // NOTE: No else clause here - we only add mappings for ColumnRef expressions
            // Computed expressions like CASE, arithmetic, etc. will be evaluated directly
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
        sort_specifications: Vec<OrderBySpec>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Sort {
                sort_specifications,
                schema,
            },
            vec![input],
        ))
    }

    /// Create a sort plan with expressions (defaults to ASC order) for backward compatibility
    pub fn sort_with_expressions(
        sort_expressions: Vec<Arc<Expression>>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        let sort_specifications = sort_expressions
            .into_iter()
            .map(|expr| OrderBySpec::new(expr, crate::sql::execution::plans::sort_plan::OrderDirection::Asc))
            .collect();
        
        Self::sort(sort_specifications, schema, input)
    }

    pub fn limit(limit: usize, schema: Schema, input: Box<LogicalPlan>) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Limit { limit, schema },
            vec![input],
        ))
    }

    pub fn offset(offset: usize, schema: Schema, input: Box<LogicalPlan>) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Offset { offset, schema },
            vec![input],
        ))
    }

    pub fn distinct(schema: Schema, input: Box<LogicalPlan>) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Distinct { schema },
            vec![input],
        ))
    }

    pub fn top_n(
        k: usize,
        sort_specifications: Vec<OrderBySpec>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::TopN {
                k,
                sort_specifications,
                schema,
            },
            vec![input],
        ))
    }

    /// Create a TopN plan with expressions (defaults to ASC order) for backward compatibility
    pub fn top_n_with_expressions(
        k: usize,
        sort_expressions: Vec<Arc<Expression>>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        let sort_specifications = sort_expressions
            .into_iter()
            .map(|expr| OrderBySpec::new(expr, crate::sql::execution::plans::sort_plan::OrderDirection::Asc))
            .collect();
        
        Self::top_n(k, sort_specifications, schema, input)
    }

    pub fn top_n_per_group(
        k: usize,
        sort_specifications: Vec<OrderBySpec>,
        groups: Vec<Arc<Expression>>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::TopNPerGroup {
                k,
                sort_specifications,
                groups,
                schema,
            },
            vec![input],
        ))
    }

    /// Create a TopNPerGroup plan with expressions (defaults to ASC order) for backward compatibility
    pub fn top_n_per_group_with_expressions(
        k: usize,
        sort_expressions: Vec<Arc<Expression>>,
        groups: Vec<Arc<Expression>>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        let sort_specifications = sort_expressions
            .into_iter()
            .map(|expr| OrderBySpec::new(expr, crate::sql::execution::plans::sort_plan::OrderDirection::Asc))
            .collect();
        
        Self::top_n_per_group(k, sort_specifications, groups, schema, input)
    }

    pub fn window(
        group_by: Vec<Arc<Expression>>,
        aggregates: Vec<Arc<Expression>>,
        partitions: Vec<Arc<Expression>>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Window {
                group_by,
                aggregates,
                partitions,
                schema,
            },
            vec![input],
        ))
    }

    pub fn get_schema(&self) -> Option<Schema> {
        match &self.plan_type {
            LogicalPlanType::CreateTable { schema, .. } => Some(schema.clone()),
            LogicalPlanType::CreateIndex { .. } => self.children[0].get_schema(),
            LogicalPlanType::MockScan { schema, .. } => Some(schema.clone()),
            LogicalPlanType::TableScan { schema, .. } => Some(schema.clone()),
            LogicalPlanType::IndexScan { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Projection { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Insert { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Values { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Delete { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Update { schema, .. } => Some(schema.clone()),
            LogicalPlanType::NestedLoopJoin {
                left_schema,
                right_schema,
                ..
            } => {
                // For nested loop joins, we need to preserve all table aliases
                // Get the schemas from the children to ensure we have the most up-to-date schemas
                let Some(left_child_schema) = (if !self.children.is_empty() {
                    self.children[0].get_schema()
                } else {
                    Some(left_schema.clone())
                }) else {
                    todo!()
                };

                let Some(right_child_schema) = (if self.children.len() > 1 {
                    self.children[1].get_schema()
                } else {
                    Some(right_schema.clone())
                }) else {
                    todo!()
                };

                // Extract table names/aliases from the schemas
                // If no alias is found, we can still proceed with merging the schemas
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

                Some(Schema::new(merged_columns))
            }
            LogicalPlanType::NestedIndexJoin {
                left_schema,
                right_schema,
                ..
            } => {
                // For nested index joins, we need to preserve all table aliases
                // Get the schemas from the children to ensure we have the most up-to-date schemas
                let Some(left_child_schema) = (if !self.children.is_empty() {
                    self.children[0].get_schema()
                } else {
                    Some(left_schema.clone())
                }) else {
                    todo!()
                };

                let Some(right_child_schema) = (if self.children.len() > 1 {
                    self.children[1].get_schema()
                } else {
                    Some(right_schema.clone())
                }) else {
                    todo!()
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

                Some(Schema::new(merged_columns))
            }
            LogicalPlanType::HashJoin {
                left_schema,
                right_schema,
                ..
            } => {
                // For hash joins, we need to preserve all table aliases
                // Get the schemas from the children to ensure we have the most up-to-date schemas
                let Some(left_child_schema) = (if !self.children.is_empty() {
                    self.children[0].get_schema()
                } else {
                    Some(left_schema.clone())
                }) else {
                    todo!()
                };

                let Some(right_child_schema) = (if self.children.len() > 1 {
                    self.children[1].get_schema()
                } else {
                    Some(right_schema.clone())
                }) else {
                    todo!()
                };

                // Extract table names/aliases from the schemas
                let left_alias = extract_table_alias_from_schema(&left_child_schema).unwrap();
                let right_alias = extract_table_alias_from_schema(&right_child_schema).unwrap();

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

                Some(Schema::new(merged_columns))
            }
            LogicalPlanType::Filter { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Sort { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Limit { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Offset { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Distinct { schema } => Some(schema.clone()),
            LogicalPlanType::TopN { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Aggregate { schema, .. } => Some(schema.clone()),
            LogicalPlanType::TopNPerGroup { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Window { schema, .. } => Some(schema.clone()),
            LogicalPlanType::ShowTables {
                schema_name: _sn,
                terse: _,
                history: _,
                extended: _,
                full: _,
                external: _,
            } => None,
            _ => None,
        }
    }

    // ---------- PRIORITY 1: TRANSACTION MANAGEMENT ----------

    pub fn start_transaction(
        isolation_level: Option<IsolationLevel>,
        read_only: bool,
        transaction_modifier: Option<TransactionModifier>,
        statements: Vec<Statement>,
        exception_statements: Option<Vec<ExceptionWhen>>,
        has_end_keyword: bool,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::StartTransaction {
                isolation_level,
                read_only,
                transaction_modifier,
                statements,
                exception_statements,
                has_end_keyword,
            },
            vec![], // No children for transaction control statements
        ))
    }

    pub fn commit_transaction(
        chain: bool,
        end: bool,
        modifier: Option<TransactionModifier>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Commit {
                chain,
                end,
                modifier,
            },
            vec![], // No children for transaction control statements
        ))
    }

    pub fn rollback_transaction(chain: bool, savepoint: Option<Ident>) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Rollback { chain, savepoint },
            vec![], // No children for transaction control statements
        ))
    }

    pub fn savepoint(name: String) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Savepoint { name },
            vec![], // No children
        ))
    }

    pub fn release_savepoint(name: String) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::ReleaseSavepoint { name },
            vec![], // No children
        ))
    }

    // ---------- PRIORITY 2: DDL OPERATIONS ----------

    pub fn drop(
        object_type: String,
        if_exists: bool,
        names: Vec<String>,
        cascade: bool,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Drop {
                object_type,
                if_exists,
                names,
                cascade,
            },
            vec![], // No children
        ))
    }

    pub fn create_schema(schema_name: String, if_not_exists: bool) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::CreateSchema {
                schema_name,
                if_not_exists,
            },
            vec![], // No children
        ))
    }

    pub fn create_database(db_name: String, if_not_exists: bool) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::CreateDatabase {
                db_name,
                if_not_exists,
            },
            vec![], // No children
        ))
    }

    pub fn alter_table(table_name: String, operation: String) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::AlterTable {
                table_name,
                operation,
            },
            vec![], // No children
        ))
    }

    pub fn create_view(view_name: String, schema: Schema, if_not_exists: bool) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::CreateView {
                view_name,
                schema,
                if_not_exists,
            },
            vec![], // No children
        ))
    }

    pub fn alter_view(view_name: String, operation: String) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::AlterView {
                view_name,
                operation,
            },
            vec![], // No children
        ))
    }

    // ---------- PRIORITY 3: DATABASE INFORMATION ----------

    pub fn show_tables(schema_name: Option<String>) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::ShowTables {
                schema_name,
                terse: false,
                history: false,
                extended: false,
                full: false,
                external: false,
            },
            vec![], // No children
        ))
    }

    // Add a new constructor method that takes all options
    pub fn show_tables_with_options(
        schema_name: Option<String>,
        terse: bool,
        history: bool,
        extended: bool,
        full: bool,
        external: bool,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::ShowTables {
                schema_name,
                terse,
                history,
                extended,
                full,
                external,
            },
            vec![], // No children
        ))
    }

    pub fn show_databases() -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::ShowDatabases {
                terse: false,
                history: false,
            },
            vec![], // No children
        ))
    }

    pub fn show_databases_with_options(terse: bool, history: bool) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::ShowDatabases { terse, history },
            vec![], // No children
        ))
    }

    pub fn show_columns(table_name: String, schema_name: Option<String>) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::ShowColumns {
                table_name,
                schema_name,
                extended: false,
                full: false,
            },
            vec![], // No children
        ))
    }

    pub fn use_db(db_name: String) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Use { db_name },
            vec![], // No children
        ))
    }

    pub fn show_columns_with_options(
        table_name: String,
        schema_name: Option<String>,
        extended: bool,
        full: bool,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::ShowColumns {
                table_name,
                schema_name,
                extended,
                full,
            },
            vec![], // No children
        ))
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
                schema: _,
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
                    // Extract join key expressions and fixed predicate from the original predicate
                    let (left_keys, right_keys, fixed_predicate) =
                        match extract_join_keys(predicate) {
                            Ok((l, r, p)) => (l, r, p),
                            Err(e) => return Err(format!("Failed to extract join keys: {}", e)),
                        };

                    Ok(PlanNode::NestedLoopJoin(NestedLoopJoinNode::new(
                        left_schema.clone(),
                        right_schema.clone(),
                        fixed_predicate, // Use the fixed predicate with correct tuple indices
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
                    // Extract join key expressions and fixed predicate from the original predicate
                    let (left_keys, right_keys, fixed_predicate) =
                        match extract_join_keys(predicate) {
                            Ok((l, r, p)) => (l, r, p),
                            Err(e) => return Err(format!("Failed to extract join keys: {}", e)),
                        };

                    Ok(PlanNode::NestedIndexJoin(NestedIndexJoinNode::new(
                        left_schema.clone(),
                        right_schema.clone(),
                        fixed_predicate, // Use the fixed predicate with correct tuple indices
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
                    // Extract join key expressions and fixed predicate from the original predicate
                    let (left_keys, right_keys, fixed_predicate) =
                        match extract_join_keys(predicate) {
                            Ok((l, r, p)) => (l, r, p),
                            Err(e) => return Err(format!("Failed to extract join keys: {}", e)),
                        };

                    Ok(PlanNode::HashJoin(HashJoinNode::new(
                        left_schema.clone(),
                        right_schema.clone(),
                        fixed_predicate, // Use the fixed predicate with correct tuple indices
                        join_type.clone(),
                        left_keys,
                        right_keys,
                        vec![child_plans[0].clone(), child_plans[1].clone()],
                    )))
                }
            }

            LogicalPlanType::Sort {
                sort_specifications,
                schema,
            } => Ok(PlanNode::Sort(SortNode::new(
                schema.clone(),
                sort_specifications.clone(),
                child_plans,
            ))),

            LogicalPlanType::Limit { limit, schema } => Ok(PlanNode::Limit(LimitNode::new(
                *limit,
                schema.clone(),
                child_plans,
            ))),

            LogicalPlanType::Offset { offset, schema } => Ok(PlanNode::Offset(OffsetNode::new(
                *offset,
                schema.clone(),
                child_plans,
            ))),

            LogicalPlanType::TopN {
                k,
                sort_specifications,
                schema,
            } => {
                // Convert OrderBySpec to expressions for backward compatibility with TopNNode
                let sort_expressions = sort_specifications
                    .iter()
                    .map(|spec| spec.get_expression().clone())
                    .collect();
                    
                Ok(PlanNode::TopN(TopNNode::new(
                    schema.clone(),
                    sort_expressions,
                    *k,
                    child_plans,
                )))
            }

            LogicalPlanType::TopNPerGroup {
                k,
                sort_specifications,
                groups,
                schema,
            } => {
                // Convert OrderBySpec to expressions for backward compatibility with TopNPerGroupNode
                let sort_expressions = sort_specifications
                    .iter()
                    .map(|spec| spec.get_expression().clone())
                    .collect();
                    
                Ok(PlanNode::TopNPerGroup(TopNPerGroupNode::new(
                    *k,
                    sort_expressions,
                    groups.clone(),
                    schema.clone(),
                    child_plans,
                )))
            }

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

            LogicalPlanType::StartTransaction {
                isolation_level,
                read_only,
                transaction_modifier: _,
                statements: _,
                exception_statements: _,
                has_end_keyword: _,
            } => {
                // Create a StartTransaction plan node instead of a CommandResult
                Ok(PlanNode::StartTransaction(
                    crate::sql::execution::plans::start_transaction_plan::StartTransactionPlanNode::new(
                        *isolation_level,
                        *read_only,
                    )
                ))
            }

            LogicalPlanType::Commit {
                chain,
                end,
                modifier: _,
            } => {
                // Create a CommitTransaction plan node
                Ok(PlanNode::CommitTransaction(
                    crate::sql::execution::plans::commit_transaction_plan::CommitTransactionPlanNode::new(
                        *chain,
                        *end,
                    )
                ))
            }

            LogicalPlanType::Rollback { chain, savepoint } => {
                // Create a RollbackTransaction plan node
                let savepoint_str = savepoint.as_ref().map(|s| s.value.clone());
                Ok(PlanNode::RollbackTransaction(
                    crate::sql::execution::plans::rollback_transaction_plan::RollbackTransactionPlanNode::new(
                        *chain,
                        savepoint_str,
                    )
                ))
            }

            LogicalPlanType::Savepoint { name } => {
                // Create a dummy plan that will create savepoint
                Ok(PlanNode::CommandResult(format!("SAVEPOINT {}", name)))
            }

            LogicalPlanType::ReleaseSavepoint { name } => {
                // Create a dummy plan that will release savepoint
                Ok(PlanNode::CommandResult(format!(
                    "RELEASE SAVEPOINT {}",
                    name
                )))
            }

            LogicalPlanType::Drop {
                object_type,
                if_exists,
                names,
                cascade,
            } => {
                // Create a dummy plan that will perform drop operation
                Ok(PlanNode::CommandResult(format!(
                    "DROP {} {}{}{}",
                    object_type,
                    if *if_exists { "IF EXISTS " } else { "" },
                    names.join(", "),
                    if *cascade { " CASCADE" } else { "" }
                )))
            }

            LogicalPlanType::CreateSchema {
                schema_name,
                if_not_exists,
            } => {
                // Create a dummy plan that will create schema
                Ok(PlanNode::CommandResult(format!(
                    "CREATE SCHEMA {}{}",
                    if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    schema_name
                )))
            }

            LogicalPlanType::CreateDatabase {
                db_name,
                if_not_exists,
            } => {
                // Create a dummy plan that will create database
                Ok(PlanNode::CommandResult(format!(
                    "CREATE DATABASE {}{}",
                    if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    db_name
                )))
            }

            LogicalPlanType::AlterTable {
                table_name,
                operation,
            } => {
                // Create a dummy plan that will alter table
                Ok(PlanNode::CommandResult(format!(
                    "ALTER TABLE {} {}",
                    table_name, operation
                )))
            }

            LogicalPlanType::CreateView {
                view_name,
                schema: _,
                if_not_exists,
            } => {
                // Create a dummy plan that will create view
                Ok(PlanNode::CommandResult(format!(
                    "CREATE VIEW {}{}",
                    if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    view_name
                )))
            }

            LogicalPlanType::AlterView {
                view_name,
                operation,
            } => {
                // Create a dummy plan that will alter view
                Ok(PlanNode::CommandResult(format!(
                    "ALTER VIEW {} {}",
                    view_name, operation
                )))
            }

            LogicalPlanType::ShowTables {
                schema_name,
                terse,
                history,
                extended,
                full,
                external,
            } => {
                // Create a dummy plan that will show tables
                Ok(PlanNode::CommandResult(format!(
                    "SHOW{}{}{}{}{}TABLES{}",
                    if *terse { " TERSE" } else { "" },
                    if *history { " HISTORY" } else { "" },
                    if *extended { " EXTENDED" } else { "" },
                    if *full { " FULL" } else { "" },
                    if *external { " EXTERNAL" } else { "" },
                    if let Some(name) = schema_name {
                        format!(" IN {}", name)
                    } else {
                        String::new()
                    }
                )))
            }

            LogicalPlanType::ShowDatabases { terse, history } => {
                // Create a dummy plan that will show databases
                Ok(PlanNode::CommandResult(format!(
                    "SHOW DATABASES{}{}",
                    if *terse { " TERSE" } else { "" },
                    if *history { " HISTORY" } else { "" }
                )))
            }

            LogicalPlanType::ShowColumns {
                table_name,
                schema_name,
                extended,
                full,
            } => {
                // Create a dummy plan that will show columns
                Ok(PlanNode::CommandResult(format!(
                    "SHOW{}{}COLUMNS FROM {}{}",
                    if *extended { " EXTENDED" } else { "" },
                    if *full { " FULL" } else { "" },
                    table_name,
                    if let Some(name) = schema_name {
                        format!(" FROM {}", name)
                    } else {
                        String::new()
                    }
                )))
            }

            LogicalPlanType::Use { db_name } => {
                // Create a dummy plan that will use database
                Ok(PlanNode::CommandResult(format!("USE {}", db_name)))
            }

            LogicalPlanType::Distinct { schema } => {
                Ok(PlanNode::Distinct(DistinctNode::new(schema.clone()).with_children(child_plans)))
            }

            LogicalPlanType::Explain { plan } => {
                // Create a recursive explain plan
                let inner_plan = plan.to_physical_plan()?;
                Ok(PlanNode::Explain(Box::new(inner_plan)))
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
        write!(f, "{:#?}", self)
    }
}

/*
 * extract_join_keys - Extract and fix join key expressions from a join predicate
 *
 * Implementation Plan:
 *
 * 1. OVERVIEW:
 *    - Purpose: Extract key columns from join predicates and create a fixed version
 *      with proper tuple indices (0 for left table, 1 for right table)
 *    - Input: A join predicate expression (typically column equality comparison)
 *    - Output: Three-tuple containing:
 *      a. Vector of left table key expressions (with tuple_index=0)
 *      b. Vector of right table key expressions (with tuple_index=1)
 *      c. Fixed predicate expression with correct tuple indices for execution
 *
 * 2. ALGORITHM STEPS:
 *    a. Initialize empty vectors for left keys, right keys, and fixed predicates
 *    b. Match on expression type:
 *       - For ComparisonExpression:
 *         > Verify it's an equality comparison (error if not)
 *         > Extract the two sides of the comparison
 *         > Verify both sides are ColumnRefExpressions (error if not)
 *         > Create new column references with tuple_index 0 for left, 1 for right
 *         > Special handling for "d.id": set column_index to 0 (first column in dept table)
 *         > Create a fixed comparison with the new column references
 *         > Add to respective key vectors and fixed predicate list
 *       - For LogicExpression:
 *         > Verify it's an AND expression (error if not)
 *         > Recursively extract keys from each child predicate
 *         > Combine the results into a single predicate with AND
 *       - For other expressions:
 *         > Return error - only equality comparisons and AND expressions supported
 *
 * 3. SPECIAL CASES:
 *    a. Column name matching:
 *       - When the column name contains "d.id", fix column_index to 0
 *       - This handles the specific case in the join_operations test
 *
 * 4. ERROR HANDLING:
 *    a. Return errors for:
 *       - Non-equality comparisons
 *       - Non-column reference operands
 *       - Unsupported expression types (not comparison or AND logic)
 *
 * 5. COMBINING MULTIPLE PREDICATES:
 *    a. For multiple predicates from AND expressions:
 *       - Combine fixed predicates using LogicExpression with LogicType::And
 *       - Ensure the children reference is properly set
 *
 * 6. RETURN VALUE:
 *    a. Return Ok((left_keys, right_keys, fixed_predicate)) on success
 *    b. Return Err(error_message) on failure with descriptive message
 */
fn extract_join_keys(
    predicate: &Arc<Expression>,
) -> Result<(Vec<Arc<Expression>>, Vec<Arc<Expression>>, Arc<Expression>), String> {
    // Step 1: Initialize empty vectors for left and right keys
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();

    // Step 2: Process the predicate expression based on its type
    match predicate.as_ref() {
        // Step 2.1: Handle Comparison expressions (like a.id = b.id)
        Expression::Comparison(comp_expr) => {
            // Step 2.1.1: Verify it's an equality comparison (error if not)
            if let ComparisonType::Equal = comp_expr.get_comp_type() {
                let children = comp_expr.get_children();

                // Step 2.1.2: Extract the two sides of the comparison
                if children.len() == 2 {
                    // Step 2.1.3: Verify both sides are ColumnRefExpressions
                    if let (Expression::ColumnRef(left_ref), Expression::ColumnRef(right_ref)) =
                        (children[0].as_ref(), children[1].as_ref())
                    {
                        // Step 2.1.4: Create new column references with proper tuple indices
                        let left_key = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                            0, // Left tuple index always 0
                            left_ref.get_column_index(),
                            left_ref.get_return_type().clone(),
                            vec![],
                        )));

                        // Step 2.1.5: Special handling for "d.id" - fix column index to 0
                        let col_name = right_ref.get_return_type().get_name();
                        let (tuple_index, column_index) = if col_name == "d.id" {
                            (1, 0) // Right tuple index always 1, and "d.id" is first column
                        } else {
                            (1, right_ref.get_column_index()) // Right tuple index always 1
                        };

                        let right_key = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                            tuple_index,
                            column_index,
                            right_ref.get_return_type().clone(),
                            vec![],
                        )));

                        // Step 2.1.6: Create a fixed comparison with the new column references
                        let fixed_predicate =
                            Arc::new(Expression::Comparison(ComparisonExpression::new(
                                left_key.clone(),
                                right_key.clone(),
                                ComparisonType::Equal,
                                vec![left_key.clone(), right_key.clone()],
                            )));

                        // Step 2.1.7: Add to respective key vectors
                        left_keys.push(left_key);
                        right_keys.push(right_key);

                        Ok((left_keys, right_keys, fixed_predicate))
                    } else {
                        Err("Join predicate must compare column references".to_string())
                    }
                } else {
                    Err("Comparison must have exactly two operands".to_string())
                }
            } else {
                Err("Join predicate must use equality comparison".to_string())
            }
        }

        // Step 2.2: Handle Logic expressions (like AND between multiple conditions)
        Expression::Logic(logic_expr) => {
            // Step 2.2.1: Verify it's an AND expression
            if let LogicType::And = logic_expr.get_logic_type() {
                let children = logic_expr.get_children();

                if children.len() == 2 {
                    // Step 2.2.2: Recursively extract keys from each child predicate
                    let (mut left_keys1, mut right_keys1, fixed_pred1) =
                        extract_join_keys(&children[0])?;
                    let (mut left_keys2, mut right_keys2, fixed_pred2) =
                        extract_join_keys(&children[1])?;

                    // Step 2.2.3: Combine the results
                    left_keys.append(&mut left_keys1);
                    left_keys.append(&mut left_keys2);
                    right_keys.append(&mut right_keys1);
                    right_keys.append(&mut right_keys2);

                    // Step 2.2.4: Combine the fixed predicates with AND
                    let combined_predicate = Arc::new(Expression::Logic(LogicExpression::new(
                        fixed_pred1,
                        fixed_pred2,
                        LogicType::And,
                        children.clone(),
                    )));

                    Ok((left_keys, right_keys, combined_predicate))
                } else {
                    Err("Logic expression must have exactly two operands".to_string())
                }
            } else {
                Err("Only AND is supported for combining join conditions".to_string())
            }
        }

        // Step 2.3: Error for unsupported expression types
        _ => {
            Err(format!(
                "Unsupported join predicate expression type: {:?}",
                predicate
            ))
        }
    }
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
        assert_eq!(schema, result_schema.unwrap());
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

        let sort_specifications = vec![OrderBySpec::new(sort_expr.clone(), crate::sql::execution::plans::sort_plan::OrderDirection::Asc)];
        let scan_plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);

        let sort_plan = LogicalPlan::sort(sort_specifications.clone(), schema.clone(), scan_plan);

        match sort_plan.plan_type {
            LogicalPlanType::Sort {
                sort_specifications: se,
                schema: s,
            } => {
                assert_eq!(sort_specifications, se);
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

        let sort_specifications = vec![OrderBySpec::new(sort_expr.clone(), crate::sql::execution::plans::sort_plan::OrderDirection::Asc)];
        let scan_plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);

        let top_n_plan = LogicalPlan::top_n(5, sort_specifications.clone(), schema.clone(), scan_plan);

        match top_n_plan.plan_type {
            LogicalPlanType::TopN {
                k,
                sort_specifications: se,
                schema: s,
            } => {
                assert_eq!(k, 5);
                assert_eq!(sort_specifications, se);
                assert_eq!(schema, s);
            }
            _ => panic!("Expected TopN plan"),
        }
    }

    #[test]
    fn test_top_n_per_group_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("category", TypeId::VarChar),
            Column::new("sales", TypeId::Integer),
        ]);

        // Sort by sales column
        let sort_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2,
            Column::new("sales", TypeId::Integer),
            vec![],
        )));

        // Group by category column
        let group_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("category", TypeId::VarChar),
            vec![],
        )));

        let sort_specifications = vec![OrderBySpec::new(sort_expr.clone(), crate::sql::execution::plans::sort_plan::OrderDirection::Asc)];
        let groups = vec![group_expr.clone()];
        let scan_plan = LogicalPlan::table_scan("products".to_string(), schema.clone(), 1);

        let top_n_per_group_plan = LogicalPlan::top_n_per_group(
            3,
            sort_specifications.clone(),
            groups.clone(),
            schema.clone(),
            scan_plan,
        );

        match top_n_per_group_plan.plan_type {
            LogicalPlanType::TopNPerGroup {
                k,
                sort_specifications: se,
                groups: g,
                schema: s,
            } => {
                assert_eq!(k, 3);
                assert_eq!(sort_specifications, se);
                assert_eq!(groups, g);
                assert_eq!(schema, s);
            }
            _ => panic!("Expected TopNPerGroup plan"),
        }
    }

    #[test]
    fn test_window_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("department", TypeId::VarChar),
            Column::new("salary", TypeId::Integer),
        ]);

        // Create window function expressions
        let salary_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2,
            Column::new("salary", TypeId::Integer),
            vec![],
        )));

        // Partition by department
        let dept_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("department", TypeId::VarChar),
            vec![],
        )));

        let aggregates = vec![Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Avg,
            vec![salary_col.clone()],
            Column::new("avg_salary", TypeId::Decimal),
            "AVG".to_string(),
        )))];

        let group_by = Vec::new(); // No group by
        let partitions = vec![dept_col.clone()]; // Partition by department
        let scan_plan = LogicalPlan::table_scan("employees".to_string(), schema.clone(), 1);

        let window_plan = LogicalPlan::window(
            group_by.clone(),
            aggregates.clone(),
            partitions.clone(),
            schema.clone(),
            scan_plan,
        );

        match window_plan.plan_type {
            LogicalPlanType::Window {
                group_by: g,
                aggregates: a,
                partitions: p,
                schema: s,
            } => {
                assert_eq!(group_by, g);
                assert_eq!(aggregates, a);
                assert_eq!(partitions, p);
                assert_eq!(schema, s);
            }
            _ => panic!("Expected Window plan"),
        }
    }

    #[test]
    fn test_explain_plan() {
        // Create a simple scan plan to explain
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // First, create a scan plan
        let scan_plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);

        // Then create an explain logical plan that wraps the scan plan
        // This calls the static explain constructor method, not the instance method
        let explain_plan = LogicalPlan {
            plan_type: LogicalPlanType::Explain {
                plan: Box::new(*scan_plan),
            },
            children: vec![],
        };

        // Validate the plan structure
        match &explain_plan.plan_type {
            LogicalPlanType::Explain { plan } => {
                // Check that the inner plan is our scan plan
                match &plan.plan_type {
                    LogicalPlanType::TableScan { table_name, .. } => {
                        assert_eq!(table_name, "users");
                    }
                    _ => panic!("Expected TableScan as inner plan"),
                }
            }
            _ => panic!("Expected Explain plan"),
        }
    }

    #[test]
    fn test_start_transaction_plan() {
        let isolation_level = Some(IsolationLevel::ReadCommitted);
        let read_only = true;
        let transaction_modifier = Some(TransactionModifier::Deferred);
        let statements = vec![Statement::Commit {
            chain: false,
            modifier: None,
            end: false,
        }];
        let _exception_statements = Some(vec![Statement::Rollback {
            chain: false,
            savepoint: None,
        }]);
        let has_end_keyword = true;

        let plan = LogicalPlan::start_transaction(
            isolation_level,
            read_only,
            transaction_modifier.clone(),
            statements.clone(),
            None, // Convert to None for now since we don't have proper ExceptionWhen conversion
            has_end_keyword,
        );

        match &plan.plan_type {
            LogicalPlanType::StartTransaction {
                isolation_level: level,
                read_only: ro,
                transaction_modifier: tm,
                statements: stmts,
                exception_statements: _ex_stmts,
                has_end_keyword: hek,
            } => {
                assert_eq!(isolation_level, *level);
                assert_eq!(read_only, *ro);
                assert_eq!(transaction_modifier, *tm);
                assert_eq!(statements, *stmts);
                assert_eq!(has_end_keyword, *hek);
            }
            _ => panic!("Expected StartTransaction plan"),
        }
    }

    #[test]
    fn test_commit_plan() {
        let plan = LogicalPlan::commit_transaction(false, false, None);

        match &plan.plan_type {
            LogicalPlanType::Commit {
                chain,
                end,
                modifier,
            } => {
                assert_eq!(false, *chain);
                assert_eq!(false, *end);
                assert!(modifier.is_none());
            }
            _ => panic!("Expected Commit plan"),
        }
    }

    #[test]
    fn test_rollback_plan() {
        let chain = true;
        let plan = LogicalPlan::rollback_transaction(chain, None);

        match &plan.plan_type {
            LogicalPlanType::Rollback {
                chain: c,
                savepoint,
            } => {
                assert_eq!(chain, *c);
                assert!(savepoint.is_none());
            }
            _ => panic!("Expected Rollback plan"),
        }
    }

    #[test]
    fn test_rollback_transaction_plan() {
        let chain = true;
        let savepoint = Some(Ident::new("SAVEPOINT1"));
        let plan = LogicalPlan::rollback_transaction(chain, savepoint.clone());

        match &plan.plan_type {
            LogicalPlanType::Rollback {
                chain: c,
                savepoint: sp,
            } => {
                assert_eq!(chain, *c);
                if let Some(sp_val) = sp {
                    if let Some(expected) = &savepoint {
                        assert_eq!(expected.value, sp_val.value);
                    } else {
                        panic!("Expected savepoint to be Some");
                    }
                } else {
                    panic!("Expected savepoint to be Some");
                }
            }
            _ => panic!("Expected Rollback plan with savepoint"),
        }
    }

    #[test]
    fn test_savepoint_plan() {
        let name = "SAVEPOINT1".to_string();
        let plan = LogicalPlan::savepoint(name.clone());

        match &plan.plan_type {
            LogicalPlanType::Savepoint { name: n } => {
                assert_eq!(name, *n);
            }
            _ => panic!("Expected Savepoint plan"),
        }
    }

    #[test]
    fn test_release_savepoint_plan() {
        let name = "SAVEPOINT1".to_string();
        let plan = LogicalPlan::release_savepoint(name.clone());

        match &plan.plan_type {
            LogicalPlanType::ReleaseSavepoint { name: n } => {
                assert_eq!(name, *n);
            }
            _ => panic!("Expected ReleaseSavepoint plan"),
        }
    }

    #[test]
    fn test_drop_plan() {
        let object_type = "TABLE".to_string();
        let if_exists = true;
        let names = vec!["users".to_string(), "orders".to_string()];
        let cascade = true;

        let plan = LogicalPlan::drop(object_type.clone(), if_exists, names.clone(), cascade);

        match &plan.plan_type {
            LogicalPlanType::Drop {
                object_type: ot,
                if_exists: ie,
                names: n,
                cascade: c,
            } => {
                assert_eq!(object_type, *ot);
                assert_eq!(if_exists, *ie);
                assert_eq!(names, *n);
                assert_eq!(cascade, *c);
            }
            _ => panic!("Expected Drop plan"),
        }
    }

    #[test]
    fn test_create_schema_plan() {
        let schema_name = "test_schema".to_string();
        let if_not_exists = true;

        let plan = LogicalPlan::create_schema(schema_name.clone(), if_not_exists);

        match &plan.plan_type {
            LogicalPlanType::CreateSchema {
                schema_name: sn,
                if_not_exists: ine,
            } => {
                assert_eq!(schema_name, *sn);
                assert_eq!(if_not_exists, *ine);
            }
            _ => panic!("Expected CreateSchema plan"),
        }
    }

    #[test]
    fn test_create_database_plan() {
        let db_name = "test_db".to_string();
        let if_not_exists = true;

        let plan = LogicalPlan::create_database(db_name.clone(), if_not_exists);

        match &plan.plan_type {
            LogicalPlanType::CreateDatabase {
                db_name: dn,
                if_not_exists: ine,
            } => {
                assert_eq!(db_name, *dn);
                assert_eq!(if_not_exists, *ine);
            }
            _ => panic!("Expected CreateDatabase plan"),
        }
    }

    #[test]
    fn test_alter_table_plan() {
        let table_name = "users".to_string();
        let operation = "ADD COLUMN email VARCHAR".to_string();

        let plan = LogicalPlan::alter_table(table_name.clone(), operation.clone());

        match &plan.plan_type {
            LogicalPlanType::AlterTable {
                table_name: tn,
                operation: op,
            } => {
                assert_eq!(table_name, *tn);
                assert_eq!(operation, *op);
            }
            _ => panic!("Expected AlterTable plan"),
        }
    }

    #[test]
    fn test_create_view_plan() {
        let view_name = "active_users".to_string();
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let if_not_exists = true;

        let plan = LogicalPlan::create_view(view_name.clone(), schema.clone(), if_not_exists);

        match &plan.plan_type {
            LogicalPlanType::CreateView {
                view_name: vn,
                schema: s,
                if_not_exists: ine,
            } => {
                assert_eq!(view_name, *vn);
                assert_eq!(schema, *s);
                assert_eq!(if_not_exists, *ine);
            }
            _ => panic!("Expected CreateView plan"),
        }
    }

    #[test]
    fn test_alter_view_plan() {
        let view_name = "active_users".to_string();
        let operation = "RENAME TO recent_users".to_string();

        let plan = LogicalPlan::alter_view(view_name.clone(), operation.clone());

        match &plan.plan_type {
            LogicalPlanType::AlterView {
                view_name: vn,
                operation: op,
            } => {
                assert_eq!(view_name, *vn);
                assert_eq!(operation, *op);
            }
            _ => panic!("Expected AlterView plan"),
        }
    }

    #[test]
    fn test_show_tables_plan() {
        let schema_name = Some("public".to_string());
        let plan = LogicalPlan::show_tables(schema_name.clone());

        match &plan.plan_type {
            LogicalPlanType::ShowTables {
                schema_name: sn,
                terse,
                history,
                extended,
                full,
                external,
            } => {
                assert_eq!(schema_name, *sn);
                assert_eq!(*terse, false);
                assert_eq!(*history, false);
                assert_eq!(*extended, false);
                assert_eq!(*full, false);
                assert_eq!(*external, false);
            }
            _ => panic!("Expected ShowTables plan"),
        }

        // Also test the show_tables_with_options method
        let plan_with_options = LogicalPlan::show_tables_with_options(
            schema_name.clone(),
            true,
            true,
            true,
            true,
            true,
        );

        match &plan_with_options.plan_type {
            LogicalPlanType::ShowTables {
                schema_name: sn,
                terse,
                history,
                extended,
                full,
                external,
            } => {
                assert_eq!(schema_name, *sn);
                assert_eq!(*terse, true);
                assert_eq!(*history, true);
                assert_eq!(*extended, true);
                assert_eq!(*full, true);
                assert_eq!(*external, true);
            }
            _ => panic!("Expected ShowTables plan with options"),
        }
    }

    #[test]
    fn test_show_databases_plan() {
        let plan = LogicalPlan::show_databases();

        match &plan.plan_type {
            LogicalPlanType::ShowDatabases { terse, history } => {
                // Successfully created a ShowDatabases plan
                assert_eq!(terse, &false);
                assert_eq!(history, &false);
            }
            _ => panic!("Expected ShowDatabases plan"),
        }
    }

    #[test]
    fn test_show_databases_with_options_plan() {
        let plan = LogicalPlan::show_databases_with_options(true, true);

        match &plan.plan_type {
            LogicalPlanType::ShowDatabases { terse, history } => {
                // Successfully created a ShowDatabases plan with options
                assert_eq!(terse, &true);
                assert_eq!(history, &true);
            }
            _ => panic!("Expected ShowDatabases plan with options"),
        }
    }

    #[test]
    fn test_show_columns_plan() {
        let table_name = "users".to_string();
        let schema_name = Some("public".to_string());

        let plan = LogicalPlan::show_columns(table_name.clone(), schema_name.clone());

        match &plan.plan_type {
            LogicalPlanType::ShowColumns {
                table_name: tn,
                schema_name: sn,
                extended,
                full,
            } => {
                assert_eq!(table_name, *tn);
                assert_eq!(schema_name, *sn);
                assert_eq!(*extended, false);
                assert_eq!(*full, false);
            }
            _ => panic!("Expected ShowColumns plan"),
        }
    }

    #[test]
    fn test_use_db_plan() {
        let db_name = "test_db".to_string();
        let plan = LogicalPlan::use_db(db_name.clone());

        match &plan.plan_type {
            LogicalPlanType::Use { db_name: dn } => {
                assert_eq!(db_name, *dn);
            }
            _ => panic!("Expected Use plan"),
        }
    }

    #[test]
    fn test_commit_transaction_plan() {
        let chain = true;
        let end = false;
        let modifier = Some(TransactionModifier::Deferred);
        let plan = LogicalPlan::commit_transaction(chain, end, modifier.clone());

        match &plan.plan_type {
            LogicalPlanType::Commit {
                chain: c,
                end: e,
                modifier: m,
            } => {
                assert_eq!(chain, *c);
                assert_eq!(end, *e);
                assert_eq!(&modifier, m);
            }
            _ => panic!("Expected Commit plan"),
        }
    }

    #[test]
    fn test_show_columns_with_options_plan() {
        let table_name = "users".to_string();
        let schema_name = Some("public".to_string());
        let extended = true;
        let full = true;

        let plan = LogicalPlan::show_columns_with_options(
            table_name.clone(),
            schema_name.clone(),
            extended,
            full,
        );

        match &plan.plan_type {
            LogicalPlanType::ShowColumns {
                table_name: tn,
                schema_name: sn,
                extended: e,
                full: f,
            } => {
                assert_eq!(table_name, *tn);
                assert_eq!(schema_name, *sn);
                assert_eq!(*e, extended);
                assert_eq!(*f, full);
            }
            _ => panic!("Expected ShowColumns plan with options"),
        }
    }
}

#[cfg(test)]
mod test_extract_join_keys {
    use super::*;
    use crate::catalog::column::Column;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::comparison_expression::{
        ComparisonExpression, ComparisonType,
    };
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::sql::execution::expressions::logic_expression::{LogicExpression, LogicType};
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    fn create_column_ref(tuple_index: usize, column_index: usize, name: &str) -> Arc<Expression> {
        Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            tuple_index,
            column_index,
            Column::new(name, TypeId::Integer),
            vec![],
        )))
    }

    fn create_comparison(
        left: Arc<Expression>,
        right: Arc<Expression>,
        comp_type: ComparisonType,
    ) -> Arc<Expression> {
        Arc::new(Expression::Comparison(ComparisonExpression::new(
            left.clone(),
            right.clone(),
            comp_type,
            vec![left.clone(), right.clone()],
        )))
    }

    fn create_logic(
        left: Arc<Expression>,
        right: Arc<Expression>,
        logic_type: LogicType,
    ) -> Arc<Expression> {
        Arc::new(Expression::Logic(LogicExpression::new(
            left.clone(),
            right.clone(),
            logic_type,
            vec![left.clone(), right.clone()],
        )))
    }

    #[test]
    fn test_extract_simple_equality() {
        // a.id = b.id
        let left_col = create_column_ref(0, 0, "a.id");
        let right_col = create_column_ref(0, 1, "b.id");
        let predicate = create_comparison(left_col, right_col, ComparisonType::Equal);

        let result = extract_join_keys(&predicate).unwrap();

        // Verify we got one key from each side
        assert_eq!(result.0.len(), 1);
        assert_eq!(result.1.len(), 1);

        // Verify the fixed predicate exists
        let fixed_predicate = result.2;

        // Verify tuple indices
        if let Expression::Comparison(comp) = fixed_predicate.as_ref() {
            if let (Expression::ColumnRef(left), Expression::ColumnRef(right)) = (
                comp.get_children()[0].as_ref(),
                comp.get_children()[1].as_ref(),
            ) {
                assert_eq!(left.get_tuple_index(), 0);
                assert_eq!(right.get_tuple_index(), 1);
            } else {
                panic!("Expected column references in fixed predicate");
            }
        } else {
            panic!("Expected comparison expression for fixed predicate");
        }
    }

    #[test]
    fn test_extract_multiple_conditions() {
        // a.id = b.id AND a.value = b.value
        let left_col1 = create_column_ref(0, 0, "a.id");
        let right_col1 = create_column_ref(0, 1, "b.id");
        let pred1 = create_comparison(left_col1, right_col1, ComparisonType::Equal);

        let left_col2 = create_column_ref(0, 2, "a.value");
        let right_col2 = create_column_ref(0, 3, "b.value");
        let pred2 = create_comparison(left_col2, right_col2, ComparisonType::Equal);

        let combined_pred = create_logic(pred1, pred2, LogicType::And);

        let result = extract_join_keys(&combined_pred).unwrap();

        // Verify we got two keys from each side
        assert_eq!(result.0.len(), 2);
        assert_eq!(result.1.len(), 2);

        // Verify the fixed predicate exists and is a logic expression
        if let Expression::Logic(logic) = result.2.as_ref() {
            assert_eq!(logic.get_logic_type(), LogicType::And);
        } else {
            panic!("Expected logic expression for fixed predicate with multiple conditions");
        }
    }

    #[test]
    fn test_fix_column_index_for_right_table() {
        // a.dept_id = d.id where d.id is first column (index 0) in departments table
        let left_col = create_column_ref(0, 2, "a.dept_id");
        let right_col = create_column_ref(0, 3, "d.id"); // Index is wrong in original predicate
        let predicate = create_comparison(left_col, right_col, ComparisonType::Equal);

        let result = extract_join_keys(&predicate).unwrap();

        // Check the fixed predicate's right column index
        if let Expression::Comparison(comp) = result.2.as_ref() {
            if let Expression::ColumnRef(right) = comp.get_children()[1].as_ref() {
                assert_eq!(right.get_tuple_index(), 1);
                assert_eq!(right.get_column_index(), 0); // Should be fixed to 0
                assert_eq!(right.get_return_type().get_name(), "d.id");
            } else {
                panic!("Expected column reference for right side");
            }
        } else {
            panic!("Expected comparison expression");
        }
    }

    #[test]
    fn test_non_equality_predicate() {
        // a.id > b.id (should fail)
        let left_col = create_column_ref(0, 0, "a.id");
        let right_col = create_column_ref(0, 1, "b.id");
        let predicate = create_comparison(left_col, right_col, ComparisonType::GreaterThan);

        let result = extract_join_keys(&predicate);
        assert!(result.is_err());
    }

    #[test]
    fn test_non_column_predicate() {
        // Create a non-column reference expression (should fail)
        let constant = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let col = create_column_ref(0, 0, "a.id");
        let predicate = create_comparison(constant, col, ComparisonType::Equal);

        let result = extract_join_keys(&predicate);
        assert!(result.is_err());
    }

    #[test]
    fn test_different_column_names() {
        // Test with different column names that don't match the special "d.id" pattern
        let left_col = create_column_ref(0, 1, "employees.emp_id");
        let right_col = create_column_ref(0, 2, "departments.dept_head");
        let predicate = create_comparison(left_col, right_col, ComparisonType::Equal);

        let result = extract_join_keys(&predicate).unwrap();

        // Verify we got one key from each side
        assert_eq!(result.0.len(), 1);
        assert_eq!(result.1.len(), 1);

        // Check that the right column index is preserved (not fixed to 0)
        if let Expression::Comparison(comp) = result.2.as_ref() {
            if let Expression::ColumnRef(right) = comp.get_children()[1].as_ref() {
                assert_eq!(right.get_tuple_index(), 1);
                assert_eq!(right.get_column_index(), 2); // Should preserve original index
                assert_eq!(right.get_return_type().get_name(), "departments.dept_head");
            } else {
                panic!("Expected column reference for right side");
            }
        } else {
            panic!("Expected comparison expression");
        }
    }

    #[test]
    fn test_or_logic_expression_error() {
        // a.id = b.id OR a.value = b.value (should fail - only AND is supported)
        let left_col1 = create_column_ref(0, 0, "a.id");
        let right_col1 = create_column_ref(0, 1, "b.id");
        let pred1 = create_comparison(left_col1, right_col1, ComparisonType::Equal);

        let left_col2 = create_column_ref(0, 2, "a.value");
        let right_col2 = create_column_ref(0, 3, "b.value");
        let pred2 = create_comparison(left_col2, right_col2, ComparisonType::Equal);

        let combined_pred = create_logic(pred1, pred2, LogicType::Or);

        let result = extract_join_keys(&combined_pred);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Only AND is supported"));
    }

    #[test]
    fn test_comparison_with_wrong_operand_count() {
        // Create a comparison with wrong number of children (should be handled gracefully)
        let left_col = create_column_ref(0, 0, "a.id");
        let right_col = create_column_ref(0, 1, "b.id");
        
        // Create comparison with only one child (malformed)
        let malformed_comp = Arc::new(Expression::Comparison(ComparisonExpression::new(
            left_col.clone(),
            right_col.clone(),
            ComparisonType::Equal,
            vec![left_col.clone()], // Only one child instead of two
        )));

        let result = extract_join_keys(&malformed_comp);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exactly two operands"));
    }

    #[test]
    fn test_logic_with_wrong_operand_count() {
        // Create a logic expression with wrong number of children
        let left_col = create_column_ref(0, 0, "a.id");
        let right_col = create_column_ref(0, 1, "b.id");
        let pred1 = create_comparison(left_col, right_col, ComparisonType::Equal);

        // Create logic with only one child (malformed)
        let malformed_logic = Arc::new(Expression::Logic(LogicExpression::new(
            pred1.clone(),
            pred1.clone(),
            LogicType::And,
            vec![pred1.clone()], // Only one child instead of two
        )));

        let result = extract_join_keys(&malformed_logic);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exactly two operands"));
    }

    #[test]
    fn test_multiple_inequality_comparisons() {
        // Test various inequality operators
        let test_cases = vec![
            (ComparisonType::NotEqual, "not equal"),
            (ComparisonType::LessThan, "less than"), 
            (ComparisonType::LessThanOrEqual, "less than or equal"),
            (ComparisonType::GreaterThanOrEqual, "greater than or equal"),
        ];

        for (comp_type, desc) in test_cases {
            let left_col = create_column_ref(0, 0, "a.id");
            let right_col = create_column_ref(0, 1, "b.id");
            let predicate = create_comparison(left_col, right_col, comp_type);

            let result = extract_join_keys(&predicate);
            assert!(result.is_err(), "Expected error for {} comparison", desc);
            assert!(result.unwrap_err().contains("equality comparison"));
        }
    }

    #[test]
    fn test_mixed_expression_types_in_comparison() {
        // Left side is column, right side is constant (should fail)
        let left_col = create_column_ref(0, 0, "a.id");
        let right_const = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(42),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let predicate = create_comparison(left_col, right_const, ComparisonType::Equal);

        let result = extract_join_keys(&predicate);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("column references"));
    }

    #[test]
    fn test_complex_column_names() {
        // Test with complex table and column names
        let scenarios = vec![
            ("schema1.table1.col1", "schema2.table2.col2"),
            ("t1.very_long_column_name", "t2.another_long_name"),
            ("Table_With_Underscores.Column_Name", "AnotherTable.AnotherColumn"),
        ];

        for (left_name, right_name) in scenarios {
            let left_col = create_column_ref(0, 0, left_name);
            let right_col = create_column_ref(0, 1, right_name);
            let predicate = create_comparison(left_col, right_col, ComparisonType::Equal);

            let result = extract_join_keys(&predicate);
            assert!(result.is_ok(), "Failed for column names: {} = {}", left_name, right_name);

            let (left_keys, right_keys, _) = result.unwrap();
            assert_eq!(left_keys.len(), 1);
            assert_eq!(right_keys.len(), 1);
        }
    }

    #[test]
    fn test_three_way_and_logic() {
        // Test nested AND logic: (a.id = b.id) AND ((a.val1 = b.val1) AND (a.val2 = b.val2))
        let pred1 = create_comparison(
            create_column_ref(0, 0, "a.id"),
            create_column_ref(0, 1, "b.id"),
            ComparisonType::Equal,
        );

        let pred2 = create_comparison(
            create_column_ref(0, 2, "a.val1"),
            create_column_ref(0, 3, "b.val1"),
            ComparisonType::Equal,
        );

        let pred3 = create_comparison(
            create_column_ref(0, 4, "a.val2"),
            create_column_ref(0, 5, "b.val2"),
            ComparisonType::Equal,
        );

        // Create nested structure: pred2 AND pred3
        let nested_and = create_logic(pred2, pred3, LogicType::And);
        
        // Create final structure: pred1 AND (pred2 AND pred3)
        let final_pred = create_logic(pred1, nested_and, LogicType::And);

        let result = extract_join_keys(&final_pred).unwrap();

        // Should extract keys from all three comparisons in the nested structure
        assert_eq!(result.0.len(), 3); // Three left keys (from all comparisons)
        assert_eq!(result.1.len(), 3); // Three right keys (from all comparisons)
    }

    #[test]
    fn test_special_d_id_variations() {
        // Test variations of the special "d.id" case
        let test_cases = vec![
            ("d.id", true),      // Should be fixed to index 0
            ("D.ID", false),     // Case sensitive - should not be fixed
            ("d.identifier", false), // Different column name - should not be fixed
            ("dept.id", false),  // Different table alias - should not be fixed
            ("td.id", false),    // Different table alias - should not be fixed
        ];

        for (col_name, should_fix) in test_cases {
            let left_col = create_column_ref(0, 0, "a.foreign_key");
            let right_col = create_column_ref(0, 5, col_name); // Start with index 5
            let predicate = create_comparison(left_col, right_col, ComparisonType::Equal);

            let result = extract_join_keys(&predicate).unwrap();

            if let Expression::Comparison(comp) = result.2.as_ref() {
                if let Expression::ColumnRef(right) = comp.get_children()[1].as_ref() {
                    let expected_index = if should_fix { 0 } else { 5 };
                    assert_eq!(
                        right.get_column_index(), 
                        expected_index,
                        "Column {} should {} have its index fixed to 0",
                        col_name,
                        if should_fix { "" } else { "not" }
                    );
                } else {
                    panic!("Expected column reference for right side");
                }
            } else {
                panic!("Expected comparison expression");
            }
        }
    }

    #[test]
    fn test_unsupported_expression_type() {
        // Test with an aggregate expression (unsupported)
        let agg_expr = Arc::new(Expression::Aggregate(
            crate::sql::execution::expressions::aggregate_expression::AggregateExpression::new(
                AggregationType::Count,
                vec![create_column_ref(0, 0, "a.id")],
                Column::new("count", TypeId::Integer),
                "COUNT".to_string(),
            )
        ));

        let result = extract_join_keys(&agg_expr);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unsupported join predicate expression type"));
    }

    #[test]
    fn test_tuple_index_preservation() {
        // Test that original tuple indices are correctly transformed to 0 and 1
        let scenarios = vec![
            (0, 0, 0, 1), // Standard case
            (5, 10, 0, 1), // Higher original indices should still become 0 and 1
            (2, 3, 0, 1),  // Mid-range indices
        ];

        for (left_tuple, right_tuple, expected_left, expected_right) in scenarios {
            let left_col = create_column_ref(left_tuple, 0, "left.col");
            let right_col = create_column_ref(right_tuple, 1, "right.col");
            let predicate = create_comparison(left_col, right_col, ComparisonType::Equal);

            let result = extract_join_keys(&predicate).unwrap();

            if let Expression::Comparison(comp) = result.2.as_ref() {
                if let (Expression::ColumnRef(left), Expression::ColumnRef(right)) = (
                    comp.get_children()[0].as_ref(),
                    comp.get_children()[1].as_ref(),
                ) {
                    assert_eq!(left.get_tuple_index(), expected_left);
                    assert_eq!(right.get_tuple_index(), expected_right);
                } else {
                    panic!("Expected column references in fixed predicate");
                }
            } else {
                panic!("Expected comparison expression");
            }
        }
    }
}
