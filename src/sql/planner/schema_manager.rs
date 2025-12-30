//! # Schema Manager
//!
//! This module provides the `SchemaManager`, a central component for handling schema-related
//! operations in the SQL query planner and executor. It bridges the gap between SQL syntax
//! (parsed by `sqlparser`) and the internal type system used by the database engine.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────┐
//! │                              Schema Manager                                     │
//! ├─────────────────────────────────────────────────────────────────────────────────┤
//! │                                                                                 │
//! │   ┌────────────────────────────────────────────────────────────────────────┐    │
//! │   │                      SQL Parsing (sqlparser)                           │    │
//! │   │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                  │    │
//! │   │  │  ColumnDef   │  │   DataType   │  │ColumnOption  │                  │    │
//! │   │  │ (name, type) │  │ (INT, VARCHAR│  │ (PK, NOT NULL│                  │    │
//! │   │  │              │  │  DECIMAL...) │  │  UNIQUE, FK) │                  │    │
//! │   │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                  │    │
//! │   └─────────┼─────────────────┼─────────────────┼─────────────────────────-┘    │
//! │             │                 │                 │                               │
//! │             ▼                 ▼                 ▼                               │
//! │   ┌─────────────────────────────────────────────────────────────────────────┐   │
//! │   │                         SchemaManager                                   │   │
//! │   │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────┐  │   │
//! │   │  │ convert_sql_type│  │parse_column_opts│  │create_column_from_sql   │  │   │
//! │   │  │ DataType→TypeId │  │ConstraintExtract│  │ Column with constraints │  │   │
//! │   │  └────────┬────────┘  └────────┬────────┘  └────────────┬────────────┘  │   │
//! │   │           │                    │                        │               │   │
//! │   │           ▼                    ▼                        ▼               │   │
//! │   │  ┌──────────────────────────────────────────────────────────────────┐   │   │
//! │   │  │                    Schema Construction                           │   │   │
//! │   │  │  create_join_schema() | create_aggregation_schema() | etc.       │   │   │
//! │   │  └──────────────────────────────────────────────────────────────────┘   │   │
//! │   └─────────────────────────────────────────────────────────────────────────┘   │
//! │                                     │                                           │
//! │                                     ▼                                           │
//! │   ┌─────────────────────────────────────────────────────────────────────────┐   │
//! │   │                      Catalog System (Output)                            │   │
//! │   │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────────┐   │   │
//! │   │  │   Column     │  │    Schema    │  │  ForeignKeyConstraint        │   │   │
//! │   │  │ (TypeId,     │  │ (Vec<Column>)│  │  (ref_table, ref_col, ...)   │   │   │
//! │   │  │  constraints)│  │              │  │                              │   │   │
//! │   │  └──────────────┘  └──────────────┘  └──────────────────────────────┘   │   │
//! │   └─────────────────────────────────────────────────────────────────────────┘   │
//! │                                                                                 │
//! └─────────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Responsibilities
//!
//! | Category                | Methods                                                    |
//! |-------------------------|-------------------------------------------------------------|
//! | **Type Conversion**     | `convert_sql_type()`, `infer_expression_type()`            |
//! | **Column Creation**     | `convert_column_defs()`, `create_column_from_sql_type...` |
//! | **Constraint Parsing**  | `parse_column_options()`, `convert_referential_action()`  |
//! | **Schema Construction** | `create_join_schema()`, `create_aggregation_output_schema()`|
//! | **Value Mapping**       | `map_values_to_schema()`, `find_value_by_name()`           |
//! | **Compatibility**       | `schemas_compatible()`, `types_compatible()`               |
//!
//! ## Type Conversion
//!
//! The `convert_sql_type()` method maps SQL data types to internal `TypeId` values:
//!
//! | SQL Type(s)                          | TypeId        |
//! |--------------------------------------|---------------|
//! | `BOOLEAN`, `BOOL`                    | `Boolean`     |
//! | `TINYINT`                            | `TinyInt`     |
//! | `SMALLINT`, `INT2`                   | `SmallInt`    |
//! | `INT`, `INTEGER`, `INT4`             | `Integer`     |
//! | `BIGINT`, `INT8`                     | `BigInt`      |
//! | `DECIMAL`, `NUMERIC`                 | `Decimal`     |
//! | `FLOAT`, `REAL`                      | `Float`       |
//! | `VARCHAR`, `TEXT`, `STRING`          | `VarChar`     |
//! | `CHAR`                               | `Char`        |
//! | `BINARY`, `VARBINARY`, `BLOB`        | `Binary`      |
//! | `DATE`                               | `Date`        |
//! | `TIME`                               | `Time`        |
//! | `TIMESTAMP`, `DATETIME`              | `Timestamp`   |
//! | `INTERVAL`                           | `Interval`    |
//! | `JSON`, `JSONB`                      | `JSON`        |
//! | `UUID`                               | `UUID`        |
//! | `ARRAY`, `VECTOR` (custom)           | `Vector`      |
//!
//! ## Constraint Handling
//!
//! The `parse_column_options()` method extracts column constraints:
//!
//! | SQL Constraint        | Extracted Information                              |
//! |-----------------------|----------------------------------------------------|
//! | `PRIMARY KEY`         | `is_primary_key=true`, `is_not_null=true`          |
//! | `NOT NULL`            | `is_not_null=true`                                 |
//! | `NULL`                | `is_not_null=false`                                |
//! | `UNIQUE`              | `is_unique=true`                                   |
//! | `REFERENCES ...`      | `ForeignKeyConstraint` with table, column, actions |
//! | `CHECK (...)`         | Check expression as string                         |
//! | `DEFAULT ...`         | Default value                                      |
//!
//! ## Schema Operations
//!
//! ### Join Schema Creation
//!
//! ```text
//! Left Schema              Right Schema             Joined Schema
//! ┌───────────────┐        ┌───────────────-┐        ┌───────────────-┐
//! │ t1.id   (INT) │   +    │ t2.user_id(INT)│   =    │ t1.id   (INT)  │
//! │ t1.name (STR) │        │ t2.email (STR) │        │ t1.name (STR)  │
//! └───────────────┘        └───────────────=┘        │ t2.user_id(INT)│
//!                                                    │ t2.email (STR) │
//!                                                    └───────────────-┘
//! ```
//!
//! ### Aggregation Schema Creation
//!
//! ```text
//! GROUP BY: [category]     AGGREGATES: [SUM(sales), COUNT(*)]
//!           ↓                          ↓
//! ┌─────────────────────────────────────────────────────────────┐
//! │ Output Schema: [category (VARCHAR), SUM(sales), COUNT(*)]   │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ### Value-to-Schema Mapping
//!
//! The `map_values_to_schema()` method supports two mapping strategies:
//!
//! 1. **Name-based mapping**: When all source column names match target columns
//! 2. **Positional mapping**: When column names don't match (fallback)
//!
//! ```text
//! Source: [name="Bob", id=42]     Target: [id, name, email]
//!         ↓ (name-based)                   ↓
//! Result: [id=42, name="Bob", email=NULL]
//! ```
//!
//! ## Example Usage
//!
//! ```rust,no_run
//! let manager = SchemaManager::new();
//!
//! // Convert SQL type to internal TypeId
//! let type_id = manager.convert_sql_type(&DataType::Integer(None))?;
//! assert_eq!(type_id, TypeId::Integer);
//!
//! // Convert column definitions from CREATE TABLE
//! let columns = manager.convert_column_defs(&column_defs)?;
//!
//! // Create join schema
//! let joined = manager.create_join_schema(&left_schema, &right_schema);
//!
//! // Check schema compatibility
//! if manager.schemas_compatible(&source_schema, &target_schema) {
//!     // Schemas have matching column types
//! }
//!
//! // Map values from source to target schema
//! let mapped = manager.map_values_to_schema(&values, &source_schema, &target_schema);
//! ```
//!
//! ## Precision and Scale
//!
//! For `DECIMAL`/`NUMERIC` and `FLOAT` types, the manager extracts precision and scale:
//!
//! ```sql
//! DECIMAL(10, 2)  → precision=10, scale=2
//! NUMERIC(8)      → precision=8,  scale=None
//! FLOAT(7)        → precision=7
//! ```
//!
//! ## Limitations
//!
//! - **Multi-column foreign keys** are not currently supported
//! - **Complex expressions** in `CHECK` constraints are stored as string representations
//! - **Default values** are stored as `Value` (may need evaluation at insert time)
//! - **Type compatibility** is strict (no implicit coercion rules)

use crate::catalog::column::Column;
use crate::catalog::column::ForeignKeyConstraint;
use crate::catalog::schema::Schema;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use log;
use log::debug;
use sqlparser::ast::{
    ColumnDef, ColumnOption, ColumnOptionDef, DataType, ExactNumberInfo, Expr, ObjectName,
    ReferentialAction,
};
use std::collections::HashSet;
use std::sync::Arc;

/// Result type for parsing column constraint options.
///
/// Returns a tuple containing:
/// - `is_primary_key`: Whether the column is a primary key
/// - `is_not_null`: Whether the column has a NOT NULL constraint
/// - `is_unique`: Whether the column has a UNIQUE constraint
/// - `foreign_key`: Optional foreign key constraint details
/// - `check_constraint`: Optional CHECK constraint expression as string
/// - `default_value`: Optional default value for the column
type ColumnOptionsResult = Result<
    (
        bool,
        bool,
        bool,
        Option<ForeignKeyConstraint>,
        Option<String>,
        Option<Value>,
    ),
    String,
>;

/// Central component for handling schema-related operations in SQL processing.
///
/// The `SchemaManager` bridges the gap between SQL syntax (parsed by `sqlparser`)
/// and the internal type system. It handles type conversion, constraint parsing,
/// schema construction, and value mapping between different schemas.
///
/// # Responsibilities
///
/// - **Type Conversion**: Maps SQL data types to internal `TypeId` values
/// - **Column Creation**: Converts `ColumnDef` AST nodes to `Column` structs
/// - **Constraint Parsing**: Extracts PRIMARY KEY, NOT NULL, UNIQUE, FOREIGN KEY, etc.
/// - **Schema Construction**: Creates join schemas, aggregation schemas, VALUES schemas
/// - **Value Mapping**: Maps values between source and target schemas
/// - **Compatibility Checking**: Validates schema type compatibility
///
/// # Example
///
/// ```rust,ignore
/// let manager = SchemaManager::new();
///
/// // Convert SQL type
/// let type_id = manager.convert_sql_type(&DataType::Integer(None))?;
///
/// // Create join schema
/// let joined = manager.create_join_schema(&left_schema, &right_schema);
///
/// // Map values to schema
/// let mapped = manager.map_values_to_schema(&values, &source, &target);
/// ```
pub struct SchemaManager {}

impl Default for SchemaManager {
    /// Creates a default `SchemaManager` instance.
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaManager {
    /// Creates a new `SchemaManager` instance.
    ///
    /// The schema manager is stateless and can be reused across multiple operations.
    pub fn new() -> Self {
        Self {}
    }

    /// Creates an output schema for aggregation operations.
    ///
    /// Combines group-by columns with aggregate expression results into a single
    /// output schema. Group-by columns appear first, followed by aggregates.
    ///
    /// # Arguments
    ///
    /// * `group_by_exprs` - Expressions used in the GROUP BY clause
    /// * `agg_exprs` - Aggregate expressions (SUM, COUNT, AVG, etc.)
    /// * `has_group_by` - Whether a GROUP BY clause is present
    ///
    /// # Returns
    ///
    /// A `Schema` containing columns for all group-by and aggregate results.
    pub fn create_aggregation_output_schema(
        &self,
        group_by_exprs: &[&Expression],
        agg_exprs: &[Arc<Expression>],
        has_group_by: bool,
    ) -> Schema {
        self.create_aggregation_output_schema_with_alias_mapping(
            group_by_exprs,
            agg_exprs,
            has_group_by,
            None,
        )
    }

    /// Creates an aggregation output schema with optional column alias mapping.
    ///
    /// Similar to [`create_aggregation_output_schema`](Self::create_aggregation_output_schema),
    /// but allows renaming columns based on SELECT aliases.
    ///
    /// # Arguments
    ///
    /// * `group_by_exprs` - Expressions used in the GROUP BY clause
    /// * `agg_exprs` - Aggregate expressions (SUM, COUNT, AVG, etc.)
    /// * `has_group_by` - Whether a GROUP BY clause is present
    /// * `alias_mapping` - Optional map from original names to alias names
    ///
    /// # Returns
    ///
    /// A `Schema` with columns named according to the alias mapping.
    pub fn create_aggregation_output_schema_with_alias_mapping(
        &self,
        group_by_exprs: &[&Expression],
        agg_exprs: &[Arc<Expression>],
        has_group_by: bool,
        alias_mapping: Option<&std::collections::HashMap<String, String>>,
    ) -> Schema {
        debug!("Creating aggregation schema:");
        debug!("  Group by expressions: {:?}", group_by_exprs);
        debug!("  Aggregate expressions: {:?}", agg_exprs);
        debug!("  Has GROUP BY: {}", has_group_by);

        let mut columns = Vec::new();
        let mut seen_columns = HashSet::new();

        // Add group by columns first if we have them
        if has_group_by {
            for expr in group_by_exprs {
                let original_name = expr.get_return_type().get_name().to_string();
                let col_name = if let Some(mapping) = alias_mapping {
                    mapping
                        .get(&original_name)
                        .cloned()
                        .unwrap_or(original_name.clone())
                } else {
                    original_name
                };

                if seen_columns.insert(col_name.clone()) {
                    let mut column = expr.get_return_type().clone();
                    column.set_name(col_name.as_str().parse().unwrap());
                    columns.push(column);
                }
            }
        }

        // Add aggregate columns
        for agg_expr in agg_exprs {
            match agg_expr.as_ref() {
                Expression::Aggregate(agg) => {
                    // Use the alias if provided, otherwise generate a name
                    let col_name = agg.get_return_type().get_name().to_string();
                    if seen_columns.insert(col_name.clone()) {
                        columns.push(agg.get_return_type().clone());
                    }
                },
                _ => {
                    let col_name = agg_expr.get_return_type().get_name().to_string();
                    if seen_columns.insert(col_name.clone()) {
                        columns.push(agg_expr.get_return_type().clone());
                    }
                },
            }
        }

        Schema::new(columns)
    }

    /// Creates a schema from a VALUES clause by inferring types from the first row.
    ///
    /// Column names are auto-generated as "column1", "column2", etc.
    ///
    /// # Arguments
    ///
    /// * `rows` - The rows from the VALUES clause
    ///
    /// # Returns
    ///
    /// * `Ok(Schema)` - Schema with inferred column types
    /// * `Err` - If the VALUES clause is empty
    ///
    /// # Example
    ///
    /// ```sql
    /// VALUES (1, 'Alice'), (2, 'Bob')
    /// -- Creates schema: [column1: Integer, column2: VarChar]
    /// ```
    pub fn create_values_schema(&self, rows: &[Vec<Expr>]) -> Result<Schema, String> {
        if rows.is_empty() {
            return Err("VALUES clause cannot be empty".to_string());
        }

        let first_row = &rows[0];
        let columns = first_row
            .iter()
            .enumerate()
            .map(|(i, expr)| {
                let type_id = self.infer_expression_type(expr)?;
                Ok(Column::new(&format!("column{}", i + 1), type_id))
            })
            .collect::<Result<Vec<_>, String>>()?;

        Ok(Schema::new(columns))
    }

    /// Converts SQL column definitions from CREATE TABLE into internal `Column` structs.
    ///
    /// Processes each column definition, extracting:
    /// - Column name and data type
    /// - Constraints (PRIMARY KEY, NOT NULL, UNIQUE, FOREIGN KEY, CHECK)
    /// - Type-specific parameters (precision, scale, length)
    /// - Default values
    ///
    /// # Arguments
    ///
    /// * `column_defs` - Parsed column definitions from the SQL AST
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<Column>)` - Vector of converted columns
    /// * `Err` - If a column type is unsupported or constraint is invalid
    pub fn convert_column_defs(&self, column_defs: &[ColumnDef]) -> Result<Vec<Column>, String> {
        let mut columns = Vec::new();

        for col_def in column_defs {
            let column_name = col_def.name.to_string();
            let type_id = self.convert_sql_type(&col_def.data_type)?;

            // Parse column options to extract constraints
            let (
                is_primary_key,
                is_not_null,
                is_unique,
                foreign_key,
                check_constraint,
                default_value,
            ) = self.parse_column_options(&col_def.options)?;

            // Extract type-specific parameters and create column
            let column = self.create_column_from_sql_type_with_constraints(
                &column_name,
                &col_def.data_type,
                type_id,
                is_primary_key,
                is_not_null,
                is_unique,
                foreign_key,
                check_constraint,
                default_value,
            )?;
            columns.push(column);
        }

        Ok(columns)
    }

    /// Parses column options to extract constraint information.
    ///
    /// Processes column-level constraints from a CREATE TABLE statement:
    ///
    /// | Constraint | Extracted As |
    /// |------------|--------------|
    /// | PRIMARY KEY | `is_primary_key=true`, `is_not_null=true` |
    /// | NOT NULL | `is_not_null=true` |
    /// | NULL | `is_not_null=false` |
    /// | UNIQUE | `is_unique=true` |
    /// | REFERENCES | `ForeignKeyConstraint` with actions |
    /// | CHECK | Expression as string |
    /// | DEFAULT | Value expression |
    ///
    /// # Arguments
    ///
    /// * `options` - Column option definitions from the SQL parser
    ///
    /// # Returns
    ///
    /// A tuple of all extracted constraint information, or an error if
    /// a multi-column foreign key is encountered.
    fn parse_column_options(&self, options: &[ColumnOptionDef]) -> ColumnOptionsResult {
        let mut is_primary_key = false;
        let mut is_not_null = false;
        let mut is_unique = false;
        let mut foreign_key = None;
        let mut check_constraint = None;
        let mut default_value = None;

        for option_def in options {
            match &option_def.option {
                ColumnOption::PrimaryKey(_) => {
                    is_primary_key = true;
                    is_not_null = true; // PRIMARY KEY implies NOT NULL
                },
                ColumnOption::Unique(_) => {
                    is_unique = true;
                },
                ColumnOption::NotNull => {
                    is_not_null = true;
                },
                ColumnOption::Null => {
                    is_not_null = false;
                },
                ColumnOption::ForeignKey(fk) => {
                    // Convert ObjectName to table name string
                    let table_name = self.object_name_to_string(&fk.foreign_table);

                    // For now, we only support single-column foreign keys
                    if fk.referred_columns.len() != 1 {
                        return Err(
                            "Multi-column foreign keys are not currently supported".to_string()
                        );
                    }

                    let column_name = fk.referred_columns[0].value.clone();

                    // Convert sqlparser ReferentialAction to our ReferentialAction
                    let on_delete_action = fk
                        .on_delete
                        .as_ref()
                        .map(|action| self.convert_referential_action(action));
                    let on_update_action = fk
                        .on_update
                        .as_ref()
                        .map(|action| self.convert_referential_action(action));

                    foreign_key = Some(ForeignKeyConstraint {
                        referenced_table: table_name,
                        referenced_column: column_name,
                        on_delete: on_delete_action,
                        on_update: on_update_action,
                    });
                },
                ColumnOption::Check(expr) => {
                    // Convert the expression to a string representation
                    check_constraint = Some(format!("{}", expr));
                },
                ColumnOption::Default(expr) => {
                    // Convert the expression to a Value
                    // For now, we'll store it as a string representation
                    default_value = Some(Value::new(format!("{}", expr)));
                },
                _ => {
                    // Ignore other column options for now
                },
            }
        }

        Ok((
            is_primary_key,
            is_not_null,
            is_unique,
            foreign_key,
            check_constraint,
            default_value,
        ))
    }

    /// Converts a parsed `ObjectName` to a string representation.
    ///
    /// Handles qualified names like `schema.table` by converting to their
    /// string representation.
    fn object_name_to_string(&self, obj_name: &ObjectName) -> String {
        obj_name.to_string()
    }

    /// Converts a sqlparser `ReferentialAction` to the internal representation.
    ///
    /// Maps foreign key actions (ON DELETE, ON UPDATE) to the catalog's
    /// `ReferentialAction` enum.
    fn convert_referential_action(
        &self,
        action: &ReferentialAction,
    ) -> crate::catalog::column::ReferentialAction {
        match action {
            ReferentialAction::Restrict => crate::catalog::column::ReferentialAction::Restrict,
            ReferentialAction::Cascade => crate::catalog::column::ReferentialAction::Cascade,
            ReferentialAction::SetNull => crate::catalog::column::ReferentialAction::SetNull,
            ReferentialAction::NoAction => crate::catalog::column::ReferentialAction::NoAction,
            ReferentialAction::SetDefault => crate::catalog::column::ReferentialAction::SetDefault,
        }
    }

    /// Creates a column from SQL DataType with all type parameters and constraints.
    ///
    /// Handles type-specific parameters:
    /// - DECIMAL/NUMERIC: precision and scale
    /// - FLOAT: precision
    /// - VARCHAR/CHAR: length
    /// - BINARY/VARBINARY: length
    /// - ARRAY: default size
    ///
    /// # Arguments
    ///
    /// * `column_name` - Name of the column
    /// * `sql_type` - Original SQL data type for parameter extraction
    /// * `type_id` - Converted internal type ID
    /// * `is_primary_key` - PRIMARY KEY constraint
    /// * `is_not_null` - NOT NULL constraint
    /// * `is_unique` - UNIQUE constraint
    /// * `foreign_key` - Optional foreign key constraint
    /// * `check_constraint` - Optional CHECK expression
    /// * `default_value` - Optional default value
    fn create_column_from_sql_type_with_constraints(
        &self,
        column_name: &str,
        sql_type: &DataType,
        type_id: TypeId,
        is_primary_key: bool,
        is_not_null: bool,
        is_unique: bool,
        foreign_key: Option<ForeignKeyConstraint>,
        check_constraint: Option<String>,
        default_value: Option<Value>,
    ) -> Result<Column, String> {
        match sql_type {
            // Decimal/Numeric types with precision and scale
            DataType::Decimal(exact_info) | DataType::Numeric(exact_info) => {
                let (precision, scale) = self.extract_precision_scale(exact_info)?;
                Ok(Column::from_sql_info(
                    column_name,
                    type_id,
                    None,
                    precision,
                    scale,
                    is_primary_key,
                    is_not_null,
                    is_unique,
                    check_constraint,
                    default_value,
                    foreign_key,
                ))
            },

            // Float types with precision
            DataType::Float(exact_info) => {
                let (precision, _) = self.extract_precision_scale(exact_info)?;
                Ok(Column::from_sql_info(
                    column_name,
                    type_id,
                    None,
                    precision,
                    None,
                    is_primary_key,
                    is_not_null,
                    is_unique,
                    check_constraint,
                    default_value,
                    foreign_key,
                ))
            },

            // Variable length string types - use default lengths for now
            DataType::Varchar(_) | DataType::String(_) => Ok(Column::from_sql_info(
                column_name,
                type_id,
                Some(255),
                None,
                None,
                is_primary_key,
                is_not_null,
                is_unique,
                check_constraint,
                default_value,
                foreign_key,
            )),
            DataType::Char(_) => Ok(Column::from_sql_info(
                column_name,
                type_id,
                Some(1),
                None,
                None,
                is_primary_key,
                is_not_null,
                is_unique,
                check_constraint,
                default_value,
                foreign_key,
            )),

            // Binary types - use default lengths for now
            DataType::Binary(_) | DataType::Varbinary(_) | DataType::Blob(_) => {
                Ok(Column::from_sql_info(
                    column_name,
                    type_id,
                    Some(255),
                    None,
                    None,
                    is_primary_key,
                    is_not_null,
                    is_unique,
                    check_constraint,
                    default_value,
                    foreign_key,
                ))
            },

            // Integer types with optional display width (we ignore display width for now)
            DataType::TinyInt(_)
            | DataType::SmallInt(_)
            | DataType::Int(_)
            | DataType::Integer(_)
            | DataType::BigInt(_) => Ok(Column::from_sql_info(
                column_name,
                type_id,
                None,
                None,
                None,
                is_primary_key,
                is_not_null,
                is_unique,
                check_constraint,
                default_value,
                foreign_key,
            )),

            // Array types
            DataType::Array(_) => {
                // Default array size, could be made configurable
                Ok(Column::from_sql_info(
                    column_name,
                    type_id,
                    Some(1024),
                    None,
                    None,
                    is_primary_key,
                    is_not_null,
                    is_unique,
                    check_constraint,
                    default_value,
                    foreign_key,
                ))
            },

            // All other types use default parameters
            _ => Ok(Column::from_sql_info(
                column_name,
                type_id,
                None,
                None,
                None,
                is_primary_key,
                is_not_null,
                is_unique,
                check_constraint,
                default_value,
                foreign_key,
            )),
        }
    }

    /// Extracts precision and scale from numeric type information.
    ///
    /// Handles three cases:
    /// - `None`: No precision/scale specified
    /// - `Precision(p)`: Only precision specified
    /// - `PrecisionAndScale(p, s)`: Both precision and scale specified
    ///
    /// # Arguments
    ///
    /// * `exact_info` - Numeric precision/scale information from SQL parser
    ///
    /// # Returns
    ///
    /// * `Ok((precision, scale))` - Extracted values as u8 (or None)
    /// * `Err` - If precision > 255, scale out of range, or scale > precision
    fn extract_precision_scale(
        &self,
        exact_info: &ExactNumberInfo,
    ) -> Result<(Option<u8>, Option<u8>), String> {
        match exact_info {
            ExactNumberInfo::None => Ok((None, None)),
            ExactNumberInfo::Precision(precision) => {
                if *precision > u8::MAX as u64 {
                    return Err(format!(
                        "Precision {} is too large (max {})",
                        precision,
                        u8::MAX
                    ));
                }
                Ok((Some(*precision as u8), None))
            },
            ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                if *precision > u8::MAX as u64 {
                    return Err(format!(
                        "Precision {} is too large (max {})",
                        precision,
                        u8::MAX
                    ));
                }
                if *scale < 0 || *scale > u8::MAX as i64 {
                    return Err(format!(
                        "Scale {} is out of valid range (0-{})",
                        scale,
                        u8::MAX
                    ));
                }
                if *scale as u64 > *precision {
                    return Err(format!(
                        "Scale {} cannot be greater than precision {}",
                        scale, precision
                    ));
                }
                Ok((Some(*precision as u8), Some(*scale as u8)))
            },
        }
    }

    /// Checks if two schemas are compatible for data transfer.
    ///
    /// Schemas are compatible if they have the same number of columns and
    /// each corresponding column pair has compatible types. Column names
    /// are ignored in the comparison.
    ///
    /// # Arguments
    ///
    /// * `source` - Source schema to compare
    /// * `target` - Target schema to compare
    ///
    /// # Returns
    ///
    /// `true` if schemas have matching column counts and types.
    pub fn schemas_compatible(&self, source: &Schema, target: &Schema) -> bool {
        if source.get_column_count() != target.get_column_count() {
            return false;
        }

        for i in 0..source.get_column_count() {
            let source_col = source.get_column(i as usize).unwrap();
            let target_col = target.get_column(i as usize).unwrap();

            if !self.types_compatible(source_col.get_type(), target_col.get_type()) {
                return false;
            }
        }

        true
    }

    /// Checks if two types are compatible for assignment/comparison.
    ///
    /// Currently uses strict type matching (same type = compatible).
    /// No implicit type coercion rules are applied.
    ///
    /// # Arguments
    ///
    /// * `source_type` - Source type ID
    /// * `target_type` - Target type ID
    ///
    /// # Returns
    ///
    /// `true` if the types are identical.
    pub fn types_compatible(&self, source_type: TypeId, target_type: TypeId) -> bool {
        // Add your type compatibility rules here
        // For example:
        match (source_type, target_type) {
            // Same types are always compatible
            (a, b) if a == b => true,
            _ => false,
        }
    }

    /// Converts a SQL data type to the internal `TypeId` representation.
    ///
    /// Supports a comprehensive range of SQL types including:
    /// - Boolean types: BOOLEAN, BOOL
    /// - Integer types: TINYINT, SMALLINT, INT, INTEGER, BIGINT, and unsigned variants
    /// - Decimal types: DECIMAL, NUMERIC, FLOAT, REAL, DOUBLE
    /// - String types: VARCHAR, TEXT, CHAR, STRING, NVARCHAR
    /// - Binary types: BINARY, VARBINARY, BLOB, BYTEA
    /// - Date/time types: DATE, TIME, TIMESTAMP, DATETIME, INTERVAL
    /// - Special types: JSON, JSONB, UUID, ENUM, STRUCT, VECTOR
    ///
    /// # Arguments
    ///
    /// * `sql_type` - The SQL data type from the parser
    ///
    /// # Returns
    ///
    /// * `Ok(TypeId)` - The corresponding internal type
    /// * `Err` - For unsupported types (MAP, TUPLE, NESTED, etc.)
    pub fn convert_sql_type(&self, sql_type: &DataType) -> Result<TypeId, String> {
        match sql_type {
            DataType::Boolean | DataType::Bool => Ok(TypeId::Boolean),
            DataType::TinyInt(_) => Ok(TypeId::TinyInt),
            DataType::SmallInt(_) => Ok(TypeId::SmallInt),
            DataType::Int(_) | DataType::Integer(_) => Ok(TypeId::Integer),
            DataType::BigInt(_) => Ok(TypeId::BigInt),
            DataType::Decimal(_) => Ok(TypeId::Decimal),
            DataType::Float(_) => Ok(TypeId::Float),
            DataType::Varchar(_) | DataType::String(_) | DataType::Text | DataType::Char(_) => {
                Ok(TypeId::VarChar)
            },
            DataType::Array(_) => Ok(TypeId::Vector),
            DataType::Timestamp(_, _) => Ok(TypeId::Timestamp),
            // Character types
            DataType::Character(_) => Ok(TypeId::Char),
            DataType::CharacterVarying(_) => Ok(TypeId::VarChar),
            DataType::CharVarying(_) => Ok(TypeId::VarChar),
            DataType::Nvarchar(_) => Ok(TypeId::VarChar),
            DataType::CharacterLargeObject(_) => Ok(TypeId::VarChar),
            DataType::CharLargeObject(_) => Ok(TypeId::VarChar),
            DataType::Clob(_) => Ok(TypeId::VarChar),
            // Binary types
            DataType::Binary(_) => Ok(TypeId::Binary),
            DataType::Varbinary(_) => Ok(TypeId::Binary),
            DataType::Blob(_) => Ok(TypeId::Binary),
            DataType::TinyBlob => Ok(TypeId::Binary),
            DataType::MediumBlob => Ok(TypeId::Binary),
            DataType::LongBlob => Ok(TypeId::Binary),
            DataType::Bytes(_) => Ok(TypeId::Binary),
            // Numeric types
            DataType::Numeric(_) => Ok(TypeId::Decimal),
            DataType::BigNumeric(_) => Ok(TypeId::Decimal),
            DataType::BigDecimal(_) => Ok(TypeId::Decimal),
            DataType::Dec(_) => Ok(TypeId::Decimal),
            DataType::TinyIntUnsigned(_) => Ok(TypeId::TinyInt),
            DataType::UTinyInt => Ok(TypeId::TinyInt),
            DataType::Int2(_) => Ok(TypeId::SmallInt),
            DataType::Int2Unsigned(_) => Ok(TypeId::SmallInt),
            DataType::SmallIntUnsigned(_) => Ok(TypeId::SmallInt),
            DataType::USmallInt => Ok(TypeId::SmallInt),
            DataType::MediumInt(_) => Ok(TypeId::Integer),
            DataType::MediumIntUnsigned(_) => Ok(TypeId::Integer),
            DataType::Int4(_) => Ok(TypeId::Integer),
            DataType::Int8(_) => Ok(TypeId::BigInt),
            DataType::Int16 => Ok(TypeId::SmallInt),
            DataType::Int32 => Ok(TypeId::Integer),
            DataType::Int64 => Ok(TypeId::BigInt),
            DataType::Int128 => Ok(TypeId::BigInt),
            DataType::Int256 => Ok(TypeId::BigInt),
            DataType::IntUnsigned(_) => Ok(TypeId::Integer),
            DataType::Int4Unsigned(_) => Ok(TypeId::Integer),
            DataType::IntegerUnsigned(_) => Ok(TypeId::Integer),
            DataType::HugeInt => Ok(TypeId::BigInt),
            DataType::UHugeInt => Ok(TypeId::BigInt),
            DataType::UInt8 => Ok(TypeId::TinyInt),
            DataType::UInt16 => Ok(TypeId::SmallInt),
            DataType::UInt32 => Ok(TypeId::Integer),
            DataType::UInt64 => Ok(TypeId::BigInt),
            DataType::UInt128 => Ok(TypeId::BigInt),
            DataType::UInt256 => Ok(TypeId::BigInt),
            DataType::BigIntUnsigned(_) => Ok(TypeId::BigInt),
            DataType::UBigInt => Ok(TypeId::BigInt),
            DataType::Int8Unsigned(_) => Ok(TypeId::BigInt),
            DataType::Signed => Ok(TypeId::Integer),
            DataType::SignedInteger => Ok(TypeId::Integer),
            DataType::Unsigned => Ok(TypeId::Integer),
            DataType::UnsignedInteger => Ok(TypeId::Integer),
            DataType::Float4 => Ok(TypeId::Float),
            DataType::Float32 => Ok(TypeId::Float),
            DataType::Float64 => Ok(TypeId::Decimal),
            DataType::Real => Ok(TypeId::Float),
            DataType::Float8 => Ok(TypeId::Decimal),
            DataType::Double(_) => Ok(TypeId::Decimal),
            DataType::DoublePrecision => Ok(TypeId::Decimal),
            // Date/Time types
            DataType::Date => Ok(TypeId::Date),
            DataType::Date32 => Ok(TypeId::Date),
            DataType::Time(_, _) => Ok(TypeId::Time),
            DataType::Datetime(_) => Ok(TypeId::Timestamp),
            DataType::Datetime64(_, _) => Ok(TypeId::Timestamp),
            DataType::TimestampNtz(_) => Ok(TypeId::Timestamp),
            DataType::Interval {
                fields: _,
                precision: _,
            } => Ok(TypeId::Interval),
            // Special types
            DataType::JSON => Ok(TypeId::JSON),
            DataType::JSONB => Ok(TypeId::JSON),
            DataType::Uuid => Ok(TypeId::UUID),
            DataType::Regclass => Err(format!("Unsupported SQL type: {:?}", sql_type)),
            DataType::TinyText => Ok(TypeId::VarChar),
            DataType::MediumText => Ok(TypeId::VarChar),
            DataType::LongText => Ok(TypeId::VarChar),
            DataType::FixedString(_) => Ok(TypeId::Char),
            DataType::Bytea => Ok(TypeId::Binary),
            DataType::Bit(_) => Ok(TypeId::Binary),
            DataType::BitVarying(_) => Ok(TypeId::Binary),
            DataType::VarBit(_) => Ok(TypeId::Binary),
            DataType::Custom(name, _) => {
                // Handle the custom VECTOR type
                if name.to_string().to_uppercase() == "VECTOR" {
                    Ok(TypeId::Vector)
                } else {
                    Err(format!("Unsupported SQL type: {:?}", sql_type))
                }
            },
            DataType::Map(_, _) => Err(format!("Unsupported SQL type: {:?}", sql_type)),
            DataType::Tuple(_) => Err(format!("Unsupported SQL type: {:?}", sql_type)),
            DataType::Nested(_) => Err(format!("Unsupported SQL type: {:?}", sql_type)),
            DataType::Enum(_, _) => Ok(TypeId::Enum),
            DataType::Set(_) => Err(format!("Unsupported SQL type: {:?}", sql_type)),
            DataType::Struct(_, _) => Ok(TypeId::Struct),
            DataType::Union(_) => Err(format!("Unsupported SQL type: {:?}", sql_type)),
            DataType::Nullable(_) => Err(format!("Unsupported SQL type: {:?}", sql_type)),
            DataType::LowCardinality(_) => Err(format!("Unsupported SQL type: {:?}", sql_type)),
            DataType::Unspecified => Err(format!("Unsupported SQL type: {:?}", sql_type)),
            DataType::Trigger => Err(format!("Unsupported SQL type: {:?}", sql_type)),
            DataType::AnyType => Err(format!("Unsupported SQL type: {:?}", sql_type)),
            DataType::GeometricType(_) => Ok(TypeId::Point),
            DataType::Table(_) => Err(format!("Unsupported SQL type: {:?}", sql_type)),
            DataType::NamedTable { .. } => Err(format!("Unsupported SQL type: {:?}", sql_type)),
            DataType::TsVector => Err(format!("Unsupported SQL type: {:?}", sql_type)),
            DataType::TsQuery => Err(format!("Unsupported SQL type: {:?}", sql_type)),
            // Unsigned numeric types
            DataType::DecimalUnsigned(_) => Ok(TypeId::Decimal),
            DataType::DecUnsigned(_) => Ok(TypeId::Decimal),
            DataType::FloatUnsigned(_) => Ok(TypeId::Float),
            DataType::RealUnsigned => Ok(TypeId::Float),
            DataType::DoubleUnsigned(_) => Ok(TypeId::Decimal),
            DataType::DoublePrecisionUnsigned => Ok(TypeId::Decimal),
        }
    }

    /// Infers the result type of a SQL expression.
    ///
    /// Currently handles literal values:
    /// - Numbers → `TypeId::Integer`
    /// - Strings → `TypeId::VarChar`
    /// - Booleans → `TypeId::Boolean`
    /// - NULL → `TypeId::Invalid`
    ///
    /// Complex expressions (binary ops, function calls, etc.) return `TypeId::Invalid`.
    ///
    /// # Arguments
    ///
    /// * `expr` - The SQL expression to analyze
    ///
    /// # Returns
    ///
    /// The inferred type, or an error for unsupported value types.
    pub fn infer_expression_type(&self, expr: &Expr) -> Result<TypeId, String> {
        match expr {
            Expr::Value(value_with_span) => match &value_with_span.value {
                sqlparser::ast::Value::Number(_, _) => Ok(TypeId::Integer),
                sqlparser::ast::Value::SingleQuotedString(_)
                | sqlparser::ast::Value::DoubleQuotedString(_) => Ok(TypeId::VarChar),
                sqlparser::ast::Value::Boolean(_) => Ok(TypeId::Boolean),
                sqlparser::ast::Value::Null => Ok(TypeId::Invalid),
                _ => Err(format!(
                    "Unsupported value type: {:?}",
                    value_with_span.value
                )),
            },
            _ => Ok(TypeId::Invalid), // Default type for complex expressions
        }
    }

    /// Creates a combined schema for a JOIN operation.
    ///
    /// Concatenates all columns from the left schema followed by all columns
    /// from the right schema. Column names are preserved as-is, including
    /// any table prefixes (e.g., "t1.id", "t2.name").
    ///
    /// # Arguments
    ///
    /// * `left_schema` - Schema of the left side of the join
    /// * `right_schema` - Schema of the right side of the join
    ///
    /// # Returns
    ///
    /// A new schema containing all columns from both schemas.
    ///
    /// # Example
    ///
    /// ```text
    /// Left: [id, name]    Right: [user_id, email]
    /// Result: [id, name, user_id, email]
    /// ```
    pub fn create_join_schema(&self, left_schema: &Schema, right_schema: &Schema) -> Schema {
        // Extract table aliases from the schemas
        let left_alias = self.extract_table_alias_from_schema(left_schema);
        let right_alias = self.extract_table_alias_from_schema(right_schema);

        debug!(
            "Creating join schema with aliases: left={:?}, right={:?}",
            left_alias, right_alias
        );

        // Create a new schema that preserves all column names with their original aliases
        let mut merged_columns = Vec::new();

        // Add all columns from the left schema, preserving their original names
        for col in left_schema.get_columns() {
            merged_columns.push(col.clone());
        }

        // Add all columns from the right schema, preserving their original names
        for col in right_schema.get_columns() {
            merged_columns.push(col.clone());
        }

        Schema::new(merged_columns)
    }

    /// Extracts the most common table alias from a schema's column names.
    ///
    /// Analyzes column names for qualified patterns (e.g., "t1.column") and
    /// returns the most frequently used table alias.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema to analyze
    ///
    /// # Returns
    ///
    /// The most common table alias, or `None` if no qualified names found.
    fn extract_table_alias_from_schema(&self, schema: &Schema) -> Option<String> {
        // Create a map to count occurrences of each alias
        let mut alias_counts = std::collections::HashMap::new();

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

    /// Resolves a qualified column reference (e.g., "t1.id") to its position and metadata.
    ///
    /// Supports two table aliases: "t1" for the left schema and "t2" for the right schema.
    /// The returned index accounts for the combined schema position.
    ///
    /// # Arguments
    ///
    /// * `table_alias` - Table qualifier ("t1" or "t2")
    /// * `column_name` - Column name without qualifier
    /// * `left_schema` - Schema for "t1"
    /// * `right_schema` - Schema for "t2"
    ///
    /// # Returns
    ///
    /// * `Ok((index, column))` - Column index in combined schema and column metadata
    /// * `Err` - If column not found or unknown table alias
    pub fn resolve_qualified_column<'a>(
        &self,
        table_alias: &str,
        column_name: &str,
        left_schema: &'a Schema,
        right_schema: &'a Schema,
    ) -> Result<(usize, &'a Column), String> {
        match table_alias {
            "t1" => {
                // Search only in left schema for t1
                for i in 0..left_schema.get_column_count() {
                    let col = left_schema.get_column(i as usize).unwrap();
                    if col.get_name() == column_name {
                        return Ok((i as usize, col));
                    }
                }
                Err(format!(
                    "Column {}.{} not found in left schema",
                    table_alias, column_name
                ))
            },
            "t2" => {
                // Search only in right schema for t2
                for i in 0..right_schema.get_column_count() {
                    let col = right_schema.get_column(i as usize).unwrap();
                    if col.get_name() == column_name {
                        return Ok(((left_schema.get_column_count() + i) as usize, col));
                    }
                }
                Err(format!(
                    "Column {}.{} not found in right schema",
                    table_alias, column_name
                ))
            },
            _ => Err(format!("Unknown table alias: {}", table_alias)),
        }
    }

    /// Creates a mapping from qualified column names to their SELECT aliases.
    ///
    /// Analyzes SELECT items to build a map from original qualified names
    /// (e.g., "e.name") to their aliases (e.g., "employee").
    ///
    /// # Arguments
    ///
    /// * `select_items` - Items from the SELECT clause
    /// * `group_by_exprs` - GROUP BY expressions for matching
    ///
    /// # Returns
    ///
    /// A `HashMap` from qualified names to alias names.
    ///
    /// # Example
    ///
    /// ```sql
    /// SELECT e.name AS employee FROM employees e
    /// -- Creates mapping: {"e.name" => "employee"}
    /// ```
    pub fn create_column_alias_mapping(
        &self,
        select_items: &[sqlparser::ast::SelectItem],
        group_by_exprs: &[&Expression],
    ) -> std::collections::HashMap<String, String> {
        debug!("Creating column alias mapping");
        let mut alias_map = std::collections::HashMap::new();

        for item in select_items {
            match item {
                sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                    // Handle direct aliases like "e.name AS employee"
                    if let Expr::CompoundIdentifier(idents) = expr
                        && idents.len() == 2
                    {
                        let qualified_name = format!("{}.{}", idents[0].value, idents[1].value);
                        alias_map.insert(qualified_name, alias.value.clone());
                    }
                },
                sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                    // For expressions without explicit aliases, check if they match GROUP BY expressions
                    if let Expr::CompoundIdentifier(idents) = expr
                        && idents.len() == 2
                    {
                        let qualified_name = format!("{}.{}", idents[0].value, idents[1].value);

                        // Check if this matches any GROUP BY expression
                        for group_expr in group_by_exprs {
                            if let Expression::ColumnRef(col_ref) = group_expr {
                                let group_col_name = col_ref.get_return_type().get_name();
                                if group_col_name == qualified_name {
                                    // Use a simplified alias based on the column name
                                    let simple_name = idents[1].value.clone();
                                    alias_map.insert(qualified_name, simple_name);
                                    break;
                                }
                            }
                        }
                    }
                },
                _ => {},
            }
        }

        debug!("Created alias mapping: {:?}", alias_map);
        alias_map
    }

    /// Maps values from a source schema to a target schema.
    ///
    /// Supports two mapping strategies:
    ///
    /// 1. **Name-based mapping**: When ALL source column names match target columns,
    ///    values are mapped by column name regardless of position. This is used for
    ///    INSERT statements with explicit column lists.
    ///
    /// 2. **Positional mapping**: When column names don't fully match, values are
    ///    mapped by position (first value → first column, etc.).
    ///
    /// # Features
    ///
    /// - Fills missing columns with NULL values (typed appropriately)
    /// - Performs type casting using `Value::cast_to()`
    /// - Failed casts result in NULL values
    ///
    /// # Arguments
    ///
    /// * `source_values` - Values to map
    /// * `source_schema` - Schema describing the source values
    /// * `target_schema` - Target schema to map into
    ///
    /// # Returns
    ///
    /// A vector of values aligned to the target schema.
    ///
    /// # Example
    ///
    /// ```text
    /// Source: [name="Bob", id=42]     Target: [id, name, email]
    /// Result: [id=42, name="Bob", email=NULL]  (name-based mapping)
    /// ```
    pub fn map_values_to_schema(
        &self,
        source_values: &[Value],
        source_schema: &Schema,
        target_schema: &Schema,
    ) -> Vec<Value> {
        let mut target_values = Vec::with_capacity(target_schema.get_column_count() as usize);

        // First, detect if we should use name-based mapping
        let should_use_name_mapping = self.detect_name_based_mapping(source_schema, target_schema);

        if should_use_name_mapping {
            // Use name-based mapping
            for target_col_idx in 0..target_schema.get_column_count() {
                let target_column = target_schema
                    .get_column(target_col_idx as usize)
                    .expect("Target column should exist");

                // Find matching column in source schema by name
                let mapped_value = self.find_value_by_name(
                    target_column.get_name(),
                    source_values,
                    source_schema,
                    target_column.get_type(),
                );

                target_values.push(mapped_value);
            }
        } else {
            // Use positional mapping
            for target_col_idx in 0..target_schema.get_column_count() {
                let target_column = target_schema
                    .get_column(target_col_idx as usize)
                    .expect("Target column should exist");

                let mapped_value = if (target_col_idx as usize) < source_values.len() {
                    // We have a value at this position - cast it to target type
                    let source_value = &source_values[target_col_idx as usize];
                    self.cast_value_to_type(source_value, target_column.get_type())
                } else {
                    // No value at this position, use NULL
                    Value::new_with_type(
                        crate::types_db::value::Val::Null,
                        target_column.get_type(),
                    )
                };

                target_values.push(mapped_value);
            }
        }

        target_values
    }

    /// Determines whether to use name-based or positional value mapping.
    ///
    /// Name-based mapping is used only when ALL source column names have
    /// exact matches in the target schema. This prevents false positives
    /// from partial matches.
    ///
    /// # Arguments
    ///
    /// * `source_schema` - Schema of source values
    /// * `target_schema` - Target schema to map to
    ///
    /// # Returns
    ///
    /// `true` if all source columns match target columns by name.
    fn detect_name_based_mapping(&self, source_schema: &Schema, target_schema: &Schema) -> bool {
        let source_count = source_schema.get_column_count();

        // If source schema is empty, we can't do name-based mapping
        if source_count == 0 {
            return false;
        }

        let mut matching_count = 0;

        // Count how many source columns have exact matches in target schema
        for source_col_idx in 0..source_count {
            let source_column = source_schema
                .get_column(source_col_idx as usize)
                .expect("Source column should exist");

            // Check if this source column name exists in target schema
            for target_col_idx in 0..target_schema.get_column_count() {
                let target_column = target_schema
                    .get_column(target_col_idx as usize)
                    .expect("Target column should exist");
                if source_column.get_name() == target_column.get_name() {
                    matching_count += 1;
                    break; // Found a match, don't double-count
                }
            }
        }

        // Use name-based mapping if we find matches for ALL source columns
        // This ensures that partial column inserts will correctly map by name
        // when column names are specified, but prevents false positives
        matching_count == source_count && matching_count > 0
    }

    /// Finds and casts a value by column name from the source schema.
    ///
    /// Searches the source schema for a column with the given name and
    /// returns the corresponding value cast to the target type.
    ///
    /// # Arguments
    ///
    /// * `target_column_name` - Name of the column to find
    /// * `source_values` - Values from the source tuple
    /// * `source_schema` - Schema describing the source values
    /// * `target_type` - Type to cast the found value to
    ///
    /// # Returns
    ///
    /// The found value (cast to target type), or a typed NULL if not found.
    fn find_value_by_name(
        &self,
        target_column_name: &str,
        source_values: &[Value],
        source_schema: &Schema,
        target_type: TypeId,
    ) -> Value {
        for source_col_idx in 0..source_schema.get_column_count() {
            let source_column = source_schema
                .get_column(source_col_idx as usize)
                .expect("Source column should exist");
            if source_column.get_name() == target_column_name {
                let source_value = &source_values[source_col_idx as usize];
                return self.cast_value_to_type(source_value, target_type);
            }
        }

        // Column not found, use NULL
        Value::new_with_type(crate::types_db::value::Val::Null, target_type)
    }

    /// Attempts to cast a value to the target type.
    ///
    /// If casting fails (e.g., "abc" to Integer), returns a typed NULL value
    /// instead of propagating the error.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to cast
    /// * `target_type` - The target type ID
    ///
    /// # Returns
    ///
    /// The cast value, or a NULL of the target type if casting fails.
    fn cast_value_to_type(&self, value: &Value, target_type: TypeId) -> Value {
        match value.cast_to(target_type) {
            Ok(casted_value) => casted_value,
            Err(_) => {
                // If casting fails, use NULL
                Value::new_with_type(crate::types_db::value::Val::Null, target_type)
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::execution::expressions::aggregate_expression::{
        AggregateExpression, AggregationType,
    };
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use sqlparser::ast::{ColumnDef, DataType, Ident, StructBracketKind, Value};
    use sqlparser::tokenizer::{Location, Span};

    #[test]
    fn test_convert_sql_types() {
        let manager = SchemaManager::new();

        assert_eq!(
            manager.convert_sql_type(&DataType::Boolean).unwrap(),
            TypeId::Boolean
        );
        assert_eq!(
            manager.convert_sql_type(&DataType::TinyInt(None)).unwrap(),
            TypeId::TinyInt
        );
        assert_eq!(
            manager.convert_sql_type(&DataType::Integer(None)).unwrap(),
            TypeId::Integer
        );
        assert_eq!(
            manager.convert_sql_type(&DataType::BigInt(None)).unwrap(),
            TypeId::BigInt
        );
        assert_eq!(
            manager.convert_sql_type(&DataType::Varchar(None)).unwrap(),
            TypeId::VarChar
        );

        // Test unsupported type
        assert!(manager.convert_sql_type(&DataType::Regclass).is_err());
    }

    #[test]
    fn test_types_compatible() {
        let manager = SchemaManager::new();

        // Same types should be compatible
        assert!(manager.types_compatible(TypeId::Integer, TypeId::Integer));
        assert!(manager.types_compatible(TypeId::VarChar, TypeId::VarChar));

        // Different types should not be compatible
        assert!(!manager.types_compatible(TypeId::Integer, TypeId::VarChar));
        assert!(!manager.types_compatible(TypeId::Boolean, TypeId::Integer));
    }

    #[test]
    fn test_infer_expression_type() {
        let manager = SchemaManager::new();

        // Test number
        let num_expr = Expr::Value(sqlparser::ast::ValueWithSpan {
            value: Value::Number("42".to_string(), false), // Empty span
            span: Span::new(Location::new(0, 0), Location::new(0, 0)),
        });
        assert_eq!(
            manager.infer_expression_type(&num_expr).unwrap(),
            TypeId::Integer
        );

        // Test string
        let str_expr = Expr::Value(sqlparser::ast::ValueWithSpan {
            value: Value::SingleQuotedString("test".to_string()),
            span: Span::new(Location::new(0, 0), Location::new(0, 0)),
        });
        assert_eq!(
            manager.infer_expression_type(&str_expr).unwrap(),
            TypeId::VarChar
        );

        // Test boolean
        let bool_expr = Expr::Value(sqlparser::ast::ValueWithSpan {
            value: Value::Boolean(true),
            span: Span::new(Location::new(0, 0), Location::new(0, 0)),
        });
        assert_eq!(
            manager.infer_expression_type(&bool_expr).unwrap(),
            TypeId::Boolean
        );

        // Test null
        let null_expr = Expr::Value(sqlparser::ast::ValueWithSpan {
            value: Value::Null,
            span: Span::new(Location::new(0, 0), Location::new(0, 0)),
        });
        assert_eq!(
            manager.infer_expression_type(&null_expr).unwrap(),
            TypeId::Invalid
        );
    }

    #[test]
    fn test_infer_expression_type_edge_cases() {
        let manager = SchemaManager::new();

        // Test empty string
        let empty_str = Expr::Value(sqlparser::ast::ValueWithSpan {
            value: Value::SingleQuotedString("".to_string()),
            span: Span::new(Location::new(0, 0), Location::new(0, 0)), // Empty span
        });
        assert_eq!(
            manager.infer_expression_type(&empty_str).unwrap(),
            TypeId::VarChar
        );

        // Test zero
        let zero = Expr::Value(sqlparser::ast::ValueWithSpan {
            value: Value::Number("0".to_string(), false),
            span: Span::new(Location::new(0, 0), Location::new(0, 0)),
        });
        assert_eq!(
            manager.infer_expression_type(&zero).unwrap(),
            TypeId::Integer
        );

        // Test negative number
        let negative = Expr::Value(sqlparser::ast::ValueWithSpan {
            value: Value::Number("-42".to_string(), false),
            span: Span::new(Location::new(0, 0), Location::new(0, 0)),
        });
        assert_eq!(
            manager.infer_expression_type(&negative).unwrap(),
            TypeId::Integer
        );

        // Test complex expression (should return Invalid)
        let complex = Expr::BinaryOp {
            left: Box::new(Expr::Value(sqlparser::ast::ValueWithSpan {
                value: Value::Number("1".to_string(), false),
                span: Span::new(Location::new(0, 0), Location::new(0, 0)),
            })),
            op: sqlparser::ast::BinaryOperator::Plus,
            right: Box::new(Expr::Value(sqlparser::ast::ValueWithSpan {
                value: Value::Number("2".to_string(), false),
                span: Span::new(Location::new(0, 0), Location::new(0, 0)),
            })),
        };
        assert_eq!(
            manager.infer_expression_type(&complex).unwrap(),
            TypeId::Invalid
        );
    }

    #[test]
    fn test_convert_column_defs() {
        let manager = SchemaManager::new();

        let column_defs = vec![
            ColumnDef {
                name: Ident::new("id"),
                data_type: DataType::Integer(None),
                options: vec![],
            },
            ColumnDef {
                name: Ident::new("name"),
                data_type: DataType::Varchar(None),
                options: vec![],
            },
        ];

        let columns = manager.convert_column_defs(&column_defs).unwrap();

        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].get_name(), "id");
        assert_eq!(columns[0].get_type(), TypeId::Integer);
        assert_eq!(columns[1].get_name(), "name");
        assert_eq!(columns[1].get_type(), TypeId::VarChar);
    }

    #[test]
    fn test_schemas_compatible() {
        let manager = SchemaManager::new();

        let schema1 = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let schema2 = Schema::new(vec![
            Column::new("user_id", TypeId::Integer),
            Column::new("user_name", TypeId::VarChar),
        ]);

        // Schemas with same types should be compatible regardless of names
        assert!(manager.schemas_compatible(&schema1, &schema2));

        let schema3 = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("active", TypeId::Boolean),
        ]);

        // Schemas with different types should not be compatible
        assert!(!manager.schemas_compatible(&schema1, &schema3));

        // Schemas with different column counts should not be compatible
        let schema4 = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        assert!(!manager.schemas_compatible(&schema1, &schema4));
    }

    #[test]
    fn test_create_values_schema() {
        let manager = SchemaManager::new();

        let values = vec![vec![
            Expr::Value(sqlparser::ast::ValueWithSpan {
                value: Value::Number("1".to_string(), false),
                span: Span::new(Location::new(0, 0), Location::new(0, 0)),
            }),
            Expr::Value(sqlparser::ast::ValueWithSpan {
                value: Value::SingleQuotedString("test_string".to_string()),
                span: Span::new(Location::new(0, 0), Location::new(0, 0)),
            }),
        ]];

        let schema = manager.create_values_schema(&values).unwrap();

        assert_eq!(schema.get_column_count(), 2);
        assert_eq!(schema.get_column(0).unwrap().get_type(), TypeId::Integer);
        assert_eq!(schema.get_column(1).unwrap().get_type(), TypeId::VarChar);

        // Test empty values
        let empty_values: Vec<Vec<Expr>> = vec![];
        assert!(manager.create_values_schema(&empty_values).is_err());
    }

    #[test]
    fn test_resolve_qualified_column() {
        let manager = SchemaManager::new();

        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let right_schema = Schema::new(vec![
            Column::new("user_id", TypeId::Integer),
            Column::new("email", TypeId::VarChar),
        ]);

        // Test finding column in left schema
        let (idx, col) = manager
            .resolve_qualified_column("t1", "name", &left_schema, &right_schema)
            .unwrap();
        assert_eq!(idx, 1);
        assert_eq!(col.get_name(), "name");

        // Test finding column in right schema
        let (idx, col) = manager
            .resolve_qualified_column("t2", "email", &left_schema, &right_schema)
            .unwrap();
        assert_eq!(idx, 3); // 2 columns in left schema + 1 (0-based index)
        assert_eq!(col.get_name(), "email");

        // Test column not found
        assert!(
            manager
                .resolve_qualified_column("t1", "unknown", &left_schema, &right_schema)
                .is_err()
        );
    }

    #[test]
    fn test_create_join_schema() {
        let manager = SchemaManager::new();

        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let right_schema = Schema::new(vec![
            Column::new("age", TypeId::Integer),
            Column::new("email", TypeId::VarChar),
        ]);

        let joined_schema = manager.create_join_schema(&left_schema, &right_schema);

        assert_eq!(joined_schema.get_column_count(), 4);
        assert_eq!(joined_schema.get_column(0).unwrap().get_name(), "id");
        assert_eq!(joined_schema.get_column(1).unwrap().get_name(), "name");
        assert_eq!(joined_schema.get_column(2).unwrap().get_name(), "age");
        assert_eq!(joined_schema.get_column(3).unwrap().get_name(), "email");
    }

    #[test]
    fn test_create_values_schema_multiple_rows() {
        let manager = SchemaManager::new();

        let values = vec![
            vec![
                Expr::Value(sqlparser::ast::ValueWithSpan {
                    value: Value::Number("1".to_string(), false),
                    span: Span::new(Location::new(0, 0), Location::new(0, 0)),
                }),
                Expr::Value(sqlparser::ast::ValueWithSpan {
                    value: Value::SingleQuotedString("test1".to_string()),
                    span: Span::new(Location::new(0, 0), Location::new(0, 0)),
                }),
            ],
            vec![
                Expr::Value(sqlparser::ast::ValueWithSpan {
                    value: Value::Number("2".to_string(), false),
                    span: Span::new(Location::new(0, 0), Location::new(0, 0)),
                }),
                Expr::Value(sqlparser::ast::ValueWithSpan {
                    value: Value::SingleQuotedString("test2".to_string()),
                    span: Span::new(Location::new(0, 0), Location::new(0, 0)),
                }),
            ],
        ];

        let schema = manager.create_values_schema(&values).unwrap();

        assert_eq!(schema.get_column_count(), 2);
        assert_eq!(schema.get_column(0).unwrap().get_name(), "column1");
        assert_eq!(schema.get_column(1).unwrap().get_name(), "column2");
    }

    #[test]
    fn test_convert_column_defs_with_varchar_length() {
        let manager = SchemaManager::new();

        let column_defs = vec![
            ColumnDef {
                name: Ident::new("name"),
                data_type: DataType::Varchar(None), // Using None for now
                options: vec![],
            },
            ColumnDef {
                name: Ident::new("description"),
                data_type: DataType::String(None),
                options: vec![],
            },
        ];

        let columns = manager.convert_column_defs(&column_defs).unwrap();

        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].get_name(), "name");
        assert_eq!(columns[0].get_type(), TypeId::VarChar);
        assert_eq!(columns[1].get_name(), "description");
        assert_eq!(columns[1].get_type(), TypeId::VarChar);

        // Test with explicit length
        let column_defs_with_length = vec![ColumnDef {
            name: Ident::new("fixed_text"),
            data_type: DataType::Char(None), // Using CHAR type
            options: vec![],
        }];

        let columns = manager
            .convert_column_defs(&column_defs_with_length)
            .unwrap();
        assert_eq!(columns[0].get_type(), TypeId::VarChar); // Should still convert to VARCHAR
    }

    #[test]
    fn test_resolve_qualified_column_duplicate_names() {
        let manager = SchemaManager::new();

        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let right_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer), // Duplicate column name
            Column::new("email", TypeId::VarChar),
        ]);

        // When searching in left schema, should get the left column
        let (idx, col) = manager
            .resolve_qualified_column("t1", "id", &left_schema, &right_schema)
            .unwrap();
        assert_eq!(idx, 0); // First column in left schema
        assert_eq!(col.get_type(), TypeId::Integer);

        // When searching in right schema's "id", should get the right column
        let (idx, col) = manager
            .resolve_qualified_column("t2", "id", &left_schema, &right_schema)
            .unwrap();
        assert_eq!(idx, 2); // Index after all left schema columns (2 columns in left schema)
        assert_eq!(col.get_type(), TypeId::Integer);

        // Test finding a unique column in right schema
        let (idx, col) = manager
            .resolve_qualified_column("t2", "email", &left_schema, &right_schema)
            .unwrap();
        assert_eq!(idx, 3); // Last column in combined schemas
        assert_eq!(col.get_name(), "email");

        // Test column not found
        assert!(
            manager
                .resolve_qualified_column("t1", "unknown", &left_schema, &right_schema)
                .is_err()
        );
    }

    #[test]
    fn test_create_aggregation_output_schema() {
        let manager = SchemaManager::new();

        // Create test columns for group by
        let group_by_col = Column::new("category", TypeId::VarChar);
        let group_by_expr = Expression::ColumnRef(ColumnRefExpression::new(
            0, // tuple_index
            0, // column_index
            group_by_col,
            vec![], // no children
        ));

        // Create test aggregate expression
        let agg_col = Column::new("amount", TypeId::Integer);
        let agg_arg = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            agg_col.clone(),
            vec![],
        )));

        let agg_expr = Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            vec![agg_arg],
            Column::new("sum", TypeId::Integer),
            "SUM".to_string(),
        ));

        let schema = manager.create_aggregation_output_schema(
            &[&group_by_expr],
            &[Arc::new(agg_expr)],
            true,
        );

        assert_eq!(schema.get_column_count(), 2);
        assert_eq!(schema.get_column(0).unwrap().get_name(), "category");
        assert_eq!(schema.get_column(1).unwrap().get_name(), "sum");
    }

    #[test]
    fn test_create_aggregation_output_schema_no_group_by() {
        let manager = SchemaManager::new();

        // Create test aggregate expressions
        let count_col = Column::new("count_all", TypeId::BigInt);
        let count_arg = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            count_col.clone(),
            vec![],
        )));

        let count_expr = Expression::Aggregate(AggregateExpression::new(
            AggregationType::Count,
            vec![count_arg],
            count_col,
            "COUNT".to_string(),
        ));

        let schema = manager.create_aggregation_output_schema(
            &[], // no group by expressions
            &[Arc::new(count_expr)],
            false, // no GROUP BY
        );

        assert_eq!(schema.get_column_count(), 1);
        assert_eq!(schema.get_column(0).unwrap().get_name(), "count_all");
    }

    #[test]
    fn test_create_aggregation_output_schema_multiple_aggregates() {
        let manager = SchemaManager::new();

        // Create test columns for group by
        let group_by_col = Column::new("category", TypeId::VarChar);
        let group_by_expr = Expression::ColumnRef(ColumnRefExpression::new(
            0, // tuple_index
            0, // column_index
            group_by_col,
            vec![], // no children
        ));

        // Create test aggregate expression
        let agg_col = Column::new("amount", TypeId::Integer);
        let agg_arg = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            agg_col.clone(),
            vec![],
        )));

        let agg_expr = Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            vec![agg_arg],
            Column::new("sum", TypeId::Integer),
            "SUM".to_string(),
        ));

        let schema = manager.create_aggregation_output_schema(
            &[&group_by_expr],
            &[Arc::new(agg_expr)],
            true,
        );

        assert_eq!(schema.get_column_count(), 2);
        assert_eq!(schema.get_column(0).unwrap().get_name(), "category");
        assert_eq!(schema.get_column(1).unwrap().get_name(), "sum");
    }

    #[test]
    fn test_create_values_schema_mixed_types() {
        let manager = SchemaManager::new();

        let values = vec![vec![
            Expr::Value(sqlparser::ast::ValueWithSpan {
                value: Value::Number("1".to_string(), false),
                span: Span::new(Location::new(0, 0), Location::new(0, 0)),
            }),
            Expr::Value(sqlparser::ast::ValueWithSpan {
                value: Value::SingleQuotedString("test1".to_string()),
                span: Span::new(Location::new(0, 0), Location::new(0, 0)),
            }),
            Expr::Value(sqlparser::ast::ValueWithSpan {
                value: Value::Boolean(true),
                span: Span::new(Location::new(0, 0), Location::new(0, 0)),
            }),
            Expr::Value(sqlparser::ast::ValueWithSpan {
                value: Value::Null,
                span: Span::new(Location::new(0, 0), Location::new(0, 0)),
            }),
        ]];

        let schema = manager.create_values_schema(&values).unwrap();

        assert_eq!(schema.get_column_count(), 4);
        assert_eq!(schema.get_column(0).unwrap().get_type(), TypeId::Integer);
        assert_eq!(schema.get_column(1).unwrap().get_type(), TypeId::VarChar);
        assert_eq!(schema.get_column(2).unwrap().get_type(), TypeId::Boolean);
        assert_eq!(schema.get_column(3).unwrap().get_type(), TypeId::Invalid);
    }

    #[test]
    fn test_create_join_schema_with_duplicate_names() {
        let manager = SchemaManager::new();

        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("common", TypeId::Integer),
        ]);

        let right_schema = Schema::new(vec![
            Column::new("user_id", TypeId::Integer),
            Column::new("common", TypeId::Integer), // Duplicate name
            Column::new("email", TypeId::VarChar),
        ]);

        let joined_schema = manager.create_join_schema(&left_schema, &right_schema);

        assert_eq!(joined_schema.get_column_count(), 6);
        assert_eq!(joined_schema.get_column(2).unwrap().get_name(), "common");
        assert_eq!(joined_schema.get_column(4).unwrap().get_name(), "common");
    }

    #[test]
    fn test_resolve_qualified_column_case_sensitivity() {
        let manager = SchemaManager::new();

        let left_schema = Schema::new(vec![
            Column::new("ID", TypeId::Integer),
            Column::new("Name", TypeId::VarChar),
        ]);

        let right_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("EMAIL", TypeId::VarChar),
        ]);

        // Test exact matches
        let result = manager.resolve_qualified_column("t1", "ID", &left_schema, &right_schema);
        assert!(result.is_ok());

        // Test case mismatches (should fail)
        let result = manager.resolve_qualified_column("t1", "id", &left_schema, &right_schema);
        assert!(result.is_err());

        let result = manager.resolve_qualified_column("t2", "email", &left_schema, &right_schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_types_compatible_edge_cases() {
        let manager = SchemaManager::new();

        // Test NULL type compatibility
        assert!(manager.types_compatible(TypeId::Invalid, TypeId::Invalid));
        assert!(!manager.types_compatible(TypeId::Invalid, TypeId::Integer));

        // Test CHAR and VARCHAR compatibility
        assert!(manager.types_compatible(TypeId::Char, TypeId::Char));
        // Note: The following might need to be updated based on your type system design
        assert!(!manager.types_compatible(TypeId::Char, TypeId::VarChar));

        // Test numeric type compatibility
        assert!(!manager.types_compatible(TypeId::Integer, TypeId::BigInt));
        assert!(!manager.types_compatible(TypeId::SmallInt, TypeId::Integer));
    }

    #[test]
    fn test_extract_precision_scale() {
        let schema_manager = SchemaManager::new();

        // Test None case
        let result = schema_manager
            .extract_precision_scale(&ExactNumberInfo::None)
            .unwrap();
        assert_eq!(result, (None, None));

        // Test Precision only
        let result = schema_manager
            .extract_precision_scale(&ExactNumberInfo::Precision(10))
            .unwrap();
        assert_eq!(result, (Some(10), None));

        // Test Precision and Scale
        let result = schema_manager
            .extract_precision_scale(&ExactNumberInfo::PrecisionAndScale(15, 5))
            .unwrap();
        assert_eq!(result, (Some(15), Some(5)));

        // Test error cases
        let result = schema_manager.extract_precision_scale(&ExactNumberInfo::Precision(300));
        assert!(result.is_err());

        let result =
            schema_manager.extract_precision_scale(&ExactNumberInfo::PrecisionAndScale(10, 15));
        assert!(result.is_err()); // Scale > Precision
    }

    #[test]
    fn test_create_column_from_sql_type_decimal() {
        let schema_manager = SchemaManager::new();

        // Test DECIMAL with precision and scale
        let decimal_type = DataType::Decimal(ExactNumberInfo::PrecisionAndScale(10, 2));
        let column = schema_manager
            .create_column_from_sql_type_with_constraints(
                "price",
                &decimal_type,
                TypeId::Decimal,
                false,
                false,
                false,
                None,
                None,
                None,
            )
            .unwrap();

        assert_eq!(column.get_name(), "price");
        assert_eq!(column.get_type(), TypeId::Decimal);
        assert_eq!(column.get_precision(), Some(10));
        assert_eq!(column.get_scale(), Some(2));

        // Test DECIMAL with precision only
        let decimal_type = DataType::Decimal(ExactNumberInfo::Precision(8));
        let column = schema_manager
            .create_column_from_sql_type_with_constraints(
                "amount",
                &decimal_type,
                TypeId::Decimal,
                false,
                false,
                false,
                None,
                None,
                None,
            )
            .unwrap();

        assert_eq!(column.get_name(), "amount");
        assert_eq!(column.get_type(), TypeId::Decimal);
        assert_eq!(column.get_precision(), Some(8));
        assert_eq!(column.get_scale(), None);

        // Test DECIMAL with no precision/scale
        let decimal_type = DataType::Decimal(ExactNumberInfo::None);
        let column = schema_manager
            .create_column_from_sql_type_with_constraints(
                "value",
                &decimal_type,
                TypeId::Decimal,
                false,
                false,
                false,
                None,
                None,
                None,
            )
            .unwrap();

        assert_eq!(column.get_name(), "value");
        assert_eq!(column.get_type(), TypeId::Decimal);
        assert_eq!(column.get_precision(), None);
        assert_eq!(column.get_scale(), None);
    }

    #[test]
    fn test_create_column_from_sql_type_varchar() {
        let schema_manager = SchemaManager::new();

        // Test VARCHAR (uses default length for now)
        let varchar_type = DataType::Varchar(None);
        let column = schema_manager
            .create_column_from_sql_type_with_constraints(
                "name",
                &varchar_type,
                TypeId::VarChar,
                false,
                false,
                false,
                None,
                None,
                None,
            )
            .unwrap();

        assert_eq!(column.get_name(), "name");
        assert_eq!(column.get_type(), TypeId::VarChar);
        assert_eq!(column.get_length(), 255); // Default length

        // Test CHAR (uses default length for now)
        let char_type = DataType::Char(None);
        let column = schema_manager
            .create_column_from_sql_type_with_constraints(
                "code",
                &char_type,
                TypeId::Char,
                false,
                false,
                false,
                None,
                None,
                None,
            )
            .unwrap();

        assert_eq!(column.get_name(), "code");
        assert_eq!(column.get_type(), TypeId::Char);
        assert_eq!(column.get_length(), 1); // Default length
    }

    #[test]
    fn test_create_column_from_sql_type_float() {
        let schema_manager = SchemaManager::new();

        // Test FLOAT with precision
        let float_type = DataType::Float(ExactNumberInfo::Precision(7));
        let column = schema_manager
            .create_column_from_sql_type_with_constraints(
                "measurement",
                &float_type,
                TypeId::Float,
                false,
                false,
                false,
                None,
                None,
                None,
            )
            .unwrap();

        assert_eq!(column.get_name(), "measurement");
        assert_eq!(column.get_type(), TypeId::Float);
        assert_eq!(column.get_precision(), Some(7));
        assert_eq!(column.get_scale(), None);

        // Test FLOAT without precision
        let float_type = DataType::Float(ExactNumberInfo::None);
        let column = schema_manager
            .create_column_from_sql_type_with_constraints(
                "ratio",
                &float_type,
                TypeId::Float,
                false,
                false,
                false,
                None,
                None,
                None,
            )
            .unwrap();

        assert_eq!(column.get_name(), "ratio");
        assert_eq!(column.get_type(), TypeId::Float);
        assert_eq!(column.get_precision(), None);
        assert_eq!(column.get_scale(), None);
    }

    #[test]
    fn test_map_values_to_schema_exact_match() {
        let manager = SchemaManager::new();

        // Schemas with exact same size and types
        let source_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let target_schema = Schema::new(vec![
            Column::new("user_id", TypeId::Integer),
            Column::new("user_name", TypeId::VarChar),
        ]);

        let source_values = vec![
            crate::types_db::value::Value::from(42),
            crate::types_db::value::Value::from("Alice"),
        ];

        let mapped = manager.map_values_to_schema(&source_values, &source_schema, &target_schema);

        assert_eq!(mapped.len(), 2);
        assert_eq!(mapped[0], crate::types_db::value::Value::from(42));
        assert_eq!(mapped[1], crate::types_db::value::Value::from("Alice"));
    }

    #[test]
    fn test_map_values_to_schema_name_based_mapping() {
        let manager = SchemaManager::new();

        // Source and target with matching column names
        let source_schema = Schema::new(vec![
            Column::new("name", TypeId::VarChar),
            Column::new("id", TypeId::Integer),
            Column::new("email", TypeId::VarChar),
        ]);

        let target_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("email", TypeId::VarChar),
        ]);

        let source_values = vec![
            crate::types_db::value::Value::from("Bob"), // name
            crate::types_db::value::Value::from(123),   // id
            crate::types_db::value::Value::from("bob@test.com"), // email
        ];

        let mapped = manager.map_values_to_schema(&source_values, &source_schema, &target_schema);

        // Should map by name: id=123, name="Bob", email="bob@test.com"
        assert_eq!(mapped[0], crate::types_db::value::Value::from(123)); // id
        assert_eq!(mapped[1], crate::types_db::value::Value::from("Bob")); // name
        assert_eq!(
            mapped[2],
            crate::types_db::value::Value::from("bob@test.com")
        ); // email
    }

    #[test]
    fn test_map_values_to_schema_type_casting() {
        let manager = SchemaManager::new();

        let source_schema = Schema::new(vec![
            Column::new("number", TypeId::Integer),
            Column::new("text", TypeId::VarChar),
            Column::new("flag", TypeId::Boolean),
        ]);

        let target_schema = Schema::new(vec![
            Column::new("string_num", TypeId::VarChar), // Cast Integer to VarChar
            Column::new("num_from_text", TypeId::Integer), // This will fail casting and become NULL
            Column::new("bool_flag", TypeId::Boolean),
        ]);

        let source_values = vec![
            crate::types_db::value::Value::from(42),
            crate::types_db::value::Value::from("not_a_number"),
            crate::types_db::value::Value::from(true),
        ];

        let mapped = manager.map_values_to_schema(&source_values, &source_schema, &target_schema);

        assert_eq!(mapped[0], crate::types_db::value::Value::from("42")); // Integer cast to VarChar
        assert!(mapped[1].is_null()); // Failed cast should be NULL
        assert_eq!(mapped[2], crate::types_db::value::Value::from(true));
    }

    #[test]
    fn test_detect_name_based_mapping_all_match() {
        let manager = SchemaManager::new();

        let source_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let target_schema = Schema::new(vec![
            Column::new("name", TypeId::VarChar),
            Column::new("id", TypeId::Integer),
            Column::new("extra", TypeId::VarChar),
        ]);

        // All source columns have matches in target
        assert!(manager.detect_name_based_mapping(&source_schema, &target_schema));
    }

    #[test]
    fn test_detect_name_based_mapping_partial_match() {
        let manager = SchemaManager::new();

        let source_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("unknown", TypeId::VarChar),
        ]);

        let target_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // Only some source columns match - should use positional
        assert!(!manager.detect_name_based_mapping(&source_schema, &target_schema));
    }

    #[test]
    fn test_detect_name_based_mapping_no_match() {
        let manager = SchemaManager::new();

        let source_schema = Schema::new(vec![
            Column::new("user_id", TypeId::Integer),
            Column::new("user_name", TypeId::VarChar),
        ]);

        let target_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // No source columns match - should use positional
        assert!(!manager.detect_name_based_mapping(&source_schema, &target_schema));
    }

    #[test]
    fn test_detect_name_based_mapping_empty_source() {
        let manager = SchemaManager::new();

        let source_schema = Schema::new(vec![]);
        let target_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // Empty source should return false
        assert!(!manager.detect_name_based_mapping(&source_schema, &target_schema));
    }

    #[test]
    fn test_find_value_by_name_found() {
        let manager = SchemaManager::new();

        let source_schema = Schema::new(vec![
            Column::new("first", TypeId::VarChar),
            Column::new("second", TypeId::Integer),
            Column::new("third", TypeId::Boolean),
        ]);

        let source_values = vec![
            crate::types_db::value::Value::from("hello"),
            crate::types_db::value::Value::from(42),
            crate::types_db::value::Value::from(true),
        ];

        let result =
            manager.find_value_by_name("second", &source_values, &source_schema, TypeId::Integer);
        assert_eq!(result, crate::types_db::value::Value::from(42));
    }

    #[test]
    fn test_find_value_by_name_not_found() {
        let manager = SchemaManager::new();

        let source_schema = Schema::new(vec![
            Column::new("first", TypeId::VarChar),
            Column::new("second", TypeId::Integer),
        ]);

        let source_values = vec![
            crate::types_db::value::Value::from("hello"),
            crate::types_db::value::Value::from(42),
        ];

        let result =
            manager.find_value_by_name("missing", &source_values, &source_schema, TypeId::VarChar);
        assert!(result.is_null());
        assert_eq!(result.get_type_id(), TypeId::VarChar);
    }

    #[test]
    fn test_cast_value_to_type_success() {
        let manager = SchemaManager::new();

        let int_value = crate::types_db::value::Value::from(42);
        let result = manager.cast_value_to_type(&int_value, TypeId::VarChar);
        assert_eq!(result, crate::types_db::value::Value::from("42"));
    }

    #[test]
    fn test_cast_value_to_type_failure() {
        let manager = SchemaManager::new();

        let text_value = crate::types_db::value::Value::from("not_a_number");
        let result = manager.cast_value_to_type(&text_value, TypeId::Integer);
        assert!(result.is_null());
        assert_eq!(result.get_type_id(), TypeId::Integer);
    }

    #[test]
    fn test_map_values_to_schema_source_larger_than_target() {
        let manager = SchemaManager::new();

        let source_schema = Schema::new(vec![
            Column::new("a", TypeId::Integer),
            Column::new("b", TypeId::VarChar),
            Column::new("c", TypeId::Boolean),
            Column::new("d", TypeId::Decimal),
        ]);

        let target_schema = Schema::new(vec![
            Column::new("x", TypeId::Integer),
            Column::new("y", TypeId::VarChar),
        ]);

        let source_values = vec![
            crate::types_db::value::Value::from(1),
            crate::types_db::value::Value::from("test"),
            crate::types_db::value::Value::from(true),
            crate::types_db::value::Value::from(99.9),
        ];

        let mapped = manager.map_values_to_schema(&source_values, &source_schema, &target_schema);

        // Should only map first 2 values positionally
        assert_eq!(mapped.len(), 2);
        assert_eq!(mapped[0], crate::types_db::value::Value::from(1));
        assert_eq!(mapped[1], crate::types_db::value::Value::from("test"));
    }

    #[test]
    fn test_map_values_to_schema_empty_source() {
        let manager = SchemaManager::new();

        let source_schema = Schema::new(vec![]);
        let target_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let source_values = vec![];

        let mapped = manager.map_values_to_schema(&source_values, &source_schema, &target_schema);

        // Should fill all with NULLs
        assert_eq!(mapped.len(), 2);
        assert!(mapped[0].is_null());
        assert!(mapped[1].is_null());
        assert_eq!(mapped[0].get_type_id(), TypeId::Integer);
        assert_eq!(mapped[1].get_type_id(), TypeId::VarChar);
    }

    #[test]
    fn test_map_values_to_schema_empty_target() {
        let manager = SchemaManager::new();

        let source_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let target_schema = Schema::new(vec![]);

        let source_values = vec![
            crate::types_db::value::Value::from(1),
            crate::types_db::value::Value::from("test"),
        ];

        let mapped = manager.map_values_to_schema(&source_values, &source_schema, &target_schema);

        // Should return empty vector
        assert_eq!(mapped.len(), 0);
    }

    #[test]
    fn test_schemas_compatible_different_counts() {
        let manager = SchemaManager::new();

        let schema1 = Schema::new(vec![Column::new("id", TypeId::Integer)]);

        let schema2 = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        assert!(!manager.schemas_compatible(&schema1, &schema2));
        assert!(!manager.schemas_compatible(&schema2, &schema1));
    }

    #[test]
    fn test_schemas_compatible_same_types_different_names() {
        let manager = SchemaManager::new();

        let schema1 = Schema::new(vec![
            Column::new("user_id", TypeId::Integer),
            Column::new("user_name", TypeId::VarChar),
        ]);

        let schema2 = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // Same types, different names should be compatible
        assert!(manager.schemas_compatible(&schema1, &schema2));
    }

    #[test]
    fn test_schemas_compatible_empty_schemas() {
        let manager = SchemaManager::new();

        let schema1 = Schema::new(vec![]);
        let schema2 = Schema::new(vec![]);

        assert!(manager.schemas_compatible(&schema1, &schema2));
    }

    #[test]
    fn test_create_join_schema_empty_schemas() {
        let manager = SchemaManager::new();

        let left_schema = Schema::new(vec![]);
        let right_schema = Schema::new(vec![]);

        let joined = manager.create_join_schema(&left_schema, &right_schema);
        assert_eq!(joined.get_column_count(), 0);
    }

    #[test]
    fn test_create_join_schema_one_empty() {
        let manager = SchemaManager::new();

        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let right_schema = Schema::new(vec![]);

        let joined = manager.create_join_schema(&left_schema, &right_schema);
        assert_eq!(joined.get_column_count(), 2);
        assert_eq!(joined.get_column(0).unwrap().get_name(), "id");
        assert_eq!(joined.get_column(1).unwrap().get_name(), "name");
    }

    #[test]
    fn test_resolve_qualified_column_invalid_alias() {
        let manager = SchemaManager::new();

        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let right_schema = Schema::new(vec![
            Column::new("age", TypeId::Integer),
            Column::new("email", TypeId::VarChar),
        ]);

        // Test with invalid table alias
        let result = manager.resolve_qualified_column("t3", "id", &left_schema, &right_schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown table alias"));
    }

    #[test]
    fn test_create_aggregation_output_schema_duplicate_columns() {
        let manager = SchemaManager::new();

        // Create expressions that would result in duplicate column names
        let group_by_col = Column::new("category", TypeId::VarChar);
        let group_by_expr =
            Expression::ColumnRef(ColumnRefExpression::new(0, 0, group_by_col, vec![]));

        // Create aggregate with same name as group by
        let agg_col = Column::new("category", TypeId::Integer); // Same name!
        let agg_arg = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            agg_col.clone(),
            vec![],
        )));

        let agg_expr = Expression::Aggregate(AggregateExpression::new(
            AggregationType::Count,
            vec![agg_arg],
            agg_col,
            "COUNT".to_string(),
        ));

        let schema = manager.create_aggregation_output_schema(
            &[&group_by_expr],
            &[Arc::new(agg_expr)],
            true,
        );

        // Should only have one column due to duplicate name handling
        assert_eq!(schema.get_column_count(), 1);
        assert_eq!(schema.get_column(0).unwrap().get_name(), "category");
    }

    #[test]
    fn test_extract_precision_scale_edge_cases() {
        let manager = SchemaManager::new();

        // Test maximum values
        let result = manager
            .extract_precision_scale(&ExactNumberInfo::Precision(255))
            .unwrap();
        assert_eq!(result, (Some(255), None));

        let result = manager
            .extract_precision_scale(&ExactNumberInfo::PrecisionAndScale(255, 255))
            .unwrap();
        assert_eq!(result, (Some(255), Some(255)));

        // Test error: precision too large
        let result = manager.extract_precision_scale(&ExactNumberInfo::Precision(256));
        assert!(result.is_err());

        // Test error: scale too large
        let result = manager.extract_precision_scale(&ExactNumberInfo::PrecisionAndScale(10, 256));
        assert!(result.is_err());
    }

    #[test]
    fn test_convert_sql_type_edge_cases() {
        let manager = SchemaManager::new();

        // Test various unsigned integer types
        assert_eq!(
            manager.convert_sql_type(&DataType::UInt32).unwrap(),
            TypeId::Integer
        );
        assert_eq!(
            manager.convert_sql_type(&DataType::UInt64).unwrap(),
            TypeId::BigInt
        );

        // Test various text types
        assert_eq!(
            manager.convert_sql_type(&DataType::TinyText).unwrap(),
            TypeId::VarChar
        );
        assert_eq!(
            manager.convert_sql_type(&DataType::MediumText).unwrap(),
            TypeId::VarChar
        );
        assert_eq!(
            manager.convert_sql_type(&DataType::LongText).unwrap(),
            TypeId::VarChar
        );

        // Test JSON types
        assert_eq!(
            manager.convert_sql_type(&DataType::JSON).unwrap(),
            TypeId::JSON
        );
        assert_eq!(
            manager.convert_sql_type(&DataType::JSONB).unwrap(),
            TypeId::JSON
        );

        // Test UUID
        assert_eq!(
            manager.convert_sql_type(&DataType::Uuid).unwrap(),
            TypeId::UUID
        );

        // Test custom VECTOR type
        let custom_vector = DataType::Custom(
            ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                Ident::new("VECTOR"),
            )]),
            vec![],
        );
        assert_eq!(
            manager.convert_sql_type(&custom_vector).unwrap(),
            TypeId::Vector
        );

        // Test unknown custom type
        let custom_unknown = DataType::Custom(
            ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                Ident::new("UNKNOWN"),
            )]),
            vec![],
        );
        assert!(manager.convert_sql_type(&custom_unknown).is_err());
    }

    #[test]
    fn test_create_column_from_sql_type_all_types() {
        let manager = SchemaManager::new();

        // Test integer types
        let int_column = manager
            .create_column_from_sql_type_with_constraints(
                "test_int",
                &DataType::Integer(None),
                TypeId::Integer,
                false,
                false,
                false,
                None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(int_column.get_name(), "test_int");
        assert_eq!(int_column.get_type(), TypeId::Integer);
        assert_eq!(int_column.get_precision(), None);
        assert_eq!(int_column.get_scale(), None);

        // Test array type
        let array_column = manager
            .create_column_from_sql_type_with_constraints(
                "test_array",
                &DataType::Array(sqlparser::ast::ArrayElemTypeDef::None),
                TypeId::Vector,
                false,
                false,
                false,
                None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(array_column.get_name(), "test_array");
        assert_eq!(array_column.get_type(), TypeId::Vector);
        assert_eq!(array_column.get_length(), 8192); // Default array size

        // Test binary types
        let binary_column = manager
            .create_column_from_sql_type_with_constraints(
                "test_binary",
                &DataType::Binary(None),
                TypeId::Binary,
                false,
                false,
                false,
                None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(binary_column.get_name(), "test_binary");
        assert_eq!(binary_column.get_type(), TypeId::Binary);
        assert_eq!(binary_column.get_length(), 255); // Default binary length
    }

    #[test]
    fn test_create_values_schema_inconsistent_types() {
        let manager = SchemaManager::new();

        // Test with different expression types that might cause issues
        let values = vec![vec![
            Expr::Value(sqlparser::ast::ValueWithSpan {
                value: Value::Number("1".to_string(), false),
                span: Span::new(Location::new(0, 0), Location::new(0, 0)),
            }),
            Expr::BinaryOp {
                left: Box::new(Expr::Value(sqlparser::ast::ValueWithSpan {
                    value: Value::Number("1".to_string(), false),
                    span: Span::new(Location::new(0, 0), Location::new(0, 0)),
                })),
                op: sqlparser::ast::BinaryOperator::Plus,
                right: Box::new(Expr::Value(sqlparser::ast::ValueWithSpan {
                    value: Value::Number("2".to_string(), false),
                    span: Span::new(Location::new(0, 0), Location::new(0, 0)),
                })),
            },
        ]];

        let schema = manager.create_values_schema(&values).unwrap();
        assert_eq!(schema.get_column_count(), 2);
        assert_eq!(schema.get_column(0).unwrap().get_type(), TypeId::Integer);
        assert_eq!(schema.get_column(1).unwrap().get_type(), TypeId::Invalid); // Complex expression
    }

    #[test]
    fn test_types_compatible_with_invalid_type() {
        let manager = SchemaManager::new();

        // Test Invalid type compatibility
        assert!(manager.types_compatible(TypeId::Invalid, TypeId::Invalid));
        assert!(!manager.types_compatible(TypeId::Invalid, TypeId::Integer));
        assert!(!manager.types_compatible(TypeId::Integer, TypeId::Invalid));

        // Test all basic type self-compatibility
        let types = [
            TypeId::Boolean,
            TypeId::TinyInt,
            TypeId::SmallInt,
            TypeId::Integer,
            TypeId::BigInt,
            TypeId::Decimal,
            TypeId::Float,
            TypeId::VarChar,
            TypeId::Char,
            TypeId::Vector,
            TypeId::Timestamp,
            TypeId::JSON,
        ];

        for type_id in types {
            assert!(
                manager.types_compatible(type_id, type_id),
                "Type {:?} should be compatible with itself",
                type_id
            );
        }
    }

    #[test]
    fn test_map_values_to_schema_mixed_scenarios() {
        let manager = SchemaManager::new();

        // Test scenario: partial name match (only some columns match)
        let source_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),      // matches target
            Column::new("unknown", TypeId::VarChar), // doesn't match target
            Column::new("active", TypeId::Boolean),  // matches target
        ]);

        let target_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("active", TypeId::Boolean),
            Column::new("extra", TypeId::Decimal),
        ]);

        let source_values = vec![
            crate::types_db::value::Value::from(1),
            crate::types_db::value::Value::from("mystery"),
            crate::types_db::value::Value::from(true),
        ];

        let mapped = manager.map_values_to_schema(&source_values, &source_schema, &target_schema);

        // Should use positional mapping since not ALL source columns match
        assert_eq!(mapped.len(), 4);
        assert_eq!(mapped[0], crate::types_db::value::Value::from(1)); // id -> id
        assert_eq!(mapped[1], crate::types_db::value::Value::from("mystery")); // unknown -> name
        assert_eq!(mapped[2], crate::types_db::value::Value::from(true)); // active -> active (but positionally)
        assert!(mapped[3].is_null()); // extra = NULL
    }

    #[test]
    fn test_create_aggregation_output_schema_with_complex_expressions() {
        let manager = SchemaManager::new();

        // Create complex expressions that might have conflicts
        let group_col1 = Column::new("t1.category", TypeId::VarChar);
        let group_expr1 = Expression::ColumnRef(ColumnRefExpression::new(0, 0, group_col1, vec![]));

        let group_col2 = Column::new("t2.region", TypeId::VarChar);
        let group_expr2 = Expression::ColumnRef(ColumnRefExpression::new(0, 1, group_col2, vec![]));

        // Create multiple aggregates
        let sum_col = Column::new("SUM(sales)", TypeId::Decimal);
        let sum_arg = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2,
            Column::new("sales", TypeId::Decimal),
            vec![],
        )));
        let sum_expr = Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            vec![sum_arg],
            sum_col,
            "SUM".to_string(),
        ));

        let count_col = Column::new("COUNT(*)", TypeId::BigInt);
        let count_arg = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            3,
            Column::new("*", TypeId::BigInt),
            vec![],
        )));
        let count_expr = Expression::Aggregate(AggregateExpression::new(
            AggregationType::Count,
            vec![count_arg],
            count_col,
            "COUNT".to_string(),
        ));

        let schema = manager.create_aggregation_output_schema(
            &[&group_expr1, &group_expr2],
            &[Arc::new(sum_expr), Arc::new(count_expr)],
            true,
        );

        assert_eq!(schema.get_column_count(), 4);
        assert_eq!(schema.get_column(0).unwrap().get_name(), "t1.category");
        assert_eq!(schema.get_column(1).unwrap().get_name(), "t2.region");
        assert_eq!(schema.get_column(2).unwrap().get_name(), "SUM(sales)");
        assert_eq!(schema.get_column(3).unwrap().get_name(), "COUNT(*)");
    }

    #[test]
    fn test_map_values_to_schema_with_null_values() {
        let manager = SchemaManager::new();

        let source_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("nullable_text", TypeId::VarChar),
            Column::new("flag", TypeId::Boolean),
        ]);

        let target_schema = Schema::new(vec![
            Column::new("user_id", TypeId::Integer),
            Column::new("description", TypeId::VarChar),
            Column::new("active", TypeId::Boolean),
            Column::new("extra", TypeId::Decimal),
        ]);

        let source_values = vec![
            crate::types_db::value::Value::from(42),
            crate::types_db::value::Value::new_with_type(
                crate::types_db::value::Val::Null,
                TypeId::VarChar,
            ),
            crate::types_db::value::Value::from(false),
        ];

        let mapped = manager.map_values_to_schema(&source_values, &source_schema, &target_schema);

        assert_eq!(mapped.len(), 4);
        assert_eq!(mapped[0], crate::types_db::value::Value::from(42));
        assert!(mapped[1].is_null());
        assert_eq!(mapped[2], crate::types_db::value::Value::from(false));
        assert!(mapped[3].is_null());
    }

    #[test]
    fn test_convert_sql_type_comprehensive_coverage() {
        let manager = SchemaManager::new();

        // Test more data types for comprehensive coverage
        assert_eq!(
            manager.convert_sql_type(&DataType::Date).unwrap(),
            TypeId::Date
        );
        assert_eq!(
            manager
                .convert_sql_type(&DataType::Time(None, sqlparser::ast::TimezoneInfo::None))
                .unwrap(),
            TypeId::Time
        );
        assert_eq!(
            manager
                .convert_sql_type(&DataType::Interval {
                    fields: None,
                    precision: None
                })
                .unwrap(),
            TypeId::Interval
        );
        assert_eq!(
            manager
                .convert_sql_type(&DataType::Enum(vec![], None))
                .unwrap(),
            TypeId::Enum
        );
        assert_eq!(
            manager
                .convert_sql_type(&DataType::Struct(vec![], StructBracketKind::Parentheses))
                .unwrap(),
            TypeId::Struct
        );

        // Test types that should error
        assert!(manager.convert_sql_type(&DataType::Regclass).is_err());
        assert!(manager.convert_sql_type(&DataType::AnyType).is_err());
        assert!(manager.convert_sql_type(&DataType::Trigger).is_err());
        assert!(manager.convert_sql_type(&DataType::Unspecified).is_err());
    }

    #[test]
    fn test_infer_expression_type_comprehensive() {
        let manager = SchemaManager::new();

        // Test double quoted string
        let double_quoted = Expr::Value(sqlparser::ast::ValueWithSpan {
            value: Value::DoubleQuotedString("test".to_string()),
            span: Span::new(Location::new(0, 0), Location::new(0, 0)),
        });
        assert_eq!(
            manager.infer_expression_type(&double_quoted).unwrap(),
            TypeId::VarChar
        );

        // Test boolean false
        let bool_false = Expr::Value(sqlparser::ast::ValueWithSpan {
            value: Value::Boolean(false),
            span: Span::new(Location::new(0, 0), Location::new(0, 0)),
        });
        assert_eq!(
            manager.infer_expression_type(&bool_false).unwrap(),
            TypeId::Boolean
        );

        // Test decimal number
        let decimal = Expr::Value(sqlparser::ast::ValueWithSpan {
            value: Value::Number("3.14".to_string(), false),
            span: Span::new(Location::new(0, 0), Location::new(0, 0)),
        });
        assert_eq!(
            manager.infer_expression_type(&decimal).unwrap(),
            TypeId::Integer
        ); // Still treated as integer

        // Test large number
        let large_num = Expr::Value(sqlparser::ast::ValueWithSpan {
            value: Value::Number("9223372036854775807".to_string(), false),
            span: Span::new(Location::new(0, 0), Location::new(0, 0)),
        });
        assert_eq!(
            manager.infer_expression_type(&large_num).unwrap(),
            TypeId::Integer
        );
    }

    #[test]
    fn test_extract_table_alias_edge_cases() {
        let manager = SchemaManager::new();

        // Test schema with no qualified column names
        let schema_no_alias = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let alias = manager.extract_table_alias_from_schema(&schema_no_alias);
        assert!(alias.is_none());

        // Test schema with mixed qualified and unqualified names
        let schema_mixed = Schema::new(vec![
            Column::new("t1.id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("t1.age", TypeId::Integer),
        ]);
        let alias = manager.extract_table_alias_from_schema(&schema_mixed);
        assert_eq!(alias, Some("t1".to_string()));

        // Test schema with multiple aliases (should return most common)
        let schema_multi = Schema::new(vec![
            Column::new("t1.id", TypeId::Integer),
            Column::new("t2.name", TypeId::VarChar),
            Column::new("t2.age", TypeId::Integer),
            Column::new("t2.email", TypeId::VarChar),
        ]);
        let alias = manager.extract_table_alias_from_schema(&schema_multi);
        assert_eq!(alias, Some("t2".to_string())); // t2 appears 3 times, t1 appears 1 time
    }

    #[test]
    fn test_create_join_schema_with_qualified_names() {
        let manager = SchemaManager::new();

        let left_schema = Schema::new(vec![
            Column::new("t1.id", TypeId::Integer),
            Column::new("t1.name", TypeId::VarChar),
        ]);

        let right_schema = Schema::new(vec![
            Column::new("t2.id", TypeId::Integer),
            Column::new("t2.email", TypeId::VarChar),
        ]);

        let joined = manager.create_join_schema(&left_schema, &right_schema);

        assert_eq!(joined.get_column_count(), 4);
        assert_eq!(joined.get_column(0).unwrap().get_name(), "t1.id");
        assert_eq!(joined.get_column(1).unwrap().get_name(), "t1.name");
        assert_eq!(joined.get_column(2).unwrap().get_name(), "t2.id");
        assert_eq!(joined.get_column(3).unwrap().get_name(), "t2.email");
    }

    #[test]
    fn test_map_values_with_type_coercion_edge_cases() {
        let manager = SchemaManager::new();

        let source_schema = Schema::new(vec![
            Column::new("str_number", TypeId::VarChar),
            Column::new("bool_val", TypeId::Boolean),
            Column::new("null_val", TypeId::VarChar),
        ]);

        let target_schema = Schema::new(vec![
            Column::new("int_from_str", TypeId::Integer),
            Column::new("str_from_bool", TypeId::VarChar),
            Column::new("any_from_null", TypeId::BigInt),
        ]);

        let source_values = vec![
            crate::types_db::value::Value::from("123"),
            crate::types_db::value::Value::from(true),
            crate::types_db::value::Value::new_with_type(
                crate::types_db::value::Val::Null,
                TypeId::VarChar,
            ),
        ];

        let mapped = manager.map_values_to_schema(&source_values, &source_schema, &target_schema);

        assert_eq!(mapped.len(), 3);
        // Note: These assertions depend on the actual casting implementation
        // The string "123" might successfully cast to integer 123, or might fail and become NULL
        // The boolean true might cast to string "true", or might fail
        // NULL values when cast should remain NULL but with the target type

        // Check that we got the right number of values and right types
        assert_eq!(mapped[0].get_type_id(), TypeId::Integer);
        assert_eq!(mapped[1].get_type_id(), TypeId::VarChar);
        assert_eq!(mapped[2].get_type_id(), TypeId::BigInt);
        assert!(mapped[2].is_null()); // NULL should remain NULL
    }

    #[test]
    fn test_schema_compatibility_comprehensive() {
        let manager = SchemaManager::new();

        // Test with all same types
        let schema_ints = Schema::new(vec![
            Column::new("a", TypeId::Integer),
            Column::new("b", TypeId::Integer),
            Column::new("c", TypeId::Integer),
        ]);
        let schema_ints2 = Schema::new(vec![
            Column::new("x", TypeId::Integer),
            Column::new("y", TypeId::Integer),
            Column::new("z", TypeId::Integer),
        ]);
        assert!(manager.schemas_compatible(&schema_ints, &schema_ints2));

        // Test with mixed types that are the same
        let schema_mixed1 = Schema::new(vec![
            Column::new("a", TypeId::Integer),
            Column::new("b", TypeId::VarChar),
            Column::new("c", TypeId::Boolean),
            Column::new("d", TypeId::Decimal),
        ]);
        let schema_mixed2 = Schema::new(vec![
            Column::new("w", TypeId::Integer),
            Column::new("x", TypeId::VarChar),
            Column::new("y", TypeId::Boolean),
            Column::new("z", TypeId::Decimal),
        ]);
        assert!(manager.schemas_compatible(&schema_mixed1, &schema_mixed2));

        // Test with one type different
        let schema_diff = Schema::new(vec![
            Column::new("a", TypeId::Integer),
            Column::new("b", TypeId::VarChar),
            Column::new("c", TypeId::Integer), // Different from boolean above
            Column::new("d", TypeId::Decimal),
        ]);
        assert!(!manager.schemas_compatible(&schema_mixed1, &schema_diff));
    }

    #[test]
    fn test_large_schema_operations() {
        let manager = SchemaManager::new();

        // Create large schemas to test performance and correctness
        let mut large_source_cols = Vec::new();
        let mut large_target_cols = Vec::new();
        let mut source_values = Vec::new();

        for i in 0..50 {
            large_source_cols.push(Column::new(&format!("col_{}", i), TypeId::Integer));
            large_target_cols.push(Column::new(&format!("target_col_{}", i), TypeId::Integer));
            source_values.push(crate::types_db::value::Value::from(i));
        }

        let large_source_schema = Schema::new(large_source_cols);
        let large_target_schema = Schema::new(large_target_cols);

        // Test mapping with large schemas
        let mapped = manager.map_values_to_schema(
            &source_values,
            &large_source_schema,
            &large_target_schema,
        );

        assert_eq!(mapped.len(), 50);
        for (i, v) in mapped.iter().enumerate().take(50) {
            assert_eq!(*v, crate::types_db::value::Value::from(i as i32));
        }

        // Test schema compatibility with large schemas
        let large_compatible_schema = Schema::new(
            (0..50)
                .map(|i| Column::new(&format!("another_col_{}", i), TypeId::Integer))
                .collect(),
        );
        assert!(manager.schemas_compatible(&large_source_schema, &large_compatible_schema));

        // Test join with large schemas
        let joined = manager.create_join_schema(&large_source_schema, &large_target_schema);
        assert_eq!(joined.get_column_count(), 100);
    }
}
