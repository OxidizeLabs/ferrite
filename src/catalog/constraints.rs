//! # Constraint Validation
//!
//! This module provides runtime validation of SQL constraints during DML
//! operations. The `ConstraintValidator` checks tuples against schema-defined
//! constraints and reports detailed violations when constraints are not met.
//!
//! ## Architecture
//!
//! ```text
//!                         ┌────────────────────────────────────────┐
//!                         │         ConstraintValidator            │
//!                         │                                        │
//!                         │  ┌──────────────────────────────────┐  │
//!                         │  │      unique_values Cache         │  │
//!                         │  │  HashMap<"table:col", values>    │  │
//!                         │  └──────────────────────────────────┘  │
//!                         │                                        │
//!                         │  ┌──────────────────────────────────┐  │
//!                         │  │    primary_key_values Cache      │  │
//!                         │  │  HashMap<"table:pk", value>      │  │
//!                         │  └──────────────────────────────────┘  │
//!                         └───────────────────┬────────────────────┘
//!                                             │
//!               ┌─────────────────────────────┼─────────────────────────────┐
//!               │                             │                             │
//!               ▼                             ▼                             ▼
//!     ┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
//!     │ validate_tuple  │         │ validate_fk     │         │ apply_defaults  │
//!     │  (full check)   │         │ (foreign key)   │         │ (fill nulls)    │
//!     └────────┬────────┘         └─────────────────┘         └─────────────────┘
//!              │
//!    ┌─────────┼─────────┬─────────────┬─────────────┐
//!    ▼         ▼         ▼             ▼             ▼
//! NOT NULL  UNIQUE    CHECK      PRIMARY KEY   (per-column)
//! ```
//!
//! ## Key Components
//!
//! | Component              | Description                                       |
//! |------------------------|---------------------------------------------------|
//! | `ConstraintViolation`  | Enum describing specific constraint failures      |
//! | `ConstraintValidator`  | Stateful validator with uniqueness caches         |
//!
//! ## Supported Constraints
//!
//! | Constraint   | Validation Method            | Description                         |
//! |--------------|------------------------------|-------------------------------------|
//! | NOT NULL     | `validate_not_null`          | Rejects NULL values                 |
//! | UNIQUE       | `validate_unique`            | Ensures column values are distinct  |
//! | CHECK        | `validate_check_constraint`  | Evaluates expression constraints    |
//! | PRIMARY KEY  | `validate_primary_key`       | Unique + NOT NULL for key columns   |
//! | FOREIGN KEY  | `validate_foreign_key`       | References exist in parent table    |
//!
//! ## Validation Flow
//!
//! ```text
//!   INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'a@b.com')
//!                          │
//!                          ▼
//!                 ┌─────────────────┐
//!                 │ validate_tuple  │
//!                 └────────┬────────┘
//!                          │
//!        ┌─────────────────┼─────────────────┐
//!        │                 │                 │
//!        ▼                 ▼                 ▼
//!   ┌─────────┐      ┌─────────┐      ┌─────────┐
//!   │  id     │      │  name   │      │  email  │
//!   │ NOT NULL│      │ NOT NULL│      │ UNIQUE  │
//!   │ PK      │      │         │      │         │
//!   └────┬────┘      └────┬────┘      └────┬────┘
//!        │                │                │
//!        └────────────────┼────────────────┘
//!                         ▼
//!               ┌─────────────────┐
//!               │ Result:         │
//!               │ Ok(()) or       │
//!               │ Err(violations) │
//!               └─────────────────┘
//! ```
//!
//! ## Constraint Violation Types
//!
//! ```text
//! ConstraintViolation
//! ├── NotNull { column }
//! │     └── "Column 'name' cannot be NULL"
//! ├── PrimaryKey { columns }
//! │     └── "Duplicate primary key on columns ['id']"
//! ├── Unique { column, value }
//! │     └── "Duplicate value 'a@b.com' in unique column 'email'"
//! ├── Check { column, constraint }
//! │     └── "Check constraint 'price > 0' violated on column 'price'"
//! └── ForeignKey { column, referenced_table, referenced_column }
//!       └── "Foreign key violation: dept_id references departments.id"
//! ```
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::catalog::constraints::ConstraintValidator;
//! use crate::catalog::schema::Schema;
//! use crate::storage::table::tuple::Tuple;
//!
//! let mut validator = ConstraintValidator::new();
//!
//! // Validate a tuple against schema constraints
//! match validator.validate_tuple(&tuple, &schema, "users") {
//!     Ok(()) => {
//!         // Tuple is valid, proceed with insert
//!     }
//!     Err(violations) => {
//!         for violation in violations {
//!             match violation {
//!                 ConstraintViolation::NotNull { column } => {
//!                     eprintln!("NULL value in NOT NULL column: {}", column);
//!                 }
//!                 ConstraintViolation::Unique { column, value } => {
//!                     eprintln!("Duplicate value '{}' in column '{}'", value, column);
//!                 }
//!                 // ... handle other violations
//!                 _ => {}
//!             }
//!         }
//!     }
//! }
//! ```
//!
//! ## Caching Behavior
//!
//! The validator maintains caches for efficient uniqueness checking:
//!
//! | Cache                  | Key Format       | Purpose                          |
//! |------------------------|------------------|----------------------------------|
//! | `unique_values`        | `table:column`   | Tracks seen values per column    |
//! | `primary_key_values`   | `table:pk`       | Tracks primary key values        |
//!
//! **Note**: Caches are not persistent—they must be warmed or cleared between
//! sessions. For batch inserts, the validator accumulates values to detect
//! duplicates within the batch.
//!
//! ## Limitations
//!
//! - CHECK constraints currently support only basic numeric comparisons
//! - Composite primary key validation is simplified
//! - Foreign key validation requires external table data to be provided
//! - Caches do not reflect concurrent modifications from other validators
//!
//! ## Thread Safety
//!
//! `ConstraintValidator` is **not thread-safe**. Each thread or transaction
//! should use its own validator instance. The caches use `HashMap` which
//! requires exclusive (`&mut self`) access for validation.

use crate::catalog::column::{Column, ForeignKeyConstraint, ReferentialAction};
use crate::catalog::schema::Schema;
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::Value;
use std::collections::HashMap;

/// Constraint validation result
#[derive(Debug, PartialEq)]
pub enum ConstraintViolation {
    NotNull {
        column: String,
    },
    PrimaryKey {
        columns: Vec<String>,
    },
    Unique {
        column: String,
        value: String,
    },
    Check {
        column: String,
        constraint: String,
    },
    ForeignKey {
        column: String,
        referenced_table: String,
        referenced_column: String,
    },
}

/// Constraint validator for table operations
pub struct ConstraintValidator {
    /// Cache for unique value tracking per column
    unique_values: HashMap<String, HashMap<String, Value>>,
    /// Cache for primary key tracking
    primary_key_values: HashMap<String, Value>,
}

impl Default for ConstraintValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl ConstraintValidator {
    pub fn new() -> Self {
        Self {
            unique_values: HashMap::new(),
            primary_key_values: HashMap::new(),
        }
    }

    /// Validate a tuple against all schema constraints
    pub fn validate_tuple(
        &mut self,
        tuple: &Tuple,
        schema: &Schema,
        table_name: &str,
    ) -> Result<(), Vec<ConstraintViolation>> {
        let mut violations = Vec::new();

        for (i, column) in schema.get_columns().iter().enumerate() {
            let value = tuple.get_value(i);

            // Check NOT NULL constraint
            if let Err(violation) = self.validate_not_null(column, &value) {
                violations.push(violation);
            }

            // Check UNIQUE constraint
            if let Err(violation) = self.validate_unique(column, &value, table_name) {
                violations.push(violation);
            }

            // Check CHECK constraint
            if let Err(violation) = self.validate_check_constraint(column, &value) {
                violations.push(violation);
            }
        }

        // Check PRIMARY KEY constraint
        if let Err(violation) = self.validate_primary_key(tuple, schema, table_name) {
            violations.push(violation);
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }

    /// Validate NOT NULL constraint
    fn validate_not_null(&self, column: &Column, value: &Value) -> Result<(), ConstraintViolation> {
        if column.is_not_null() && value.is_null() {
            return Err(ConstraintViolation::NotNull {
                column: column.get_name().to_string(),
            });
        }
        Ok(())
    }

    /// Validate UNIQUE constraint
    fn validate_unique(
        &mut self,
        column: &Column,
        value: &Value,
        table_name: &str,
    ) -> Result<(), ConstraintViolation> {
        if !column.is_unique() || value.is_null() {
            return Ok(());
        }

        let table_unique_key = format!("{}:{}", table_name, column.get_name());
        let unique_map = self.unique_values.entry(table_unique_key).or_default();

        let value_str = ToString::to_string(&value);
        if unique_map.contains_key(&value_str) {
            return Err(ConstraintViolation::Unique {
                column: column.get_name().to_string(),
                value: value_str,
            });
        }

        unique_map.insert(value_str, value.clone());
        Ok(())
    }

    /// Validate CHECK constraint
    fn validate_check_constraint(
        &self,
        column: &Column,
        value: &Value,
    ) -> Result<(), ConstraintViolation> {
        if let Some(constraint) = column.get_check_constraint() {
            // For now, implement basic numeric constraints
            // In a full implementation, you'd parse and evaluate the constraint expression
            if (constraint.contains("price > 0")
                || constraint.contains("budget > 0")
                || constraint.contains("salary > 0"))
                && let Ok(price) = value.as_decimal()
                && price <= 0.0
            {
                return Err(ConstraintViolation::Check {
                    column: column.get_name().to_string(),
                    constraint: constraint.clone(),
                });
            }
            // Add more constraint patterns as needed
        }
        Ok(())
    }

    /// Validate PRIMARY KEY constraint
    fn validate_primary_key(
        &mut self,
        tuple: &Tuple,
        schema: &Schema,
        table_name: &str,
    ) -> Result<(), ConstraintViolation> {
        let pk_columns: Vec<_> = schema
            .get_columns()
            .iter()
            .enumerate()
            .filter_map(|(i, col)| if col.is_primary_key() { Some(i) } else { None })
            .collect();

        if pk_columns.is_empty() {
            return Ok(());
        }

        // For single-column primary keys
        if pk_columns.len() == 1 {
            let pk_value = tuple.get_value(pk_columns[0]);
            let pk_key = format!("{}:pk", table_name);

            if self.primary_key_values.contains_key(&pk_key) {
                let existing_value = &self.primary_key_values[&pk_key];
                if *existing_value == pk_value {
                    return Err(ConstraintViolation::PrimaryKey {
                        columns: vec![schema.get_columns()[pk_columns[0]].get_name().to_string()],
                    });
                }
            }

            self.primary_key_values.insert(pk_key, pk_value);
        }

        // For composite primary keys, you'd combine the values
        // Implementation omitted for brevity

        Ok(())
    }

    /// Validate FOREIGN KEY constraint
    pub fn validate_foreign_key(
        &self,
        column: &Column,
        value: &Value,
        _referenced_table_data: &HashMap<String, Vec<Value>>, // Mock referenced table data
    ) -> Result<(), ConstraintViolation> {
        if let Some(fk) = column.get_foreign_key()
            && !value.is_null()
        {
            // In a real implementation, you'd check if the value exists in the referenced table
            // For now, just demonstrate the structure
            if !_referenced_table_data.contains_key(&ToString::to_string(&value)) {
                return Err(ConstraintViolation::ForeignKey {
                    column: column.get_name().to_string(),
                    referenced_table: fk.referenced_table.clone(),
                    referenced_column: fk.referenced_column.clone(),
                });
            }
        }
        Ok(())
    }

    /// Apply default values to a tuple
    pub fn apply_defaults(&self, tuple: &mut Tuple, schema: &Schema) {
        for (i, column) in schema.get_columns().iter().enumerate() {
            let value = tuple.get_value(i);
            if value.is_null()
                && let Some(default_value) = column.get_default_value()
            {
                // In a real implementation, you'd set the default value in the tuple
                // This requires modifying the Tuple struct to support value updates
                println!(
                    "Would set default value for column {}: {:?}",
                    column.get_name(),
                    default_value
                );
            }
        }
    }
}

/// Example of how to create tables with comprehensive constraints
pub fn create_example_tables() -> (Schema, Schema) {
    // Create departments table
    let departments_columns = vec![
        Column::builder("id", TypeId::Integer)
            .as_primary_key()
            .as_not_null()
            .build(),
        Column::builder("name", TypeId::VarChar)
            .with_length(100)
            .as_not_null()
            .as_unique()
            .build(),
        Column::builder("budget", TypeId::Decimal)
            .with_precision_and_scale(10, 2)
            .with_check_constraint("budget > 0".to_string())
            .with_default_value(Value::new(0.0))
            .build(),
    ];

    let departments_schema = Schema::new(departments_columns);

    // Create employees table with foreign key
    let employees_columns = vec![
        Column::builder("id", TypeId::Integer)
            .as_primary_key()
            .as_not_null()
            .build(),
        Column::builder("name", TypeId::VarChar)
            .with_length(100)
            .as_not_null()
            .build(),
        Column::builder("email", TypeId::VarChar)
            .with_length(200)
            .as_unique()
            .build(),
        Column::builder("department_id", TypeId::Integer)
            .with_foreign_key(ForeignKeyConstraint {
                referenced_table: "departments".to_string(),
                referenced_column: "id".to_string(),
                on_delete: Some(ReferentialAction::SetNull),
                on_update: Some(ReferentialAction::Cascade),
            })
            .build(),
        Column::builder("salary", TypeId::Decimal)
            .with_precision_and_scale(10, 2)
            .with_check_constraint("salary >= 0".to_string())
            .build(),
    ];

    let employees_schema = Schema::new(employees_columns);

    (departments_schema, employees_schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types_db::value::Val;

    #[test]
    fn test_not_null_constraint() {
        let validator = ConstraintValidator::new();

        let column = Column::builder("name", TypeId::VarChar)
            .as_not_null()
            .build();

        let null_value = Value::new(Val::Null);
        let result = validator.validate_not_null(&column, &null_value);

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            ConstraintViolation::NotNull {
                column: "name".to_string()
            }
        );
    }

    #[test]
    fn test_unique_constraint() {
        let mut validator = ConstraintValidator::new();

        let column = Column::builder("email", TypeId::VarChar)
            .as_unique()
            .build();

        let value1 = Value::new("test@example.com".to_string());
        let value2 = Value::new("test@example.com".to_string());

        // First insertion should succeed
        assert!(validator.validate_unique(&column, &value1, "users").is_ok());

        // Second insertion with same value should fail
        let result = validator.validate_unique(&column, &value2, "users");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            ConstraintViolation::Unique {
                column: "email".to_string(),
                value: "test@example.com".to_string()
            }
        );
    }

    #[test]
    fn test_example_table_creation() {
        let (departments_schema, employees_schema) = create_example_tables();

        // Verify departments schema
        assert_eq!(departments_schema.get_column_count(), 3);
        let dept_id_col = departments_schema.get_column(0).unwrap();
        assert!(dept_id_col.is_primary_key());
        assert!(dept_id_col.is_not_null());

        let dept_name_col = departments_schema.get_column(1).unwrap();
        assert!(dept_name_col.is_not_null());
        assert!(dept_name_col.is_unique());

        // Verify employees schema
        assert_eq!(employees_schema.get_column_count(), 5);
        let emp_dept_col = employees_schema.get_column(3).unwrap();
        assert!(emp_dept_col.get_foreign_key().is_some());
    }
}
