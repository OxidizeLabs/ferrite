//! # Schema Definition
//!
//! This module provides the `Schema` type representing the structure of a
//! database table. A schema is an ordered collection of columns with computed
//! storage layout information used by the tuple storage layer.
//!
//! ## Architecture
//!
//! ```text
//!                        ┌─────────────────────────────────────────────┐
//!                        │                  Schema                     │
//!                        │                                             │
//!                        │  ┌───────────────────────────────────────┐  │
//!                        │  │          columns: Vec<Column>         │  │
//!                        │  │  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐      │  │
//!                        │  │  │ id  │ │name │ │ age │ │email│ ...  │  │
//!                        │  │  │off:0│ │off:4│ │off:12│off:16│      │  │
//!                        │  │  └─────┘ └─────┘ └─────┘ └─────┘      │  │
//!                        │  └───────────────────────────────────────┘  │
//!                        │                                             │
//!                        │  ┌───────────────────────────────────────┐  │
//!                        │  │         Storage Metadata              │  │
//!                        │  │  • length: 24 (total inlined bytes)   │  │
//!                        │  │  • tuple_is_inlined: false            │  │
//!                        │  │  • unlined_columns: [1, 3]            │  │
//!                        │  │  • primary_key_columns: [0]           │  │
//!                        │  └───────────────────────────────────────┘  │
//!                        └─────────────────────────────────────────────┘
//! ```
//!
//! ## Storage Layout
//!
//! During construction, the schema calculates byte offsets for each column:
//!
//! ```text
//!   Tuple Memory Layout:
//!   ┌────────┬────────────┬────────┬────────────┐
//!   │  id    │  name_ptr  │  age   │ email_ptr  │
//!   │ 4 bytes│  8 bytes   │ 4 bytes│  8 bytes   │
//!   │ off: 0 │  off: 4    │ off: 12│  off: 16   │
//!   └────────┴────────────┴────────┴────────────┘
//!         ▲                              ▲
//!         │                              │
//!      Inlined                    Non-inlined (pointer)
//!      (Integer)                     (VarChar)
//! ```
//!
//! - **Inlined columns**: Fixed-size types stored directly in the tuple
//! - **Non-inlined columns**: Variable-length types stored as pointers
//!
//! ## Key Operations
//!
//! | Method                       | Description                                |
//! |------------------------------|--------------------------------------------|
//! | `new(columns)`               | Create schema, compute offsets             |
//! | `copy_schema(from, attrs)`   | Create schema from subset of columns       |
//! | `get_column_index(name)`     | Lookup column index by name                |
//! | `get_qualified_column_index` | Lookup with `table.column` syntax support  |
//! | `merge(left, right)`         | Concatenate two schemas                    |
//! | `merge_with_aliases`         | Merge with table alias prefixes            |
//!
//! ## Column Name Resolution
//!
//! The schema supports both simple and qualified column names:
//!
//! ```text
//!   get_qualified_column_index("users.id")
//!                    │
//!       ┌────────────┴────────────┐
//!       ▼                         ▼
//!   Try exact match         Parse as table.column
//!   "users.id"                    │
//!       │                    ┌────┴────┐
//!       │                    ▼         ▼
//!       │              table="users"  col="id"
//!       │                    │
//!       ▼                    ▼
//!   Not found?         Match "users.id" in schema
//!       │                    │
//!       ▼                    ▼
//!   Try unqualified    Or match just "id" if no dots
//!   fallback
//! ```
//!
//! ## Schema Merging (Joins)
//!
//! ```text
//!   left_schema: [id, name]     right_schema: [dept_id, salary]
//!        │                              │
//!        └──────────┬───────────────────┘
//!                   ▼
//!          merge(&left, &right)
//!                   │
//!                   ▼
//!   merged_schema: [id, name, dept_id, salary]
//!
//!   With aliases:
//!          merge_with_aliases(&left, &right, Some("u"), Some("e"))
//!                   │
//!                   ▼
//!   merged_schema: [u.id, u.name, e.dept_id, e.salary]
//! ```
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::catalog::schema::Schema;
//! use crate::catalog::column::Column;
//! use crate::types_db::type_id::TypeId;
//!
//! // Create a schema
//! let columns = vec![
//!     Column::new("id", TypeId::Integer),
//!     Column::new("name", TypeId::VarChar),
//!     Column::new("age", TypeId::Integer),
//! ];
//! let mut schema = Schema::new(columns);
//!
//! // Set primary key
//! schema.set_primary_key_columns(vec![0]);
//!
//! // Query schema properties
//! assert_eq!(schema.get_column_count(), 3);
//! assert_eq!(schema.get_column_index("name"), Some(1));
//! assert!(!schema.is_inlined()); // VarChar is non-inlined
//!
//! // Copy subset of columns
//! let projection = Schema::copy_schema(&schema, &[0, 2]); // id, age only
//!
//! // Merge for joins
//! let other = Schema::new(vec![Column::new("dept", TypeId::VarChar)]);
//! let joined = Schema::merge(&schema, &other);
//! ```
//!
//! ## Serialization
//!
//! `Schema` implements `bincode::Encode` and `bincode::Decode` for efficient
//! binary serialization, used when persisting schema metadata to the system
//! catalog.
//!
//! ## Thread Safety
//!
//! `Schema` is `Send + Sync`. It is typically constructed once and then shared
//! immutably. Mutable operations like `set_primary_key_columns` are used during
//! schema construction phases.

use std::fmt;
use std::fmt::{Display, Formatter};
use std::mem::size_of;

use bincode::{Decode, Encode};

use crate::catalog::column::Column;

/// Represents the structure of a database table.
///
/// A schema is an ordered collection of columns with computed storage layout
/// information. During construction, byte offsets are calculated for each
/// column based on their storage sizes.
///
/// # Storage Layout
/// - **Inlined columns**: Fixed-size types stored directly in the tuple
/// - **Non-inlined columns**: Variable-length types stored as pointers
///
/// # Example
/// ```rust,ignore
/// let columns = vec![
///     Column::new("id", TypeId::Integer),
///     Column::new("name", TypeId::VarChar),
/// ];
/// let schema = Schema::new(columns);
/// ```
#[derive(Debug, Clone, Encode, Decode)]
pub struct Schema {
    /// The ordered list of columns in this schema.
    columns: Vec<Column>,
    /// Total storage size in bytes for inlined data.
    length: u32,
    /// Whether all columns are stored inline (no variable-length types).
    tuple_is_inlined: bool,
    /// Indices of columns that are not inlined (e.g., VarChar).
    unlined_columns: Vec<u32>,
    /// Indices of columns that form the primary key.
    primary_key_columns: Vec<usize>,
}

impl Schema {
    /// Creates a new schema from a vector of columns.
    ///
    /// This constructor processes each column to compute byte offsets and
    /// identifies which columns require non-inlined storage (e.g., VarChar).
    ///
    /// # Parameters
    /// - `columns`: A vector of [`Column`] definitions for the schema.
    ///
    /// # Returns
    /// A new `Schema` with computed storage layout metadata.
    ///
    /// # Storage Layout Computation
    /// - Each column's offset is set based on cumulative storage sizes
    /// - Inlined columns use their actual storage size
    /// - Non-inlined columns reserve `size_of::<usize>()` bytes for a pointer
    pub fn new(columns: Vec<Column>) -> Schema {
        let mut curr_offset = 0;
        let mut tuple_is_inlined = true;
        let mut uninlined_columns: Vec<u32> = Vec::new();
        let mut columns_processed = Vec::new();

        for (index, mut column) in columns.into_iter().enumerate() {
            if !column.is_inlined() {
                tuple_is_inlined = false;
                uninlined_columns.push(index as u32);
            }
            column.set_offset(curr_offset);
            if column.is_inlined() {
                curr_offset += column.get_storage_size();
            } else {
                curr_offset += size_of::<usize>();
            }

            columns_processed.push(column);
        }

        Schema {
            tuple_is_inlined,
            columns: columns_processed,
            length: curr_offset as u32,
            unlined_columns: uninlined_columns,
            primary_key_columns: Vec::new(),
        }
    }

    /// Creates a new schema by copying specified columns from another schema.
    ///
    /// This is useful for creating projection schemas that contain only a
    /// subset of columns from the original table schema.
    ///
    /// # Parameters
    /// - `from`: The source schema to copy columns from.
    /// - `attrs`: Column indices to include in the new schema.
    ///
    /// # Returns
    /// A new `Schema` containing clones of the specified columns.
    ///
    /// # Panics
    /// Panics if any index in `attrs` is out of bounds for the source schema.
    ///
    /// # Note
    /// The new schema preserves the original's storage metadata (length,
    /// inlined status, primary key columns) rather than recomputing them.
    pub fn copy_schema(from: &Schema, attrs: &[usize]) -> Schema {
        let columns: Vec<Column> = attrs.iter().map(|&i| from.columns[i].clone()).collect();
        Schema {
            columns,
            length: from.length,
            tuple_is_inlined: from.tuple_is_inlined,
            unlined_columns: from.unlined_columns.clone(),
            primary_key_columns: from.primary_key_columns.clone(),
        }
    }

    /// Returns a reference to all columns in the schema.
    ///
    /// Use this for iterating over columns or accessing multiple columns.
    /// For single column access, prefer [`get_column`](Self::get_column).
    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    /// Returns a mutable reference to all columns in the schema.
    ///
    /// Use this when modifying column properties (e.g., setting aliases
    /// during query planning).
    ///
    /// # Warning
    /// Modifying columns after schema construction may invalidate cached
    /// storage layout metadata (offsets, inlined status).
    pub fn get_columns_mut(&mut self) -> &mut Vec<Column> {
        &mut self.columns
    }

    /// Returns a reference to the column at the specified index.
    ///
    /// # Parameters
    /// - `column_index`: Zero-based index of the column.
    ///
    /// # Returns
    /// `Some(&Column)` if the index is valid, `None` otherwise.
    pub fn get_column(&self, column_index: usize) -> Option<&Column> {
        self.columns.get(column_index)
    }

    /// Returns the index of a column by its exact name.
    ///
    /// Performs a case-sensitive linear search through the columns.
    /// For qualified name lookup (e.g., `table.column`), use
    /// [`get_qualified_column_index`](Self::get_qualified_column_index).
    ///
    /// # Parameters
    /// - `column_name`: The exact column name to search for.
    ///
    /// # Returns
    /// `Some(index)` if found, `None` otherwise.
    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        for (index, column) in self.columns.iter().enumerate() {
            if column.get_name() == column_name {
                return Some(index);
            }
        }
        None
    }

    /// Returns the index of a column by qualified name (`table.column` format).
    ///
    /// This method implements flexible column resolution for SQL queries,
    /// handling both qualified and unqualified column references.
    ///
    /// # Resolution Strategy
    /// 1. **Exact match**: Try to find a column with the exact name
    /// 2. **Qualified input** (`table.column`):
    ///    - Search for columns matching `table.column` pattern
    ///    - Fall back to matching just the column part (unqualified)
    /// 3. **Unqualified input**:
    ///    - Search qualified columns for matching column part
    ///
    /// # Parameters
    /// - `column_name`: Column name, optionally qualified with table alias.
    ///
    /// # Returns
    /// `Some(index)` if a matching column is found, `None` otherwise.
    ///
    /// # Examples
    /// ```rust,ignore
    /// // Given schema with columns: ["users.id", "users.name", "age"]
    /// schema.get_qualified_column_index("users.id");  // Some(0)
    /// schema.get_qualified_column_index("id");        // Some(0) - matches users.id
    /// schema.get_qualified_column_index("age");       // Some(2) - exact match
    /// ```
    pub fn get_qualified_column_index(&self, column_name: &str) -> Option<usize> {
        // First try exact match (for already qualified names)
        if let Some(idx) = self.get_column_index(column_name) {
            return Some(idx);
        }

        // If the column name contains a dot (table.column format)
        if column_name.contains('.') {
            // Split the qualified name into parts
            let parts: Vec<&str> = column_name.split('.').collect();
            if parts.len() == 2 {
                let table_alias = parts[0];
                let col_name = parts[1];

                // Try to find a column with this qualified name pattern
                for (index, column) in self.columns.iter().enumerate() {
                    let col_name_parts: Vec<&str> = column.get_name().split('.').collect();
                    if col_name_parts.len() == 2
                        && col_name_parts[0] == table_alias
                        && col_name_parts[1] == col_name
                    {
                        return Some(index);
                    }
                }

                // If not found with qualification, try to find the unqualified column name
                // This helps when the schema has unqualified names but we're using qualified references
                for (index, column) in self.columns.iter().enumerate() {
                    if column.get_name() == col_name {
                        return Some(index);
                    }
                }
            }
        } else {
            // If it's an unqualified name, try to find it in any qualified column
            for (index, column) in self.columns.iter().enumerate() {
                let col_name_parts: Vec<&str> = column.get_name().split('.').collect();
                if col_name_parts.len() == 2 && col_name_parts[1] == column_name {
                    return Some(index);
                }
            }
        }

        None
    }

    /// Returns a reference to the indices of non-inlined columns.
    ///
    /// Non-inlined columns (e.g., `VarChar`) store pointers in the tuple
    /// rather than the actual data. This list is used by the tuple storage
    /// layer for proper serialization.
    pub fn get_unlined_columns(&self) -> &Vec<u32> {
        &self.unlined_columns
    }

    /// Returns the count of non-inlined (variable-length) columns.
    pub fn get_unlined_column_count(&self) -> u32 {
        self.unlined_columns.len() as u32
    }

    /// Returns the total number of columns in the schema.
    pub fn get_column_count(&self) -> u32 {
        self.columns.len() as u32
    }

    /// Returns the total storage size in bytes for inlined tuple data.
    ///
    /// This represents the fixed-size portion of a tuple that is stored
    /// directly in the page. Variable-length columns contribute only their
    /// pointer size (`size_of::<usize>()`) to this value.
    pub fn get_inlined_storage_size(&self) -> u32 {
        self.length
    }

    /// Returns whether all columns are stored inline.
    ///
    /// A schema is fully inlined if it contains no variable-length types
    /// (e.g., `VarChar`). Fully inlined tuples are simpler to process
    /// and have predictable sizes.
    pub fn is_inlined(&self) -> bool {
        self.tuple_is_inlined
    }

    /// Merges two schemas into a new schema by concatenating their columns.
    ///
    /// This is used during join operations to create the output schema.
    /// Columns from the left schema appear first, followed by right schema columns.
    ///
    /// # Parameters
    /// - `left`: The left (outer) schema.
    /// - `right`: The right (inner) schema.
    ///
    /// # Returns
    /// A new `Schema` containing all columns from both inputs with
    /// recomputed storage layout.
    ///
    /// # Example
    /// ```rust,ignore
    /// let users = Schema::new(vec![Column::new("id", TypeId::Integer)]);
    /// let orders = Schema::new(vec![Column::new("total", TypeId::Decimal)]);
    /// let joined = Schema::merge(&users, &orders);
    /// // joined contains: [id, total]
    /// ```
    pub fn merge(left: &Schema, right: &Schema) -> Schema {
        let mut merged_columns = left.get_columns().clone();
        merged_columns.extend(right.get_columns().iter().cloned());
        Schema::new(merged_columns)
    }

    /// Merges two schemas with optional table aliases for column qualification.
    ///
    /// Similar to [`merge`](Self::merge), but prefixes column names with table
    /// aliases to avoid ambiguity in join results. This is essential when both
    /// schemas contain columns with the same name.
    ///
    /// # Parameters
    /// - `left`: The left (outer) schema.
    /// - `right`: The right (inner) schema.
    /// - `left_alias`: Optional alias prefix for left columns (e.g., `"u"` → `"u.id"`).
    /// - `right_alias`: Optional alias prefix for right columns.
    ///
    /// # Returns
    /// A new `Schema` with aliased column names and recomputed storage layout.
    ///
    /// # Alias Behavior
    /// - If a column already has a qualified name and the alias differs, it is updated
    /// - If a column already has the same alias, it is left unchanged
    /// - If no alias is provided, columns are copied as-is
    ///
    /// # Example
    /// ```rust,ignore
    /// let users = Schema::new(vec![Column::new("id", TypeId::Integer)]);
    /// let orders = Schema::new(vec![Column::new("id", TypeId::Integer)]);
    /// let joined = Schema::merge_with_aliases(&users, &orders, Some("u"), Some("o"));
    /// // joined contains: [u.id, o.id]
    /// ```
    pub fn merge_with_aliases(
        left: &Schema,
        right: &Schema,
        left_alias: Option<&str>,
        right_alias: Option<&str>,
    ) -> Schema {
        let mut merged_columns = Vec::new();

        // Add left columns with alias if provided
        for col in left.get_columns() {
            if let Some(alias) = left_alias {
                // Only add alias if the column name doesn't already have one
                if !col.get_name().contains('.') {
                    let mut new_col = col.clone();
                    new_col.set_name(format!("{}.{}", alias, col.get_name()));
                    merged_columns.push(new_col);
                } else {
                    // If the column already has an alias but it's different, update it
                    let parts: Vec<&str> = col.get_name().split('.').collect();
                    if parts.len() == 2 && parts[0] != alias {
                        let mut new_col = col.clone();
                        new_col.set_name(format!("{}.{}", alias, parts[1]));
                        merged_columns.push(new_col);
                    } else {
                        merged_columns.push(col.clone());
                    }
                }
            } else {
                merged_columns.push(col.clone());
            }
        }

        // Add right columns with alias if provided
        for col in right.get_columns() {
            if let Some(alias) = right_alias {
                // Only add alias if the column name doesn't already have one
                if !col.get_name().contains('.') {
                    let mut new_col = col.clone();
                    new_col.set_name(format!("{}.{}", alias, col.get_name()));
                    merged_columns.push(new_col);
                } else {
                    // If the column already has an alias but it's different, update it
                    let parts: Vec<&str> = col.get_name().split('.').collect();
                    if parts.len() == 2 && parts[0] != alias {
                        let mut new_col = col.clone();
                        new_col.set_name(format!("{}.{}", alias, parts[1]));
                        merged_columns.push(new_col);
                    } else {
                        merged_columns.push(col.clone());
                    }
                }
            } else {
                merged_columns.push(col.clone());
            }
        }

        Schema::new(merged_columns)
    }

    /// Returns the indices of columns that form the primary key.
    ///
    /// An empty vector indicates no primary key is defined.
    /// For composite primary keys, multiple indices are returned.
    pub fn get_primary_key_columns(&self) -> &Vec<usize> {
        &self.primary_key_columns
    }

    /// Sets the primary key column indices.
    ///
    /// # Parameters
    /// - `columns`: Vector of column indices that form the primary key.
    ///
    /// # Note
    /// No bounds checking is performed. Callers should ensure all indices
    /// are valid for this schema. For composite keys, provide multiple indices.
    ///
    /// # Example
    /// ```rust,ignore
    /// let mut schema = Schema::new(columns);
    /// schema.set_primary_key_columns(vec![0]);       // Single PK
    /// schema.set_primary_key_columns(vec![0, 1]);    // Composite PK
    /// schema.set_primary_key_columns(vec![]);        // Clear PK
    /// ```
    pub fn set_primary_key_columns(&mut self, columns: Vec<usize>) {
        self.primary_key_columns = columns;
    }
}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        self.columns == other.columns
            && self.length == other.length
            && self.tuple_is_inlined == other.tuple_is_inlined
            && self.unlined_columns == other.unlined_columns
            && self.primary_key_columns == other.primary_key_columns
    }
}

impl Display for Schema {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            // Detailed format
            let column_strings: Vec<String> = self
                .columns
                .iter()
                .map(|col| format!("{:#}", col))
                .collect();

            write!(
                f,
                "Schema[NumColumns: {}, IsInlined: {}, Length: {}] :: ({})",
                self.get_column_count(),
                self.tuple_is_inlined,
                self.length,
                column_strings.join(", ")
            )
        } else {
            // Basic format
            let column_strings: Vec<String> =
                self.columns.iter().map(|col| format!("{}", col)).collect();

            write!(f, "Schema ({})", column_strings.join(", "))
        }
    }
}

impl Default for Schema {
    fn default() -> Self {
        Schema {
            columns: Vec::new(),
            length: 0,
            tuple_is_inlined: true,
            unlined_columns: Vec::new(),
            primary_key_columns: Vec::new(),
        }
    }
}

impl AsRef<Schema> for Schema {
    fn as_ref(&self) -> &Schema {
        self
    }
}

#[cfg(test)]
mod unit_tests {
    use bincode::config;

    use super::*;
    use crate::types_db::type_id::TypeId;

    #[test]
    fn schema_serialization() {
        let columns = vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ];
        let schema = Schema::new(columns);

        let config = config::standard();
        let serialized = bincode::encode_to_vec(&schema, config).unwrap();
        let (deserialized, _): (Schema, usize) =
            bincode::decode_from_slice(&serialized, config).unwrap();

        assert_eq!(schema, deserialized);
    }

    #[test]
    fn schema_default() {
        let default_schema = Schema::default();
        assert_eq!(default_schema.get_column_count(), 0);
        assert_eq!(default_schema.get_inlined_storage_size(), 0);
        assert!(default_schema.is_inlined());
    }

    #[test]
    fn test_schema_merge() {
        let left_columns = vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ];
        let right_columns = vec![
            Column::new("age", TypeId::Integer),
            Column::new("email", TypeId::VarChar),
        ];

        let left_schema = Schema::new(left_columns);
        let right_schema = Schema::new(right_columns);
        let merged_schema = Schema::merge(&left_schema, &right_schema);

        assert_eq!(merged_schema.get_column_count(), 4);
        assert_eq!(merged_schema.get_column_index("id"), Some(0));
        assert_eq!(merged_schema.get_column_index("email"), Some(3));
    }

    #[test]
    fn test_schema_column_operations() {
        let columns = vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ];
        let schema = Schema::new(columns);

        // Test column retrieval
        assert_eq!(schema.get_column(0).unwrap().get_name(), "id");
        assert_eq!(schema.get_column(1).unwrap().get_name(), "name");
        assert!(schema.get_column(5).is_none());

        // Test column index lookup
        assert_eq!(schema.get_column_index("age"), Some(2));
        assert_eq!(schema.get_column_index("nonexistent"), None);

        // Test inlined status
        assert!(!schema.is_inlined()); // Because VarChar is not inlined
        assert_eq!(schema.get_unlined_column_count(), 1); // One VarChar column
    }

    #[test]
    fn test_schema_copy() {
        let original_columns = vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ];
        let original_schema = Schema::new(original_columns);

        // Copy only id and age columns
        let copied_schema = Schema::copy_schema(&original_schema, &[0, 2]);

        assert_eq!(copied_schema.get_column_count(), 2);
        assert_eq!(copied_schema.get_column(0).unwrap().get_name(), "id");
        assert_eq!(copied_schema.get_column(1).unwrap().get_name(), "age");
    }

    #[test]
    fn test_schema_display() {
        let columns = vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ];
        let schema = Schema::new(columns);

        // Test basic format
        let basic_format = format!("{}", schema);
        assert!(basic_format.contains("id"));
        assert!(basic_format.contains("name"));

        // Test detailed format
        let detailed_format = format!("{:#}", schema);
        assert!(detailed_format.contains("NumColumns: 2"));
        assert!(detailed_format.contains("IsInlined: false"));
    }

    #[test]
    fn test_schema_storage_layout() {
        let columns = vec![
            Column::new("id", TypeId::Integer),
            Column::new("flag", TypeId::Boolean),
            Column::new("name", TypeId::VarChar),
        ];
        let schema = Schema::new(columns);

        // Check storage offsets are calculated correctly
        let columns = schema.get_columns();
        assert_eq!(columns[0].get_offset(), 0); // id starts at 0
        assert!(columns[1].get_offset() > columns[0].get_offset()); // flag comes after id
        assert!(columns[2].get_offset() > columns[1].get_offset()); // name comes after flag

        // Check uninlined columns
        let uninlined = schema.get_unlined_columns();
        assert_eq!(uninlined.len(), 1); // Only VarChar is uninlined
        assert_eq!(uninlined[0], 2); // VarChar is at index 2
    }

    #[test]
    fn test_primary_key_operations() {
        let columns = vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ];
        let mut schema = Schema::new(columns);

        // Test initial state (empty primary key)
        assert!(schema.get_primary_key_columns().is_empty());

        // Test setting single primary key
        schema.set_primary_key_columns(vec![0]); // Set 'id' as primary key
        assert_eq!(schema.get_primary_key_columns(), &vec![0]);

        // Test setting multiple primary keys
        schema.set_primary_key_columns(vec![0, 2]); // Set 'id' and 'age' as primary keys
        assert_eq!(schema.get_primary_key_columns(), &vec![0, 2]);

        // Test clearing primary keys
        schema.set_primary_key_columns(vec![]);
        assert!(schema.get_primary_key_columns().is_empty());

        // Test setting primary keys with invalid indices
        schema.set_primary_key_columns(vec![5]); // Index out of bounds
        assert_eq!(schema.get_primary_key_columns(), &vec![5]); // Note: Currently no bounds checking
    }

    #[test]
    fn test_primary_key_serialization() {
        let columns = vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ];
        let mut schema = Schema::new(columns);
        schema.set_primary_key_columns(vec![0]);

        // Test that primary keys are preserved during serialization
        let config = config::standard();
        let serialized = bincode::encode_to_vec(&schema, config).unwrap();
        let (deserialized, _): (Schema, usize) =
            bincode::decode_from_slice(&serialized, config).unwrap();

        assert_eq!(
            schema.get_primary_key_columns(),
            deserialized.get_primary_key_columns()
        );
    }

    #[test]
    fn test_primary_key_copy() {
        let columns = vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ];
        let mut original_schema = Schema::new(columns);
        original_schema.set_primary_key_columns(vec![0, 2]);

        // Test that primary keys are preserved when copying schema
        let copied_schema = Schema::copy_schema(&original_schema, &[0, 1, 2]);
        assert_eq!(
            original_schema.get_primary_key_columns(),
            copied_schema.get_primary_key_columns()
        );

        // Test that primary keys are preserved when copying subset of columns
        let partial_schema = Schema::copy_schema(&original_schema, &[0, 2]);
        assert_eq!(
            original_schema.get_primary_key_columns(),
            partial_schema.get_primary_key_columns()
        );
    }
}
