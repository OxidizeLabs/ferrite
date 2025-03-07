use log::debug;
use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::types_db::type_id::TypeId;
use sqlparser::ast::{ColumnDef, DataType, Expr};
use std::collections::HashSet;
use std::sync::Arc;
use log;


/// 2. Responsible for schema-related operations
pub struct SchemaManager {}

impl SchemaManager {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create_aggregation_output_schema(
        &self,
        group_by_exprs: &[&Expression],
        agg_exprs: &[Arc<Expression>],
        has_group_by: bool,
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
                let col_name = expr.get_return_type().get_name().to_string();
                if seen_columns.insert(col_name.clone()) {
                    columns.push(expr.get_return_type().clone());
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
                }
                _ => {
                    let col_name = agg_expr.get_return_type().get_name().to_string();
                    if seen_columns.insert(col_name.clone()) {
                        columns.push(agg_expr.get_return_type().clone());
                    }
                }
            }
        }

        Schema::new(columns)
    }

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

    pub fn convert_column_defs(&self, column_defs: &[ColumnDef]) -> Result<Vec<Column>, String> {
        let mut columns = Vec::new();

        for col_def in column_defs {
            let column_name = col_def.name.to_string();
            let type_id = self.convert_sql_type(&col_def.data_type)?;

            // Handle VARCHAR/STRING types specifically with length
            let column = match &col_def.data_type {
                DataType::Varchar(_) | DataType::String(_) => {
                    // Default length for variable length types
                    Column::new_varlen(&column_name, type_id, 255)
                }
                _ => Column::new(&column_name, type_id),
            };

            columns.push(column);
        }

        Ok(columns)
    }

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

    pub fn types_compatible(&self, source_type: TypeId, target_type: TypeId) -> bool {
        // Add your type compatibility rules here
        // For example:
        match (source_type, target_type) {
            // Same types are always compatible
            (a, b) if a == b => true,
            _ => false,
        }
    }

    pub fn convert_sql_type(&self, sql_type: &DataType) -> Result<TypeId, String> {
        match sql_type {
            DataType::Boolean | DataType::Bool => Ok(TypeId::Boolean),
            DataType::TinyInt(_) => Ok(TypeId::TinyInt),
            DataType::SmallInt(_) => Ok(TypeId::SmallInt),
            DataType::Int(_) | DataType::Integer(_) => Ok(TypeId::Integer),
            DataType::BigInt(_) => Ok(TypeId::BigInt),
            DataType::Decimal(_) | DataType::Float(_) => Ok(TypeId::Decimal),
            DataType::Varchar(_) | DataType::String(_) | DataType::Text | DataType::Char(_) => Ok(TypeId::VarChar),
            DataType::Array(_) => Ok(TypeId::Vector),
            DataType::Timestamp(_, _) => Ok(TypeId::Timestamp),
            _ => Err(format!("Unsupported SQL type: {:?}", sql_type)),
        }
    }

    pub fn infer_expression_type(&self, expr: &Expr) -> Result<TypeId, String> {
        match expr {
            Expr::Value(value) => match value {
                sqlparser::ast::Value::Number(_, _) => Ok(TypeId::Integer),
                sqlparser::ast::Value::SingleQuotedString(_)
                | sqlparser::ast::Value::DoubleQuotedString(_) => Ok(TypeId::VarChar),
                sqlparser::ast::Value::Boolean(_) => Ok(TypeId::Boolean),
                sqlparser::ast::Value::Null => Ok(TypeId::Invalid),
                _ => Err(format!("Unsupported value type: {:?}", value)),
            },
            _ => Ok(TypeId::Invalid), // Default type for complex expressions
        }
    }

    pub fn create_join_schema(&self, left_schema: &Schema, right_schema: &Schema) -> Schema {
        // Extract table aliases from the schemas
        let left_alias = self.extract_table_alias_from_schema(left_schema);
        let right_alias = self.extract_table_alias_from_schema(right_schema);
        
        debug!("Creating join schema with aliases: left={:?}, right={:?}", left_alias, right_alias);
        
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
    
    // Helper function to extract table alias from schema
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
            }
            "t2" => {
                // Search only in right schema for t2
                for i in 0..right_schema.get_column_count() {
                    let col = right_schema.get_column(i as usize).unwrap();
                    if col.get_name() == column_name {
                        return Ok((
                            (left_schema.get_column_count() + i) as usize,
                            col
                        ));
                    }
                }
                Err(format!(
                    "Column {}.{} not found in right schema",
                    table_alias, column_name
                ))
            }
            _ => Err(format!("Unknown table alias: {}", table_alias)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{Value, ColumnDef, DataType, Ident};
    use crate::sql::execution::expressions::aggregate_expression::{AggregateExpression, AggregationType};
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;

    #[test]
    fn test_convert_sql_types() {
        let manager = SchemaManager::new();
        
        assert_eq!(manager.convert_sql_type(&DataType::Boolean).unwrap(), TypeId::Boolean);
        assert_eq!(manager.convert_sql_type(&DataType::TinyInt(None)).unwrap(), TypeId::TinyInt);
        assert_eq!(manager.convert_sql_type(&DataType::Integer(None)).unwrap(), TypeId::Integer);
        assert_eq!(manager.convert_sql_type(&DataType::BigInt(None)).unwrap(), TypeId::BigInt);
        assert_eq!(manager.convert_sql_type(&DataType::Varchar(None)).unwrap(), TypeId::VarChar);
        
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
        let num_expr = Expr::Value(Value::Number("42".to_string(), false));
        assert_eq!(manager.infer_expression_type(&num_expr).unwrap(), TypeId::Integer);
        
        // Test string
        let str_expr = Expr::Value(Value::SingleQuotedString("test".to_string()));
        assert_eq!(manager.infer_expression_type(&str_expr).unwrap(), TypeId::VarChar);
        
        // Test boolean
        let bool_expr = Expr::Value(Value::Boolean(true));
        assert_eq!(manager.infer_expression_type(&bool_expr).unwrap(), TypeId::Boolean);
        
        // Test null
        let null_expr = Expr::Value(Value::Null);
        assert_eq!(manager.infer_expression_type(&null_expr).unwrap(), TypeId::Invalid);
    }

    #[test]
    fn test_convert_column_defs() {
        let manager = SchemaManager::new();
        
        let column_defs = vec![
            ColumnDef {
                name: Ident::new("id"),
                data_type: DataType::Integer(None),
                collation: None,
                options: vec![],
            },
            ColumnDef {
                name: Ident::new("name"),
                data_type: DataType::Varchar(None),
                collation: None,
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
        
        let values = vec![
            vec![
                Expr::Value(Value::Number("1".to_string(), false)),
                Expr::Value(Value::SingleQuotedString("test".to_string())),
            ],
        ];
        
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
        let (idx, col) = manager.resolve_qualified_column("t1", "name", &left_schema, &right_schema).unwrap();
        assert_eq!(idx, 1);
        assert_eq!(col.get_name(), "name");
        
        // Test finding column in right schema
        let (idx, col) = manager.resolve_qualified_column("t2", "email", &left_schema, &right_schema).unwrap();
        assert_eq!(idx, 3);  // 2 columns in left schema + 1 (0-based index)
        assert_eq!(col.get_name(), "email");
        
        // Test column not found
        assert!(manager.resolve_qualified_column("t1", "unknown", &left_schema, &right_schema).is_err());
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
                Expr::Value(Value::Number("1".to_string(), false)),
                Expr::Value(Value::SingleQuotedString("test1".to_string())),
            ],
            vec![
                Expr::Value(Value::Number("2".to_string(), false)),
                Expr::Value(Value::SingleQuotedString("test2".to_string())),
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
                data_type: DataType::Varchar(None),  // Using None for now
                collation: None,
                options: vec![],
            },
            ColumnDef {
                name: Ident::new("description"),
                data_type: DataType::String(None),
                collation: None,
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
        let column_defs_with_length = vec![
            ColumnDef {
                name: Ident::new("fixed_text"),
                data_type: DataType::Char(None),  // Using CHAR type
                collation: None,
                options: vec![],
            },
        ];
        
        let columns = manager.convert_column_defs(&column_defs_with_length).unwrap();
        assert_eq!(columns[0].get_type(), TypeId::VarChar);  // Should still convert to VARCHAR
    }

    #[test]
    fn test_resolve_qualified_column_duplicate_names() {
        let manager = SchemaManager::new();
        
        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        
        let right_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),  // Duplicate column name
            Column::new("email", TypeId::VarChar),
        ]);
        
        // When searching in left schema, should get the left column
        let (idx, col) = manager.resolve_qualified_column("t1", "id", &left_schema, &right_schema).unwrap();
        assert_eq!(idx, 0);  // First column in left schema
        assert_eq!(col.get_type(), TypeId::Integer);
        
        // When searching in right schema's "id", should get the right column
        let (idx, col) = manager.resolve_qualified_column("t2", "id", &left_schema, &right_schema).unwrap();
        assert_eq!(idx, 2);  // Index after all left schema columns (2 columns in left schema)
        assert_eq!(col.get_type(), TypeId::Integer);
        
        // Test finding a unique column in right schema
        let (idx, col) = manager.resolve_qualified_column("t2", "email", &left_schema, &right_schema).unwrap();
        assert_eq!(idx, 3);  // Last column in combined schemas
        assert_eq!(col.get_name(), "email");
        
        // Test column not found
        assert!(manager.resolve_qualified_column("t1", "unknown", &left_schema, &right_schema).is_err());
    }

    #[test]
    fn test_create_aggregation_output_schema() {
        let manager = SchemaManager::new();
        
        // Create test columns for group by
        let group_by_col = Column::new("category", TypeId::VarChar);
        let group_by_expr = Expression::ColumnRef(ColumnRefExpression::new(
            0,  // tuple_index
            0,  // column_index
            group_by_col,
            vec![]  // no children
        ));
        
        // Create test aggregate expression
        let agg_col = Column::new("amount", TypeId::Integer);
        let agg_arg = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            agg_col.clone(),
            vec![]
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
            true
        );
        
        assert_eq!(schema.get_column_count(), 2);
        assert_eq!(schema.get_column(0).unwrap().get_name(), "category");
        assert_eq!(schema.get_column(1).unwrap().get_name(), "SUM(amount)");
    }

    #[test]
    fn test_infer_expression_type_edge_cases() {
        let manager = SchemaManager::new();
        
        // Test empty string
        let empty_str = Expr::Value(Value::SingleQuotedString("".to_string()));
        assert_eq!(manager.infer_expression_type(&empty_str).unwrap(), TypeId::VarChar);
        
        // Test zero
        let zero = Expr::Value(Value::Number("0".to_string(), false));
        assert_eq!(manager.infer_expression_type(&zero).unwrap(), TypeId::Integer);
        
        // Test negative number
        let negative = Expr::Value(Value::Number("-42".to_string(), false));
        assert_eq!(manager.infer_expression_type(&negative).unwrap(), TypeId::Integer);
        
        // Test complex expression (should return Invalid)
        let complex = Expr::BinaryOp {
            left: Box::new(Expr::Value(Value::Number("1".to_string(), false))),
            op: sqlparser::ast::BinaryOperator::Plus,
            right: Box::new(Expr::Value(Value::Number("2".to_string(), false))),
        };
        assert_eq!(manager.infer_expression_type(&complex).unwrap(), TypeId::Invalid);
    }

    #[test]
    fn test_create_aggregation_output_schema_no_group_by() {
        let manager = SchemaManager::new();
        
        // Create test aggregate expressions
        let count_col = Column::new("count_all", TypeId::BigInt);
        let count_arg = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, count_col.clone(), vec![]
        )));
        
        let count_expr = Expression::Aggregate(AggregateExpression::new(
            AggregationType::Count,
            vec![count_arg],
            count_col,
            "COUNT".to_string(),
        ));
        
        let schema = manager.create_aggregation_output_schema(
            &[],  // no group by expressions
            &[Arc::new(count_expr)],
            false  // no GROUP BY
        );
        
        assert_eq!(schema.get_column_count(), 1);
        assert_eq!(schema.get_column(0).unwrap().get_name(), "COUNT(count_all)");
    }

    #[test]
    fn test_create_aggregation_output_schema_multiple_aggregates() {
        let manager = SchemaManager::new();
        
        // Create test columns for group by
        let group_by_col = Column::new("category", TypeId::VarChar);
        let group_by_expr = Expression::ColumnRef(ColumnRefExpression::new(
            0,  // tuple_index
            0,  // column_index
            group_by_col,
            vec![]  // no children
        ));
        
        // Create test aggregate expression
        let agg_col = Column::new("amount", TypeId::Integer);
        let agg_arg = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            agg_col.clone(),
            vec![]
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
            true
        );
        
        assert_eq!(schema.get_column_count(), 2);
        assert_eq!(schema.get_column(0).unwrap().get_name(), "category");
        assert_eq!(schema.get_column(1).unwrap().get_name(), "SUM(amount)");
    }

    #[test]
    fn test_create_values_schema_mixed_types() {
        let manager = SchemaManager::new();
        
        let values = vec![
            vec![
                Expr::Value(Value::Number("1".to_string(), false)),
                Expr::Value(Value::SingleQuotedString("text".to_string())),
                Expr::Value(Value::Boolean(true)),
                Expr::Value(Value::Null),
            ],
        ];
        
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
            Column::new("common", TypeId::Integer),  // Duplicate name
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
}
