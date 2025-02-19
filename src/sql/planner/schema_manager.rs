use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::aggregate_expression::AggregationType;
use crate::types_db::type_id::TypeId;
use sqlparser::ast::{ColumnDef, DataType, Expr};
use std::collections::HashSet;
use std::sync::Arc;

/// 2. Responsible for schema-related operations
pub struct SchemaManager {}

impl SchemaManager {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create_aggregation_output_schema(
        &self,
        group_bys: &[&Expression],
        aggregates: &[Arc<Expression>],
        has_group_by: bool,
    ) -> Schema {
        let mut columns = Vec::new();
        let mut seen_columns = HashSet::new();

        // Add group by columns first if we have them
        if has_group_by {
            for expr in group_bys {
                let col_name = expr.get_return_type().get_name().to_string();
                if seen_columns.insert(col_name.clone()) {
                    columns.push(expr.get_return_type().clone());
                }
            }
        }

        // Add aggregate columns
        for agg_expr in aggregates {
            match agg_expr.as_ref() {
                Expression::Aggregate(agg) => {
                    let col_name = match agg.get_agg_type() {
                        AggregationType::CountStar => "COUNT(*)".to_string(),
                        _ => format!(
                            "{}({})",
                            agg.get_agg_type().to_string(),
                            agg.get_arg().get_return_type().get_name()
                        ),
                    };

                    if seen_columns.insert(col_name.clone()) {
                        let col_type = match agg.get_agg_type() {
                            AggregationType::Count | AggregationType::CountStar => TypeId::BigInt,
                            AggregationType::Sum => agg.get_arg().get_return_type().get_type(),
                            AggregationType::Avg => TypeId::Decimal,
                            AggregationType::Min | AggregationType::Max => {
                                agg.get_arg().get_return_type().get_type()
                            }
                        };
                        columns.push(Column::new(&col_name, col_type));
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
            DataType::Varchar(_) | DataType::String(_) | DataType::Text => Ok(TypeId::VarChar),
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
}
