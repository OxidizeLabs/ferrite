use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

use sqlparser::ast::BinaryOperator;

use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::aggregate_expression::AggregateExpression;
use crate::sql::execution::expressions::all_expression::AllExpression;
use crate::sql::execution::expressions::any_expression::AnyExpression;
use crate::sql::execution::expressions::arithmetic_expression::ArithmeticExpression;
use crate::sql::execution::expressions::array_expression::ArrayExpression;
use crate::sql::execution::expressions::assignment_expression::AssignmentExpression;
use crate::sql::execution::expressions::at_timezone_expression::AtTimeZoneExpression;
use crate::sql::execution::expressions::between_expression::BetweenExpression;
use crate::sql::execution::expressions::binary_op_expression::BinaryOpExpression;
use crate::sql::execution::expressions::case_expression::CaseExpression;
use crate::sql::execution::expressions::cast_expression::CastExpression;
use crate::sql::execution::expressions::ceil_floor_expression::CeilFloorExpression;
use crate::sql::execution::expressions::coalesce_expression::CoalesceExpression;
use crate::sql::execution::expressions::collate_expression::CollateExpression;
use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::sql::execution::expressions::comparison_expression::ComparisonExpression;
use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
use crate::sql::execution::expressions::convert_expression::ConvertExpression;
use crate::sql::execution::expressions::datetime_expression::DateTimeExpression;
use crate::sql::execution::expressions::exists_expression::ExistsExpression;
use crate::sql::execution::expressions::extract_expression::ExtractExpression;
use crate::sql::execution::expressions::filter_expression::FilterExpression;
use crate::sql::execution::expressions::function_expression::FunctionExpression;
use crate::sql::execution::expressions::grouping_sets_expression::GroupingSetsExpression;
use crate::sql::execution::expressions::in_expression::InExpression;
use crate::sql::execution::expressions::interval_expression::IntervalExpression;
use crate::sql::execution::expressions::is_check_expression::IsCheckExpression;
use crate::sql::execution::expressions::is_distinct_expression::IsDistinctExpression;
use crate::sql::execution::expressions::like_expression::LikeExpression;
use crate::sql::execution::expressions::literal_value_expression::LiteralValueExpression;
use crate::sql::execution::expressions::logic_expression::LogicExpression;
use crate::sql::execution::expressions::map_access_expression::MapAccessExpression;
use crate::sql::execution::expressions::method_expression::MethodExpression;
use crate::sql::execution::expressions::mock_expression::MockExpression;
use crate::sql::execution::expressions::overlay_expression::OverlayExpression;
use crate::sql::execution::expressions::position_expression::PositionExpression;
use crate::sql::execution::expressions::qualified_wildcard_expression::QualifiedWildcardExpression;
use crate::sql::execution::expressions::random_expression::RandomExpression;
use crate::sql::execution::expressions::regex_expression::RegexExpression;
use crate::sql::execution::expressions::string_expression::StringExpression;
use crate::sql::execution::expressions::struct_expression::StructExpression;
use crate::sql::execution::expressions::subquery_expression::SubqueryExpression;
use crate::sql::execution::expressions::subscript_expression::SubscriptExpression;
use crate::sql::execution::expressions::substring_expression::SubstringExpression;
use crate::sql::execution::expressions::trim_expression::TrimExpression;
use crate::sql::execution::expressions::tuple_expression::TupleExpression;
use crate::sql::execution::expressions::typed_string_expression::TypedStringExpression;
use crate::sql::execution::expressions::unary_op_expression::UnaryOpExpression;
use crate::sql::execution::expressions::wildcard_expression::WildcardExpression;
use crate::sql::execution::expressions::window_expression::WindowExpression;
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Case(CaseExpression),
    Cast(CastExpression),
    Constant(ConstantExpression),
    ColumnRef(ColumnRefExpression),
    Arithmetic(ArithmeticExpression),
    Comparison(ComparisonExpression),
    Logic(LogicExpression),
    String(StringExpression),
    Array(ArrayExpression),
    Assignment(AssignmentExpression),
    Mock(MockExpression),
    Aggregate(AggregateExpression),
    Window(WindowExpression),
    Function(FunctionExpression),
    Subquery(SubqueryExpression),
    In(InExpression),
    Between(BetweenExpression),
    Like(LikeExpression),
    Extract(ExtractExpression),
    Exists(ExistsExpression),
    Regex(RegexExpression),
    DateTime(DateTimeExpression),
    Coalesce(CoalesceExpression),
    Random(RandomExpression),
    Trim(TrimExpression),
    Interval(IntervalExpression),
    GroupingSets(GroupingSetsExpression),
    Filter(FilterExpression),
    IsDistinct(IsDistinctExpression),
    Position(PositionExpression),
    Method(MethodExpression),
    Struct(StructExpression),
    Overlay(OverlayExpression),
    Collate(CollateExpression),
    AtTimeZone(AtTimeZoneExpression),
    MapAccess(MapAccessExpression),
    Tuple(TupleExpression),
    Wildcard(WildcardExpression),
    QualifiedWildcard(QualifiedWildcardExpression),
    TypedString(TypedStringExpression),
    Subscript(SubscriptExpression),
    IsCheck(IsCheckExpression),
    BinaryOp(BinaryOpExpression),
    Any(AnyExpression),
    All(AllExpression),
    UnaryOp(UnaryOpExpression),
    Convert(ConvertExpression),
    CeilFloor(CeilFloorExpression),
    Substring(SubstringExpression),
    Literal(LiteralValueExpression),
}

pub trait ExpressionOps {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError>;
    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError>;
    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression>;
    fn get_children(&self) -> &Vec<Arc<Expression>>;
    fn get_return_type(&self) -> &Column;
    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression>;
    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError>;
}

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

impl ExpressionOps for Expression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        match self {
            Self::Constant(expr) => expr.evaluate(tuple, schema),
            Self::ColumnRef(expr) => expr.evaluate(tuple, schema),
            Self::Arithmetic(expr) => expr.evaluate(tuple, schema),
            Self::Comparison(expr) => expr.evaluate(tuple, schema),
            Self::Logic(expr) => expr.evaluate(tuple, schema),
            Self::String(expr) => expr.evaluate(tuple, schema),
            Self::Array(expr) => expr.evaluate(tuple, schema),
            Self::Assignment(expr) => expr.evaluate(tuple, schema),
            Self::Mock(expr) => expr.evaluate(tuple, schema),
            Self::Aggregate(expr) => expr.evaluate(tuple, schema),
            Self::Window(expr) => expr.evaluate(tuple, schema),
            Self::Cast(expr) => expr.evaluate(tuple, schema),
            Self::Case(expr) => expr.evaluate(tuple, schema),
            Self::Function(expr) => expr.evaluate(tuple, schema),
            Self::Subquery(expr) => expr.evaluate(tuple, schema),
            Self::In(expr) => expr.evaluate(tuple, schema),
            Self::Between(expr) => expr.evaluate(tuple, schema),
            Self::Like(expr) => expr.evaluate(tuple, schema),
            Self::Extract(expr) => expr.evaluate(tuple, schema),
            Self::Exists(expr) => expr.evaluate(tuple, schema),
            Self::Regex(expr) => expr.evaluate(tuple, schema),
            Self::DateTime(expr) => expr.evaluate(tuple, schema),
            Self::Coalesce(expr) => expr.evaluate(tuple, schema),
            Self::Random(expr) => expr.evaluate(tuple, schema),
            Self::Trim(expr) => expr.evaluate(tuple, schema),
            Self::Interval(expr) => expr.evaluate(tuple, schema),
            Self::GroupingSets(expr) => expr.evaluate(tuple, schema),
            Self::Filter(expr) => expr.evaluate(tuple, schema),
            Self::IsDistinct(expr) => expr.evaluate(tuple, schema),
            Self::Position(expr) => expr.evaluate(tuple, schema),
            Self::Method(expr) => expr.evaluate(tuple, schema),
            Self::Struct(expr) => expr.evaluate(tuple, schema),
            Self::Overlay(expr) => expr.evaluate(tuple, schema),
            Self::Collate(expr) => expr.evaluate(tuple, schema),
            Self::AtTimeZone(expr) => expr.evaluate(tuple, schema),
            Self::MapAccess(expr) => expr.evaluate(tuple, schema),
            Self::Tuple(expr) => expr.evaluate(tuple, schema),
            Self::Wildcard(expr) => expr.evaluate(tuple, schema),
            Self::QualifiedWildcard(expr) => expr.evaluate(tuple, schema),
            Self::TypedString(expr) => expr.evaluate(tuple, schema),
            Self::Subscript(expr) => expr.evaluate(tuple, schema),
            Self::IsCheck(expr) => expr.evaluate(tuple, schema),
            Self::BinaryOp(expr) => expr.evaluate(tuple, schema),
            Self::Any(expr) => expr.evaluate(tuple, schema),
            Self::All(expr) => expr.evaluate(tuple, schema),
            Self::UnaryOp(expr) => expr.evaluate(tuple, schema),
            Self::Convert(expr) => expr.evaluate(tuple, schema),
            Self::CeilFloor(expr) => expr.evaluate(tuple, schema),
            Self::Substring(expr) => expr.evaluate(tuple, schema),
            Self::Literal(expr) => expr.evaluate(tuple, schema),
        }
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        match self {
            Self::Constant(expr) => Ok(expr.get_value().clone()),
            Self::ColumnRef(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            },
            Self::Arithmetic(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            },
            Self::Comparison(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            },
            Self::Logic(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            },
            Self::String(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            },
            Self::Array(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            },
            Self::Assignment(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            },
            Self::Mock(expr) => {
                // For mock expressions, we'll just use the regular evaluate
                expr.evaluate(left_tuple, left_schema)
            },
            Self::Aggregate(expr) => expr.evaluate(left_tuple, right_schema),
            Self::Window(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            },
            Self::Cast(expr) => expr.evaluate(left_tuple, right_schema),
            Self::Case(expr) => expr.evaluate(left_tuple, right_schema),
            Self::Function(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            },
            Self::Subquery(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            },
            Self::In(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            },
            Self::Between(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            },
            Self::Like(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            },
            Self::Extract(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            },
            Self::Exists(expr) => expr.evaluate(left_tuple, left_schema),
            Self::Regex(expr) => expr.evaluate(left_tuple, left_schema),
            Self::DateTime(expr) => expr.evaluate(left_tuple, left_schema),
            Self::Coalesce(expr) => expr.evaluate(left_tuple, left_schema),
            Self::Random(expr) => expr.evaluate(left_tuple, left_schema),
            Self::Trim(expr) => expr.evaluate(left_tuple, left_schema),
            Self::Interval(expr) => expr.evaluate(left_tuple, left_schema),
            Self::GroupingSets(expr) => expr.evaluate(left_tuple, left_schema),
            Self::Filter(expr) => expr.evaluate(left_tuple, left_schema),
            Self::IsDistinct(expr) => expr.evaluate(left_tuple, left_schema),
            Self::Position(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            },
            Self::Method(expr) => expr.evaluate(left_tuple, left_schema),
            Self::Struct(expr) => expr.evaluate(left_tuple, left_schema),
            Self::Overlay(expr) => expr.evaluate(left_tuple, left_schema),
            Self::Collate(expr) => expr.evaluate(left_tuple, left_schema),
            Self::AtTimeZone(expr) => expr.evaluate(left_tuple, left_schema),
            Self::MapAccess(expr) => expr.evaluate(left_tuple, left_schema),
            Self::Tuple(expr) => expr.evaluate(left_tuple, left_schema),
            Self::Wildcard(expr) => expr.evaluate(left_tuple, left_schema),
            Self::QualifiedWildcard(expr) => expr.evaluate(left_tuple, left_schema),
            Self::TypedString(expr) => expr.evaluate(left_tuple, left_schema),
            Self::Subscript(expr) => expr.evaluate(left_tuple, left_schema),
            Self::IsCheck(expr) => expr.evaluate(left_tuple, left_schema),
            Self::BinaryOp(expr) => expr.evaluate(left_tuple, left_schema),
            Self::Any(expr) => expr.evaluate(left_tuple, left_schema),
            Self::All(expr) => expr.evaluate(left_tuple, left_schema),
            Self::UnaryOp(expr) => expr.evaluate(left_tuple, left_schema),
            Self::Convert(expr) => expr.evaluate(left_tuple, left_schema),
            Self::CeilFloor(expr) => expr.evaluate(left_tuple, left_schema),
            Self::Substring(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            },
            Self::Literal(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            },
        }
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        match self {
            Self::Arithmetic(expr) => expr.get_child_at(child_idx),
            Self::Constant(expr) => expr.get_child_at(child_idx),
            Self::ColumnRef(expr) => expr.get_child_at(child_idx),
            Self::Comparison(expr) => expr.get_child_at(child_idx),
            Self::Logic(expr) => expr.get_child_at(child_idx),
            Self::String(expr) => expr.get_child_at(child_idx),
            Self::Array(expr) => expr.get_child_at(child_idx),
            Self::Assignment(expr) => expr.get_child_at(child_idx),
            Self::Mock(expr) => expr.get_child_at(child_idx),
            Self::Aggregate(expr) => expr.get_child_at(child_idx),
            Self::Window(expr) => expr.get_child_at(child_idx),
            Self::Cast(expr) => expr.get_child_at(child_idx),
            Self::Case(expr) => expr.get_child_at(child_idx),
            Self::Function(expr) => expr.get_child_at(child_idx),
            Self::Subquery(expr) => expr.get_child_at(child_idx),
            Self::In(expr) => expr.get_child_at(child_idx),
            Self::Between(expr) => expr.get_child_at(child_idx),
            Self::Like(expr) => expr.get_child_at(child_idx),
            Self::Extract(expr) => expr.get_child_at(child_idx),
            Self::Exists(expr) => expr.get_child_at(child_idx),
            Self::Regex(expr) => expr.get_child_at(child_idx),
            Self::DateTime(expr) => expr.get_child_at(child_idx),
            Self::Coalesce(expr) => expr.get_child_at(child_idx),
            Self::Random(expr) => expr.get_child_at(child_idx),
            Self::Trim(expr) => expr.get_child_at(child_idx),
            Self::Interval(expr) => expr.get_child_at(child_idx),
            Self::GroupingSets(expr) => expr.get_child_at(child_idx),
            Self::Filter(expr) => expr.get_child_at(child_idx),
            Self::IsDistinct(expr) => expr.get_child_at(child_idx),
            Self::Position(expr) => expr.get_child_at(child_idx),
            Self::Method(expr) => expr.get_child_at(child_idx),
            Self::Struct(expr) => expr.get_child_at(child_idx),
            Self::Overlay(expr) => expr.get_child_at(child_idx),
            Self::Collate(expr) => expr.get_child_at(child_idx),
            Self::AtTimeZone(expr) => expr.get_child_at(child_idx),
            Self::MapAccess(expr) => expr.get_child_at(child_idx),
            Self::Tuple(expr) => expr.get_child_at(child_idx),
            Self::Wildcard(expr) => expr.get_child_at(child_idx),
            Self::QualifiedWildcard(expr) => expr.get_child_at(child_idx),
            Self::TypedString(expr) => expr.get_child_at(child_idx),
            Self::Subscript(expr) => expr.get_child_at(child_idx),
            Self::IsCheck(expr) => expr.get_child_at(child_idx),
            Self::BinaryOp(expr) => expr.get_child_at(child_idx),
            Self::Any(expr) => expr.get_child_at(child_idx),
            Self::All(expr) => expr.get_child_at(child_idx),
            Self::UnaryOp(expr) => expr.get_child_at(child_idx),
            Self::Convert(expr) => expr.get_child_at(child_idx),
            Self::CeilFloor(expr) => expr.get_child_at(child_idx),
            Self::Substring(expr) => expr.get_child_at(child_idx),
            Self::Literal(expr) => expr.get_child_at(child_idx),
        }
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        match self {
            Self::Arithmetic(expr) => expr.get_children(),
            Self::Constant(expr) => expr.get_children(),
            Self::ColumnRef(expr) => expr.get_children(),
            Self::Comparison(expr) => expr.get_children(),
            Self::Logic(expr) => expr.get_children(),
            Self::String(expr) => expr.get_children(),
            Self::Array(expr) => expr.get_children(),
            Self::Assignment(expr) => expr.get_children(),
            Self::Mock(expr) => expr.get_children(),
            Self::Aggregate(expr) => expr.get_children(),
            Self::Window(expr) => expr.get_children(),
            Self::Cast(expr) => expr.get_children(),
            Self::Case(expr) => expr.get_children(),
            Self::Function(expr) => expr.get_children(),
            Self::Subquery(expr) => expr.get_children(),
            Self::In(expr) => expr.get_children(),
            Self::Between(expr) => expr.get_children(),
            Self::Like(expr) => expr.get_children(),
            Self::Extract(expr) => expr.get_children(),
            Self::Exists(expr) => expr.get_children(),
            Self::Regex(expr) => expr.get_children(),
            Self::DateTime(expr) => expr.get_children(),
            Self::Coalesce(expr) => expr.get_children(),
            Self::Random(expr) => expr.get_children(),
            Self::Trim(expr) => expr.get_children(),
            Self::Interval(expr) => expr.get_children(),
            Self::GroupingSets(expr) => expr.get_children(),
            Self::Filter(expr) => expr.get_children(),
            Self::IsDistinct(expr) => expr.get_children(),
            Self::Position(expr) => expr.get_children(),
            Self::Method(expr) => expr.get_children(),
            Self::Struct(expr) => expr.get_children(),
            Self::Overlay(expr) => expr.get_children(),
            Self::Collate(expr) => expr.get_children(),
            Self::AtTimeZone(expr) => expr.get_children(),
            Self::MapAccess(expr) => expr.get_children(),
            Self::Tuple(expr) => expr.get_children(),
            Self::Wildcard(expr) => expr.get_children(),
            Self::QualifiedWildcard(expr) => expr.get_children(),
            Self::TypedString(expr) => expr.get_children(),
            Self::Subscript(expr) => expr.get_children(),
            Self::IsCheck(expr) => expr.get_children(),
            Self::BinaryOp(expr) => expr.get_children(),
            Self::Any(expr) => expr.get_children(),
            Self::All(expr) => expr.get_children(),
            Self::UnaryOp(expr) => expr.get_children(),
            Self::Convert(expr) => expr.get_children(),
            Self::CeilFloor(expr) => expr.get_children(),
            Self::Substring(expr) => expr.get_children(),
            Self::Literal(expr) => expr.get_children(),
        }
    }

    fn get_return_type(&self) -> &Column {
        match self {
            Self::Constant(expr) => expr.get_return_type(),
            Self::ColumnRef(expr) => expr.get_return_type(),
            Self::Arithmetic(expr) => expr.get_return_type(),
            Self::Comparison(expr) => expr.get_return_type(),
            Self::Logic(expr) => expr.get_return_type(),
            Self::String(expr) => expr.get_return_type(),
            Self::Array(expr) => expr.get_return_type(),
            Self::Assignment(expr) => expr.get_return_type(),
            Self::Mock(expr) => expr.get_return_type(),
            Self::Aggregate(expr) => expr.get_return_type(),
            Self::Window(expr) => expr.get_return_type(),
            Self::Cast(expr) => expr.get_return_type(),
            Self::Case(expr) => expr.get_return_type(),
            Self::Function(expr) => expr.get_return_type(),
            Self::Subquery(expr) => expr.get_return_type(),
            Self::In(expr) => expr.get_return_type(),
            Self::Between(expr) => expr.get_return_type(),
            Self::Like(expr) => expr.get_return_type(),
            Self::Extract(expr) => expr.get_return_type(),
            Self::Exists(expr) => expr.get_return_type(),
            Self::Regex(expr) => expr.get_return_type(),
            Self::DateTime(expr) => expr.get_return_type(),
            Self::Coalesce(expr) => expr.get_return_type(),
            Self::Random(expr) => expr.get_return_type(),
            Self::Trim(expr) => expr.get_return_type(),
            Self::Interval(expr) => expr.get_return_type(),
            Self::GroupingSets(expr) => expr.get_return_type(),
            Self::Filter(expr) => expr.get_return_type(),
            Self::IsDistinct(expr) => expr.get_return_type(),
            Self::Position(expr) => expr.get_return_type(),
            Self::Method(expr) => expr.get_return_type(),
            Self::Struct(expr) => expr.get_return_type(),
            Self::Overlay(expr) => expr.get_return_type(),
            Self::Collate(expr) => expr.get_return_type(),
            Self::AtTimeZone(expr) => expr.get_return_type(),
            Self::MapAccess(expr) => expr.get_return_type(),
            Self::Tuple(expr) => expr.get_return_type(),
            Self::Wildcard(expr) => expr.get_return_type(),
            Self::QualifiedWildcard(expr) => expr.get_return_type(),
            Self::TypedString(expr) => expr.get_return_type(),
            Self::Subscript(expr) => expr.get_return_type(),
            Self::IsCheck(expr) => expr.get_return_type(),
            Self::BinaryOp(expr) => expr.get_return_type(),
            Self::Any(expr) => expr.get_return_type(),
            Self::All(expr) => expr.get_return_type(),
            Self::UnaryOp(expr) => expr.get_return_type(),
            Self::Convert(expr) => expr.get_return_type(),
            Self::CeilFloor(expr) => expr.get_return_type(),
            Self::Substring(expr) => expr.get_return_type(),
            Self::Literal(expr) => expr.get_return_type(),
        }
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        match self {
            Self::Arithmetic(expr) => expr.clone_with_children(children),
            Self::Constant(expr) => expr.clone_with_children(children),
            Self::ColumnRef(expr) => expr.clone_with_children(children),
            Self::Comparison(expr) => expr.clone_with_children(children),
            Self::Logic(expr) => expr.clone_with_children(children),
            Self::String(expr) => expr.clone_with_children(children),
            Self::Array(expr) => expr.clone_with_children(children),
            Self::Assignment(expr) => expr.clone_with_children(children),
            Self::Mock(expr) => expr.clone_with_children(children),
            Self::Aggregate(expr) => expr.clone_with_children(children),
            Self::Window(expr) => expr.clone_with_children(children),
            Self::Cast(expr) => expr.clone_with_children(children),
            Self::Case(expr) => expr.clone_with_children(children),
            Self::Function(expr) => expr.clone_with_children(children),
            Self::Subquery(expr) => expr.clone_with_children(children),
            Self::In(expr) => expr.clone_with_children(children),
            Self::Between(expr) => expr.clone_with_children(children),
            Self::Like(expr) => expr.clone_with_children(children),
            Self::Extract(expr) => expr.clone_with_children(children),
            Self::Exists(expr) => expr.clone_with_children(children),
            Self::Regex(expr) => expr.clone_with_children(children),
            Self::DateTime(expr) => expr.clone_with_children(children),
            Self::Coalesce(expr) => expr.clone_with_children(children),
            Self::Random(expr) => expr.clone_with_children(children),
            Self::Trim(expr) => expr.clone_with_children(children),
            Self::Interval(expr) => expr.clone_with_children(children),
            Self::GroupingSets(expr) => expr.clone_with_children(children),
            Self::Filter(expr) => expr.clone_with_children(children),
            Self::IsDistinct(expr) => expr.clone_with_children(children),
            Self::Position(expr) => expr.clone_with_children(children),
            Self::Method(expr) => expr.clone_with_children(children),
            Self::Struct(expr) => expr.clone_with_children(children),
            Self::Overlay(expr) => expr.clone_with_children(children),
            Self::Collate(expr) => expr.clone_with_children(children),
            Self::AtTimeZone(expr) => expr.clone_with_children(children),
            Self::MapAccess(expr) => expr.clone_with_children(children),
            Self::Tuple(expr) => expr.clone_with_children(children),
            Self::Wildcard(expr) => expr.clone_with_children(children),
            Self::QualifiedWildcard(expr) => expr.clone_with_children(children),
            Self::TypedString(expr) => expr.clone_with_children(children),
            Self::Subscript(expr) => expr.clone_with_children(children),
            Self::IsCheck(expr) => expr.clone_with_children(children),
            Self::BinaryOp(expr) => expr.clone_with_children(children),
            Self::Any(expr) => expr.clone_with_children(children),
            Self::All(expr) => expr.clone_with_children(children),
            Self::UnaryOp(expr) => expr.clone_with_children(children),
            Self::Convert(expr) => expr.clone_with_children(children),
            Self::CeilFloor(expr) => expr.clone_with_children(children),
            Self::Substring(expr) => expr.clone_with_children(children),
            Self::Literal(expr) => expr.clone_with_children(children),
        }
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        match self {
            Self::Constant(_) => Ok(()), // Constants are always valid
            Self::ColumnRef(expr) => {
                // Validate column reference exists in schema
                let col_idx = expr.get_column_index();
                if col_idx >= schema.get_column_count() as usize {
                    let error = ExpressionError::InvalidColumnReference(format!(
                        "Column index {} out of bounds for schema with {} columns",
                        col_idx,
                        schema.get_column_count()
                    ));
                    return Err(error);
                }
                Ok(())
            },
            // Other expression types validate their children
            _ => {
                for child in self.get_children() {
                    child.validate(schema)?;
                }
                Ok(())
            },
        }
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Constant(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::ColumnRef(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Arithmetic(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Comparison(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Logic(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::String(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Array(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Assignment(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Mock(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Aggregate(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Window(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Cast(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Case(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Function(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Subquery(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::In(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Between(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Like(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Extract(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Exists(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Regex(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Coalesce(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Random(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Trim(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Interval(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::GroupingSets(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Filter(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::IsDistinct(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Position(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Method(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Struct(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Overlay(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Collate(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::AtTimeZone(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::MapAccess(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Tuple(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Wildcard(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::QualifiedWildcard(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::TypedString(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Subscript(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::IsCheck(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::BinaryOp(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Any(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::All(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::UnaryOp(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Convert(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::CeilFloor(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Substring(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::Literal(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
            Self::DateTime(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            },
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::mock_expression::MockExpression;
    use crate::types_db::type_id::TypeId;

    #[test]
    fn constant_expression() {
        let value = Value::new(42);
        let ret_type = Column::new("const", TypeId::Integer);
        let expr = Expression::Constant(ConstantExpression::new(value.clone(), ret_type, vec![]));

        let schema = Schema::new(vec![]);
        let rid = RID::new(0, 0);
        let tuple = Tuple::new(&[], &schema, rid);

        assert_eq!(expr.evaluate(&tuple, &schema).unwrap(), value);
        assert_eq!(expr.get_children().len(), 0);
        assert_eq!(expr.to_string(), "42");
    }

    #[test]
    fn test_mock_expression() {
        let mock = MockExpression::new("test".to_string(), TypeId::Integer);
        let expr = Expression::Mock(mock);

        let schema = Schema::new(vec![]);
        let rid = RID::new(0, 0);
        let tuple = Tuple::new(&[], &schema, rid);

        // The actual evaluation will depend on your MockExpression implementation
        assert!(expr.evaluate(&tuple, &schema).is_ok());
        assert_eq!(expr.get_children().len(), 0);
        assert!(expr.to_string().contains("test"));
    }

    #[test]
    fn test_mock_expression_in_children() {
        let mock = MockExpression::new("test".to_string(), TypeId::Integer);
        let mock_expr = Arc::new(Expression::Mock(mock));

        // Create an array expression with mock child
        let array_expr = Expression::Array(ArrayExpression::new(
            vec![mock_expr],
            Column::new("test", TypeId::Integer),
        ));

        assert_eq!(array_expr.get_children().len(), 1);
        match &array_expr.get_children()[0].as_ref() {
            Expression::Mock(_) => (),
            _ => panic!("Expected Mock expression"),
        }
    }

    #[test]
    fn test_window_expression() {
        use crate::sql::execution::plans::window_plan::WindowFunctionType;

        let function_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("salary", TypeId::Integer),
            vec![],
        )));

        let window_expr = Expression::Window(WindowExpression::new(
            WindowFunctionType::Sum,
            function_expr,
            vec![],
            vec![],
            Column::new("total", TypeId::Integer),
        ));

        let schema = Schema::new(vec![Column::new("salary", TypeId::Integer)]);
        let tuple = Tuple::new(&[Value::new(100)], &schema, RID::new(0, 0));

        // Window functions can't be evaluated on a single tuple
        assert!(window_expr.evaluate(&tuple, &schema).is_err());
        assert_eq!(window_expr.to_string(), "Sum(salary)");
    }
}
