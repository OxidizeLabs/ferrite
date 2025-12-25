use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum FunctionType {
    Scalar(ScalarFunctionType),
    Aggregate(AggregateFunctionType),
    Window(WindowFunctionType),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarFunctionType {
    String,   // UPPER, LOWER, CONCAT
    Numeric,  // ABS, ROUND
    DateTime, // NOW, EXTRACT
    Cast,     // CAST, CONVERT
    Other,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunctionType {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Statistical, // STDDEV, VARIANCE
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowFunctionType {
    Ranking,   // ROW_NUMBER, RANK
    Analytic,  // LEAD, LAG
    Aggregate, // SUM OVER, AVG OVER
}

impl fmt::Display for FunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionType::Scalar(st) => write!(f, "Scalar({:?})", st),
            FunctionType::Aggregate(at) => write!(f, "Aggregate({:?})", at),
            FunctionType::Window(wt) => write!(f, "Window({:?})", wt),
        }
    }
}

pub fn get_function_type(name: &str) -> FunctionType {
    match name.to_uppercase().as_str() {
        // Scalar String Functions
        "UPPER" | "LOWER" | "CONCAT" | "SUBSTRING" => {
            FunctionType::Scalar(ScalarFunctionType::String)
        },

        // Scalar Numeric Functions
        "ABS" | "ROUND" | "CEIL" | "FLOOR" => FunctionType::Scalar(ScalarFunctionType::Numeric),

        // Scalar DateTime Functions
        "NOW" | "EXTRACT" => FunctionType::Scalar(ScalarFunctionType::DateTime),

        // Cast Functions
        "CAST" | "CONVERT" => FunctionType::Scalar(ScalarFunctionType::Cast),

        // Aggregate Functions
        "COUNT" => FunctionType::Aggregate(AggregateFunctionType::Count),
        "SUM" => FunctionType::Aggregate(AggregateFunctionType::Sum),
        "AVG" => FunctionType::Aggregate(AggregateFunctionType::Avg),
        "MIN" => FunctionType::Aggregate(AggregateFunctionType::Min),
        "MAX" => FunctionType::Aggregate(AggregateFunctionType::Max),
        "STDDEV" | "VARIANCE" => FunctionType::Aggregate(AggregateFunctionType::Statistical),

        // Window Functions
        "ROW_NUMBER" | "RANK" | "DENSE_RANK" => FunctionType::Window(WindowFunctionType::Ranking),
        "LEAD" | "LAG" | "FIRST_VALUE" | "LAST_VALUE" => {
            FunctionType::Window(WindowFunctionType::Analytic)
        },

        // Default to Other Scalar Function
        _ => FunctionType::Scalar(ScalarFunctionType::Other),
    }
}
