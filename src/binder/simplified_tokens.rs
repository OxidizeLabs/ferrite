use std::fmt;
use std::fmt::Display;

/// Simplified tokens are a simplified (dense) representation of the lexer
/// used for simple syntax highlighting in the tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimplifiedTokenType {
    Identifier,
    NumericConstant,
    StringConstant,
    Operator,
    Keyword,
    Comment,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimplifiedToken {
    pub token_type: SimplifiedTokenType,
    pub start: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeywordCategory {
    Reserved,
    Unreserved,
    TypeFunc,
    ColName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParserKeyword {
    pub name: String,
    pub category: KeywordCategory,
}

impl Display for SimplifiedTokenType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SimplifiedTokenType::Identifier => write!(f, "Identifier"),
            SimplifiedTokenType::NumericConstant => write!(f, "NumericConstant"),
            SimplifiedTokenType::StringConstant => write!(f, "StringConstant"),
            SimplifiedTokenType::Operator => write!(f, "Operator"),
            SimplifiedTokenType::Keyword => write!(f, "Keyword"),
            SimplifiedTokenType::Comment => write!(f, "Comment"),
        }
    }
}

impl Display for KeywordCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeywordCategory::Reserved => write!(f, "Reserved"),
            KeywordCategory::Unreserved => write!(f, "Unreserved"),
            KeywordCategory::TypeFunc => write!(f, "TypeFunc"),
            KeywordCategory::ColName => write!(f, "ColName"),
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn simplified_token() {
        let token = SimplifiedToken {
            token_type: SimplifiedTokenType::Keyword,
            start: 10,
        };
        assert_eq!(token.token_type, SimplifiedTokenType::Keyword);
        assert_eq!(token.start, 10);
    }

    #[test]
    fn parser_keyword() {
        let keyword = ParserKeyword {
            name: "SELECT".to_string(),
            category: KeywordCategory::Reserved,
        };
        assert_eq!(keyword.name, "SELECT");
        assert_eq!(keyword.category, KeywordCategory::Reserved);
    }

    #[test]
    fn display_implementations() {
        assert_eq!(format!("{}", SimplifiedTokenType::Identifier), "Identifier");
        assert_eq!(format!("{}", KeywordCategory::TypeFunc), "TypeFunc");
    }
}