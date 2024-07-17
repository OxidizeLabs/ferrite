// Every possible SQL type ID
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeId {
    Invalid = 0,
    Boolean,
    Integer,
    Decimal,
    VarChar,
    Timestamp,
    Vector,
    TinyInt,
    SmallInt,
    BigInt
}


// fn main() {
//     let type_id = TypeId::Integer;
//     println!("{:?}", type_id);
// }
