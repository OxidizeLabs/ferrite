use std::rc::Rc;
use crate::catalogue::schema::Schema;

#[derive(Debug, Clone)]
pub struct IndexScanNode {
    output_schema: Rc<Schema>,
}