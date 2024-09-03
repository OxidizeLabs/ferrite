use crate::catalogue::schema::Schema;
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct IndexScanNode {
    output_schema: Rc<Schema>,
}