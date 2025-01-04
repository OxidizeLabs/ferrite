use crate::common::rid::RID;
use crate::concurrency::transaction::Transaction;
use crate::storage::index::index::{Index, IndexInfo};
use crate::storage::table::tuple::Tuple;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::Val;
use log::debug;
use parking_lot::RwLock;
use std::fmt::Debug;
use std::sync::Arc;
use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::catalogue::schema::Schema;
use crate::common::config::IndexOidT;
use crate::storage::index::index::IndexType::BPlusTreeIndex;

#[derive(Debug, Clone, PartialEq)]
pub enum NodeType {
    Internal,
    Leaf,
}

#[derive(Debug, Clone)]
struct BPlusTreeNode {
    node_type: NodeType,
    keys: Vec<Tuple>,
    children: Vec<Arc<RwLock<BPlusTreeNode>>>,
    values: Vec<RID>,
    next_leaf: Option<Arc<RwLock<BPlusTreeNode>>>,
}

pub struct BPlusTree {
    root: Arc<RwLock<BPlusTreeNode>>,
    order: usize,
    metadata: Option<Box<IndexInfo>>,
}
impl BPlusTreeNode {
    fn new(node_type: NodeType) -> Self {
        BPlusTreeNode {
            node_type,
            keys: Vec::new(),
            children: Vec::new(),
            values: Vec::new(),
            next_leaf: None,
        }
    }
}

impl BPlusTree {
    pub fn new(order: usize) -> Self {
        BPlusTree {
            root: Arc::new(RwLock::new(BPlusTreeNode::new(NodeType::Leaf))),
            order,
            metadata: None,
        }
    }

    pub fn new_with_metadata(order: usize, metadata: Box<IndexInfo>) -> Self {
        BPlusTree {
            root: Arc::new(RwLock::new(BPlusTreeNode::new(NodeType::Leaf))),
            order,
            metadata: Some(metadata),
        }
    }

    pub fn insert(&self, key: Tuple, rid: RID) -> bool {
        let mut root_guard = self.root.write();

        // Split root if full
        if root_guard.keys.len() == self.order - 1 {
            let new_root = BPlusTreeNode::new(NodeType::Internal);
            let old_root = std::mem::replace(&mut *root_guard, new_root);
            root_guard.children.push(Arc::new(RwLock::new(old_root)));
            self.split_child(&mut root_guard, 0);
        }

        self.insert_non_full(&mut root_guard, key, rid)
    }

    pub fn delete(&self, key: &Tuple, rid: RID) -> bool {
        let mut success = false;
        let mut current_node = Arc::clone(&self.root);
        let mut path: Vec<(Arc<RwLock<BPlusTreeNode>>, usize)> = Vec::new();

        // First, find the leaf node containing the key
        loop {
            let node = current_node.read();
            match node.node_type {
                NodeType::Internal => {
                    let pos = node
                        .keys
                        .iter()
                        .position(|k| self.compare_keys(k, key).is_gt())
                        .unwrap_or(node.keys.len());

                    let next_node = Arc::clone(&node.children[pos]);
                    path.push((Arc::clone(&current_node), pos));
                    drop(node);
                    current_node = next_node;
                }
                NodeType::Leaf => {
                    // Find exact key-RID pair position
                    let delete_pos = node
                        .keys
                        .iter()
                        .enumerate()
                        .find(|(idx, k)| {
                            self.compare_keys(k, key).is_eq() && node.values[*idx] == rid
                        })
                        .map(|(idx, _)| idx);

                    // If found, perform deletion
                    if let Some(pos) = delete_pos {
                        drop(node);
                        // Get write lock to perform deletion
                        let mut node = current_node.write();
                        node.keys.remove(pos);
                        node.values.remove(pos);
                        success = true;
                    }
                    break;
                }
            }
        }

        // If we deleted something and node is underfull, rebalance
        if success {
            self.rebalance_after_delete(path);
        }

        success
    }

    pub fn scan_range(&self, start_key: &Tuple, end_key: &Tuple, result: &mut Vec<(Tuple, RID)>) {
        debug!(
            "Starting range scan from key {:?} to {:?}",
            start_key.get_value(0),
            end_key.get_value(0)
        );

        let mut current_node = Arc::clone(&self.root);

        // Find starting leaf node
        loop {
            let node = current_node.read();
            match node.node_type {
                NodeType::Internal => {
                    let pos = node
                        .keys
                        .iter()
                        .position(|k| self.compare_keys(k, start_key).is_gt())
                        .unwrap_or(node.keys.len());
                    debug!(
                        "At internal node with {} keys, going to child {}",
                        node.keys.len(),
                        pos
                    );
                    let next_node = Arc::clone(&node.children[pos]);
                    drop(node);
                    current_node = next_node;
                }
                NodeType::Leaf => break,
            }
        }

        // Process leaf nodes
        let mut leaf_count = 0;
        loop {
            let node = current_node.read();
            debug_assert!(matches!(node.node_type, NodeType::Leaf));
            leaf_count += 1;
            debug!(
                "Processing leaf node {} with {} keys",
                leaf_count,
                node.keys.len()
            );

            // Find starting position in current leaf
            let start_pos = node
                .keys
                .iter()
                .position(|k| self.compare_keys(k, start_key).is_ge())
                .unwrap_or(node.keys.len());
            debug!("Start position in current leaf: {}", start_pos);

            // Process all qualifying keys in this leaf
            for i in start_pos..node.keys.len() {
                let current_key = &node.keys[i];

                // Check if we've passed the end key
                match self.compare_keys(current_key, end_key) {
                    std::cmp::Ordering::Greater => {
                        debug!("Found key greater than end key, stopping scan");
                        break;
                    }
                    std::cmp::Ordering::Equal | std::cmp::Ordering::Less => {
                        debug!("Adding key to result: {:?}", current_key.get_value(0));
                        result.push((current_key.clone(), node.values[i]));
                    }
                }
            }

            // Move to next leaf if exists
            match &node.next_leaf {
                Some(next_leaf) => {
                    debug!("Moving to next leaf node");
                    let next_node = Arc::clone(next_leaf);
                    drop(node);
                    current_node = next_node;
                }
                None => {
                    debug!("No more leaf nodes, ending scan");
                    break;
                }
            }

            // Safety check
            if leaf_count > 100 {
                debug!("Safety limit reached, possible cycle in leaf nodes");
                break;
            }
        }

        // Sort the results based on key comparison
        result.sort_by(|(key1, _), (key2, _)| self.compare_keys(key1, key2));

        debug!("Range scan complete, found {} results", result.len());
    }

    /// Visualizes the B+ tree structure, showing:
    /// - Internal nodes with their keys
    /// - Leaf nodes with keys and values
    /// - Leaf node links
    /// Returns a string representation of the tree
    pub fn visualize(&self) -> String {
        let mut result = String::new();
        result.push_str("B+ Tree Visualization:\n");
        result.push_str("===================\n\n");

        // Helper to format a key-value for display
        let format_key = |key: &Tuple| -> String {
            match key.get_value(0).get_value() {
                Val::Integer(i) => i.to_string(),
                Val::VarLen(s) => s.clone(),
                _ => format!("{:?}", key.get_value(0)),
            }
        };

        // Track leaf nodes for drawing links
        let mut leaf_nodes = Vec::new();

        // Recursive helper function to build tree structure
        fn visualize_node(
            node: &BPlusTreeNode,
            level: usize,
            index: usize,
            format_key: &dyn Fn(&Tuple) -> String,
            result: &mut String,
            leaf_nodes: &mut Vec<String>,
        ) {
            let indent = "    ".repeat(level);
            let node_prefix = if level == 0 { "Root" } else { "Node" };

            match node.node_type {
                NodeType::Internal => {
                    result.push_str(&format!(
                        "{}{}[{:02}] Internal: [{}]\n",
                        indent,
                        node_prefix,
                        index,
                        node.keys
                            .iter()
                            .map(|k| format_key(k))
                            .collect::<Vec<_>>()
                            .join("|")
                    ));

                    // Visualize children
                    for (i, child) in node.children.iter().enumerate() {
                        visualize_node(&child.read(), level + 1, i, format_key, result, leaf_nodes);
                    }
                }
                NodeType::Leaf => {
                    // Format leaf node representation
                    let leaf_str = format!(
                        "{}{}[{:02}] Leaf: [{}]",
                        indent,
                        node_prefix,
                        index,
                        node.keys
                            .iter()
                            .enumerate()
                            .map(|(i, k)| format!("{}→{}", format_key(k), node.values[i]))
                            .collect::<Vec<_>>()
                            .join("|")
                    );
                    result.push_str(&leaf_str);
                    result.push('\n');

                    // Store leaf node for link visualization
                    leaf_nodes.push(leaf_str);
                }
            }
        }

        // Start visualization from root
        visualize_node(
            &self.root.read(),
            0,
            0,
            &format_key,
            &mut result,
            &mut leaf_nodes,
        );

        // Add leaf node linkage visualization
        if !leaf_nodes.is_empty() {
            result.push_str("\nLeaf Node Links:\n");
            result.push_str("===============\n");

            let mut current = Arc::clone(&self.root);

            // Find leftmost leaf
            loop {
                let node = current.read();
                match node.node_type {
                    NodeType::Internal => {
                        let next = Arc::clone(&node.children[0]);
                        drop(node);
                        current = next;
                    }
                    NodeType::Leaf => break,
                }
            }

            // Follow leaf node links
            let mut link_count = 0;
            result.push_str("Leaf[00]");

            loop {
                let node = current.read();
                match &node.next_leaf {
                    Some(next) => {
                        result.push_str(" → ");
                        link_count += 1;
                        result.push_str(&format!("Leaf[{:02}]", link_count));
                        let next_node = Arc::clone(next);
                        drop(node);
                        current = next_node;
                    }
                    None => {
                        result.push_str(" → ∅\n");
                        break;
                    }
                }

                // Safety check
                if link_count > 100 {
                    result.push_str("\nWarning: Possible cycle in leaf nodes!");
                    break;
                }
            }
        }

        result
    }

    fn insert_non_full(&self, node: &mut BPlusTreeNode, key: Tuple, rid: RID) -> bool {
        match node.node_type {
            NodeType::Leaf => {
                let pos = node
                    .keys
                    .iter()
                    .position(|k| self.compare_keys(k, &key).is_ge())
                    .unwrap_or(node.keys.len());

                node.keys.insert(pos, key);
                node.values.insert(pos, rid);
                true
            }
            NodeType::Internal => {
                // First, find the position where we should insert
                let pos = node
                    .keys
                    .iter()
                    .position(|k| self.compare_keys(k, &key).is_gt())
                    .unwrap_or(node.keys.len());

                // Check if we need to split child before getting the write lock
                let should_split = {
                    let child_guard = node.children[pos].read();
                    let needs_split = child_guard.keys.len() == self.order - 1;
                    drop(child_guard); // Explicitly drop the read guard
                    needs_split
                };

                // Split if necessary
                if should_split {
                    self.split_child(node, pos);
                    // Recalculate position after split
                    let new_pos = if !node.keys.is_empty()
                        && self.compare_keys(&node.keys[pos], &key).is_lt()
                    {
                        pos + 1
                    } else {
                        pos
                    };
                    let mut child_guard = node.children[new_pos].write();
                    self.insert_non_full(&mut child_guard, key, rid)
                } else {
                    // No split needed, proceed with insertion
                    let mut child_guard = node.children[pos].write();
                    self.insert_non_full(&mut child_guard, key, rid)
                }
            }
        }
    }

    fn split_child(&self, parent: &mut BPlusTreeNode, child_idx: usize) {
        debug!("Splitting child at index {}", child_idx);

        let new_node = {
            let child = &parent.children[child_idx];
            let mut child_guard = child.write();
            let middle = self.order / 2;

            let mut new_node = BPlusTreeNode::new(child_guard.node_type.clone());

            // Split keys and values
            new_node.keys = child_guard.keys.split_off(middle);
            if let NodeType::Leaf = child_guard.node_type {
                new_node.values = child_guard.values.split_off(middle);
                // Maintain leaf node chain
                new_node.next_leaf = child_guard.next_leaf.take();
                let new_node_arc = Arc::new(RwLock::new(new_node.clone()));
                child_guard.next_leaf = Some(Arc::clone(&new_node_arc));
                debug!("Updated leaf node links during split");
            } else {
                new_node.children = child_guard.children.split_off(middle + 1);
                let mid_key = child_guard.keys.pop().unwrap();
                parent.keys.insert(child_idx, mid_key);
            }

            new_node
        };

        // Insert new node into parent
        parent
            .children
            .insert(child_idx + 1, Arc::new(RwLock::new(new_node)));
        debug!("Completed node split");
    }

    fn compare_keys(&self, key1: &Tuple, key2: &Tuple) -> std::cmp::Ordering {
        // Compare tuples based on the indexed columns defined in metadata
        let metadata = self.metadata.as_ref().expect("Metadata not initialized");
        let key_attrs = metadata.get_key_attrs();
        for &attr in key_attrs {
            let val1 = key1.get_value(attr);
            let val2 = key2.get_value(attr);

            // Use explicit comparisons to handle all comparison cases
            match (
                val1.compare_less_than(val2),
                val1.compare_greater_than(val2),
            ) {
                (CmpBool::CmpTrue, _) => return std::cmp::Ordering::Less,
                (_, CmpBool::CmpTrue) => return std::cmp::Ordering::Greater,
                _ => continue, // If values are equal, continue to next attribute
            }
        }
        std::cmp::Ordering::Equal
    }

    fn rebalance_after_delete(&self, path: Vec<(Arc<RwLock<BPlusTreeNode>>, usize)>) {
        let min_keys = (self.order - 1) / 2;

        for (node_arc, child_idx) in path.iter().rev() {
            let node = node_arc.read();
            let child = node.children[*child_idx].read();

            if child.keys.len() < min_keys {
                drop(child);
                drop(node);

                let mut parent = node_arc.write();
                self.rebalance_node(&mut parent, *child_idx);
            } else {
                // No rebalancing needed at this level
                break;
            }
        }
    }

    fn rebalance_node(&self, parent: &mut BPlusTreeNode, child_idx: usize) {
        let min_keys = (self.order - 1) / 2;

        // Try borrowing from left sibling
        if child_idx > 0 {
            let mut left_sibling = parent.children[child_idx - 1].write();
            if left_sibling.keys.len() > min_keys {
                let mut child = parent.children[child_idx].write();
                child.keys.insert(0, parent.keys[child_idx - 1].clone());
                parent.keys[child_idx - 1] = left_sibling.keys.pop().unwrap();

                if let NodeType::Internal = child.node_type {
                    child
                        .children
                        .insert(0, left_sibling.children.pop().unwrap());
                } else {
                    child.values.insert(0, left_sibling.values.pop().unwrap());
                }
                return;
            }
        }

        // Try borrowing from right sibling
        if child_idx < parent.children.len() - 1 {
            let mut right_sibling = parent.children[child_idx + 1].write();
            if right_sibling.keys.len() > min_keys {
                let mut child = parent.children[child_idx].write();
                child.keys.push(parent.keys[child_idx].clone());
                parent.keys[child_idx] = right_sibling.keys.remove(0);

                if let NodeType::Internal = child.node_type {
                    child.children.push(right_sibling.children.remove(0));
                } else {
                    child.values.push(right_sibling.values.remove(0));
                }
                return;
            }
        }

        // If we can't borrow, we need to merge
        self.merge_nodes(parent, child_idx);
    }

    fn merge_nodes(&self, parent: &mut BPlusTreeNode, child_idx: usize) {
        let merge_idx = if child_idx > 0 {
            child_idx - 1
        } else {
            child_idx
        };
        let (left_idx, right_idx) = (merge_idx, merge_idx + 1);

        {
            let mut left = parent.children[left_idx].write();
            let mut right = parent.children[right_idx].write();

            // Move parent key down
            left.keys.push(parent.keys[merge_idx].clone());

            // Move all keys and children from right to left
            left.keys.extend(right.keys.drain(..));
            if let NodeType::Internal = left.node_type {
                left.children.extend(right.children.drain(..));
            } else {
                left.values.extend(right.values.drain(..));
            }
        }

        // Remove the merged node from parent
        parent.keys.remove(merge_idx);
        parent.children.remove(right_idx);
    }
}

impl Index for BPlusTree {
    fn new(metadata: Box<IndexInfo>) -> Self {
        BPlusTree::new_with_metadata(4, metadata)
    }

    fn get_metadata(&self) -> &IndexInfo {
        self.metadata
            .as_ref()
            .expect("Index metadata not initialized")
    }

    fn insert_entry(&self, key: &Tuple, rid: RID, _transaction: &Transaction) -> bool {
        let metadata = self.get_metadata();
        let key_schema = metadata.get_key_schema().clone();
        let key_attrs = metadata.get_key_attrs().clone();
        let key_tuple = key.key_from_tuple(key_schema, key_attrs);

        // Moving insert logic here instead of calling self.insert
        let mut root_guard = self.root.write();

        // Split root if full
        if root_guard.keys.len() == self.order - 1 {
            let new_root = BPlusTreeNode::new(NodeType::Internal);
            let old_root = std::mem::replace(&mut *root_guard, new_root);
            root_guard.children.push(Arc::new(RwLock::new(old_root)));
            self.split_child(&mut root_guard, 0);
        }

        self.insert_non_full(&mut root_guard, key_tuple, rid)
    }

    fn delete_entry(&self, key: &Tuple, rid: RID, _transaction: &Transaction) {
        let metadata = self.get_metadata();
        let key_schema = metadata.get_key_schema().clone();
        let key_attrs = metadata.get_key_attrs().clone();
        let key_tuple = key.key_from_tuple(key_schema, key_attrs);

        let mut success = false;
        let mut current_node = Arc::clone(&self.root);
        let mut path: Vec<(Arc<RwLock<BPlusTreeNode>>, usize)> = Vec::new();

        // Moving delete logic here
        loop {
            let node = current_node.read();
            match node.node_type {
                NodeType::Internal => {
                    let pos = node
                        .keys
                        .iter()
                        .position(|k| self.compare_keys(k, &key_tuple).is_gt())
                        .unwrap_or(node.keys.len());

                    let next_node = Arc::clone(&node.children[pos]);
                    path.push((Arc::clone(&current_node), pos));
                    drop(node);
                    current_node = next_node;
                }
                NodeType::Leaf => {
                    let delete_pos = node
                        .keys
                        .iter()
                        .enumerate()
                        .find(|(idx, k)| {
                            self.compare_keys(k, &key_tuple).is_eq() && node.values[*idx] == rid
                        })
                        .map(|(idx, _)| idx);

                    if let Some(pos) = delete_pos {
                        drop(node);
                        let mut node = current_node.write();
                        node.keys.remove(pos);
                        node.values.remove(pos);
                        success = true;
                    }
                    break;
                }
            }
        }

        if success {
            self.rebalance_after_delete(path);
        }
    }

    fn scan_key(&self, key: &Tuple, result: &mut Vec<RID>, _transaction: &Transaction) {
        let metadata = self.get_metadata();
        let key_schema = metadata.get_key_schema().clone();
        let key_attrs = metadata.get_key_attrs().clone();
        let search_key = key.key_from_tuple(key_schema, key_attrs);

        // Moving scan logic here
        let mut current_node = Arc::clone(&self.root);

        loop {
            let node = current_node.read();
            match node.node_type {
                NodeType::Internal => {
                    let pos = node
                        .keys
                        .iter()
                        .position(|k| self.compare_keys(k, &search_key).is_gt())
                        .unwrap_or(node.keys.len());

                    let next_node = Arc::clone(&node.children[pos]);
                    drop(node);
                    current_node = next_node;
                }
                NodeType::Leaf => {
                    for (idx, k) in node.keys.iter().enumerate() {
                        if self.compare_keys(k, &search_key).is_eq() {
                            result.push(node.values[idx]);
                        }
                    }
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod test_utils {
    use super::*;
    use crate::catalogue::column::Column;
    use crate::catalogue::schema::Schema;
    use crate::storage::index::index::{Index, IndexInfo, IndexType};
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    pub fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::VarChar),
        ])
    }

    pub fn create_key_schema() -> Schema {
        // Key schema only includes the 'id' column
        Schema::new(vec![Column::new("id", TypeId::Integer)])
    }

    pub fn create_tuple(id: i32, value: &str, schema: &Schema) -> Tuple {
        let values = vec![Value::new(id), Value::new(value)];
        Tuple::new(&values, schema.clone(), RID::new(0, 0))
    }

    pub fn create_test_metadata(order: usize) -> IndexInfo {
        IndexInfo::new(
            create_key_schema(), // Use key schema for metadata
            "test_index".to_string(),
            0,
            "test_table".to_string(),
            4,
            false,
            IndexType::BPlusTreeIndex,
            vec![0], // Index on 'id' column
        )
    }

    pub fn create_test_tree(order: usize) -> BPlusTree {
        let metadata = create_test_metadata(order);
        BPlusTree::new_with_metadata(order, Box::new(metadata))
    }
}

#[cfg(test)]
mod unit_tests {
    use super::test_utils::*;
    use super::*;

    #[test]
    fn test_create_tree() {
        let tree = create_test_tree(4);
        let root = tree.root.read();
        assert_eq!(root.node_type, NodeType::Leaf);
        assert!(root.keys.is_empty());
        assert!(root.values.is_empty());

        // Verify metadata is initialized
        assert!(tree.metadata.is_some());
    }

    #[test]
    fn test_compare_keys() {
        let schema = create_test_schema();
        let tuple1 = create_tuple(1, "value1", &schema);
        let tuple2 = create_tuple(2, "value2", &schema);
        let tree = create_test_tree(4);

        assert_eq!(
            tree.compare_keys(&tuple1, &tuple2),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            tree.compare_keys(&tuple2, &tuple1),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            tree.compare_keys(&tuple1, &tuple1),
            std::cmp::Ordering::Equal
        );
    }

    #[test]
    fn test_node_creation() {
        let leaf = BPlusTreeNode::new(NodeType::Leaf);
        let internal = BPlusTreeNode::new(NodeType::Internal);
        assert_eq!(leaf.node_type, NodeType::Leaf);
        assert_eq!(internal.node_type, NodeType::Internal);
    }

    #[test]
    fn test_metadata_initialization() {
        let tree = create_test_tree(4);
        let metadata = tree
            .metadata
            .as_ref()
            .expect("Metadata should be initialized");
        assert_eq!(metadata.get_key_attrs(), &vec![0]);
        assert_eq!(metadata.get_key_schema().get_column_count(), 1);
        assert_eq!(metadata.get_index_column_count(), 1);
    }
}

#[cfg(test)]
mod basic_behavior_tests {
    use super::test_utils::*;
    use super::*;
    use crate::common::config::TxnId;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::types_db::value::{Val, Value};
    use std::collections::HashSet;

    fn get_integer_from_value(value: &Value) -> i32 {
        match value.get_value() {
            Val::Integer(i) => *i,
            _ => panic!("Expected integer value"),
        }
    }

    #[test]
    fn test_basic_insertion() {
        let schema = create_test_schema();
        let tree = create_test_tree(4);
        let tuple1 = create_tuple(1, "value1", &schema);
        let tuple2 = create_tuple(2, "value2", &schema);

        assert!(tree.insert(tuple1.clone(), RID::new(0, 1)));
        assert!(tree.insert(tuple2.clone(), RID::new(0, 2)));

        let root = tree.root.read();
        assert!(!root.keys.is_empty());
    }

    #[test]
    fn test_basic_deletion() {
        let schema = create_test_schema();
        let tree = create_test_tree(4);
        let tuple = create_tuple(1, "value1", &schema);
        let rid = RID::new(0, 1);

        tree.insert(tuple.clone(), rid);
        assert!(tree.delete(&tuple, rid));

        let mut result = Vec::new();
        tree.scan_key(
            &tuple,
            &mut result,
            &Transaction::new(0, IsolationLevel::ReadUncommitted),
        );
        assert!(result.is_empty());
    }

    #[test]
    fn test_range_scan() {
        initialize_logger();
        let schema = create_test_schema();
        let tree = create_test_tree(4);

        // Insert test data ensuring full tuples
        for i in 1..=5 {
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            tree.insert_entry(
                &tuple,
                RID::new(0, i as u32),
                &Transaction::new(i as TxnId, IsolationLevel::ReadCommitted),
            );
        }

        // Visualize tree to understand its structure
        println!("Tree Structure:\n{}", tree.visualize());

        // Test scanning range [2, 4]
        let start = create_tuple(2, "value2", &schema);
        let end = create_tuple(4, "value4", &schema);
        let mut result = Vec::new();
        tree.scan_range(&start, &end, &mut result);

        // Debug print results
        println!("Range Scan Results:");
        for (tuple, rid) in &result {
            println!("Key: {}, RID: {}", tuple.get_value(0), rid);
        }

        // Verify results
        assert_eq!(result.len(), 3, "Expected 3 values in range [2, 4]");

        // Check that we got the correct values
        let result_ids: HashSet<i32> = result
            .iter()
            .map(|(tuple, _)| get_integer_from_value(tuple.get_value(0)))
            .collect();
        let expected_ids: HashSet<i32> = vec![2, 3, 4].into_iter().collect();
        assert_eq!(result_ids, expected_ids);

        // Verify ordering
        let ids: Vec<i32> = result
            .iter()
            .map(|(tuple, _)| get_integer_from_value(tuple.get_value(0)))
            .collect();
        assert!(
            ids.windows(2).all(|w| w[0] <= w[1]),
            "Results not in ascending order"
        );
    }

    #[test]
    fn test_empty_range_scan() {
        let schema = create_test_schema();
        let tree = create_test_tree(4);

        let start = create_tuple(2, "value2", &schema);
        let end = create_tuple(4, "value4", &schema);
        let mut result = Vec::new();
        tree.scan_range(&start, &end, &mut result);
        assert!(result.is_empty(), "Expected empty result for empty tree");
    }

    #[test]
    fn test_single_value_range_scan() {
        let schema = create_test_schema();
        let tree = create_test_tree(4);

        let tuple = create_tuple(3, "value3", &schema);
        tree.insert_entry(
            &tuple,
            RID::new(0, 3),
            &Transaction::new(0, IsolationLevel::ReadCommitted),
        );

        let mut result = Vec::new();
        tree.scan_range(&tuple, &tuple, &mut result);
        assert_eq!(result.len(), 1, "Expected single value for point query");

        // Verify the value
        assert_eq!(
            get_integer_from_value(result[0].0.get_value(0)),
            3,
            "Expected value 3 in point query result"
        );
    }

    #[test]
    fn test_leaf_node_links() {
        let schema = create_test_schema();
        let tree = create_test_tree(4);

        // Insert enough values to cause splits
        for i in (0..10).rev() {
            // Insert in reverse order
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            tree.insert_entry(
                &tuple,
                RID::new(0, i as u32),
                &Transaction::new(i as TxnId, IsolationLevel::ReadCommitted),
            );
        }

        // Scan entire range to verify leaf node links
        let start = create_tuple(0, "value0", &schema);
        let end = create_tuple(9, "value9", &schema);
        let mut result = Vec::new();
        tree.scan_range(&start, &end, &mut result);

        // Verify all values are present and in order
        assert_eq!(result.len(), 10, "Should have found all 10 values");

        let ids: Vec<i32> = result
            .iter()
            .map(|(tuple, _)| get_integer_from_value(tuple.get_value(0)))
            .collect();

        // Verify ordering
        for i in 0..9 {
            assert!(
                ids[i] < ids[i + 1],
                "Values not in ascending order at position {}: {} >= {}",
                i,
                ids[i],
                ids[i + 1]
            );
        }
    }
}

#[cfg(test)]
mod concurrency_tests {
    use super::test_utils::*;
    use super::*;
    use crate::common::config::TxnId;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_concurrent_insertions() {
        let schema = create_test_schema();
        let tree = Arc::new(create_test_tree(4));
        let mut handles = vec![];

        for i in 0..10 {
            let tree_clone = Arc::clone(&tree);
            let schema_clone = schema.clone();

            let handle = thread::spawn(move || {
                let tuple = create_tuple(i, &format!("value{}", i), &schema_clone);
                tree_clone.insert(tuple, RID::new(0, i as u32));
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let mut result = Vec::new();
        for i in 0..10 {
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            tree.scan_key(
                &tuple,
                &mut result,
                &Transaction::new(i as TxnId, IsolationLevel::ReadUncommitted),
            );
            assert!(!result.is_empty());
            result.clear();
        }
    }

    #[test]
    fn test_concurrent_reads_and_writes() {
        let schema = create_test_schema();
        let tree = Arc::new(create_test_tree(4));
        let mut handles = vec![];

        // Insert initial data
        for i in 0..5 {
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            tree.insert(tuple, RID::new(0, i as u32));
        }

        // Reader threads
        for _ in 0..3 {
            let tree_clone = Arc::clone(&tree);
            let schema_clone = schema.clone();
            let handle = thread::spawn(move || {
                let tuple = create_tuple(1, "value1", &schema_clone);
                let mut result = Vec::new();
                tree_clone.scan_key(
                    &tuple,
                    &mut result,
                    &Transaction::new(0, IsolationLevel::ReadUncommitted),
                );
            });
            handles.push(handle);
        }

        // Writer threads
        for i in 5..8 {
            let tree_clone = Arc::clone(&tree);
            let schema_clone = schema.clone();
            let handle = thread::spawn(move || {
                let tuple = create_tuple(i, &format!("value{}", i), &schema_clone);
                tree_clone.insert(tuple, RID::new(0, i as u32));
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}

#[cfg(test)]
mod edge_case_tests {
    use super::test_utils::*;
    use super::*;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};

    #[test]
    fn test_empty_tree_operations() {
        let schema = create_test_schema();
        let tree = create_test_tree(4);
        let tuple = create_tuple(1, "value1", &schema);

        let mut result = Vec::new();
        tree.scan_key(
            &tuple,
            &mut result,
            &Transaction::new(0, IsolationLevel::ReadUncommitted),
        );
        assert!(result.is_empty());
        assert!(!tree.delete(&tuple, RID::new(0, 1)));
    }

    #[test]
    fn test_duplicate_keys() {
        let schema = create_test_schema();
        let tree = create_test_tree(4);
        let tuple = create_tuple(1, "value1", &schema);

        assert!(tree.insert(tuple.clone(), RID::new(0, 1)));
        assert!(tree.insert(tuple.clone(), RID::new(0, 2))); // Different RID

        let mut result = Vec::new();
        tree.scan_key(
            &tuple,
            &mut result,
            &Transaction::new(0, IsolationLevel::ReadCommitted),
        );
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_max_order_boundary() {
        let schema = create_test_schema();
        let tree = create_test_tree(3);

        for i in 0..10 {
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            assert!(tree.insert(tuple, RID::new(0, i as u32)));
        }
    }

    #[test]
    fn test_delete_root_entries() {
        let schema = create_test_schema();
        let tree = create_test_tree(4);
        let tuple = create_tuple(1, "value1", &schema);
        let rid = RID::new(0, 1);

        tree.insert(tuple.clone(), rid);
        assert!(tree.delete(&tuple, rid));

        let root = tree.root.read();
        assert!(root.keys.is_empty());
        assert_eq!(root.node_type, NodeType::Leaf);
    }
}

#[cfg(test)]
mod visualization_tests {
    use super::test_utils::*;
    use super::*;
    use crate::common::config::TxnId;
    use crate::concurrency::transaction::IsolationLevel;
    use crate::types_db::value::Value;

    pub fn get_integer_from_value(value: &Value) -> i32 {
        match value.get_value() {
            Val::Integer(i) => *i,
            _ => panic!("Expected integer value"),
        }
    }

    #[test]
    fn test_tree_visualization() {
        let schema = create_test_schema();
        let tree = create_test_tree(4);

        // Empty tree
        println!("Empty tree:\n{}", tree.visualize());

        // Insert one value
        let tuple = create_tuple(1, "value1", &schema);
        tree.insert_entry(
            &tuple,
            RID::new(0, 1),
            &Transaction::new(0, IsolationLevel::ReadUncommitted),
        );
        println!("\nTree with one value:\n{}", tree.visualize());

        // Insert more values to cause splits
        for i in 2..=7 {
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            tree.insert_entry(
                &tuple,
                RID::new(0, i as u32),
                &Transaction::new(i as TxnId, IsolationLevel::ReadCommitted),
            );
        }
        println!("\nTree with multiple splits:\n{}", tree.visualize());

        // Verify visualization contains key information
        let viz = tree.visualize();
        assert!(viz.contains("Root"));
        assert!(viz.contains("Leaf"));
        assert!(viz.contains("Leaf Node Links"));
    }

    #[test]
    fn test_visualization_with_operations() {
        let schema = create_test_schema();
        let tree = create_test_tree(4);

        // Insert values in mixed order
        let values = vec![3, 1, 4, 2, 5];
        for &i in &values {
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            tree.insert_entry(
                &tuple,
                RID::new(0, i as u32),
                &Transaction::new(i as TxnId, IsolationLevel::ReadUncommitted),
            );
            println!("\nAfter inserting {}:\n{}", i, tree.visualize());
        }

        // Do a range scan
        let start = create_tuple(2, "value2", &schema);
        let end = create_tuple(4, "value4", &schema);
        let mut result = Vec::new();
        tree.scan_range(&start, &end, &mut result);

        println!("\nFinal tree state:\n{}", tree.visualize());
        println!(
            "\nRange scan result: {:?}",
            result
                .iter()
                .map(|(t, rid)| format!("{}→{:?}", get_integer_from_value(t.get_value(0)), rid))
                .collect::<Vec<_>>()
        );
    }
}
