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

#[derive(Debug, Clone)]
pub struct BPlusTree {
    root: Arc<RwLock<BPlusTreeNode>>,
    order: usize,
    metadata: Arc<IndexInfo>,
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
    pub fn new(order: usize, metadata: Arc<IndexInfo>) -> Self {
        BPlusTree {
            root: Arc::new(RwLock::new(BPlusTreeNode::new(NodeType::Leaf))),
            order,
            metadata,
        }
    }

    pub fn get_metadata(&self) -> Arc<IndexInfo> {
        Arc::clone(&self.metadata)
    }

    pub fn insert(&self, key: Tuple, rid: RID) -> bool {
        let mut root_guard = self.root.write();

        // Split root if full
        if root_guard.keys.len() == self.order - 1 {
            debug!("Root is full, creating new root");
            let new_root = BPlusTreeNode::new(NodeType::Internal);
            let old_root = std::mem::replace(&mut *root_guard, new_root);

            // Put old root as first child
            root_guard.children.push(Arc::new(RwLock::new(old_root)));

            if !self.split_child(&mut *root_guard, 0) {
                debug!("Root split failed");
                return false;
            }

            drop(root_guard);
            debug!("Root split complete, proceeding with insertion");
            self.insert_non_full(&mut *self.root.write(), key, rid)
        } else {
            self.insert_non_full(&mut root_guard, key, rid)
        }
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

    pub fn scan_range(
        &self,
        start_key: &Tuple,
        end_key: &Tuple,
        include_end: bool,
        result: &mut Vec<(Tuple, RID)>,
    ) {
        let mut current_node = Arc::clone(&self.root);
        let doing_full_scan = start_key.get_value(0) == end_key.get_value(0) &&
            match start_key.get_value(0).get_value() {
                Val::Integer(i) => i == &0, // Check if it's a zeroed value indicating no predicate
                _ => false,
            };

        // Find leftmost leaf for full scan, or appropriate leaf for range scan
        loop {
            let node = current_node.read();
            match node.node_type {
                NodeType::Internal => {
                    let pos = if doing_full_scan {
                        0 // Always go left for full scan
                    } else {
                        node.keys
                            .iter()
                            .position(|k| self.compare_keys(k, start_key).is_gt())
                            .unwrap_or(node.keys.len())
                    };
                    let next_node = Arc::clone(&node.children[pos]);
                    drop(node);
                    current_node = next_node;
                }
                NodeType::Leaf => break,
            }
        }

        // Process leaf nodes
        'outer: loop {
            let node = current_node.read();
            for i in 0..node.keys.len() {
                let current_key = &node.keys[i];

                // For full scan, add all entries
                if doing_full_scan {
                    result.push((current_key.clone(), node.values[i]));
                    continue;
                }

                // Otherwise do range comparison
                if self.compare_keys(current_key, start_key).is_lt() {
                    continue;
                }

                let end_comparison = self.compare_keys(current_key, end_key);
                if end_comparison.is_gt() ||
                    (end_comparison.is_eq() && !include_end) {
                    break 'outer;
                }

                result.push((current_key.clone(), node.values[i]));
            }

            match &node.next_leaf {
                Some(next_leaf) => {
                    let next_node = Arc::clone(next_leaf);
                    drop(node);
                    current_node = next_node;
                }
                None => break,
            }
        }

        result.sort_by(|(key1, _), (key2, _)| self.compare_keys(key1, key2));
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

                // Ensure the children vector is long enough
                if pos >= node.children.len() {
                    debug!(
                        "Position {} is out of bounds for children vector length {}",
                        pos,
                        node.children.len()
                    );
                    return false;
                }

                // Check if we need to split child before getting the write lock
                let should_split = {
                    let child_guard = node.children[pos].read();
                    let needs_split = child_guard.keys.len() == self.order - 1;
                    drop(child_guard); // Explicitly drop the read guard
                    needs_split
                };

                if should_split {
                    self.split_child(node, pos);
                    // Recalculate position after split
                    let new_pos = if !node.keys.is_empty()
                        && pos < node.keys.len()
                        && self.compare_keys(&node.keys[pos], &key).is_lt()
                    {
                        pos + 1
                    } else {
                        pos
                    };

                    // Verify new position is valid
                    if new_pos >= node.children.len() {
                        debug!("New position {} after split is out of bounds for children vector length {}",
                          new_pos, node.children.len());
                        return false;
                    }

                    let mut child_guard = node.children[new_pos].write();
                    self.insert_non_full(&mut child_guard, key, rid)
                } else {
                    let mut child_guard = node.children[pos].write();
                    self.insert_non_full(&mut child_guard, key, rid)
                }
            }
        }
    }

    fn split_child(&self, parent: &mut BPlusTreeNode, child_idx: usize) -> bool {
        if child_idx >= parent.children.len() {
            debug!(
                "Child index {} out of bounds (len={})",
                child_idx,
                parent.children.len()
            );
            return false;
        }

        debug!("Splitting child at index {}", child_idx);

        let (new_node_arc, split_key) = {
            let child = &parent.children[child_idx];
            let mut child_guard = child.write();
            let middle = self.order / 2;

            let mut new_node = BPlusTreeNode::new(child_guard.node_type.clone());

            match child_guard.node_type {
                NodeType::Leaf => {
                    // Create owned vectors of keys and values
                    let keys: Vec<_> = child_guard.keys.iter().cloned().collect();
                    let values: Vec<_> = child_guard.values.iter().cloned().collect();

                    // Create pairs and sort them
                    let mut entries: Vec<_> = keys.into_iter().zip(values.into_iter()).collect();
                    entries.sort_by(|(key1, rid1), (key2, rid2)| {
                        match self.compare_keys(key1, key2) {
                            std::cmp::Ordering::Equal => rid1.cmp(rid2),
                            ord => ord,
                        }
                    });

                    // Clear existing entries
                    child_guard.keys.clear();
                    child_guard.values.clear();

                    // Redistribute sorted entries
                    for (key, rid) in entries.iter().take(middle) {
                        child_guard.keys.push(key.clone());
                        child_guard.values.push(*rid);
                    }

                    for (key, rid) in entries.iter().skip(middle) {
                        new_node.keys.push(key.clone());
                        new_node.values.push(*rid);
                    }

                    let split_key = new_node.keys[0].clone();
                    let new_node_arc = Arc::new(RwLock::new(new_node));

                    // Update leaf chain
                    let next_leaf = child_guard.next_leaf.take();
                    let mut new_node_guard = new_node_arc.write();
                    new_node_guard.next_leaf = next_leaf;
                    drop(new_node_guard);
                    child_guard.next_leaf = Some(Arc::clone(&new_node_arc));

                    debug!("Leaf split - splitting at position {}", middle);
                    (new_node_arc, split_key)
                }
                NodeType::Internal => {
                    // For internal nodes:
                    let split_key = child_guard.keys[middle].clone();
                    new_node.keys = child_guard.keys[middle + 1..].to_vec();
                    child_guard.keys.truncate(middle);
                    new_node.children = child_guard.children[middle + 1..].to_vec();
                    child_guard.children.truncate(middle + 1);

                    let new_node_arc = Arc::new(RwLock::new(new_node));
                    debug!("Internal split - middle key at position {}", middle);
                    (new_node_arc, split_key)
                }
            }
        };

        // Insert split key and new node into parent
        debug!(
            "Parent before split - keys: {}, children: {}",
            parent.keys.len(),
            parent.children.len()
        );

        parent.keys.insert(child_idx, split_key);
        parent.children.insert(child_idx + 1, new_node_arc);

        debug!(
            "Parent after split - keys: {}, children: {}",
            parent.keys.len(),
            parent.children.len()
        );
        true
    }

    // Update compare_keys to use metadata directly
    pub(crate) fn compare_keys(&self, key1: &Tuple, key2: &Tuple) -> std::cmp::Ordering {
        let key_attrs = self.metadata.get_key_attrs();
        for &attr in key_attrs {
            let val1 = key1.get_value(attr);
            let val2 = key2.get_value(attr);

            let is_less = val1.compare_less_than(val2);
            let is_greater = val1.compare_greater_than(val2);

            match (is_less, is_greater) {
                (CmpBool::CmpTrue, _) => return std::cmp::Ordering::Less,
                (_, CmpBool::CmpTrue) => return std::cmp::Ordering::Greater,
                (CmpBool::CmpFalse, CmpBool::CmpFalse) => continue, // Equal for this attribute, check next
                _ => panic!("Invalid comparison state: cannot be both less and greater"),
            }
        }
        std::cmp::Ordering::Equal // All attributes were equal
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
        // Use a reasonable default order, or extract from metadata
        let order = 4; // Could also be configurable through metadata
        BPlusTree::new(order, Arc::new(*metadata))
    }

    fn get_metadata(&self) -> Arc<IndexInfo> {
        Arc::clone(&self.metadata)
    }

    fn insert_entry(&self, key: &Tuple, rid: RID, _transaction: &Transaction) -> bool {
        // Create key tuple using the metadata
        let key_schema = self.metadata.get_key_schema().clone();
        let key_attrs = self.metadata.get_key_attrs().clone();
        let key_tuple = key.key_from_tuple(key_schema, key_attrs);

        // Rest of the insertion logic
        let mut root_guard = self.root.write();
        if root_guard.keys.len() == self.order - 1 {
            let new_root = BPlusTreeNode::new(NodeType::Internal);
            let old_root = std::mem::replace(&mut *root_guard, new_root);
            root_guard.children.push(Arc::new(RwLock::new(old_root)));
            self.split_child(&mut root_guard, 0);
        }
        self.insert_non_full(&mut root_guard, key_tuple, rid)
    }

    fn delete_entry(&self, key: &Tuple, rid: RID, _transaction: &Transaction) -> bool {
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
        success
    }

    fn scan_key(&self, key: &Tuple, result: &mut Vec<RID>, _transaction: &Transaction) {
        let mut current_node = Arc::clone(&self.root);

        // Find the first leaf node
        loop {
            let next_node = {
                let node = current_node.read();
                match node.node_type {
                    NodeType::Internal => {
                        let pos = node
                            .keys
                            .iter()
                            .position(|k| self.compare_keys(k, key).is_gt())
                            .unwrap_or(node.keys.len());
                        Some(Arc::clone(&node.children[pos]))
                    }
                    NodeType::Leaf => None,
                }
            };

            match next_node {
                Some(next) => current_node = next,
                None => break,
            }
        }

        // Process all leaf nodes until we find one with no matching keys
        loop {
            let next_leaf = {
                let node = current_node.read();

                // Process current leaf
                for (idx, k) in node.keys.iter().enumerate() {
                    if self.compare_keys(k, key).is_eq() {
                        result.push(node.values[idx]);
                    }
                }

                // Get next leaf if it exists
                node.next_leaf.as_ref().map(Arc::clone)
            };

            match next_leaf {
                Some(next) => current_node = next,
                None => break,
            }
        }
    }
}

#[cfg(test)]
mod test_utils {
    use super::*;
    use crate::catalogue::column::Column;
    use crate::catalogue::schema::Schema;
    use crate::storage::index::index::{IndexInfo, IndexType};
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

    pub fn create_test_metadata() -> IndexInfo {
        IndexInfo::new(
            create_key_schema(),
            "index_name".to_string(),
            0,
            "table_name".to_string(),
            4,
            false,
            IndexType::BPlusTreeIndex,
            vec![0], // Index on 'id' column
        )
    }

    pub fn create_test_tree(order: usize, index_info: Arc<IndexInfo>) -> Arc<RwLock<BPlusTree>> {
        Arc::new(RwLock::new(BPlusTree::new(order, index_info)))
    }
}

#[cfg(test)]
mod unit_tests {
    use super::test_utils::*;
    use super::*;

    #[test]
    fn test_create_tree() {
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let tree_guard = tree.read();
        let root = tree_guard.root.read();
        assert_eq!(root.node_type, NodeType::Leaf);
        assert!(root.keys.is_empty());
        assert!(root.values.is_empty());
    }

    #[test]
    fn test_compare_keys() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();

        let tuple1 = create_tuple(1, "value1", &schema);
        let tuple2 = create_tuple(2, "value2", &schema);
        let tree = create_test_tree(4, Arc::from(index_info));
        let tree_read_guard = tree.read();

        assert_eq!(
            tree_read_guard.compare_keys(&tuple1, &tuple2),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            tree_read_guard.compare_keys(&tuple2, &tuple1),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            tree_read_guard.compare_keys(&tuple1, &tuple1),
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
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let tree_read_guard = tree.read();
        let metadata = tree_read_guard.get_metadata();
        assert_eq!(metadata.get_key_attrs(), &vec![0]);
        assert_eq!(metadata.get_key_schema().get_column_count(), 1);
        assert_eq!(metadata.get_index_column_count(), 1);
    }
}

#[cfg(test)]
mod basic_behavior_tests {
    use super::test_utils::*;
    use super::*;
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
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let tree_write_guard = tree.write();
        let tuple1 = create_tuple(1, "value1", &schema);
        let tuple2 = create_tuple(2, "value2", &schema);

        assert!(tree_write_guard.insert(tuple1.clone(), RID::new(0, 1)));
        assert!(tree_write_guard.insert(tuple2.clone(), RID::new(0, 2)));

        let root = tree_write_guard.root.read();
        assert!(!root.keys.is_empty());
    }

    #[test]
    fn test_basic_deletion() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let tree_write_guard = tree.write();
        let tuple = create_tuple(1, "value1", &schema);
        let rid = RID::new(0, 1);

        tree_write_guard.insert(tuple.clone(), rid);
        assert!(tree_write_guard.delete(&tuple, rid));

        let mut result = Vec::new();
        tree_write_guard.scan_key(
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
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let tree_write_guard = tree.write();

        // Insert test data ensuring full tuples
        for i in 1..=5 {
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            tree_write_guard.insert(tuple, RID::new(0, i as u32));
        }

        // Visualize tree to understand its structure
        println!("Tree Structure:\n{}", tree_write_guard.visualize());

        // Test scanning range [2, 4]
        let start = create_tuple(2, "value2", &schema);
        let end = create_tuple(4, "value4", &schema);
        let mut result: Vec<(_, _)> = Vec::new();
        tree_write_guard.scan_range(&start, &end, true, &mut result);

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
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let tree_read_guard = tree.read();

        let start = create_tuple(2, "value2", &schema);
        let end = create_tuple(4, "value4", &schema);
        let mut result = Vec::new();
        tree_read_guard.scan_range(&start, &end, true, &mut result);
        assert!(result.is_empty(), "Expected empty result for empty tree");
    }

    #[test]
    fn test_single_value_range_scan() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let tree_write_guard = tree.write();

        let tuple = create_tuple(3, "value3", &schema);
        tree_write_guard.insert(tuple.clone(), RID::new(0, 3));

        let mut result = Vec::new();
        tree_write_guard.scan_range(&tuple, &tuple, true, &mut result);
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
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let tree_write_guard = tree.write();

        // Insert enough values to cause splits
        for i in (0..10).rev() {
            // Insert in reverse order
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            tree_write_guard.insert(tuple, RID::new(0, i as u32));
        }

        // Scan entire range to verify leaf node links
        let start = create_tuple(0, "value0", &schema);
        let end = create_tuple(9, "value9", &schema);
        let mut result = Vec::new();
        tree_write_guard.scan_range(&start, &end, true, &mut result);

        // Verify all values are present and in order
        assert_eq!(result.len(), 10, "Should have found all 10 values");

        let ids: Vec<i32> = result
            .iter()
            .map(|(tuple, _): &(_, _)| get_integer_from_value(tuple.get_value(0)))
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
    use crate::common::logger::initialize_logger;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use log::debug;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;

    #[test]
    fn test_concurrent_insertions() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let mut handles = vec![];
        let success_flags = Vec::from_iter((0..10).map(|_| Arc::new(AtomicBool::new(false))));

        // Spawn insertion threads
        for i in 0..10 {
            let schema_clone = schema.clone();
            let tree_clone = Arc::clone(&tree);
            let success_flag = Arc::clone(&success_flags[i]);

            let handle = thread::spawn(move || {
                let tuple = create_tuple(i as i32, &format!("value{}", i), &schema_clone);
                let result = {
                    let tree_guard = tree_clone.write();
                    debug!("Thread {} acquiring write lock for insertion", i);
                    let success = tree_guard.insert(tuple, RID::new(0, i as u32));
                    debug!("Thread {} completed insertion: {}", i, success);
                    success
                };
                success_flag.store(result, Ordering::SeqCst);
            });
            handles.push(handle);
        }

        // Wait for all insertions to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all insertions were successful
        for (i, flag) in success_flags.iter().enumerate() {
            assert!(flag.load(Ordering::SeqCst), "Insertion {} failed", i);
        }

        // Verify we can read back all values
        let tree_guard = tree.read();
        debug!("Tree state after insertions:\n{}", tree_guard.visualize());

        let mut result = Vec::new();
        for i in 0..10 {
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            result.clear();
            tree_guard.scan_key(
                &tuple,
                &mut result,
                &Transaction::new(i as TxnId, IsolationLevel::ReadCommitted),
            );
            assert!(!result.is_empty(), "Value {} not found in tree", i);
        }
    }

    #[test]
    fn test_concurrent_reads_and_writes() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let mut handles = vec![];

        // Insert initial data
        {
            let tree_guard = tree.write();
            debug!("Inserting initial data");
            for i in 0..5 {
                let tuple = create_tuple(i, &format!("value{}", i), &schema);
                let success = tree_guard.insert(tuple, RID::new(0, i as u32));
                assert!(success, "Initial insertion {} failed", i);
            }
            debug!("Initial tree state:\n{}", tree_guard.visualize());
        }

        // Reader threads
        for id in 0..3 {
            let schema_clone = schema.clone();
            let tree_clone = Arc::clone(&tree);
            let handle = thread::spawn(move || {
                let tuple = create_tuple(1, "value1", &schema_clone);
                let mut result = Vec::new();
                let tree_guard = tree_clone.read();
                debug!("Reader {} scanning for key 1", id);
                tree_guard.scan_key(
                    &tuple,
                    &mut result,
                    &Transaction::new(0, IsolationLevel::ReadCommitted),
                );
                assert!(!result.is_empty(), "Reader {} failed to find value", id);
            });
            handles.push(handle);
        }

        // Writer threads
        for i in 5..8 {
            let schema_clone = schema.clone();
            let tree_clone = Arc::clone(&tree);
            let handle = thread::spawn(move || {
                let tuple = create_tuple(i, &format!("value{}", i), &schema_clone);
                let tree_guard = tree_clone.write();
                debug!("Writer inserting value {}", i);
                let success = tree_guard.insert(tuple, RID::new(0, i as u32));
                assert!(success, "Writer insertion {} failed", i);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify final state
        let tree_guard = tree.read();
        debug!("Final tree state:\n{}", tree_guard.visualize());

        let mut result = Vec::new();
        for i in 0..8 {
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            result.clear();
            tree_guard.scan_key(
                &tuple,
                &mut result,
                &Transaction::new(i as TxnId, IsolationLevel::ReadCommitted),
            );
            assert!(!result.is_empty(), "Value {} not found in final state", i);
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
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let tree_write_guard = tree.write();
        let tuple = create_tuple(1, "value1", &schema);

        let mut result = Vec::new();
        tree_write_guard.scan_key(
            &tuple,
            &mut result,
            &Transaction::new(0, IsolationLevel::ReadUncommitted),
        );
        assert!(result.is_empty());
        assert!(!tree_write_guard.delete(&tuple, RID::new(0, 1)));
    }

    #[test]
    fn test_duplicate_keys() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let tree_write_guard = tree.write();
        let tuple = create_tuple(1, "value1", &schema);

        assert!(tree_write_guard.insert(tuple.clone(), RID::new(0, 1)));
        assert!(tree_write_guard.insert(tuple.clone(), RID::new(0, 2))); // Different RID

        let mut result = Vec::new();
        tree_write_guard.scan_key(
            &tuple,
            &mut result,
            &Transaction::new(0, IsolationLevel::ReadCommitted),
        );
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_max_order_boundary() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let tree_write_guard = tree.write();

        for i in 0..10 {
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            assert!(tree_write_guard.insert(tuple, RID::new(0, i as u32)));
        }
    }

    #[test]
    fn test_delete_root_entries() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let tree_write_guard = tree.write();
        let tuple = create_tuple(1, "value1", &schema);
        let rid = RID::new(0, 1);

        tree_write_guard.insert(tuple.clone(), rid);
        assert!(tree_write_guard.delete(&tuple, rid));

        let root = tree_write_guard.root.read();
        assert!(root.keys.is_empty());
        assert_eq!(root.node_type, NodeType::Leaf);
    }
}

#[cfg(test)]
mod visualization_tests {
    use super::test_utils::*;
    use super::*;
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
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let tree_write_guard = tree.write();

        // Empty tree
        println!("Empty tree:\n{}", tree_write_guard.visualize());

        // Insert one value
        let tuple = create_tuple(1, "value1", &schema);
        tree_write_guard.insert(tuple, RID::new(0, 1));
        println!("\nTree with one value:\n{}", tree_write_guard.visualize());

        // Insert more values to cause splits
        for i in 2..=7 {
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            tree_write_guard.insert(tuple, RID::new(0, i as u32));
        }
        println!(
            "\nTree with multiple splits:\n{}",
            tree_write_guard.visualize()
        );

        // Verify visualization contains key information
        let viz = tree_write_guard.visualize();
        assert!(viz.contains("Root"));
        assert!(viz.contains("Leaf"));
        assert!(viz.contains("Leaf Node Links"));
    }

    #[test]
    fn test_visualization_with_operations() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let tree_write_guard = tree.write();

        // Insert values in mixed order
        let values = vec![3, 1, 4, 2, 5];
        for &i in &values {
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            tree_write_guard.insert(tuple, RID::new(0, i as u32));
            println!("\nAfter inserting {}:\n{}", i, tree_write_guard.visualize());
        }

        // Do a range scan
        let start = create_tuple(2, "value2", &schema);
        let end = create_tuple(4, "value4", &schema);
        let mut result = Vec::new();
        tree_write_guard.scan_range(&start, &end, true, &mut result);

        println!("\nFinal tree state:\n{}", tree_write_guard.visualize());
        println!(
            "\nRange scan result: {:?}",
            result
                .iter()
                .map(|(t, rid): &(_, _)| format!(
                    "{}→{:?}",
                    get_integer_from_value(t.get_value(0)),
                    rid
                ))
                .collect::<Vec<_>>()
        );
    }
}

#[cfg(test)]
mod advanced_tests {
    use super::test_utils::*;
    use super::*;
    use crate::concurrency::transaction::Transaction;
    use crate::types_db::value::{Val, Value};
    use rand::prelude::*;

    fn get_integer_from_value(value: &Value) -> i32 {
        match value.get_value() {
            Val::Integer(i) => *i,
            _ => panic!("Expected integer value"),
        }
    }

    /// Test ascending order inserts
    #[test]
    fn test_insertion_ascending() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));

        {
            let tree_guard = tree.write();
            for i in 0..10 {
                let tuple = create_tuple(i, &format!("val{}", i), &schema);
                assert!(
                    tree_guard.insert(tuple, RID::new(0, i as u32)),
                    "Insertion of ascending key {} failed",
                    i
                );
            }
            // Visualize after insertion
            println!("[Ascending] Tree:\n{}", tree_guard.visualize());
        }

        // Validate that all keys exist
        let tree_guard = tree.read();
        for i in 0..10 {
            let tuple = create_tuple(i, &format!("val{}", i), &schema);
            let mut result = Vec::new();
            tree_guard.scan_key(&tuple, &mut result, &Transaction::default());
            assert_eq!(
                result.len(),
                1,
                "Expected exactly one entry for key = {}, got {}",
                i,
                result.len()
            );
        }
    }

    /// Test descending order inserts
    #[test]
    fn test_insertion_descending() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));

        {
            let tree_guard = tree.write();
            for i in (0..10).rev() {
                let tuple = create_tuple(i, &format!("val{}", i), &schema);
                assert!(
                    tree_guard.insert(tuple, RID::new(0, i as u32)),
                    "Insertion of descending key {} failed",
                    i
                );
            }
            // Visualize after insertion
            println!("[Descending] Tree:\n{}", tree_guard.visualize());
        }

        // Validate that all keys exist
        let tree_guard = tree.read();
        for i in 0..10 {
            let tuple = create_tuple(i, &format!("val{}", i), &schema);
            let mut result = Vec::new();
            tree_guard.scan_key(&tuple, &mut result, &Transaction::default());
            assert_eq!(
                result.len(),
                1,
                "Expected exactly one entry for key = {}, got {}",
                i,
                result.len()
            );
        }
    }

    /// Test random order inserts
    #[test]
    fn test_insertion_random() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));

        let mut rng = rand::rng();
        let mut numbers: Vec<i32> = (0..10).collect();
        numbers.shuffle(&mut rng);

        {
            let tree_guard = tree.write();
            for &num in &numbers {
                let tuple = create_tuple(num, &format!("random{}", num), &schema);
                assert!(
                    tree_guard.insert(tuple, RID::new(0, num as u32), ),
                    "Insertion of random key {} failed",
                    num
                );
            }
            println!("[Random] Tree:\n{}", tree_guard.visualize());
        }

        // Validate that all keys exist
        let tree_guard = tree.read();
        for i in 0..10 {
            let tuple = create_tuple(i, &format!("random{}", i), &schema);
            let mut result = Vec::new();
            tree_guard.scan_key(&tuple, &mut result, &Transaction::default());
            assert_eq!(
                result.len(),
                1,
                "Expected exactly one entry for key = {}, got {}",
                i,
                result.len()
            );
        }
    }

    /// Test repeated insertions: same key, same RID (idempotent) and same key, different RIDs.
    #[test]
    fn test_repeated_keys_and_rids() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let tuple = create_tuple(42, "forty-two", &schema);

        {
            let tree_guard = tree.write();

            // Same key, same RID twice
            assert!(
                tree_guard.insert(tuple.clone(), RID::new(0, 42)),
                "First insertion of key 42 failed"
            );
            assert!(
                tree_guard.insert(tuple.clone(), RID::new(0, 42)),
                "Second insertion of same (key, rid) should be idempotent (return true or handle duplicates)."
            );

            // Same key, different RID
            assert!(
                tree_guard.insert(tuple.clone(), RID::new(0, 43)),
                "Insertion of same key but different RID failed"
            );
        }

        // Check that we have at least 2 entries (depending on how duplicates are handled)
        let tree_guard = tree.read();
        let mut result = Vec::new();
        tree_guard.scan_key(&tuple, &mut result, &Transaction::default());
        assert!(
            result.len() >= 2,
            "Expected at least two entries for key=42"
        );

        // Visualize after insertion
        println!("[Repeated Keys] Tree:\n{}", tree_guard.visualize());
    }

    /// Test deleting a non-existent key
    #[test]
    fn test_delete_non_existent_key() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));

        {
            // Insert something else
            let tree_guard = tree.write();
            let existing_tuple = create_tuple(100, "exists", &schema);
            tree_guard.insert(existing_tuple, RID::new(0, 100));
        }

        let missing_tuple = create_tuple(999, "missing", &schema);
        let tree_guard = tree.write();
        assert!(
            !tree_guard.delete(&missing_tuple, RID::new(0, 999)),
            "Delete should fail (return false) for non-existent key"
        );
    }

    /// Test bulk insertion (large scale)
    #[test]
    fn test_bulk_insertion() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));

        {
            let tree_guard = tree.write();
            for i in 0..100 {
                let tuple = create_tuple(i, &format!("val{}", i), &schema);
                let inserted = tree_guard.insert(tuple, RID::new(0, i as u32));
                assert!(inserted, "Insertion failed for key {}", i);
            }
            // Visualize after many inserts
            println!("[Bulk Insertion] Tree:\n{}", tree_guard.visualize());
        }

        // Verify correctness
        let tree_guard = tree.read();
        for i in 0..100 {
            let tuple = create_tuple(i, &format!("val{}", i), &schema);
            let mut result = Vec::new();
            tree_guard.scan_key(&tuple, &mut result, &Transaction::default());
            assert_eq!(
                result.len(),
                1,
                "Expected single entry for key {}, got {}",
                i,
                result.len()
            );
        }
    }

    /// Test repeatedly deleting entries until the tree is empty again.
    #[test]
    fn test_root_shrinking() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));

        // Insert multiple entries
        {
            let tree_guard = tree.write();
            for i in 0..10 {
                let tuple = create_tuple(i, &format!("val{}", i), &schema);
                assert!(
                    tree_guard.insert(tuple, RID::new(0, i as u32)),
                    "Insertion {} failed",
                    i
                );
            }
            println!("Tree before deletions:\n{}", tree_guard.visualize());
        }

        // Delete them all
        {
            let tree_guard = tree.write();
            for i in 0..10 {
                let tuple = create_tuple(i, &format!("val{}", i), &schema);
                assert!(
                    tree_guard.delete(&tuple, RID::new(0, i as u32)),
                    "Deletion of {} failed",
                    i
                );
            }

            // Visualize after deletions
            println!("Tree after deletions:\n{}", tree_guard.visualize());
            let root_guard = tree_guard.root.read();
            assert!(
                root_guard.keys.is_empty(),
                "Root should be empty after all entries are deleted"
            );
            assert_eq!(
                root_guard.node_type,
                NodeType::Leaf,
                "Root node should remain a Leaf if everything is deleted"
            );
        }
    }

    /// Test re-inserting after deletion of same key
    #[test]
    fn test_insert_delete_insert_same_key() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let tuple = create_tuple(5, "val5", &schema);
        let rid = RID::new(0, 5);

        {
            let tree_guard = tree.write();
            // Insert
            assert!(
                tree_guard.insert(tuple.clone(), rid),
                "First insertion failed"
            );
            // Delete
            assert!(tree_guard.delete(&tuple, rid), "Deletion failed");
            // Insert again
            assert!(
                tree_guard.insert(tuple.clone(), rid),
                "Re-insertion of same key failed"
            );

            // Verify
            let mut result = Vec::new();
            tree_guard.scan_key(&tuple, &mut result, &Transaction::default());
            assert_eq!(result.len(), 1, "Should find exactly one re-inserted entry");
        }
    }

    /// Test repeated deletion of the same key (idempotent or ignoring duplicates).
    #[test]
    fn test_repeated_deletion() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let tuple = create_tuple(7, "val7", &schema);
        let rid = RID::new(0, 7);

        {
            // Insert once
            let tree_guard = tree.write();
            assert!(
                tree_guard.insert(tuple.clone(), rid),
                "Insertion for repeated deletion test failed"
            );
        }

        {
            // Delete multiple times
            let tree_guard = tree.write();
            assert!(
                tree_guard.delete(&tuple, rid),
                "First delete should succeed"
            );
            let second_delete = tree_guard.delete(&tuple, rid);
            assert!(
                !second_delete,
                "Second delete of same (key, RID) should fail or return false"
            );

            // Final check
            let mut result = Vec::new();
            tree_guard.scan_key(&tuple, &mut result, &Transaction::default());
            assert!(result.is_empty(), "Key 7 should be deleted");
        }
    }
}

#[cfg(test)]
mod split_behavior_tests {
    use super::test_utils::*;
    use super::*;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::transaction::IsolationLevel;

    /// Test leaf node splitting behavior with a small order
    #[test]
    fn test_leaf_node_split() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        // Using order 3 to force splits frequently
        let tree = create_test_tree(3, Arc::from(index_info));
        let tree_write_guard = tree.write();

        // Insert 2 keys (should not split yet as order is 3)
        let tuple1 = create_tuple(1, "value1", &schema);
        let tuple2 = create_tuple(2, "value2", &schema);

        println!("Initial tree state:");
        println!("{}", tree_write_guard.visualize());

        assert!(tree_write_guard.insert(tuple1.clone(), RID::new(0, 1)));
        assert!(tree_write_guard.insert(tuple2.clone(), RID::new(0, 2)));

        println!("\nAfter inserting 2 keys (before split):");
        println!("{}", tree_write_guard.visualize());

        // This insert should cause a split
        let tuple3 = create_tuple(3, "value3", &schema);
        assert!(tree_write_guard.insert(tuple3.clone(), RID::new(0, 3)));

        println!("\nAfter inserting 3rd key (after split):");
        println!("{}", tree_write_guard.visualize());

        // Verify the structure after split
        let root = tree_write_guard.root.read();
        assert_eq!(
            root.node_type,
            NodeType::Internal,
            "Root should be internal after split"
        );
        assert_eq!(root.keys.len(), 1, "Root should have 1 key after split");
        assert_eq!(
            root.children.len(),
            2,
            "Root should have 2 children after split"
        );

        // Check leaf nodes are properly linked
        let left_child = root.children[0].read();
        let right_child = root.children[1].read();
        assert!(
            matches!(left_child.next_leaf, Some(_)),
            "Left leaf should have next pointer"
        );
        assert!(
            matches!(right_child.next_leaf, None),
            "Right leaf should have no next pointer"
        );
    }

    /// Test cascading splits from leaf to root
    #[test]
    fn test_cascading_splits() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        // Using order 3 to force splits frequently
        let tree = create_test_tree(3, Arc::from(index_info));
        let tree_write_guard = tree.write();

        println!("Initial tree state:");
        println!("{}", tree_write_guard.visualize());

        // Insert keys in ascending order to force splits
        for i in 1..=7 {
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            assert!(tree_write_guard.insert(tuple.clone(), RID::new(0, i as u32)));
            println!("\nAfter inserting key {}:", i);
            println!("{}", tree_write_guard.visualize());
        }

        // Verify final structure
        let root = tree_write_guard.root.read();
        assert_eq!(
            root.node_type,
            NodeType::Internal,
            "Root should be internal"
        );
        assert!(root.keys.len() > 0, "Root should have at least one key");

        // Verify all leaf nodes are properly linked
        let mut current_node = Arc::clone(&tree_write_guard.root);
        let mut leaf_count = 0;

        // Navigate to leftmost leaf
        loop {
            // Acquire the read lock, figure out what the next child is,
            // and store that in a temporary variable. Then drop the read lock
            // before we try to update `current_node`.
            let next_child = {
                let node = current_node.read(); // read lock
                match node.node_type {
                    NodeType::Internal => Some(Arc::clone(&node.children[0])),
                    NodeType::Leaf => None,
                }
            }; // read lock is dropped here

            match next_child {
                Some(child) => {
                    current_node = child; // now safe to reassign
                }
                None => {
                    break;
                }
            }
        }

        // Follow leaf node chain
        loop {
            let next_leaf = {
                let node = current_node.read();
                leaf_count += 1;
                node.next_leaf.clone() // store in a temporary
            };

            match next_leaf {
                Some(next) => current_node = next, // safe to reassign now
                None => break,
            }
        }

        assert!(
            leaf_count > 1,
            "Should have multiple leaf nodes after splits"
        );
    }

    /// Test node splits with duplicate keys
    #[test]
    fn test_splits_with_duplicates() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(3, Arc::from(index_info));
        let tree_write_guard = tree.write();

        println!("Initial tree state:");
        println!("{}", tree_write_guard.visualize());

        // Insert multiple entries with the same key
        for i in 1..=5 {
            let tuple = create_tuple(1, &format!("value1_{}", i), &schema);
            assert!(tree_write_guard.insert(tuple.clone(), RID::new(0, i)));
            println!("\nAfter inserting duplicate 1 (#{}):", i);
            println!("{}", tree_write_guard.visualize());
        }

        // Collect all values
        let mut result = Vec::new();
        let search_tuple = create_tuple(1, "value1_1", &schema);
        tree_write_guard.scan_key(
            &search_tuple,
            &mut result,
            &Transaction::new(0, IsolationLevel::ReadUncommitted),
        );

        assert_eq!(result.len(), 5, "Should find all 5 duplicate entries");
    }

    /// Test node splits during interleaved insertions
    #[test]
    fn test_interleaved_splits() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(3, Arc::from(index_info));
        let tree_write_guard = tree.write();

        println!("Initial tree state:");
        println!("{}", tree_write_guard.visualize());

        // Insert values that will force splits in different parts of the tree
        let insert_sequence = vec![5, 3, 7, 2, 4, 6, 8];

        for &i in &insert_sequence {
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            assert!(tree_write_guard.insert(tuple.clone(), RID::new(0, i as u32)));
            println!("\nAfter inserting key {}:", i);
            println!("{}", tree_write_guard.visualize());
        }

        // Verify the final structure maintains order
        let mut result = Vec::new();
        let start = create_tuple(1, "value1", &schema);
        let end = create_tuple(9, "value9", &schema);
        tree_write_guard.scan_range(&start, &end, true, &mut result);

        let values: Vec<i32> = result
            .iter()
            .map(|(tuple, _)| match tuple.get_value(0).get_value() {
                Val::Integer(i) => *i,
                _ => panic!("Expected integer value"),
            })
            .collect();

        // Verify ordering
        for i in 0..values.len() - 1 {
            assert!(
                values[i] < values[i + 1],
                "Values should be in ascending order"
            );
        }
    }

    /// Test that splits maintain correct next-leaf pointers
    #[test]
    fn test_split_leaf_pointers() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(3, Arc::from(index_info));
        let tree_write_guard = tree.write();

        // Insert enough values to cause multiple splits
        for i in (1..=6).rev() {
            // Insert in reverse order
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            assert!(tree_write_guard.insert(tuple.clone(), RID::new(0, i as u32)));
            println!("\nAfter inserting key {}:", i);
            println!("{}", tree_write_guard.visualize());
        }

        // Perform a range scan to verify leaf node links
        let mut result = Vec::new();
        let start = create_tuple(1, "value1", &schema);
        let end = create_tuple(6, "value6", &schema);
        tree_write_guard.scan_range(&start, &end, true, &mut result);

        // Verify we got all values in order
        assert_eq!(result.len(), 6, "Should find all 6 values");
        let values: Vec<i32> = result
            .iter()
            .map(|(tuple, _)| match tuple.get_value(0).get_value() {
                Val::Integer(i) => *i,
                _ => panic!("Expected integer value"),
            })
            .collect();

        // Check that values are in sequence, proving leaf links are correct
        for i in 0..values.len() - 1 {
            assert_eq!(
                values[i] + 1,
                values[i + 1],
                "Values should be consecutive, indicating correct leaf node traversal"
            );
        }
    }
}

#[cfg(test)]
mod scan_key_tests {
    use super::test_utils::*;
    use super::*;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::transaction::IsolationLevel;
    use std::collections::HashSet;

    /// Test scanning for a key in an empty tree
    #[test]
    fn test_scan_key_empty_tree() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(3, Arc::from(index_info));
        let tree_guard = tree.write();

        let mut result = Vec::new();
        let search_tuple = create_tuple(1, "value1", &schema);
        tree_guard.scan_key(
            &search_tuple,
            &mut result,
            &Transaction::new(0, IsolationLevel::ReadUncommitted),
        );
        assert!(result.is_empty(), "Empty tree should return no results");
    }

    /// Test scanning for a key that doesn't exist in a non-empty tree
    #[test]
    fn test_scan_key_nonexistent() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(3, Arc::from(index_info));
        let tree_guard = tree.write();

        // Insert some data
        for i in 1..=5 {
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            assert!(tree_guard.insert(tuple, RID::new(0, i as u32)));
        }

        // Search for non-existent key
        let mut result = Vec::new();
        let search_tuple = create_tuple(10, "value10", &schema);
        tree_guard.scan_key(
            &search_tuple,
            &mut result,
            &Transaction::new(0, IsolationLevel::ReadUncommitted),
        );
        assert!(
            result.is_empty(),
            "Should find no results for non-existent key"
        );
    }

    /// Test scanning for duplicates in single leaf node
    #[test]
    fn test_scan_key_duplicates_single_leaf() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info)); // Larger order to keep in single leaf

        let tree_guard = tree.write();

        // Insert duplicates
        for i in 1..=3 {
            let tuple = create_tuple(1, &format!("value1_{}", i), &schema);
            assert!(tree_guard.insert(tuple, RID::new(0, i)));
        }

        println!("Tree state:\n{}", tree_guard.visualize());

        // Search for duplicates
        let mut result = Vec::new();
        let search_tuple = create_tuple(1, "value1_1", &schema);
        tree_guard.scan_key(
            &search_tuple,
            &mut result,
            &Transaction::new(0, IsolationLevel::ReadUncommitted),
        );

        assert_eq!(
            result.len(),
            3,
            "Should find all 3 duplicates in single leaf"
        );

        // Verify RIDs
        let rids: HashSet<_> = result.iter().collect();
        assert_eq!(rids.len(), 3, "Should have 3 unique RIDs");
        for i in 1..=3 {
            assert!(rids.contains(&RID::new(0, i)), "Missing RID {}", i);
        }
    }

    /// Test scanning for duplicates across leaf nodes
    #[test]
    fn test_scan_key_duplicates_multiple_leaves() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(3, Arc::from(index_info)); // Small order to force splits
        let tree_guard = tree.write();

        // Insert enough duplicates to cause splits
        for i in 1..=5 {
            let tuple = create_tuple(1, &format!("value1_{}", i), &schema);
            assert!(tree_guard.insert(tuple, RID::new(0, i)));
            println!("\nAfter inserting duplicate 1 (#{}):", i);
            println!("{}", tree_guard.visualize());
        }

        // Search for duplicates
        let mut result = Vec::new();
        let search_tuple = create_tuple(1, "value1_1", &schema);
        tree_guard.scan_key(
            &search_tuple,
            &mut result,
            &Transaction::new(0, IsolationLevel::ReadUncommitted),
        );

        assert_eq!(
            result.len(),
            5,
            "Should find all 5 duplicates across leaves"
        );

        // Verify RIDs
        let rids: HashSet<_> = result.iter().collect();
        assert_eq!(rids.len(), 5, "Should have 5 unique RIDs");
        for i in 1..=5 {
            assert!(rids.contains(&RID::new(0, i)), "Missing RID {}", i);
        }
    }

    /// Test scanning with mixed values including duplicates
    #[test]
    fn test_scan_key_mixed_values() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(3, Arc::from(index_info));
        let tree_guard = tree.write();

        // Insert mix of values and duplicates
        // Insert key 1 three times
        for i in 1..=3 {
            let tuple = create_tuple(1, &format!("value1_{}", i), &schema);
            assert!(tree_guard.insert(tuple, RID::new(0, i)));
        }

        // Insert some other values
        let tuple2 = create_tuple(2, "value2", &schema);
        let tuple3 = create_tuple(3, "value3", &schema);
        assert!(tree_guard.insert(tuple2, RID::new(0, 4)));
        assert!(tree_guard.insert(tuple3, RID::new(0, 5)));

        // Insert more duplicates of key 1
        for i in 6..=7 {
            let tuple = create_tuple(1, &format!("value1_{}", i - 2), &schema);
            assert!(tree_guard.insert(tuple, RID::new(0, i)));
        }

        println!("Final tree state:\n{}", tree_guard.visualize());

        // Search for key 1
        let mut result = Vec::new();
        let search_tuple = create_tuple(1, "value1_1", &schema);
        tree_guard.scan_key(
            &search_tuple,
            &mut result,
            &Transaction::new(0, IsolationLevel::ReadUncommitted),
        );

        assert_eq!(result.len(), 5, "Should find all 5 duplicates of key 1");

        // Verify correct RIDs were found
        let expected_rids: HashSet<_> = vec![1, 2, 3, 6, 7]
            .into_iter()
            .map(|i| RID::new(0, i))
            .collect();
        let found_rids: HashSet<_> = result.into_iter().collect();
        assert_eq!(
            found_rids, expected_rids,
            "Found RIDs don't match expected RIDs"
        );
    }

    /// Test scanning after deletion of some duplicates
    #[test]
    fn test_scan_key_after_deletions() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(3, Arc::from(index_info));
        let tree_guard = tree.write();

        // Store tuples for later deletion
        let mut inserted_tuples = Vec::new();

        // Insert duplicates
        for i in 1..=5 {
            let tuple = create_tuple(1, &format!("value1_{}", i), &schema);
            assert!(tree_guard.insert(tuple.clone(), RID::new(0, i)));
            inserted_tuples.push((tuple, RID::new(0, i)));
        }

        println!("Tree before deletions:\n{}", tree_guard.visualize());

        // Delete some duplicates using the exact same tuples we inserted
        assert!(
            tree_guard.delete(&inserted_tuples[1].0, inserted_tuples[1].1),
            "Failed to delete second tuple"
        );
        assert!(
            tree_guard.delete(&inserted_tuples[3].0, inserted_tuples[3].1),
            "Failed to delete fourth tuple"
        );

        println!("Tree after deletions:\n{}", tree_guard.visualize());

        // Search for remaining duplicates
        let mut result = Vec::new();
        let search_tuple = create_tuple(1, "value1_1", &schema);
        tree_guard.scan_key(
            &search_tuple,
            &mut result,
            &Transaction::new(0, IsolationLevel::ReadUncommitted),
        );

        assert_eq!(result.len(), 3, "Should find remaining 3 duplicates");

        // Verify correct RIDs remain
        let expected_rids: HashSet<_> = vec![1, 3, 5].into_iter().map(|i| RID::new(0, i)).collect();
        let found_rids: HashSet<_> = result.into_iter().collect();
        assert_eq!(
            found_rids, expected_rids,
            "Found RIDs don't match expected RIDs"
        );
    }
}
