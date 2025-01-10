use crate::common::rid::RID;
use crate::concurrency::transaction::Transaction;
use crate::storage::index::index::{Index, IndexInfo};
use crate::storage::table::tuple::Tuple;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};
use log::debug;
use parking_lot::RwLock;
use std::cmp::PartialEq;
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
    keys: Vec<Value>, // Changed from Tuple to Value
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

    pub fn insert(&mut self, key: Value, rid: RID) -> bool {
        let root_is_full = {
            let root_guard = self.root.read();
            root_guard.keys.len() == self.order - 1
        };

        if root_is_full {
            debug!("Root is full, creating new root");
            let new_root = BPlusTreeNode::new(NodeType::Internal);
            let old_root = {
                let mut root_guard = self.root.write();
                let old_root = std::mem::replace(&mut *root_guard, new_root);
                Arc::new(RwLock::new(old_root))
            };

            // Update the new root
            {
                let mut root_guard = self.root.write();
                root_guard.children.push(old_root);
                if !self.split_child(&mut *root_guard, 0) {
                    debug!("Root split failed");
                    return false;
                }
                debug!("Root split complete");
            }

            // Now insert into the split root
            let mut root_guard = self.root.write();
            self.insert_non_full(&mut *root_guard, key, rid)
        } else {
            let mut root_guard = self.root.write();
            self.insert_non_full(&mut *root_guard, key, rid)
        }
    }

    pub fn delete(&self, key: &Value, rid: RID) -> bool {
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
                        .position(|k| k.compare_greater_than(key) == CmpBool::CmpTrue)
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
                            k.compare_equals(key) == CmpBool::CmpTrue && node.values[*idx] == rid
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
        start_tuple: &Tuple,
        end_tuple: &Tuple,
        include_end: bool,
    ) -> Result<Vec<(Value, RID)>, String> {
        debug!("Starting range scan");

        // Extract key values from tuples
        let key_attrs = self.metadata.get_key_attrs();
        let start_key = start_tuple.get_value(key_attrs[0]);
        let end_key = end_tuple.get_value(key_attrs[0]);
        debug!("Scanning range: [{:?}, {:?}]", start_key, end_key);

        let mut current_node = Arc::clone(&self.root);

        // Find leftmost leaf that could contain our range
        debug!("Finding start leaf node");
        loop {
            let node = current_node.read();
            match node.node_type {
                NodeType::Internal => {
                    debug!("At internal node with keys: {:?}", node.keys);
                    let child_idx = node
                        .keys
                        .iter()
                        .position(|k| k.compare_greater_than(&start_key) == CmpBool::CmpTrue)
                        .unwrap_or(0);
                    debug!("Selected child index: {}", child_idx);

                    let next_node = Arc::clone(&node.children[child_idx]);
                    drop(node);
                    current_node = next_node;
                }
                NodeType::Leaf => {
                    debug!("Found leaf node");
                    break;
                }
            }
        }

        // Collect all values in range from leaf nodes
        debug!("Collecting entries from leaves");
        let mut entries = Vec::new();
        'scan: loop {
            let node = current_node.read();
            debug!("Processing leaf node with keys: {:?}", node.keys);

            // Add all values from current leaf
            for (key, rid) in node.keys.iter().zip(node.values.iter()) {
                debug!("Found entry - Key: {:?}, RID: {:?}", key, rid);
                // Only add entries within our range
                if key.compare_less_than(&start_key) == CmpBool::CmpTrue {
                    continue;
                }
                if key.compare_greater_than(&end_key) == CmpBool::CmpTrue
                    || (!include_end && key.compare_equals(&end_key) == CmpBool::CmpTrue)
                {
                    break 'scan;
                }
                entries.push((key.clone(), *rid));
            }

            // Move to next leaf
            match &node.next_leaf {
                Some(next) => {
                    debug!("Moving to next leaf");
                    let next_node = Arc::clone(next);
                    drop(node);
                    current_node = next_node;
                }
                None => {
                    debug!("Reached last leaf");
                    break 'scan;
                }
            }
        }

        debug!("Sorting {} collected entries", entries.len());
        // Sort entries to handle any out-of-order keys
        entries.sort_by(|(key1, _), (key2, _)| {
            if key1.compare_less_than(key2) == CmpBool::CmpTrue {
                std::cmp::Ordering::Less
            } else if key1.compare_greater_than(key2) == CmpBool::CmpTrue {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        });

        debug!("Final result contains {} entries", entries.len());
        Ok(entries)
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
        let format_key = |key: &Value| -> String {
            match key.get_value() {
                Val::Integer(i) => i.to_string(),
                Val::VarLen(s) => s.clone(),
                _ => format!("{:?}", key.get_value()),
            }
        };

        // Track leaf nodes for drawing links
        let mut leaf_nodes = Vec::new();

        // Recursive helper function to build tree structure
        fn visualize_node(
            node: &BPlusTreeNode,
            level: usize,
            index: usize,
            format_key: &dyn Fn(&Value) -> String,
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

    pub fn compare_keys(&self, key1: &Value, key2: &Value) -> CmpBool {
        // First check less than
        if key1.compare_less_than(key2) == CmpBool::CmpTrue {
            return CmpBool::CmpTrue;
        }

        // Then check greater than
        if key1.compare_greater_than(key2) == CmpBool::CmpTrue {
            return CmpBool::CmpTrue;
        }

        // If neither less than nor greater than, must be equal
        CmpBool::CmpFalse
    }

    pub fn compare_keys_ordering(&self, key1: &Value, key2: &Value) -> std::cmp::Ordering {
        if key1.compare_less_than(key2) == CmpBool::CmpTrue {
            std::cmp::Ordering::Less
        } else if key1.compare_greater_than(key2) == CmpBool::CmpTrue {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    }

    // Helper method to find insertion position in sorted order
    fn find_insert_position(&self, keys: &[Value], key: &Value) -> usize {
        keys.iter()
            .position(|k| k.compare_greater_than_equals(key) == CmpBool::CmpTrue)
            .unwrap_or(keys.len())
    }

    // Update insert_non_full to maintain sorted order
    fn insert_non_full(&self, node: &mut BPlusTreeNode, key: Value, rid: RID) -> bool {
        match node.node_type {
            NodeType::Leaf => {
                let pos = self.find_insert_position(&node.keys, &key);
                
                // Check for duplicate key-RID pair
                if pos < node.keys.len() {
                    let has_duplicate = node.keys[pos..].iter().zip(node.values[pos..].iter())
                        .any(|(k, r)| k.compare_equals(&key) == CmpBool::CmpTrue && *r == rid);
                    if has_duplicate {
                        debug!("Duplicate key-RID pair found, skipping insertion");
                        return false;
                    }
                }

                // Insert the key-RID pair
                debug!("Inserting key {:?} with RID {:?} at position {}", key, rid, pos);
                node.keys.insert(pos, key);
                node.values.insert(pos, rid);
                true
            }
            NodeType::Internal => {
                let pos = self.find_insert_position(&node.keys, &key);
                let child_arc = Arc::clone(&node.children[pos]);
                
                // Create a new scope for the write guard
                {
                    let mut child_guard = child_arc.write();
                    
                    if child_guard.keys.len() == self.order - 1 {
                        drop(child_guard);
                        self.split_child(node, pos);
                        
                        // After split, find new position and get new child
                        let pos = self.find_insert_position(&node.keys, &key);
                        let child_arc = Arc::clone(&node.children[pos]);
                        let mut child_guard = child_arc.write();
                        return self.insert_non_full(&mut *child_guard, key, rid);
                    }
                    
                    // No split needed, insert directly
                    self.insert_non_full(&mut *child_guard, key, rid)
                }
            }
        }
    }

    // Update split_child to maintain sorted order
    fn split_child(&self, parent: &mut BPlusTreeNode, child_idx: usize) -> bool {
        let child = Arc::clone(&parent.children[child_idx]);
        let mut child_guard = child.write();
        let mut new_node = BPlusTreeNode::new(child_guard.node_type.clone());
        let middle = (self.order - 1) / 2;

        match child_guard.node_type {
            NodeType::Leaf => {
                debug!("Splitting leaf node at position {}", middle);
                // Move half of the entries to the new node
                let split_point = child_guard.keys.len() / 2;
                
                // Move entries to new node
                new_node.keys = child_guard.keys.split_off(split_point);
                new_node.values = child_guard.values.split_off(split_point);

                // Update leaf node links
                new_node.next_leaf = child_guard.next_leaf.clone();
                let new_node_arc = Arc::new(RwLock::new(new_node));
                child_guard.next_leaf = Some(Arc::clone(&new_node_arc));

                // Insert the new node into parent
                // Use the first key of the new node as the separator
                let split_key = new_node_arc.read().keys[0].clone();
                
                // Find the correct position in parent
                let parent_pos = child_idx + 1;
                parent.keys.insert(parent_pos - 1, split_key);
                parent.children.insert(parent_pos, new_node_arc.clone());

                debug!(
                    "Leaf split complete - Left node: {:?}, Right node: {:?}",
                    child_guard.keys,
                    new_node_arc.read().keys
                );
            }
            NodeType::Internal => {
                // Move half of the keys and children to the new node
                let split_point = child_guard.keys.len() / 2;
                
                // Get the middle key that will move up to the parent
                let middle_key = child_guard.keys.remove(split_point);
                
                // Move entries to new node
                new_node.keys = child_guard.keys.split_off(split_point);
                new_node.children = child_guard.children.split_off(split_point + 1);

                // Insert the middle key and new node into parent
                let parent_pos = child_idx + 1;
                parent.keys.insert(parent_pos - 1, middle_key);
                parent.children.insert(parent_pos, Arc::new(RwLock::new(new_node)));
            }
        }
        true
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
                break;
            }
        }
    }

    fn rebalance_node(&self, parent: &mut BPlusTreeNode, child_idx: usize) {
        let min_keys = (self.order - 1) / 2;

        // Try borrowing from left sibling
        if child_idx > 0 {
            let left_sibling = Arc::clone(&parent.children[child_idx - 1]);
            let mut child = parent.children[child_idx].write();
            let mut left = left_sibling.write();

            if left.keys.len() > min_keys {
                // Move rightmost key-value pair from left sibling
                let key = left.keys.pop().unwrap();
                let value = left.values.pop().unwrap();

                child.keys.insert(0, key.clone());
                child.values.insert(0, value);

                // Update parent's separator key
                parent.keys[child_idx - 1] = key;
                return;
            }
        }

        // Try borrowing from right sibling
        if child_idx < parent.children.len() - 1 {
            let right_sibling = Arc::clone(&parent.children[child_idx + 1]);
            let mut child = parent.children[child_idx].write();
            let mut right = right_sibling.write();

            if right.keys.len() > min_keys {
                // Move leftmost key-value pair from right sibling
                let key = right.keys.remove(0);
                let value = right.values.remove(0);

                child.keys.push(key.clone());
                child.values.push(value);

                // Update parent's separator key
                parent.keys[child_idx] = right.keys[0].clone();
                return;
            }
        }

        // If we can't borrow, merge with a sibling
        if child_idx > 0 {
            self.merge_with_left_sibling(parent, child_idx);
        } else {
            self.merge_with_right_sibling(parent, child_idx);
        }
    }

    fn merge_with_left_sibling(&self, parent: &mut BPlusTreeNode, child_idx: usize) {
        let left = Arc::clone(&parent.children[child_idx - 1]);
        let right = Arc::clone(&parent.children[child_idx]);

        let mut left_guard = left.write();
        let right_guard = right.read();

        // Collect all entries and sort them
        let mut entries: Vec<_> = left_guard
            .keys
            .iter()
            .cloned()
            .zip(left_guard.values.iter().cloned())
            .chain(
                right_guard
                    .keys
                    .iter()
                    .cloned()
                    .zip(right_guard.values.iter().cloned()),
            )
            .collect();

        entries.sort_by(|(key1, _), (key2, _)| {
            if key1.compare_less_than(key2) == CmpBool::CmpTrue {
                std::cmp::Ordering::Less
            } else if key1.compare_greater_than(key2) == CmpBool::CmpTrue {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        });

        // Update left node with sorted entries
        left_guard.keys.clear();
        left_guard.values.clear();
        for (key, rid) in entries {
            left_guard.keys.push(key);
            left_guard.values.push(rid);
        }

        // Update leaf node links
        left_guard.next_leaf = right_guard.next_leaf.clone();

        // Remove right child and separating key from parent
        parent.children.remove(child_idx);
        parent.keys.remove(child_idx - 1);
    }

    fn merge_with_right_sibling(&self, parent: &mut BPlusTreeNode, child_idx: usize) {
        let left = Arc::clone(&parent.children[child_idx]);
        let right = Arc::clone(&parent.children[child_idx + 1]);

        let mut left_guard = left.write();
        let right_guard = right.read();

        // Move all keys and values from right to left
        left_guard.keys.extend(right_guard.keys.iter().cloned());
        left_guard.values.extend(right_guard.values.iter().cloned());

        // Update leaf node links
        left_guard.next_leaf = right_guard.next_leaf.clone();

        // Remove right child and separating key from parent
        parent.children.remove(child_idx + 1);
        parent.keys.remove(child_idx);
    }

    fn extract_key(&self, tuple: &Tuple) -> Value {
        let key_attrs = self.metadata.get_key_attrs();
        assert_eq!(key_attrs.len(), 1, "Only single key indices are supported");
        tuple.get_value(key_attrs[0]).clone()
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

    fn insert_entry(&mut self, tuple: &Tuple, rid: RID, _transaction: &Transaction) -> bool {
        // Extract key from tuple using metadata
        let key = self.extract_key(tuple);
        self.insert(key, rid)
    }

    fn delete_entry(&self, tuple: &Tuple, rid: RID, _transaction: &Transaction) -> bool {
        let key = self.extract_key(tuple);
        let mut current_node = Arc::clone(&self.root);
        let mut path: Vec<(Arc<RwLock<BPlusTreeNode>>, usize)> = Vec::new();

        // Special case for root node if it's a leaf
        {
            let root = current_node.read();
            if root.node_type == NodeType::Leaf {
                let pos = root
                    .keys
                    .iter()
                    .zip(root.values.iter())
                    .position(|(k, r)| k.compare_equals(&key) == CmpBool::CmpTrue && *r == rid);

                if let Some(idx) = pos {
                    drop(root);
                    let mut root = current_node.write();
                    root.keys.remove(idx);
                    root.values.remove(idx);
                    return true;
                }
                return false;
            }
        }

        // Find the leaf node containing the key
        loop {
            let node = current_node.read();
            match node.node_type {
                NodeType::Internal => {
                    let pos = node
                        .keys
                        .iter()
                        .position(|k| k.compare_greater_than(&key) == CmpBool::CmpTrue)
                        .unwrap_or(node.keys.len());

                    let next_node = Arc::clone(&node.children[pos]);
                    path.push((Arc::clone(&current_node), pos));
                    drop(node);
                    current_node = next_node;
                }
                NodeType::Leaf => {
                    let pos =
                        node.keys.iter().zip(node.values.iter()).position(|(k, r)| {
                            k.compare_equals(&key) == CmpBool::CmpTrue && *r == rid
                        });

                    if let Some(idx) = pos {
                        drop(node);
                        let mut node_write = current_node.write();
                        node_write.keys.remove(idx);
                        node_write.values.remove(idx);

                        let is_underfull = node_write.keys.len() < (self.order - 1) / 2;
                        drop(node_write);

                        if is_underfull && !path.is_empty() {
                            self.rebalance_after_delete(path);
                        }
                        return true;
                    }
                    drop(node);
                    return false;
                }
            }
        }
    }

    fn scan_key(&self, key: &Tuple, _transaction: &Transaction) -> Result<Vec<(Value, RID)>, String> {
        // Find the first leaf node that could contain our key
        let result = self.scan_range(key, key, true).unwrap();
        debug!("Scan complete, found {} matching entries", result.len());
        Ok(result)
    }
}

#[cfg(test)]
mod test_utils {
    use super::*;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
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

    pub fn get_integer_from_value(value: &Value) -> i32 {
        match value.get_value() {
            Val::Integer(i) => *i,
            _ => panic!("Expected integer value"),
        }
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

        let binding = tuple1.keys_from_tuple(vec![0]);
        let key1 = binding.get(0).unwrap();
        let binding = tuple2.keys_from_tuple(vec![0]);
        let key2 = binding.get(0).unwrap();

        assert_eq!(
            tree_read_guard.compare_keys_ordering(key1, &key2),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            tree_read_guard.compare_keys_ordering(&key2, &key1),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            tree_read_guard.compare_keys_ordering(&key1, &key1),
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
    use crate::types_db::value::{Val, Value};
    use std::collections::HashSet;



    #[test]
    fn test_basic_insertion() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let mut tree_write_guard = tree.write();
        let tuple1 = create_tuple(1, "value1", &schema);
        let tuple2 = create_tuple(2, "value2", &schema);

        assert!(tree_write_guard.insert_entry(&tuple1, RID::new(0, 1), &Default::default()));
        assert!(tree_write_guard.insert_entry(&tuple2, RID::new(0, 2), &Default::default()));

        let root = tree_write_guard.root.read();
        assert!(!root.keys.is_empty());
    }

    #[test]
    fn test_basic_deletion() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let mut tree_write_guard = tree.write();

        // Insert a value
        let tuple = create_tuple(1, "value1", &schema);
        let rid = RID::new(0, 1);
        assert!(tree_write_guard.insert_entry(&tuple, rid, &Default::default()));

        // Verify the value exists
        let result = tree_write_guard.scan_key(&tuple, &Default::default()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1, rid);

        // Delete the value
        assert!(tree_write_guard.delete_entry(&tuple, rid, &Default::default()));

        // Verify the value is gone
        let result2 = tree_write_guard.scan_key(&tuple, &Default::default()).unwrap();
        assert!(result2.is_empty());
    }

    #[test]
    fn test_range_scan() {
        // Force debug logging for this test
        std::env::set_var("RUST_LOG", "debug");
        std::env::set_var("RUST_TEST", "1");
        initialize_logger();

        debug!("Starting range scan test");
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let mut tree_write_guard = tree.write();

        debug!("Inserting test data");
        // Insert test data ensuring full tuples
        for i in 1..=5 {
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            assert!(tree_write_guard.insert_entry(
                &tuple,
                RID::new(0, i as u32),
                &Default::default()
            ));
            debug!("Inserted value {}", i);
        }

        // Visualize tree to understand its structure
        debug!(
            "Tree structure after insertions:\n{}",
            tree_write_guard.visualize()
        );

        // Test scanning range [2, 4]
        let start = create_tuple(2, "value2", &schema);
        let end = create_tuple(4, "value4", &schema);
        debug!("Scanning range [2, 4]");
        let result = tree_write_guard.scan_range(&start, &end, true).unwrap();

        // Debug print results
        debug!("Range scan results:");
        for (value, rid) in &result {
            debug!("Found entry - Key: {}, RID: {}", value, rid);
        }

        // Verify results
        assert_eq!(result.len(), 3, "Expected 3 values in range [2, 4]");

        // Check that we got the correct values
        let result_values: HashSet<i32> = result
            .iter()
            .map(|(value, _)| {
                let val = get_integer_from_value(value);
                debug!("Extracted value: {}", val);
                val
            })
            .collect();
        let expected_values: HashSet<i32> = vec![2, 3, 4].into_iter().collect();
        debug!("Result values: {:?}", result_values);
        debug!("Expected values: {:?}", expected_values);
        assert_eq!(result_values, expected_values);

        // Verify ordering
        let result_ids: Vec<i32> = result
            .iter()
            .map(|(value, _)| get_integer_from_value(value))
            .collect();
        debug!("Final ordered results: {:?}", result_ids);
        assert!(
            result_ids.windows(2).all(|w| w[0] <= w[1]),
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
        let result = tree_read_guard.scan_range(&start, &end, true).unwrap();
        assert!(result.is_empty(), "Expected empty result for empty tree");
    }

    #[test]
    fn test_single_value_range_scan() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let mut tree_write_guard = tree.write();

        let tuple = create_tuple(3, "value3", &schema);
        tree_write_guard.insert_entry(&tuple, RID::new(0, 3), &Default::default());

        let result = tree_write_guard.scan_range(&tuple, &tuple, true).unwrap();
        assert_eq!(result.len(), 1, "Expected single value for point query");

        // Verify the value
        assert!(
            result[0].0.compare_equals(&Value::new(3)) == CmpBool::CmpTrue,
            "Expected value 3 in point query result"
        );
    }

    #[test]
    fn test_leaf_node_links() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let mut tree_write_guard = tree.write();

        // Insert enough values to cause splits
        for i in (0..10).rev() {
            // Insert in reverse order
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            tree_write_guard.insert_entry(&tuple, RID::new(0, i as u32), &Default::default());
        }

        // Scan entire range to verify leaf node links
        let start = create_tuple(0, "value0", &schema);
        let end = create_tuple(9, "value9", &schema);
        let result = tree_write_guard.scan_range(&start, &end, true).unwrap();

        // Verify all values are present and in order
        assert_eq!(result.len(), 10, "Should have found all 10 values");

        // Verify ordering
        let result_ids: Vec<i32> = result
            .iter()
            .map(|(value, _)| match value.get_value() {
                Val::Integer(i) => *i,
                _ => panic!("Expected integer value"),
            })
            .collect();
        assert!(
            result_ids.windows(2).all(|w| w[0] <= w[1]),
            "Results not in ascending order"
        );
    }
}

#[cfg(test)]
mod advanced_tests {
    use super::test_utils::*;
    use super::*;
    use crate::concurrency::transaction::Transaction;
    use rand::prelude::*;
    use rand::rng;
    use crate::common::logger::initialize_logger;

    #[test]
    fn test_insertion_ascending() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));

        {
            let mut tree_guard = tree.write();
            for i in 0..10 {
                let tuple = create_tuple(i, &format!("val{}", i), &schema);
                assert!(
                    tree_guard.insert_entry(&tuple, RID::new(0, i as u32), &Default::default()),
                    "Insertion of ascending key {} failed",
                    i
                );
            }
            println!("[Ascending] Tree:\n{}", tree_guard.visualize());
        }

        // Validate that all keys exist
        let tree_guard = tree.read();
        for i in 0..10 {
            let tuple = create_tuple(i, &format!("val{}", i), &schema);
            let result = tree_guard.scan_key(&tuple, &Transaction::default()).unwrap();
            assert_eq!(result.len(), 1);
        }
    }

    #[test]
    fn test_insertion_descending() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));

        {
            let mut tree_guard = tree.write();
            for i in (0..10).rev() {
                let tuple = create_tuple(i, &format!("val{}", i), &schema);
                assert!(
                    tree_guard.insert_entry(&tuple, RID::new(0, i as u32), &Default::default()),
                    "Insertion of descending key {} failed",
                    i
                );
            }
            println!("[Descending] Tree:\n{}", tree_guard.visualize());
        }

        // Validate that all keys exist
        let tree_guard = tree.read();
        for i in 0..10 {
            let tuple = create_tuple(i, &format!("val{}", i), &schema);
            let result = tree_guard.scan_key(&tuple, &Default::default()).unwrap();
            assert_eq!(result.len(), 1);
        }
    }

    #[test]
    fn test_insertion_random() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));

        let mut rng = rng();
        let mut numbers: Vec<i32> = (0..10).collect();
        numbers.shuffle(&mut rng);

        {
            let mut tree_guard = tree.write();
            for &num in &numbers {
                let tuple = create_tuple(num, &format!("random{}", num), &schema);
                assert!(
                    tree_guard.insert_entry(&tuple, RID::new(0, num as u32), &Default::default()),
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
            let result = tree_guard.scan_key(&tuple, &Default::default()).unwrap();
            assert_eq!(result.len(), 1);
        }
    }

    #[test]
    fn test_repeated_keys_and_rids() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));
        let tuple = create_tuple(42, "forty-two", &schema);
        let rid = RID::new(0, 42);

        {
            let mut tree_guard = tree.write();
            assert!(tree_guard.insert_entry(&tuple, rid, &Default::default()));
            assert!(!tree_guard.insert_entry(&tuple, rid, &Default::default())); // Duplicate should fail
            assert!(tree_guard.insert_entry(&tuple, RID::new(0, 43), &Default::default()));
            // Different RID
        }

        let tree_guard = tree.read();
        let result = tree_guard.scan_key(&tuple, &Default::default()).unwrap();
        assert_eq!(result.len(), 2, "Expected exactly two entries for key=42");
    }

    #[test]
    fn test_delete_non_existent_key() {
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(4, Arc::from(index_info));

        {
            let mut tree_guard = tree.write();
            let existing_tuple = create_tuple(100, "exists", &schema);
            assert!(tree_guard.insert_entry(
                &existing_tuple,
                RID::new(0, 100),
                &Default::default()
            ));
        }

        let missing_tuple = create_tuple(999, "missing", &schema);
        let tree_guard = tree.write();
        assert!(!tree_guard.delete_entry(&missing_tuple, RID::new(0, 999), &Default::default()));
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
        let mut tree_write_guard = tree.write();

        // Insert 2 keys (should not split yet as order is 3)
        let tuple1 = create_tuple(1, "value1", &schema);
        let tuple2 = create_tuple(2, "value2", &schema);

        println!("Initial tree state:");
        println!("{}", tree_write_guard.visualize());

        assert!(tree_write_guard.insert_entry(&tuple1, RID::new(0, 1), &Default::default()));
        assert!(tree_write_guard.insert_entry(&tuple2, RID::new(0, 2), &Default::default()));

        println!("\nAfter inserting 2 keys (before split):");
        println!("{}", tree_write_guard.visualize());

        // This insert should cause a split
        let tuple3 = create_tuple(3, "value3", &schema);
        assert!(tree_write_guard.insert_entry(&tuple3, RID::new(0, 3), &Default::default()));

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
        let mut tree_write_guard = tree.write();

        println!("Initial tree state:");
        println!("{}", tree_write_guard.visualize());

        // Insert keys in ascending order to force splits
        for i in 1..=7 {
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            assert!(tree_write_guard.insert_entry(&tuple, RID::new(0, i as u32), &Default::default()));
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
        let mut tree_write_guard = tree.write();

        println!("Initial tree state:");
        println!("{}", tree_write_guard.visualize());

        // Insert multiple entries with the same key
        for i in 1..=5 {
            let tuple = create_tuple(1, &format!("value1_{}", i), &schema);
            assert!(tree_write_guard.insert_entry(&tuple, RID::new(0, i), &Default::default()));
            println!("\nAfter inserting duplicate 1 (#{}):", i);
            println!("{}", tree_write_guard.visualize());
        }

        // Collect all values
        let search_tuple = create_tuple(1, "value1_1", &schema);
        let result = tree_write_guard.scan_key(&search_tuple, &Default::default()).unwrap();


        assert_eq!(result.len(), 5, "Should find all 5 duplicate entries");
    }

    /// Test node splits during interleaved insertions
    #[test]
    fn test_interleaved_splits() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(3, Arc::from(index_info));
        let mut tree_write_guard = tree.write();

        println!("Initial tree state:");
        println!("{}", tree_write_guard.visualize());

        // Insert values that will force splits in different parts of the tree
        let insert_sequence = vec![5, 3, 7, 2, 4, 6, 8];

        for &i in &insert_sequence {
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            assert!(tree_write_guard.insert_entry(&tuple, RID::new(0, i as u32), &Default::default()));
            println!("\nAfter inserting key {}:", i);
            println!("{}", tree_write_guard.visualize());
        }

        // Verify the final structure maintains order
        let start = create_tuple(1, "value1", &schema);
        let end = create_tuple(9, "value9", &schema);
        let result = tree_write_guard.scan_range(&start, &end, true).unwrap();

        let values: Vec<i32> = result
            .iter()
            .map(|(value, _)| get_integer_from_value(value))
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
        let mut tree_write_guard = tree.write();

        // Insert enough values to cause multiple splits
        for i in (1..=6).rev() {
            // Insert in reverse order
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            assert!(tree_write_guard.insert_entry(&tuple, RID::new(0, i as u32), &Default::default()));
            println!("\nAfter inserting key {}:", i);
            println!("{}", tree_write_guard.visualize());
        }

        // Perform a range scan to verify leaf node links
        let start = create_tuple(1, "value1", &schema);
        let end = create_tuple(6, "value6", &schema);
        let result = tree_write_guard.scan_range(&start, &end, true).unwrap();

        // Verify we got all values in order
        assert_eq!(result.len(), 6, "Should find all 6 values");
        let values: Vec<i32> = result
            .iter()
            .map(|(tuple, _)| match tuple.get_value() {
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

        let search_tuple = create_tuple(1, "value1", &schema);
        let result = tree_guard.scan_key(&search_tuple, &Default::default()).unwrap();
        assert!(result.is_empty(), "Empty tree should return no results");
    }

    /// Test scanning for a key that doesn't exist in a non-empty tree
    #[test]
    fn test_scan_key_nonexistent() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(3, Arc::from(index_info));
        let mut tree_guard = tree.write();

        // Insert some data
        for i in 1..=5 {
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            assert!(tree_guard.insert_entry(&tuple, RID::new(0, i as u32), &Default::default()));
        }

        // Search for non-existent key
        let search_tuple = create_tuple(10, "value10", &schema);
        let result = tree_guard.scan_key(&search_tuple, &Default::default()).unwrap();
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

        let mut tree_guard = tree.write();

        // Insert duplicates
        for i in 1..=3 {
            let tuple = create_tuple(1, &format!("value1_{}", i), &schema);
            assert!(tree_guard.insert_entry(&tuple, RID::new(0, i as u32), &Default::default()));
        }

        println!("Tree state:\n{}", tree_guard.visualize());

        // Search for duplicates
        let search_tuple = create_tuple(1, "value1_1", &schema);
        let result = tree_guard.scan_key(&search_tuple, &Default::default()).unwrap();
        assert_eq!(
            result.len(),
            3,
            "Should find all 3 duplicates in single leaf"
        );

        // Verify RIDs
        // let rids: HashSet<_> = result.iter().collect();
        // assert_eq!(rids.len(), 3, "Should have 3 unique RIDs");
        // for i in 1..=3 {
        //     assert!(rids.contains(&RID::new(0, i)), "Missing RID {}", i);
        // }
    }

    /// Test scanning for duplicates across leaf nodes
    #[test]
    fn test_scan_key_duplicates_multiple_leaves() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(3, Arc::from(index_info)); // Small order to force splits
        let mut tree_guard = tree.write();

        // Insert enough duplicates to cause splits
        for i in 1..=5 {
            let tuple = create_tuple(1, &format!("value1_{}", i), &schema);
            assert!(tree_guard.insert_entry(&tuple, RID::new(0, i as u32), &Default::default()));
            println!("\nAfter inserting duplicate 1 (#{}):", i);
            println!("{}", tree_guard.visualize());
        }

        // Search for duplicates
        let search_tuple = create_tuple(1, "value1_1", &schema);
        let result = tree_guard.scan_key(&search_tuple, &Default::default()).unwrap();
        assert_eq!(
            result.len(),
            5,
            "Should find all 5 duplicates across leaves"
        );

        // Verify RIDs
        // let rids: HashSet<_> = result.iter().collect();
        // assert_eq!(rids.len(), 5, "Should have 5 unique RIDs");
        // for i in 1..=5 {
        //     assert!(rids.contains(&RID::new(0, i)), "Missing RID {}", i);
        // }
    }

    /// Test scanning with mixed values including duplicates
    #[test]
    fn test_scan_key_mixed_values() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(3, Arc::from(index_info));
        let mut tree_guard = tree.write();

        // Insert mix of values and duplicates
        // Insert key 1 three times
        for i in 1..=3 {
            let tuple = create_tuple(1, &format!("value1_{}", i), &schema);
            assert!(tree_guard.insert_entry(&tuple, RID::new(0, i as u32), &Default::default()));
        }

        // Insert some other values
        let tuple2 = create_tuple(2, "value2", &schema);
        let tuple3 = create_tuple(3, "value3", &schema);
        assert!(tree_guard.insert_entry(&tuple2, RID::new(0, 4), &Default::default()));
        assert!(tree_guard.insert_entry(&tuple3, RID::new(0, 5), &Default::default()));

        // Insert more duplicates of key 1
        for i in 6..=7 {
            let tuple = create_tuple(1, &format!("value1_{}", i - 2), &schema);
            assert!(tree_guard.insert_entry(&tuple, RID::new(0, i as u32), &Default::default()));
        }

        println!("Final tree state:\n{}", tree_guard.visualize());

        // Search for key 1
        let search_tuple = create_tuple(1, "value1_1", &schema);
        let result = tree_guard.scan_key(&search_tuple, &Default::default()).unwrap();
        assert_eq!(result.len(), 5, "Should find all 5 duplicates of key 1");

        // Verify correct RIDs were found
    }

    /// Test scanning after deletion of some duplicates
    #[test]
    fn test_scan_key_after_deletions() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(3, Arc::from(index_info));
        let mut tree_guard = tree.write();

        // Store tuples for later deletion
        let mut inserted_tuples = Vec::new();

        // Insert duplicates
        for i in 1..=5 {
            let tuple = create_tuple(1, &format!("value1_{}", i), &schema);
            assert!(tree_guard.insert_entry(&tuple, RID::new(0, i), &Default::default()));
            inserted_tuples.push((tuple, RID::new(0, i)));
        }

        println!("Tree before deletions:\n{}", tree_guard.visualize());

        // Delete some duplicates using the exact same tuples we inserted
        assert!(
            tree_guard.delete_entry(&inserted_tuples[1].0, inserted_tuples[1].1, &Default::default()),
            "Failed to delete second tuple"
        );
        assert!(
            tree_guard.delete_entry(&inserted_tuples[3].0, inserted_tuples[3].1, &Default::default()),
            "Failed to delete fourth tuple"
        );

        println!("Tree after deletions:\n{}", tree_guard.visualize());

        // Search for remaining duplicates
        let search_tuple = create_tuple(1, "value1_1", &schema);
        let result = tree_guard.scan_key(&search_tuple, &Default::default()).unwrap();
        assert_eq!(result.len(), 3, "Should find remaining 3 duplicates");

        // Verify correct RIDs remain
    }
}