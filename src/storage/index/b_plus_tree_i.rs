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
        debug!("Inserting key {:?} with RID {:?}", key, rid);

        let root_is_full = {
            let root_guard = self.root.read();
            let is_full = root_guard.keys.len() == self.order - 1;
            debug!(
                "Root node status: keys={}, is_full={}",
                root_guard.keys.len(),
                is_full
            );
            is_full
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
        debug!("Starting delete for key {:?} with RID {:?}", key, rid);
        let mut nodes_to_check = vec![(Arc::clone(&self.root), Vec::new())];
        let found = false;

        // Search through all possible paths that could contain our key
        while let Some((current_node, path)) = nodes_to_check.pop() {
            let node = current_node.read();
            match node.node_type {
                NodeType::Internal => {
                    debug!("At internal node with keys: {:?}", node.keys);

                    // Find all children that could contain our key
                    let mut child_indices = Vec::new();

                    // Check each key to find potential paths
                    for (i, k) in node.keys.iter().enumerate() {
                        if k.compare_greater_than(key) == CmpBool::CmpTrue {
                            // If this key is greater, we need to check the left child
                            child_indices.push(i);
                        } else if k.compare_equals(key) == CmpBool::CmpTrue {
                            // If this key equals our search key, check both children
                            child_indices.push(i);
                            child_indices.push(i + 1);
                        }
                    }

                    // Always check the rightmost child if we haven't found a greater key
                    if !child_indices.contains(&(&node.children.len() - 1)) {
                        child_indices.push(node.children.len() - 1);
                    }

                    // Add all potential paths to our search queue
                    for child_idx in child_indices {
                        debug!("Adding child index {} for checking", child_idx);
                        let mut new_path = path.clone();
                        new_path.push((Arc::clone(&current_node), child_idx));
                        nodes_to_check.push((Arc::clone(&node.children[child_idx]), new_path));
                    }
                    drop(node);
                }
                NodeType::Leaf => {
                    debug!("Processing leaf node with keys: {:?}", node.keys);

                    // Check all entries in current leaf
                    for (i, (k, r)) in node.keys.iter().zip(node.values.iter()).enumerate() {
                        debug!("Checking entry {} - Key: {:?}, RID: {:?}", i, k, r);

                        // Check for exact match
                        if k.compare_equals(key) == CmpBool::CmpTrue && *r == rid {
                            debug!("Found matching key-RID pair at position {}", i);
                            drop(node);
                            let mut node_write = current_node.write();

                            // Remove the key and value
                            node_write.keys.remove(i);
                            node_write.values.remove(i);

                            // Check if node needs rebalancing
                            let is_underfull = node_write.keys.len() < (self.order - 1) / 2;
                            debug!(
                                "After deletion: keys={}, min_keys={}, needs_rebalancing={}",
                                node_write.keys.len(),
                                (self.order - 1) / 2,
                                is_underfull
                            );
                            drop(node_write);

                            if is_underfull && !path.is_empty() {
                                debug!("Node is underfull, starting rebalancing");
                                self.rebalance_after_delete(path);
                            }
                            return true;
                        }
                    }
                    drop(node);
                }
            }
        }

        if !found {
            debug!("Key-RID pair not found");
        }
        false
    }

    pub fn scan_range(
        &self,
        start_key: &Tuple,
        end_key: &Tuple,
        include_end: bool,
    ) -> Result<Vec<(Value, RID)>, String> {
        let start_key = self.extract_key(start_key);
        let end_key = self.extract_key(end_key);

        debug!("Starting range scan from {:?} to {:?}", start_key, end_key);
        let mut current_node = Arc::clone(&self.root);
        let mut entries = Vec::new();

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
                        .position(|k| k.compare_greater_than_equals(&start_key) == CmpBool::CmpTrue)
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
        loop {
            let node = current_node.read();
            debug!("Processing leaf node with keys: {:?}", node.keys);

            // Add values from current leaf that are within range
            for (key, rid) in node.keys.iter().zip(node.values.iter()) {
                if key.compare_less_than(&start_key) == CmpBool::CmpTrue {
                    continue;
                }
                if key.compare_greater_than(&end_key) == CmpBool::CmpTrue
                    || (!include_end && key.compare_equals(&end_key) == CmpBool::CmpTrue)
                {
                    debug!("Found key beyond range, ending scan");
                    return Ok(entries);
                }
                debug!("Found entry in range - Key: {:?}, RID: {:?}", key, rid);
                entries.push((key.clone(), *rid));
            }

            // Move to next leaf if it exists
            match &node.next_leaf {
                Some(next) => {
                    debug!("Moving to next leaf");
                    let next_node = Arc::clone(next);
                    drop(node);
                    current_node = next_node;
                }
                None => {
                    debug!("Reached last leaf, ending scan");
                    break;
                }
            }
        }

        debug!("Scan complete, collected {} entries", entries.len());
        Ok(entries)
    }

    pub fn scan_full(&self) -> Result<Vec<(Value, RID)>, String> {
        debug!("Starting full scan");

        let mut current_node = Arc::clone(&self.root);
        let mut entries = Vec::new();

        // Find leftmost leaf
        debug!("Finding leftmost leaf node");
        loop {
            let node = current_node.read();
            match node.node_type {
                NodeType::Internal => {
                    debug!("At internal node with keys: {:?}", node.keys);
                    // Always take leftmost child for full scan
                    let next_node = Arc::clone(&node.children[0]);
                    drop(node);
                    current_node = next_node;
                }
                NodeType::Leaf => {
                    debug!("Found leftmost leaf node");
                    break;
                }
            }
        }

        // Collect all values from leaf nodes
        debug!("Collecting entries from leaves");
        loop {
            let node = current_node.read();
            debug!("Processing leaf node with keys: {:?}", node.keys);

            // Pre-allocate space for entries from this leaf
            entries.reserve(node.keys.len());

            // Add all values from current leaf
            for (key, rid) in node.keys.iter().zip(node.values.iter()) {
                debug!("Found entry - Key: {:?}, RID: {:?}", key, rid);
                entries.push((key.clone(), *rid));
            }

            // Move to next leaf if it exists
            if let Some(next) = &node.next_leaf {
                debug!("Moving to next leaf");
                let next_node = Arc::clone(next);
                drop(node);
                current_node = next_node;
            } else {
                debug!("Reached last leaf, ending scan");
                break;
            }
        }

        debug!("Scan complete, collected {} entries", entries.len());
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
            match key.get_val() {
                Val::Integer(i) => i.to_string(),
                Val::VarLen(s) => s.clone(),
                _ => format!("{:?}", key.get_val()),
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
        debug!("Finding insert position for key {:?}", key);
        debug!("Current keys: {:?}", keys);

        // For duplicate keys, we want to insert after existing equal keys
        // This ensures proper routing in internal nodes
        let pos = keys
            .iter()
            .position(|k| {
                match k.compare_equals(key) {
                    CmpBool::CmpTrue => false, // Keep looking, want to insert after equals
                    _ => k.compare_greater_than(key) == CmpBool::CmpTrue,
                }
            })
            .unwrap_or(keys.len());

        debug!("Found insert position: {}", pos);
        pos
    }

    // Update insert_non_full to maintain sorted order
    fn insert_non_full(&self, node: &mut BPlusTreeNode, key: Value, rid: RID) -> bool {
        debug!("Inserting into non-full node: key={:?}, rid={:?}", key, rid);
        debug!(
            "Current node: type={:?}, keys={:?}",
            node.node_type, node.keys
        );

        match node.node_type {
            NodeType::Leaf => {
                let pos = self.find_insert_position(&node.keys, &key);
                debug!("Found insert position {} in leaf node", pos);

                // Check for duplicates by scanning all entries with the same key
                let mut i = pos;
                // Check entries before pos with same key
                while i > 0 {
                    i -= 1;
                    if node.keys[i].compare_equals(&key) == CmpBool::CmpTrue {
                        if node.values[i] == rid {
                            debug!("Found duplicate key-RID pair at position {}", i);
                            return false;
                        }
                    } else {
                        break;
                    }
                }
                // Check entries from pos onwards with same key
                i = pos;
                while i < node.keys.len() {
                    if node.keys[i].compare_equals(&key) == CmpBool::CmpTrue {
                        if node.values[i] == rid {
                            debug!("Found duplicate key-RID pair at position {}", i);
                            return false;
                        }
                    } else {
                        break;
                    }
                    i += 1;
                }

                debug!(
                    "Inserting at position {}: key={:?}, rid={:?}",
                    pos, key, rid
                );
                node.keys.insert(pos, key);
                node.values.insert(pos, rid);
                debug!("Leaf node after insertion: keys={:?}", node.keys);
                true
            }
            NodeType::Internal => {
                let pos = self.find_insert_position(&node.keys, &key);
                debug!("Found child position {} in internal node", pos);

                // Ensure we don't go past the last child
                let child_idx = if pos >= node.children.len() {
                    debug!(
                        "Adjusting child index from {} to {}",
                        pos,
                        node.children.len() - 1
                    );
                    node.children.len() - 1
                } else {
                    pos
                };

                let child_arc = Arc::clone(&node.children[child_idx]);
                debug!(
                    "Checking child node at position {} of {}",
                    child_idx,
                    node.children.len()
                );

                {
                    let mut child_guard = child_arc.write();
                    let needs_split = child_guard.keys.len() == self.order - 1;
                    debug!(
                        "Child node status: keys={}, needs_split={}",
                        child_guard.keys.len(),
                        needs_split
                    );

                    if needs_split {
                        drop(child_guard);
                        debug!("Child node is full, performing split");
                        self.split_child(node, child_idx);

                        // After split, find new position and get new child
                        let new_pos = self.find_insert_position(&node.keys, &key);
                        debug!("After split, new insert position is {}", new_pos);

                        // Again ensure we don't go past the last child
                        let new_child_idx = if new_pos >= node.children.len() {
                            debug!(
                                "Adjusting new child index from {} to {}",
                                new_pos,
                                node.children.len() - 1
                            );
                            node.children.len() - 1
                        } else {
                            new_pos
                        };

                        let child_arc = Arc::clone(&node.children[new_child_idx]);
                        let mut child_guard = child_arc.write();
                        return self.insert_non_full(&mut *child_guard, key, rid);
                    }

                    debug!("Recursing into child node");
                    self.insert_non_full(&mut *child_guard, key, rid)
                }
            }
        }
    }

    // Update split_child to handle routing keys properly
    fn split_child(&self, parent: &mut BPlusTreeNode, child_idx: usize) -> bool {
        let child = Arc::clone(&parent.children[child_idx]);
        let mut child_guard = child.write();
        let mut new_node = BPlusTreeNode::new(child_guard.node_type.clone());
        let middle = (self.order - 1) / 2;

        match child_guard.node_type {
            NodeType::Leaf => {
                debug!("Splitting leaf node at position {}", middle);
                let split_point = child_guard.keys.len() / 2;

                // Move entries to new node
                new_node.keys = child_guard.keys.split_off(split_point);
                new_node.values = child_guard.values.split_off(split_point);

                // Update leaf node links
                new_node.next_leaf = child_guard.next_leaf.clone();
                let new_node_arc = Arc::new(RwLock::new(new_node));
                child_guard.next_leaf = Some(Arc::clone(&new_node_arc));

                // For leaf splits, use the first key of the right node as separator
                let split_key = new_node_arc.read().keys[0].clone();

                let mut insert_pos = self.find_insert_position(&parent.keys, &split_key);
                insert_pos = insert_pos.min(parent.keys.len());

                debug!(
                    "Inserting separator key at position {} in parent (len={})",
                    insert_pos,
                    parent.keys.len()
                );

                parent.keys.insert(insert_pos, split_key);
                parent.children.insert(insert_pos + 1, new_node_arc.clone());

                debug!(
                    "Leaf split complete - Left node: {:?}, Right node: {:?}, Parent keys: {:?}",
                    child_guard.keys,
                    new_node_arc.read().keys,
                    parent.keys
                );
            }
            NodeType::Internal => {
                debug!("Splitting internal node at position {}", middle);

                // For internal nodes, we need to handle the middle key differently
                let split_point = child_guard.keys.len() / 2;

                // Get the middle key that will move up to the parent
                let middle_key = child_guard.keys[split_point].clone();

                // Move entries to new node (excluding middle key)
                new_node.keys = child_guard.keys.split_off(split_point + 1);
                new_node.children = child_guard.children.split_off(split_point + 1);

                // Remove the middle key from the child (it moves up)
                child_guard.keys.remove(split_point);

                // For internal splits, ensure the routing key is greater than all keys in left subtree
                let mut insert_pos = self.find_insert_position(&parent.keys, &middle_key);
                insert_pos = insert_pos.min(parent.keys.len());

                debug!("Inserting middle key at position {} in parent", insert_pos);

                parent.keys.insert(insert_pos, middle_key.clone());
                parent
                    .children
                    .insert(insert_pos + 1, Arc::new(RwLock::new(new_node.clone())));

                debug!(
                    "Internal split complete - Left node: {:?}, Middle key: {:?}, Right node: {:?}",
                    child_guard.keys, middle_key, new_node.keys
                );
            }
        }
        true
    }

    fn rebalance_after_delete(&self, path: Vec<(Arc<RwLock<BPlusTreeNode>>, usize)>) {
        let mut current_path = path;

        while !current_path.is_empty() {
            let (parent_arc, child_idx) = current_path.pop().unwrap();
            let mut parent = parent_arc.write();

            // Check if child needs rebalancing
            let child = Arc::clone(&parent.children[child_idx]);
            let child_guard = child.read();
            let min_keys = (self.order - 1) / 2;

            if child_guard.keys.len() >= min_keys {
                debug!(
                    "Child has enough keys ({} >= {}), no rebalancing needed",
                    child_guard.keys.len(),
                    min_keys
                );
                return;
            }
            drop(child_guard);

            // Try to borrow from siblings
            let borrowed = if child_idx > 0 {
                // Try left sibling
                let left_sibling = Arc::clone(&parent.children[child_idx - 1]);
                let left_guard = left_sibling.read();

                if left_guard.keys.len() > min_keys {
                    drop(left_guard);
                    debug!("Borrowing from left sibling");
                    self.borrow_from_left_sibling(&mut parent, child_idx);
                    true
                } else {
                    drop(left_guard);
                    false
                }
            } else {
                false
            } || if child_idx < parent.children.len() - 1 {
                // Try right sibling
                let right_sibling = Arc::clone(&parent.children[child_idx + 1]);
                let right_guard = right_sibling.read();

                if right_guard.keys.len() > min_keys {
                    drop(right_guard);
                    debug!("Borrowing from right sibling");
                    self.borrow_from_right_sibling(&mut parent, child_idx);
                    true
                } else {
                    drop(right_guard);
                    false
                }
            } else {
                false
            };

            if !borrowed {
                debug!("Could not borrow from siblings, need to merge");
                // If we get here, we need to merge
                if child_idx > 0 {
                    debug!("Merging with left sibling");
                    self.merge_with_left_sibling(&mut parent, child_idx);
                } else if child_idx < parent.children.len() - 1 {
                    debug!("Merging with right sibling");
                    self.merge_with_right_sibling(&mut parent, child_idx);
                }

                // Check if parent needs rebalancing (unless it's the root)
                if parent.keys.is_empty() && Arc::ptr_eq(&parent_arc, &self.root) {
                    debug!("Root is empty after merge, updating tree height");
                    // Root is empty after merge, make the first child the new root
                    if !parent.children.is_empty() {
                        let new_root = Arc::clone(&parent.children[0]);
                        let new_root_node = {
                            let node = new_root.write();
                            BPlusTreeNode {
                                node_type: node.node_type.clone(),
                                keys: node.keys.clone(),
                                children: node.children.clone(),
                                values: node.values.clone(),
                                next_leaf: node.next_leaf.clone(),
                            }
                        };
                        drop(parent);
                        let mut root = self.root.write();
                        *root = new_root_node;
                    }
                    return;
                }

                if parent.keys.len() < min_keys && !current_path.is_empty() {
                    debug!("Parent needs rebalancing, continuing up the tree");
                    continue;
                }
            }

            debug!("Rebalancing complete at this level");
            break;
        }
    }

    fn borrow_from_left_sibling(&self, parent: &mut BPlusTreeNode, child_idx: usize) {
        let left_sibling = Arc::clone(&parent.children[child_idx - 1]);
        let child = Arc::clone(&parent.children[child_idx]);

        let mut left = left_sibling.write();
        let mut right = child.write();

        match right.node_type {
            NodeType::Leaf => {
                // Move rightmost entry from left to right
                let key = left.keys.pop().unwrap();
                let value = left.values.pop().unwrap();
                right.keys.insert(0, key.clone());
                right.values.insert(0, value);
                // Update parent's separator key
                parent.keys[child_idx - 1] = key;
            }
            NodeType::Internal => {
                // Move parent's separator down to right child
                let separator = parent.keys[child_idx - 1].clone();
                right.keys.insert(0, separator);

                // Move rightmost child from left to right
                if !left.children.is_empty() {
                    let child = Arc::clone(&left.children[left.children.len() - 1]);
                    left.children.pop();
                    right.children.insert(0, child);
                }

                // Move rightmost key from left to parent
                parent.keys[child_idx - 1] = left.keys.pop().unwrap();
            }
        }
    }

    fn borrow_from_right_sibling(&self, parent: &mut BPlusTreeNode, child_idx: usize) {
        let child = Arc::clone(&parent.children[child_idx]);
        let right_sibling = Arc::clone(&parent.children[child_idx + 1]);

        let mut left = child.write();
        let mut right = right_sibling.write();

        match left.node_type {
            NodeType::Leaf => {
                // Move leftmost entry from right to left
                let key = right.keys.remove(0);
                let value = right.values.remove(0);
                left.keys.push(key.clone());
                left.values.push(value);
                // Update parent's separator key
                parent.keys[child_idx] = right.keys[0].clone();
            }
            NodeType::Internal => {
                // Move parent's separator down to left child
                let separator = parent.keys[child_idx].clone();
                left.keys.push(separator);

                // Move leftmost child from right to left
                if !right.children.is_empty() {
                    let child = Arc::clone(&right.children[0]);
                    right.children.remove(0);
                    left.children.push(child);
                }

                // Move leftmost key from right to parent
                parent.keys[child_idx] = right.keys.remove(0);
            }
        }
    }

    fn merge_with_left_sibling(&self, parent: &mut BPlusTreeNode, child_idx: usize) {
        debug!("Merging node {} with its left sibling", child_idx);
        let left_sibling = Arc::clone(&parent.children[child_idx - 1]);
        let child = Arc::clone(&parent.children[child_idx]);

        let mut left = left_sibling.write();
        let mut right = child.write();

        match right.node_type {
            NodeType::Leaf => {
                // Move all entries from right to left
                left.keys.extend(right.keys.drain(..));
                left.values.extend(right.values.drain(..));
                // Update leaf node links
                left.next_leaf = right.next_leaf.take();
            }
            NodeType::Internal => {
                // Move parent's separator key down to left sibling
                let separator = parent.keys.remove(child_idx - 1);
                left.keys.push(separator);

                // Move all keys and children from right to left
                left.keys.extend(right.keys.drain(..));
                left.children.extend(right.children.drain(..));
            }
        }

        // Remove the right child from parent
        parent.children.remove(child_idx);

        // Remove the separator key from parent if it exists
        if child_idx - 1 < parent.keys.len() {
            parent.keys.remove(child_idx - 1);
        }

        debug!("Merge complete - Parent keys: {:?}", parent.keys);
    }

    fn merge_with_right_sibling(&self, parent: &mut BPlusTreeNode, child_idx: usize) {
        debug!("Merging node {} with its right sibling", child_idx);
        let child = Arc::clone(&parent.children[child_idx]);
        let right_sibling = Arc::clone(&parent.children[child_idx + 1]);

        let mut left = child.write();
        let mut right = right_sibling.write();

        match left.node_type {
            NodeType::Leaf => {
                // Move all entries from right to left
                left.keys.extend(right.keys.drain(..));
                left.values.extend(right.values.drain(..));
                // Update leaf node links
                left.next_leaf = right.next_leaf.take();
            }
            NodeType::Internal => {
                // Move parent's separator key down to left node
                let separator = parent.keys.remove(child_idx);
                left.keys.push(separator);

                // Move all keys and children from right to left
                left.keys.extend(right.keys.drain(..));
                left.children.extend(right.children.drain(..));
            }
        }

        // Remove the right sibling from parent
        parent.children.remove(child_idx + 1);

        // Remove the separator key from parent
        if child_idx < parent.keys.len() {
            parent.keys.remove(child_idx);
        }

        debug!("Merge complete - Parent keys: {:?}", parent.keys);
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

    fn insert_entry(&mut self, tuple: &Tuple, rid: RID, _transaction: &Transaction) -> bool {
        // Extract key from tuple using metadata
        let key = self.extract_key(tuple);
        self.insert(key, rid)
    }

    fn delete_entry(&self, tuple: &Tuple, rid: RID, _transaction: &Transaction) -> bool {
        let key = self.extract_key(tuple);
        self.delete(&key, rid)
    }

    fn scan_key(
        &self,
        key: &Tuple,
        _transaction: &Transaction,
    ) -> Result<Vec<(Value, RID)>, String> {
        let result = self.scan_range(key, key, true).unwrap();
        debug!("Scan complete, found {} matching entries", result.len());
        Ok(result)
    }

    fn get_metadata(&self) -> Arc<IndexInfo> {
        Arc::clone(&self.metadata)
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
        match value.get_val() {
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
        let result = tree_write_guard
            .scan_key(&tuple, &Default::default())
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1, rid);

        // Delete the value
        assert!(tree_write_guard.delete_entry(&tuple, rid, &Default::default()));

        // Verify the value is gone
        let result2 = tree_write_guard
            .scan_key(&tuple, &Default::default())
            .unwrap();
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
        assert_eq!(
            result[0].0.compare_equals(&Value::new(3)),
            CmpBool::CmpTrue,
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
            .map(|(value, _)| match value.get_val() {
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
    use crate::common::logger::initialize_logger;
    use crate::concurrency::transaction::Transaction;
    use rand::prelude::*;
    use rand::rng;

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
            let result = tree_guard
                .scan_key(&tuple, &Transaction::default())
                .unwrap();
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
            assert!(tree_write_guard.insert_entry(
                &tuple,
                RID::new(0, i as u32),
                &Default::default()
            ));
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
            assert!(tree_write_guard.insert_entry(
                &tuple,
                RID::new(0, i as u32),
                &Default::default()
            ));
            println!("\nAfter inserting duplicate 1 (#{}):", i);
            println!("{}", tree_write_guard.visualize());
        }

        // Collect all values
        let search_tuple = create_tuple(1, "value1_1", &schema);
        let result = tree_write_guard
            .scan_key(&search_tuple, &Default::default())
            .unwrap();

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
            assert!(tree_write_guard.insert_entry(
                &tuple,
                RID::new(0, i as u32),
                &Default::default()
            ));
            println!("\nAfter inserting key {}:", i);
            println!("{}", tree_write_guard.visualize());
        }

        // Verify the final structure maintains order
        let start = create_tuple(1, "value1", &schema);
        let end = create_tuple(9, "value9", &schema);
        let result = tree_write_guard.scan_range(&start, &end, true).unwrap();

        let values: Vec<i32> = result
            .iter()
            .map(|(value, _)| match value.get_val() {
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
        let mut tree_write_guard = tree.write();

        // Insert enough values to cause multiple splits
        for i in (1..=6).rev() {
            // Insert in reverse order
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            assert!(tree_write_guard.insert_entry(
                &tuple,
                RID::new(0, i as u32),
                &Default::default()
            ));
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
            .map(|(tuple, _)| match tuple.get_val() {
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

    #[test]
    fn test_internal_node_split_routing_keys() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(3, Arc::from(index_info));
        let mut tree_guard = tree.write();

        // Insert values that will cause internal node splits
        for i in 1..=5 {
            let tuple = create_tuple(i, &format!("value1_{}", i), &schema);
            assert!(
                tree_guard.insert_entry(&tuple, RID::new(0, i as u32), &Default::default()),
                "Failed to insert tuple {}",
                i
            );
            println!(
                "Tree after inserting tuple {}:\n{}",
                i,
                tree_guard.visualize()
            );
        }

        println!("Tree after insertions:\n{}", tree_guard.visualize());

        // Verify internal node structure
        let root = tree_guard.root.read();
        assert_eq!(
            root.node_type,
            NodeType::Internal,
            "Root should be internal"
        );
        assert!(!root.keys.is_empty(), "Root should have routing keys");

        // Check each internal node has proper routing keys
        for (i, child) in root.children.iter().enumerate() {
            let child_node = child.read();
            if child_node.node_type == NodeType::Internal {
                assert!(
                    !child_node.keys.is_empty(),
                    "Internal node at position {} should have routing keys",
                    i
                );
            }
        }

        // Try to delete entries to verify routing works
        drop(root);
        for i in 1..=5 {
            let tuple = create_tuple(i, &format!("value1_{}", i), &schema);
            assert!(
                tree_guard.delete_entry(&tuple, RID::new(0, i as u32), &Default::default()),
                "Failed to delete tuple {}",
                i
            );
            println!(
                "Tree after deleting tuple {}:\n{}",
                i,
                tree_guard.visualize()
            );
        }
    }
}

#[cfg(test)]
mod scan_key_tests {
    use super::test_utils::*;
    use super::*;
    use crate::common::logger::initialize_logger;

    /// Test scanning for a key in an empty tree
    #[test]
    fn test_scan_key_empty_tree() {
        initialize_logger();
        let schema = create_test_schema();
        let index_info = create_test_metadata();
        let tree = create_test_tree(3, Arc::from(index_info));
        let tree_guard = tree.write();

        let search_tuple = create_tuple(1, "value1", &schema);
        let result = tree_guard
            .scan_key(&search_tuple, &Default::default())
            .unwrap();
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
        let result = tree_guard
            .scan_key(&search_tuple, &Default::default())
            .unwrap();
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
        let result = tree_guard
            .scan_key(&search_tuple, &Default::default())
            .unwrap();
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
        let tree = create_test_tree(3, Arc::from(index_info));
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
        let result = tree_guard
            .scan_key(&search_tuple, &Default::default())
            .unwrap();
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
        let result = tree_guard
            .scan_key(&search_tuple, &Default::default())
            .unwrap();
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
            tree_guard.delete_entry(
                &inserted_tuples[1].0,
                inserted_tuples[1].1,
                &Default::default()
            ),
            "Failed to delete second tuple"
        );
        assert!(
            tree_guard.delete_entry(
                &inserted_tuples[3].0,
                inserted_tuples[3].1,
                &Default::default()
            ),
            "Failed to delete fourth tuple"
        );

        println!("Tree after deletions:\n{}", tree_guard.visualize());

        // Search for remaining duplicates
        let search_tuple = create_tuple(1, "value", &schema);
        let result = tree_guard
            .scan_key(&search_tuple, &Default::default())
            .unwrap();
        assert_eq!(result.len(), 3, "Should find remaining 3 duplicates");

        // Verify correct RIDs remain
    }
}
