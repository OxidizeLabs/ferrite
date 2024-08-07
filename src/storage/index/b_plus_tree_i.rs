use log::{debug, info};
use spin::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
enum NodeType {
    Internal,
    Leaf,
}

#[derive(Clone)]
pub struct BPlusTreeNode<K: Ord + Clone, V: Clone> {
    node_type: NodeType,
    keys: Vec<K>,
    children: Vec<Arc<RwLock<BPlusTreeNode<K, V>>>>, // for internal nodes
    values: Vec<V>,                                  // for leaf nodes
}

impl<K: Ord + Clone, V: Clone> BPlusTreeNode<K, V> {
    fn new(node_type: NodeType) -> Self {
        BPlusTreeNode {
            node_type,
            keys: Vec::new(),
            children: Vec::new(),
            values: Vec::new(),
        }
    }
}

pub struct BPlusTree<K: Ord + Clone, V: Clone> {
    root: Arc<RwLock<BPlusTreeNode<K, V>>>,
    order: usize, // Max number of children per node
}

impl<K: Ord + Clone + std::fmt::Debug, V: Clone + std::fmt::Debug> BPlusTree<K, V> {
    /// Creates a new B+Tree with the given order.
    pub fn new(order: usize) -> Self {
        BPlusTree {
            root: Arc::new(RwLock::new(BPlusTreeNode::new(NodeType::Leaf))),
            order,
        }
    }

    /// Searches for the value associated with the given key.
    pub fn search(&self, key: &K) -> Option<V> {
        let root = self.root.read();
        self.search_node(&root, key)
    }

    fn search_node(&self, node: &BPlusTreeNode<K, V>, key: &K) -> Option<V> {
        match node.node_type {
            NodeType::Internal => {
                let i = node
                    .keys
                    .iter()
                    .position(|k| key < k)
                    .unwrap_or(node.keys.len());
                let child = node.children[i].read();
                self.search_node(&child, key)
            }
            NodeType::Leaf => {
                if let Some(index) = node.keys.iter().position(|k| k == key) {
                    Some(node.values[index].clone())
                } else {
                    None
                }
            }
        }
    }

    fn find_key_index(&self, node: &BPlusTreeNode<K, V>, key: &K) -> usize {
        node.keys
            .iter()
            .position(|k| key < k)
            .unwrap_or(node.keys.len())
    }

    /// Inserts a key-value pair into the B+Tree.
    pub fn insert(&self, key: K, value: V) {
        let mut root_guard = self.root.write();
        let root_clone = (*root_guard).clone();
        if root_guard.keys.len() == self.order - 1 {
            let mut new_root = BPlusTreeNode::new(NodeType::Internal);
            new_root.children.push(Arc::new(RwLock::new(root_clone)));
            self.split_child(&mut new_root, 0);
            *root_guard = new_root;
        }
        self.insert_non_full(&mut root_guard, key, value);
    }

    fn insert_non_full(&self, node: &mut BPlusTreeNode<K, V>, key: K, value: V) {
        match node.node_type {
            NodeType::Leaf => {
                let i = node
                    .keys
                    .iter()
                    .position(|k| k > &key)
                    .unwrap_or(node.keys.len());
                info!("Inserting key: {:?} at position: {}", key, i);
                node.keys.insert(i, key);
                node.values.insert(i, value);
            }
            NodeType::Internal => {
                let i = node
                    .keys
                    .iter()
                    .position(|k| k > &key)
                    .unwrap_or(node.keys.len());
                info!("Traversing to child index: {}", i);

                if node.children[i].read().keys.len() == self.order - 1 {
                    self.split_child(node, i);
                }

                let mut child = node.children[i].write();
                self.insert_non_full(&mut child, key, value);
            }
        }
    }

    fn split_child(&self, parent: &mut BPlusTreeNode<K, V>, index: usize) {
        let order = self.order;
        let mut new_child;
        {
            let mut full_child = parent.children[index].write();
            let mid_index = order / 2;
            let mid_key = full_child.keys[mid_index].clone();
            info!("Splitting node at index: {}, mid_key: {:?}", index, mid_key);

            new_child = BPlusTreeNode::new(full_child.node_type.clone());

            new_child.keys = full_child.keys.split_off(mid_index);
            if let NodeType::Leaf = full_child.node_type {
                new_child.values = full_child.values.split_off(mid_index);
            } else {
                new_child.children = full_child.children.split_off(mid_index + 1);
            }

            full_child.keys.truncate(mid_index);
            if let NodeType::Leaf = full_child.node_type {
                full_child.values.truncate(mid_index);
            }

            parent.keys.insert(index, mid_key);
        } // `full_child` is dropped here

        parent
            .children
            .insert(index + 1, Arc::new(RwLock::new(new_child)));

        info!("Parent node after split: {:?}", parent.keys);
        info!(
            "Left child keys after split: {:?}",
            parent.children[index].read().keys
        );
        info!(
            "Right child keys after split: {:?}",
            parent.children[index + 1].read().keys
        );
    }

    /// Deletes a key-value pair from the B+Tree.
    pub fn delete(&mut self, key: &K) {
        info!("Starting deletion of key: {:?}", key);
        if let Some((node, index)) = self.find_node_for_key(key) {
            let mut node_guard = node.write();
            if self.delete_from_node(&mut node_guard, key, index) && node_guard.keys.is_empty() {
                if node_guard.node_type == NodeType::Internal && !node_guard.children.is_empty() {
                    self.root = node_guard.children.remove(0);
                } else {
                    self.root = Arc::new(RwLock::new(BPlusTreeNode::new(NodeType::Leaf)));
                }
            }
        } else {
            info!("Key not found: {:?}", key);
        }
    }

    fn find_node_for_key(&self, key: &K) -> Option<(Arc<RwLock<BPlusTreeNode<K, V>>>, usize)> {
        self.find_node_for_key_recursive(self.root.clone(), key)
    }

    fn find_node_for_key_recursive(
        &self,
        node: Arc<RwLock<BPlusTreeNode<K, V>>>,
        key: &K,
    ) -> Option<(Arc<RwLock<BPlusTreeNode<K, V>>>, usize)> {
        let current_guard = node.read();
        info!("Searching node with keys: {:?}", current_guard.keys);
        match current_guard.node_type {
            NodeType::Leaf => {
                if let Some(index) = current_guard.keys.iter().position(|k| k == key) {
                    info!(
                        "Found key in leaf node: {:?} at index: {:?}",
                        current_guard.keys, index
                    );
                    Some((node.clone(), index))
                } else {
                    info!(
                        "Key not found in leaf node with keys: {:?}",
                        current_guard.keys
                    );
                    None
                }
            }
            NodeType::Internal => {
                let i = self.find_key_index(&current_guard, key);
                info!(
                    "Traversing internal node: {:?}, going to child index: {}",
                    current_guard.keys, i
                );
                drop(current_guard); // Drop the read lock before recursing
                let child = node.read().children[i].clone();
                self.find_node_for_key_recursive(child, key)
            }
        }
    }

    fn delete_from_internal_node(&self, node: &mut BPlusTreeNode<K, V>, index: usize) {
        info!("Deleting from internal node: {:?}", node.keys);

        // Find the predecessor key and value
        let (predecessor_key, predecessor_value) = {
            let mut current = node.children[index].clone();
            loop {
                let current_guard = current.read();
                match current_guard.node_type {
                    NodeType::Leaf => {
                        let key = current_guard.keys.last().unwrap().clone();
                        let value = current_guard.values.last().unwrap().clone();
                        break (key, value);
                    }
                    NodeType::Internal => {
                        let next = current_guard.children.last().unwrap().clone();
                        drop(current_guard); // Release the lock before reassigning
                        current = next;
                    }
                }
            }
        };

        // Replace the key in the internal node with the predecessor key
        node.keys[index] = predecessor_key.clone();

        // Delete the predecessor key from the child node
        {
            let child_len;
            {
                let mut child = node.children[index].write();
                child_len = child.keys.len();
                self.delete_from_node(&mut child, &predecessor_key, child_len - 1);
            }

            // Balance the tree after deletion
            self.balance_after_deletion(node, index);
        }
    }

    fn delete_from_node(&self, node: &mut BPlusTreeNode<K, V>, key: &K, index: usize) -> bool {
        match node.node_type {
            NodeType::Leaf => {
                if let Some(index) = node.keys.iter().position(|k| k == key) {
                    node.keys.remove(index);
                    node.values.remove(index);
                    true
                } else {
                    false
                }
            }
            NodeType::Internal => {
                let i = self.find_key_index(node, key);
                if i < node.keys.len() && key == &node.keys[i] {
                    self.delete_from_internal_node(node, i);
                    true
                } else {
                    let delete_successful;
                    {
                        let mut child = node.children[i].write();
                        delete_successful = self.delete_from_node(&mut child, key, i);
                    }
                    if delete_successful {
                        self.balance_after_deletion(node, i);
                    }
                    delete_successful
                }
            }
        }
    }

    fn balance_after_deletion(&self, node: &mut BPlusTreeNode<K, V>, index: usize) {
        let order = self.order;

        let underfull;
        {
            let child_guard = node.children[index].read();
            underfull = child_guard.keys.len() < (order - 1) / 2;
        }

        if underfull {
            debug!("Node is underfull: {:?}", node.keys);
            let left_sibling_index = if index > 0 { Some(index - 1) } else { None };
            let right_sibling_index = if index < node.children.len() - 1 {
                Some(index + 1)
            } else {
                None
            };

            if let Some(left_index) = left_sibling_index {
                let borrow_needed;
                {
                    let left_sibling = node.children[left_index].read();
                    borrow_needed = left_sibling.keys.len() > (self.order - 1) / 2;
                }

                if borrow_needed {
                    debug!("Borrowing from left sibling: {:?}", node.keys);
                    self.borrow_from_left_sibling(node, index, left_index);
                    return;
                }
            }

            if let Some(right_index) = right_sibling_index {
                let borrow_needed;
                {
                    let right_sibling = node.children[right_index].read();
                    borrow_needed = right_sibling.keys.len() > (self.order - 1) / 2;
                }

                if borrow_needed {
                    debug!("Borrowing from right sibling: {:?}", node.keys);
                    self.borrow_from_right_sibling(node, index, right_index);
                    return;
                }
            }

            if let Some(left_index) = left_sibling_index {
                debug!("Merging with left sibling: {:?}", node.keys);
                self.merge_nodes(node, left_index, index);
            } else if let Some(right_index) = right_sibling_index {
                debug!("Merging with right sibling: {:?}", node.keys);
                self.merge_nodes(node, index, right_index);
            }
        }
    }

    fn borrow_from_left_sibling(
        &self,
        node: &mut BPlusTreeNode<K, V>,
        index: usize,
        left_index: usize,
    ) {
        let mut left_sibling = node.children[left_index].write();
        let mut child = node.children[index].write();

        child.keys.insert(0, node.keys[left_index].clone());
        node.keys[left_index] = left_sibling.keys.pop().unwrap();

        if let NodeType::Internal = child.node_type {
            child
                .children
                .insert(0, left_sibling.children.pop().unwrap());
        } else {
            child.values.insert(0, left_sibling.values.pop().unwrap());
        }
    }

    fn borrow_from_right_sibling(
        &self,
        node: &mut BPlusTreeNode<K, V>,
        index: usize,
        right_index: usize,
    ) {
        let mut right_sibling = node.children[right_index].write();
        let mut child = node.children[index].write();

        child.keys.push(node.keys[index].clone());
        node.keys[index] = right_sibling.keys.remove(0);

        if let NodeType::Internal = child.node_type {
            child.children.push(right_sibling.children.remove(0));
        } else {
            child.values.push(right_sibling.values.remove(0));
        }
    }

    fn merge_nodes(&self, node: &mut BPlusTreeNode<K, V>, left_index: usize, right_index: usize) {
        let (left_keys, left_values, right_keys, right_values) = {
            let mut left_child_guard = node.children[left_index].write();
            let mut right_child_guard = node.children[right_index].write();

            // Perform the merge
            let (left_keys, left_values, right_keys, right_values) =
                if let NodeType::Leaf = left_child_guard.node_type {
                    (
                        std::mem::take(&mut left_child_guard.keys),
                        std::mem::take(&mut left_child_guard.values),
                        std::mem::take(&mut right_child_guard.keys),
                        std::mem::take(&mut right_child_guard.values),
                    )
                } else {
                    left_child_guard.keys.push(node.keys[left_index].clone());
                    (
                        std::mem::take(&mut left_child_guard.keys),
                        vec![],
                        std::mem::take(&mut right_child_guard.keys),
                        vec![],
                    )
                };

            info!(
                "Merging nodes: left = {:?}, right = {:?}",
                left_keys, right_keys
            );

            (left_keys, left_values, right_keys, right_values)
        }; // Drop child guards here

        if left_values.is_empty() {
            node.keys.remove(left_index);
        }

        {
            let mut left_child_guard = node.children[left_index].write();
            let mut right_child_guard = node.children[right_index].write();

            left_child_guard.keys.extend(right_keys);
            left_child_guard.values.extend(right_values);

            if !left_values.is_empty() {
                left_child_guard.values.extend(left_values);
            } else {
                left_child_guard
                    .children
                    .extend(right_child_guard.children.drain(..));
            }
        } // Drop child guards here

        node.children.remove(right_index);

        // Step 2: Update root if necessary
        if node.keys.is_empty() {
            info!("Parent node is empty after merge, updating root");
            if !node.children.is_empty() {
                let new_root_guard = node.children.remove(0);
                let new_root = new_root_guard.write();
                self.root.write().clone_from(&*new_root);
            } else {
                // If there are no children left, set root to None
                *self.root.write() = BPlusTreeNode::new(NodeType::Leaf);
            }
        }
    }

    /// Returns a string representation of the B+Tree.
    pub fn to_string(&self) -> String {
        let root = self.root.read();
        self.node_to_string(&root, 0)
    }

    fn node_to_string(&self, node: &BPlusTreeNode<K, V>, level: usize) -> String {
        let mut result = String::new();
        if node.node_type == NodeType::Internal {
            result.push_str(&format!(
                "{}Internal Node: {:?}\n",
                "  ".repeat(level),
                node.keys
            ));
            for child in &node.children {
                result.push_str(&self.node_to_string(&child.read(), level + 1));
            }
        } else {
            result.push_str(&format!(
                "{}Leaf Node: {:?}\n",
                "  ".repeat(level),
                node.keys
            ));
            for value in &node.values {
                result.push_str(&format!("{}  Value: {:?}\n", "  ".repeat(level + 1), value));
            }
        }
        result
    }
}
