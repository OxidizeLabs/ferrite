use std::sync::Arc;
use crate::common::spinlock::Spinlock;

/// The type of a node in the B+ tree.
#[derive(Debug, Clone, PartialEq)]
enum NodeType {
    Internal,
    Leaf,
}

/// A node in the B+ tree.
#[derive(Clone)]
pub struct BPlusTreeNode<K: Ord + Clone, V: Clone> {
    node_type: NodeType,
    keys: Vec<K>,
    children: Vec<Arc<Spinlock<BPlusTreeNode<K, V>>>>, // for internal nodes
    values: Vec<V>, // for leaf nodes
}

impl<K: Ord + Clone, V: Clone> BPlusTreeNode<K, V> {
    /// Creates a new `BPlusTreeNode`.
    fn new(node_type: NodeType) -> Self {
        BPlusTreeNode {
            node_type,
            keys: Vec::new(),
            children: Vec::new(),
            values: Vec::new(),
        }
    }
}

/// A B+ tree.
pub struct BPlusTree<K: Ord + Clone, V: Clone> {
    root: Arc<Spinlock<BPlusTreeNode<K, V>>>,
    order: usize, // Max number of children per node
}

impl<K: Ord + Clone, V: Clone> BPlusTree<K, V> {
    /// Creates a new `BPlusTree` with the given order.
    ///
    /// # Examples
    ///
    /// ```
    /// use tkdb::storage::index::b_plus_tree_i::BPlusTree;
    /// let bpt: BPlusTree<i32, String> = BPlusTree::new(4);
    /// ```
    pub fn new(order: usize) -> Self {
        BPlusTree {
            root: Arc::new(Spinlock::new(BPlusTreeNode::new(NodeType::Leaf))),
            order,
        }
    }

    /// Searches for a key in the B+ tree and returns the associated value if found.
    ///
    /// # Examples
    ///
    /// ```
    /// use tkdb::storage::index::b_plus_tree_i::BPlusTree;
    /// let bpt = BPlusTree::new(4);
    /// bpt.insert(1, "value".to_string());
    /// let result = bpt.search(&1);
    /// assert_eq!(result, Some("value".to_string()));
    /// ```
    pub fn search(&self, key: &K) -> Option<V> {
        let root = self.root.lock();
        self.search_node(&root, key)
    }

    fn search_node(&self, node: &BPlusTreeNode<K, V>, key: &K) -> Option<V> {
        match node.node_type {
            NodeType::Internal => {
                let mut i = 0;
                while i < node.keys.len() && key >= &node.keys[i] {
                    i += 1;
                }
                let child = node.children[i].lock();
                self.search_node(&child, key)
            }
            NodeType::Leaf => {
                for (i, k) in node.keys.iter().enumerate() {
                    if key == k {
                        return Some(node.values[i].clone());
                    }
                }
                None
            }
        }
    }

    /// Inserts a key-value pair into the B+ tree.
    ///
    /// # Examples
    ///
    /// ```
    /// use tkdb::storage::index::b_plus_tree_i::BPlusTree;
    /// let bpt = BPlusTree::new(4);
    /// bpt.insert(1, "value".to_string());
    /// ```
    pub fn insert(&self, key: K, value: V) {
        let mut root_guard = self.root.lock();
        let root_clone = (*root_guard).clone();
        if root_guard.keys.len() == self.order - 1 {
            let mut new_root = BPlusTreeNode::new(NodeType::Internal);
            new_root.children.push(Arc::new(Spinlock::new(root_clone)));
            self.split_child(&mut new_root, 0);
            *root_guard = new_root;
        }
        self.insert_non_full(&mut root_guard, key, value);
    }

    fn insert_non_full(&self, node: &mut BPlusTreeNode<K, V>, key: K, value: V) {
        match node.node_type {
            NodeType::Leaf => {
                let mut i = 0;
                while i < node.keys.len() && key > node.keys[i] {
                    i += 1;
                }
                node.keys.insert(i, key);
                node.values.insert(i, value);
            }
            NodeType::Internal => {
                let mut i = 0;
                while i < node.keys.len() && key > node.keys[i] {
                    i += 1;
                }
                {
                    let child = node.children[i].lock();
                    if child.keys.len() == self.order - 1 {
                        drop(child); // Unlock before borrowing node mutably
                        self.split_child(node, i);
                    }
                }
                let mut child = node.children[i].lock();
                self.insert_non_full(&mut child, key, value);
            }
        }
    }

    fn split_child(&self, parent: &mut BPlusTreeNode<K, V>, index: usize) {
        let order = self.order;
        let mut new_child;
        {
            let mut full_child = parent.children[index].lock();
            new_child = BPlusTreeNode::new(full_child.node_type.clone());

            new_child.keys = full_child.keys.split_off(order / 2);
            if let NodeType::Leaf = full_child.node_type {
                new_child.values = full_child.values.split_off(order / 2);
            } else {
                new_child.children = full_child.children.split_off(order / 2);
            }

            parent.keys.insert(index, full_child.keys.pop().unwrap());
        } // `full_child` is dropped here

        parent.children.insert(index + 1, Arc::new(Spinlock::new(new_child)));
    }

    /// Deletes a key-value pair from the B+ tree.
    ///
    /// # Examples
    ///
    /// ```
    /// use tkdb::storage::index::b_plus_tree_i::BPlusTree;
    /// let mut bpt = BPlusTree::new(4);
    /// bpt.insert(1, "value".to_string());
    /// bpt.delete(&1);
    /// let result = bpt.search(&1);
    /// assert_eq!(result, None);
    /// ```
    pub fn delete(&mut self, key: &K) {
        let new_root;
        {
            let mut root_guard = self.root.lock();
            self.delete_from_node(&mut root_guard, key);
            if root_guard.keys.is_empty() && root_guard.node_type == NodeType::Internal {
                new_root = Some(root_guard.children[0].clone());
            } else {
                new_root = None;
            }
        } // `root_guard` is dropped here

        if let Some(new_root) = new_root {
            self.root = new_root;
        }
    }

    fn delete_from_node(&self, node: &mut BPlusTreeNode<K, V>, key: &K) {
        match node.node_type {
            NodeType::Leaf => {
                if let Some(index) = node.keys.iter().position(|k| k == key) {
                    node.keys.remove(index);
                    node.values.remove(index);
                }
            }
            NodeType::Internal => {
                let mut i = 0;
                while i < node.keys.len() && key > &node.keys[i] {
                    i += 1;
                }
                if i < node.keys.len() && key == &node.keys[i] {
                    self.delete_from_internal_node(node, i);
                } else {
                    {
                        let mut child = node.children[i].lock();
                        self.delete_from_node(&mut child, key);
                    }
                    self.balance_after_deletion(node, i);
                }
            }
        }
    }

    fn delete_from_internal_node(&self, node: &mut BPlusTreeNode<K, V>, index: usize) {
        let predecessor_node = self.get_predecessor_node(node, index);
        let predecessor_key;
        let predecessor_value;

        {
            let predecessor_node_guard = predecessor_node.lock();
            predecessor_key = predecessor_node_guard.keys.last().unwrap().clone();
            predecessor_value = predecessor_node_guard.values.last().unwrap().clone();
        } // `predecessor_node_guard` is dropped here

        node.keys[index] = predecessor_key.clone();
        node.values[index] = predecessor_value.clone();

        {
            let mut child = node.children[index].lock();
            self.delete_from_node(&mut child, &predecessor_key);
        } // `child` is dropped here

        self.balance_after_deletion(node, index);
    }


    fn get_predecessor_node(&self, node: &BPlusTreeNode<K, V>, index: usize) -> Arc<Spinlock<BPlusTreeNode<K, V>>> {
        let mut child = node.children[index].clone();
        loop {
            let next_child = child.clone();
            let next_child_guard = next_child.lock();
            if next_child_guard.node_type == NodeType::Leaf {
                return child;
            } else {
                child = next_child_guard.children.last().unwrap().clone();
            }
        }
    }


    fn balance_after_deletion(&self, node: &mut BPlusTreeNode<K, V>, index: usize) {
        let order = self.order;

        // Ensure the child_guard is dropped before making any mutable borrow on node.
        let underfull;
        {
            let child_guard = node.children[index].lock();
            underfull = child_guard.keys.len() < (order - 1) / 2;
        }

        if underfull {
            let left_sibling_index = if index > 0 { Some(index - 1) } else { None };
            let right_sibling_index = if index < node.children.len() - 1 { Some(index + 1) } else { None };

            if let Some(left_index) = left_sibling_index {
                let borrow_needed;
                {
                    let left_sibling = node.children[left_index].lock();
                    borrow_needed = left_sibling.keys.len() > (self.order - 1) / 2;
                } // left_sibling is dropped here

                if borrow_needed {
                    self.borrow_from_left_sibling(node, index, left_index);
                    return;
                }
            }

            if let Some(right_index) = right_sibling_index {
                let borrow_needed;
                {
                    let right_sibling = node.children[right_index].lock();
                    borrow_needed = right_sibling.keys.len() > (self.order - 1) / 2;
                } // right_sibling is dropped here

                if borrow_needed {
                    self.borrow_from_right_sibling(node, index, right_index);
                    return;
                }
            }

            if let Some(left_index) = left_sibling_index {
                self.merge_nodes(node, left_index, index);
            } else if let Some(right_index) = right_sibling_index {
                self.merge_nodes(node, index, right_index);
            }
        }
    }

    fn borrow_from_left_sibling(&self, node: &mut BPlusTreeNode<K, V>, index: usize, left_index: usize) {
        let mut left_sibling = node.children[left_index].lock();
        let mut child = node.children[index].lock();

        child.keys.insert(0, node.keys[left_index].clone());
        node.keys[left_index] = left_sibling.keys.pop().unwrap();

        if let NodeType::Internal = child.node_type {
            child.children.insert(0, left_sibling.children.pop().unwrap());
        } else {
            child.values.insert(0, left_sibling.values.pop().unwrap());
        }
    }

    fn borrow_from_right_sibling(&self, node: &mut BPlusTreeNode<K, V>, index: usize, right_index: usize) {
        let mut right_sibling = node.children[right_index].lock();
        let mut child = node.children[index].lock();

        child.keys.push(node.keys[index].clone());
        node.keys[index] = right_sibling.keys.remove(0);

        if let NodeType::Internal = child.node_type {
            child.children.push(right_sibling.children.remove(0));
        } else {
            child.values.push(right_sibling.values.remove(0));
        }
    }

    fn merge_nodes(&self, node: &mut BPlusTreeNode<K, V>, left_index: usize, right_index: usize) {
        {
            let mut left_child = node.children[left_index].lock();
            let mut right_child = node.children[right_index].lock();

            left_child.keys.push(node.keys.remove(left_index));
            left_child.keys.append(&mut right_child.keys);

            if let NodeType::Internal = left_child.node_type {
                left_child.children.append(&mut right_child.children);
            } else {
                left_child.values.append(&mut right_child.values);
            }
        } // left_child and right_child are dropped here

        node.children.remove(right_index);
    }

}
