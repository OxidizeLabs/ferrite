// use std::any::{Any, TypeId as StdTypeId};
// use std::collections::HashMap;
// use std::marker::PhantomData;
//
// use crate::common::config::PageId;
// use crate::storage::index::b_plus_tree_index::{KeyComparator, KeyType};
// use crate::storage::page::page::{BasicPage, Page, PageTrait, PageType};
// use crate::storage::page::page_types::b_plus_tree_internal_page::BPlusTreeInternalPage;
// use crate::storage::page::page_types::b_plus_tree_leaf_page::BPlusTreeLeafPage;
// use crate::storage::page::page_types::table_page::TablePage;
// use crate::types_db::type_id::TypeId;
// use crate::types_db::value::Value;
//
// // New trait to create pages of specific types
// pub trait PageFactory<P: Page + ?Sized>: Send + Sync {
//     fn create_page(&self, page_id: PageId) -> Box<dyn PageTrait>;
// }
//
// // Default factory for basic pages
// struct BasicPageFactory;
// impl PageFactory<BasicPage> for BasicPageFactory {
//     fn create_page(&self, page_id: PageId) -> Box<dyn PageTrait> {
//         Box::new(BasicPage::new(page_id))
//     }
// }
//
// // Default factory for table pages
// struct TablePageFactory;
// impl PageFactory<TablePage> for TablePageFactory {
//     fn create_page(&self, page_id: PageId) -> Box<dyn PageTrait> {
//         Box::new(TablePage::new(page_id))
//     }
// }
//
// // Generic BTree leaf page factory
// pub struct BTreeLeafPageFactory<K, V, C> {
//     comparator: C,
//     _k: PhantomData<K>,
//     _v: PhantomData<V>,
// }
//
// impl<K, V, C> BTreeLeafPageFactory<K, V, C>
// where
//     K: KeyType + Clone + Send + Sync + 'static,
//     V: Clone + Send + Sync + 'static,
//     C: KeyComparator<K> + Clone + Send + Sync + 'static,
// {
//     pub fn new(comparator: C) -> Self {
//         Self {
//             comparator,
//             _k: PhantomData,
//             _v: PhantomData,
//         }
//     }
// }
//
// impl<K, V, C> PageFactory<BPlusTreeLeafPage<K, V, C>> for BTreeLeafPageFactory<K, V, C>
// where
//     K: KeyType + Clone + Send + Sync + 'static,
//     V: Clone + Send + Sync + 'static,
//     C: KeyComparator<K> + Clone + Send + Sync + 'static,
//     BPlusTreeLeafPage<K, V, C>: Page,
// {
//     fn create_page(&self, page_id: PageId) -> Box<dyn PageTrait> {
//         // Create a B+ tree leaf page with the stored comparator
//         let page = BPlusTreeLeafPage::new_with_options(page_id, 100, self.comparator.clone());
//         Box::new(page) as Box<dyn PageTrait>
//     }
// }
//
// // Generic BTree leaf page factory
// pub struct BTreeInternalPageFactory<K, C> {
//     _k: PhantomData<K>,
//     comparator: C,
// }
//
// impl<K, C> BTreeInternalPageFactory<K, C>
// where
//     K: KeyType + Clone + Send + Sync + 'static,
//     C: KeyComparator<K> + Clone + Send + Sync + 'static,
// {
//     pub fn new(comparator: C) -> Self {
//         Self {
//             comparator,
//             _k: PhantomData,
//         }
//     }
// }
//
// impl<K, C> PageFactory<BPlusTreeInternalPage<K, C>> for BTreeInternalPageFactory<K, C>
// where
//     K: KeyType + Clone + Send + Sync + 'static,
//     C: KeyComparator<K> + Clone + Send + Sync + 'static,
//     BPlusTreeInternalPage<K, C>: Page,
// {
//     fn create_page(&self, page_id: PageId) -> Box<dyn PageTrait> {
//         // Create a B+ tree leaf page with the stored comparator
//         let page = BPlusTreeInternalPage::new_with_options(page_id, 100, self.comparator.clone());
//         Box::new(page) as Box<dyn PageTrait>
//     }
// }
//
// // Main registry of page factories
// pub struct PageFactoryRegistry {
//     // Map page types to their corresponding factories
//     page_factories: HashMap<PageType, Box<dyn Any + Send + Sync>>,
//     // Map key type IDs to their corresponding BTree factories
//     btree_factories: HashMap<TypeId, HashMap<PageType, Box<dyn Any + Send + Sync>>>,
// }
//
// impl PageFactoryRegistry {
//     pub fn new() -> Self {
//         let mut registry = Self {
//             page_factories: HashMap::new(),
//             btree_factories: HashMap::new(),
//         };
//
//         // Register default factories
//         registry.register_default_factories();
//         registry
//     }
//
//     fn register_default_factories(&mut self) {
//         // Register basic factories for common page types
//         self.page_factories.insert(
//             PageType::Basic,
//             Box::new(BasicPageFactory) as Box<dyn Any + Send + Sync>,
//         );
//
//         self.page_factories.insert(
//             PageType::Table,
//             Box::new(TablePageFactory) as Box<dyn Any + Send + Sync>,
//         );
//     }
//
//     // Register a BTree leaf page factory for a specific key type
//     pub fn register_btree_leaf_factory<K, V, C>(&mut self, comparator: C)
//     where
//         K: KeyType + Clone + Send + Sync + 'static,
//         V: Clone + Send + Sync + 'static,
//         C: KeyComparator<K> + Clone + Send + Sync + 'static,
//         BPlusTreeLeafPage<K, V, C>: Page,
//     {
//         let key_type_id = self.get_type_id_for_key_type::<K>();
//         let factory = BTreeLeafPageFactory::new(comparator);
//
//         let type_map = self
//             .btree_factories
//             .entry(key_type_id)
//             .or_insert_with(HashMap::new);
//
//         type_map.insert(
//             PageType::BTreeLeaf,
//             Box::new(factory) as Box<dyn Any + Send + Sync>,
//         );
//     }
//
//     pub fn register_btree_internal_factory<K, C>(&mut self, comparator: C)
//     where
//         K: KeyType + Clone + Send + Sync + 'static,
//         C: KeyComparator<K> + Clone + Send + Sync + 'static,
//         BPlusTreeInternalPage<K, C>: Page,
//     {
//         let key_type_id = self.get_type_id_for_key_type::<K>();
//         let factory = BPlusTreeInternalPage::new(comparator);
//
//         let type_map = self
//             .btree_factories
//             .entry(key_type_id)
//             .or_insert_with(HashMap::new);
//
//         type_map.insert(
//             PageType::BTreeInternal,
//             Box::new(factory) as Box<dyn Any + Send + Sync>,
//         );
//     }
//
//     // Create a page of the specified type
//     pub fn create_page(&self, page_type: PageType, page_id: PageId) -> Box<dyn PageTrait> {
//         // First check if we have a factory for this page type
//         if let Some(factory_any) = self.page_factories.get(&page_type) {
//             return self.create_page_with_factory(factory_any, page_id);
//         }
//
//         // For BTree pages, we need to determine the key type from the page data
//         // This would typically be done at read time
//         match page_type {
//             PageType::BTreeLeaf | PageType::BTreeInternal | PageType::BTreeHeader => {
//                 // Create a placeholder basic page for now
//                 // The actual page will be properly initialized when data is loaded
//                 let page = BasicPage::new(page_id);
//                 Box::new(page)
//             }
//             // For any other page type, use a basic page as a fallback
//             _ => {
//                 let mut page = BasicPage::new(page_id);
//                 Box::new(page);
//             }
//         }
//     }
//
//     // Create page with a specific key type for BTree pages
//     pub fn create_btree_page<K: KeyType + 'static>(
//         &self,
//         page_type: PageType,
//         page_id: PageId,
//     ) -> Box<dyn PageTrait> {
//         let key_type_id = self.get_type_id_for_key_type::<K>();
//
//         // Try to find factory for this key type and page type
//         if let Some(type_map) = self.btree_factories.get(&key_type_id) {
//             if let Some(factory_any) = type_map.get(&page_type) {
//                 return self.create_page_with_factory(factory_any, page_id);
//             }
//         }
//
//         // Fallback to basic page if no factory is found
//         let page = BasicPage::new(page_id);
//         Box::new(page)
//     }
//
//     // Helper method to create a page using any factory
//     fn create_page_with_factory(
//         &self,
//         factory_any: &Box<dyn Any + Send + Sync>,
//         page_id: PageId,
//     ) -> Box<dyn PageTrait> {
//         // Try to downcast to various known factory types
//
//         // Try BasicPageFactory
//         if let Some(factory) = factory_any.downcast_ref::<BasicPageFactory>() {
//             return factory.create_page(page_id);
//         }
//
//         // Try TablePageFactory
//         if let Some(factory) = factory_any.downcast_ref::<TablePageFactory>() {
//             return factory.create_page(page_id);
//         }
//
//         // Generic handling for BTreeLeafPageFactory - this is more complex
//         // We'd need to check against all possible key/value type combinations
//         // that might be registered
//
//         // For demonstration, let's assume a few common types
//         // In practice, you might need a more sophisticated approach
//
//         // Integer keys, any value
//         if let Some(factory) = factory_any.downcast_ref::<BTreeLeafPageFactory<i32, Value, _>>() {
//             return factory.create_page(page_id);
//         }
//
//         // String keys, any value
//         if let Some(factory) = factory_any.downcast_ref::<BTreeLeafPageFactory<String, Value, _>>()
//         {
//             return factory.create_page(page_id);
//         }
//
//         // Fallback
//         let page = BasicPage::new(page_id);
//         Box::new(page)
//     }
//
//     // Helper method to determine TypeId for a specific key type
//     fn get_type_id_for_key_type<K: KeyType + 'static>(&self) -> TypeId {
//         // Implementation based on your TypeId system
//         match StdTypeId::of::<K>() {
//             id if id == StdTypeId::of::<i32>() => TypeId::Integer,
//             id if id == StdTypeId::of::<String>() => TypeId::VarChar,
//             // Add mappings for other key types you support
//             _ => TypeId::Invalid, // Fallback for unsupported types
//         }
//     }
// }
