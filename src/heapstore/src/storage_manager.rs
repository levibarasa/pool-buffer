#[allow(unused_imports)]
use crate::heapfile::HeapFile;
#[allow(unused_imports)]
use crate::heapfileiter::HeapFileIterator;
#[allow(unused_imports)]
use crate::page::Page;
#[allow(unused_imports)]
use common::ids::{ContainerId, PageId, Permissions, TransactionId, ValueId};
#[allow(unused_imports)]
use common::storage_trait::StorageTrait;
#[allow(unused_imports)]
use common::testutil::gen_random_dir;
#[allow(unused_imports)]
use common::{CrustyError, PAGE_SIZE};
#[allow(unused_imports)]
use std::collections::HashMap;
#[allow(unused_imports)]
use std::fs;
#[allow(unused_imports)]
use std::path::PathBuf;
#[allow(unused_imports)]
use std::sync::atomic::Ordering;
#[allow(unused_imports)]
use std::sync::{Arc, RwLock};


/// The StorageManager struct
pub struct StorageManager {
    hash_map: Arc<RwLock<HashMap<ContainerId, Arc<HeapFile>>>>,
    pub storage_path: String,
    is_temp: bool, // just used for testing, checks if it's a temporary directory
        //if temp==true when we drop the sm we should be deleting everything
}

/// The required functions in HeapStore's StorageManager that are specific for HeapFiles
impl StorageManager {
    /*  get_page
     *      purpose: Get a page if exists for a given container.
     *  Inputs: 
     *      &self: 
     *      container_id: 
     *      _tid:
     *      _perm: 
     *      _pin:
     *  Outputs:
     *      the page requested
     */ 
    pub(crate) fn get_page( &self, container_id: ContainerId, page_id: PageId, _tid: TransactionId,
        _perm: Permissions, _pin: bool,) -> Option<Page> {
        let map = &*self.hash_map.read().unwrap();
        if !map.contains_key(&container_id){
            None
        } else {
            let heapfile = map[&container_id].clone();
            let ret_page = HeapFile::read_page_from_file(&heapfile, page_id);
            Some(ret_page.unwrap())
        }    
    }
    /*  write_page
     *      purpose: write a page to the heapfile
     *  Inputs: 
     *      &self: a reference to the storage manager that we are writing a page to 
     *      container_id: the heapfile's unique identifier
     *      page: the page that we want to write into the heapfile
     *      _tid: unique identifier for the transaction id
     *  Outputs: 
     *      Ok() since we just wrote a page to the heapfile
     */ 
    pub(crate) fn write_page(&self, container_id: ContainerId, page: Page, _tid: TransactionId,) -> Result<(), CrustyError> {
        // get the hashmap
        let map = &*self.hash_map.read().unwrap();
        // get the heapfile we want to write the page into using container_id as the identifier
        let mut hf = map.get(&container_id).unwrap();
        // just write it to the page
        HeapFile::write_page_to_file(&hf, page);
        Ok(())
    }
    /*  get_num_pages
     *      purpose: get the number of pages for a container
     *  Inputs:
     *      &self: a reference to the storage manager
     *      container_id: unique identifier for the heapfile that we want to get the number of pages of
     *  Outputs: 
     *      the number of pages found in the heapfile returned as a PageId type
     */ 
    fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        let map = &*self.hash_map.read().unwrap();
        let mut hf = map.get(&container_id).unwrap();
        let num_pages = HeapFile::num_pages(&hf);
        return num_pages;
    }
    /*  get_hf_read_write_count
     *      purpose: counts the reads and writes served by the heapfile
     *  Inputs: 
     *      &self: 
     *      container_id:
     *  Outputs:
     *      A tuple (read,write) 
     *  Note:
     *      can return (0,0) for invalid container_ids
     */  
    #[allow(dead_code)]
    pub(crate) fn get_hf_read_write_count(&self, container_id: ContainerId) -> (u16, u16) {
        let map = &*self.hash_map.read().unwrap();
        if !map.contains_key(&container_id){
            println!("container_id: {:?} wasn't found in the hashmap", container_id);
            return (0,0);
        } else {
            let hf = map.get(&container_id).unwrap();
            let read_count = hf.read_count.load(Ordering::Relaxed);
            let write_count = hf.write_count.load(Ordering::Relaxed);
            return (read_count, write_count);
        }
    }
}

/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIterator;
    /*  new
     *      purpose: create a new stoarge manager that will use storage_path as the location to persist data   
     *  Inputs:
     *      storage_path: the location that future data will ultimately be stored to
     *  Outputs:
     *      a new storage manager
     */ 
    fn new(storage_path: String) -> Self {
        let new_sm = StorageManager{hash_map: Arc::new(RwLock::new(HashMap::new())), storage_path: storage_path, is_temp: false};
        return new_sm;
    }
    /*  new_test_sm
     *      purpose: create a new storage manager for testing
     *  Inputs: 
     *      None
     *  Outputs:
     *      a storage manager for testing
     *  Notes: 
     *      - Creates a temporary directory that will have to be cleaned up once it leaves the scope
     */ 
    fn new_test_sm() -> Self {
        let storage_path = gen_random_dir().to_string_lossy().to_string();
        debug!("Making new temp storage_manager {}", storage_path);
        let new_sm = StorageManager{hash_map: Arc::new(RwLock::new(HashMap::new())), storage_path: storage_path, is_temp: true};
        return new_sm;
    }
    /*  insert_value
     *      purpose: insert some bytes into a container for a particular value
     *  Inputs:
     *      &self: 
     *      container_id:
     *      value:  
     *      tid: 
     *  Output:
     *      returns the value id associated with the stored value
     *  Notes:
     *      - Any validation will be assumed to happen before.
     *      - Function will need to find the first page that can hold the value.
     *      - A new page may need to be created if no space on existing pages can be found.
     */ 
    fn insert_value(&self, container_id: ContainerId, value: Vec<u8>, tid: TransactionId,) -> ValueId {
        // Check
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        } else {
            // get the actual heapfile from the hash map
            let map = &*self.hash_map.read().unwrap();
            let mut hf = map.get(&container_id).unwrap();
            // once we have the heapfile, find all the keys and their corresponding heapfiles
            let mut page_id = 0;
            let num_pages = HeapFile::num_pages(hf);

            while page_id < num_pages{
                match hf.read_page_from_file(page_id){ 
                    Ok(mut page) => {
                        match page.add_value(&value){ 
                            Some(slot_id) => {
                                return ValueId{
                                    container_id: hf.container_id,
                                    segment_id: None,
                                    page_id: Some(page.header.page_id),
                                    slot_id: Some(slot_id),
                                }
                            } // closes Some(slot_id)
                            None => {
                                // go to the next page
                                page_id +=1; 
                            } // closes None
                        } // closes match page.add_value(&value)
                    } // closes Ok(mut page)
                    _ => {
                        panic!("doesn't work");
                    } // closes _ 
                } //closes match.hf.read_page_from_file(page_id)
            }

            let mut new_page = Page::new(page_id);
            hf.write_page_to_file(new_page);
            let new_val_id = ValueId{ 
                container_id: hf.container_id,
                segment_id: None,
                page_id: Some(page_id),
                slot_id: Some(0),
            };
            return new_val_id;


            // need to make a new page
            // write the value into the page
            // return a value_id

        }
    }
    /*  insert_values 
     *      purpose: insert some bytes into a container for a vector of values
     *  Inputs: 
     *      &self: 
     *      container_id:
     *      values:
     *      tid: 
     *  Outputs:   
     *      Returns a vector of value ids associated with the stored values.
     *  Notes:
     *      - Any validation will be assumed to happen before.
     *      - Returns a vector of value ids associated with the stored values.
     */ 
    fn insert_values(&self, container_id: ContainerId, values: Vec<Vec<u8>>,tid: TransactionId,
    ) -> Vec<ValueId> {

        panic!("TODO milestone hs");
    }
    /*  delete_value
     *      purpose: Delete the data for a value. 
     *  Inputs: 
     *      &self: 
     *      id: 
     *      tid: 
     *  Outputs:    
     *      Ok()
     *  Notes:
     *      - If the valueID is not found it returns Ok() still.
     */ 
    fn delete_value(&self, id: ValueId, tid: TransactionId) -> Result<(), CrustyError> {
        panic!("TODO milestone hs");
    }
    /*  update_value
     *      purpose: updates a value
     *  Inputs: 
     *      &self: 
     *      value: 
     *      id:
     *      _tid: 
     *  Outputs:
     *      The value_id or an error
     *  Notes:
     *      - Returns record ID on update (which may have changed).
     *      - Any process that needs to determine if a value changed will need to compare the return valueId against the sent value.
     */ 
    fn update_value(&self, value: Vec<u8>, id: ValueId,_tid: TransactionId,
    ) -> Result<ValueId, CrustyError> {
        panic!("TODO milestone hs");
    }
    /*  create_container
     *      purpose: create a new container (heapfile) to be stored
     *  Inputs:
     *      &self: 
     *      container_id
     *  Outputs: 
     *      Ok(())
     */ 
    fn create_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        /*
        let mut map = &mut self.hash_map.write().unwrap().clone();
        if map.contains_key(&container_id) {
            debug!("memstore::create_container container_id: {:?} already exists", &container_id);
            return Ok(());
        }
        debug!("memstore::create_container container_id: {:?} does not exist yet", &container_id);
        //get the path
        let path = &mut self.storage_path.clone();
        // make the new path
        path.push_str(&container_id.to_string());
        let buffer = PathBuf::from(path.clone());
        let mut new_hf = HeapFile::new(buffer, container_id).unwrap();
        map.insert(container_id, Arc::new(new_hf));
        Ok(())
        */
        let mut map = self.hash_map.write().unwrap();
        let hf = HeapFile::new(self.  

        let map = &*self.hash_map.read().unwrap();
        let mut hf = map.get(&container_id).unwrap();


        let mut map = &mut self.hash_map.read().unwrap().clone();
        //get the path
        let path = &mut self.storage_path.clone();
        // make the new path
        path.push_str(&container_id.to_string());
        let buffer = PathBuf::from(path.clone());
        let mut new_hf = HeapFile::new(buffer, container_id).unwrap();
        println!("container_id: {:?}", container_id);
        map.insert(container_id, Arc::new(new_hf));
        Ok(())
        
    }
    /*  remove_container
     *      purpose: remove the container and all the stored values in the container
     *  Inputs:
     *      &self:
     *      container_id:
     *  Outputs:
     *      Ok(())
     *  Notes: 
     *      - If the container is persisted remove the underlying files
     *      - fs::remove_dir_all()
     */ 
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        panic!("TODO milestone hs");
    }
    /*  get_iterator
     *      purpose: gets an iterator that returns all valid records
     *  Inputs:
     *      &self: 
     *      container_id:
     *      tid: 
     *      _perm:
     *  Outputs:
     *      A ValIterator
     */ 
    fn get_iterator(&self, container_id: ContainerId, tid: TransactionId, _perm: Permissions,
    ) -> Self::ValIterator {
        panic!("TODO milestone hs");
    }
    /*  get_value
     *      purpose: get the data for a particular ValueId
     *  Inputs: 
     *      &self: 
     *      id: 
     *      tid:
     *      perm:
     *  Outputs:
     *      The value that we wanted to retrieve in vector form or an Error
     */ 
    fn get_value(&self, id: ValueId, tid: TransactionId,perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        panic!("TODO milestone hs");
    }

    /// Notify the storage manager that the transaction is finished so that any held resources can be released.
    /*  transaction_finished
     *      purpose: notify the SM that the trasnaction is finished so that any held resources can be released
     *  Inputs:
     *      &self:
     *      tid: 
     *  Outputs:
     *      i actually don't know
     */ 
    fn transaction_finished(&self, tid: TransactionId) {
        panic!("TODO milestone tm");
    }
    /*  reset
     *      purpose: Testing utility to reset all state associated the storage manager.
     *  Inputs: 
     *      &self: 
     *  Outputs: 
     *      Not sure
     *  Notes:
     *      - If there is a buffer pool it should be reset.
     */ 
    fn reset(&self) {
        panic!("TODO milestone hs");
    }
    /*  shutdown
     *      purpose: shut down the SM
     *  Inputs: 
     *      &self: 
     *  Outputs:
     *      none(?)
     *  Notes: 
     *      - If temp, this should remove all stored files.
     *      - Can call drop. Should be safe to call multiple times.
     *      - Implement shut down and then call it in drop!!!!!
     */ 
    fn shutdown(&self) {
        panic!("TODO milestone hs");
    }
}

/// Trait Impl for Drop
impl Drop for StorageManager {
    /*  drop
     *      purpose: shutdown the storage manager
     *  Inputs: 
     *      &mut self:
     *  Outputs:
     *      None, just shuts the storage manager down
     *  Notes: 
     *      - Can call be called by shutdown. 
     *      - Should be safe to call multiple times.
     *      - If temp, this should remove all stored files.
     */ 
    fn drop(&mut self) {
        //switch around with drop
        println!("srry");
        //panic!("TODO milestone hs");
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use crate::storage_manager::StorageManager;
    use common::storage_trait::StorageTrait;
    use common::testutil::*;

    #[test]
    fn hs_sm_a_insert() {
        println!("getting here at least");
        init();
        let sm = StorageManager::new_test_sm(); // create a new Storage Manager
        let cid = 1;
        sm.create_container(cid); // create a new container, which is equivalent to creating a new heapfile
        let bytes = get_random_byte_vec(40);
        let tid = TransactionId::new(); 
        println!("GOT HERE TOO");
        let val1 = sm.insert_value(cid, bytes.clone(), tid);
        println!("val1: {:?}", val1);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val1.page_id.unwrap());
        assert_eq!(0, val1.slot_id.unwrap());
        
        let p1 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();

        let val2 = sm.insert_value(cid, bytes.clone(), tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val2.page_id.unwrap());
        assert_eq!(1, val2.slot_id.unwrap());

        let p2 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();
        assert_ne!(p1.get_bytes()[..], p2.get_bytes()[..]);
    }

    #[test]
    fn hs_sm_b_iter_small() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_container(cid);
        let tid = TransactionId::new();

        //Test one page
        let mut byte_vec: Vec<Vec<u8>> = Vec::new();
        byte_vec.push(get_random_byte_vec(400));
        byte_vec.push(get_random_byte_vec(400));
        byte_vec.push(get_random_byte_vec(400));
        for val in &byte_vec {
            sm.insert_value(cid, val.clone(), tid);
        }
        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x);
        }

        let mut byte_vec2: Vec<Vec<u8>> = Vec::new();
        // Should be on two pages
        byte_vec2.push(get_random_byte_vec(400));
        byte_vec2.push(get_random_byte_vec(400));
        byte_vec2.push(get_random_byte_vec(400));
        byte_vec2.push(get_random_byte_vec(400));

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x);
        }

        let mut byte_vec2: Vec<Vec<u8>> = Vec::new();
        // Should be on 3 pages
        byte_vec2.push(get_random_byte_vec(300));
        byte_vec2.push(get_random_byte_vec(500));
        byte_vec2.push(get_random_byte_vec(400));

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x);
        }
    }

    #[test]
    #[ignore]
    fn hs_sm_b_iter_large() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;

        sm.create_container(cid).unwrap();
        let tid = TransactionId::new();

        let vals = get_random_vec_of_byte_vec(1000, 40, 400);
        sm.insert_values(cid, vals, tid);
        let mut count = 0;
        for _ in sm.get_iterator(cid, tid, Permissions::ReadOnly) {
            count += 1;
        }
        assert_eq!(1000, count);
    }
}
