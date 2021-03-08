#[allow(unused_imports)]
use crate::heapfile::HeapFile;
#[allow(unused_imports)]
use crate::page::PageIter;
#[allow(unused_imports)]
use common::ids::{ContainerId, PageId, TransactionId};
#[allow(unused_imports)]
use std::sync::Arc;

#[allow(dead_code)]
/// The struct for a HeapFileIterator.
/// We use a slightly different approach for HeapFileIterator than
/// standard way of Rust's IntoIter for simplicity (avoiding lifetime issues).
/// This should store the state/metadata required to iterate through the file.
///
/// HINT: This will need an Arc<HeapFile>
/*  struct HeapFileIterator
 *      Purpose: let's the storage manager iterate through all the values stored in a heapfile
 *  Elements:
 *      container_id: 
 *      txn_id:
 *      hfile: 
 *  Notes:
 *      - Needs to walk through all the pages, and for each page walk through all the values
 */ 
pub struct HeapFileIterator {
    container_id: ContainerId,
    tid: TransactionId,
    hf: Arc<HeapFile>,
    curr_p_iter: PageIter,
    curr_pid: PageId
}

impl HeapFileIterator {
    /*  new
     *      purpose: creates a new HeapFileIterator 
     *  Inputs:
     *      container_id: the containerID associated with the heapfile
     *      tid: the transaction id
     *      hf: the heapfile itself
     *  Outputs:
     *      a new heapfile iterator
     *  Notes:
     *      - When you implement HeapFile, there is also a method you need to implement called num_pages
     *      - After implementing this, you can call this method to get the number of pages in the heapfile you are iterating through.
     */ 
    pub(crate) fn new(container_id: ContainerId, tid: TransactionId, hf: Arc<HeapFile>) -> Self {
       
        let mut file = &hf.clone();
        let mut p = HeapFile::read_page_from_file(file, 0).unwrap(); //understand this part later. 
        let mut iter = p.into_iter();
        let new_hf = HeapFileIterator{container_id: container_id, 
                                        tid: tid,
                                        hf: hf, 
                                        curr_p_iter: iter, 
                                        curr_pid: 0,};
        return new_hf;
    }
    
}

impl Iterator for HeapFileIterator {
    type Item = Vec<u8>;
    /*  next
     *      purpose: move onto the next page within the heapfile
     *  Inputs:
     *      &mut self: a mutable reference to the heapfile that we are iterating through
     *  Outputs:
     *      the stuff in the heapfile
     *  Note:
     *      - Note this will need to iterate through the pages and their respective iterators.
     */
    fn next(&mut self) -> Option<Self::Item> {
        // the number of pages to be iterated through
        let pageCnt = self.hf.num_pages();
        // loop through all the pages
        while self.curr_pid <= pageCnt {
            match self.curr_p_iter.next(){
                Some(data) => {
                    return Some(data);
                }
                None => {
                    //increment the current page id
                    self.curr_pid += 1;
                    // read the next page that we need to iterate through
                    let mut file = &self.hf.clone();
                    let p = HeapFile::read_page_from_file(file, self.curr_pid).unwrap();
                    // create the iterator for that page
                    let iter = p.into_iter();
                    // set the new iterator in the HeapFileIterator struct
                    self.curr_p_iter = iter;
                    
                }
            }
        }
        return None;  
    }

}
