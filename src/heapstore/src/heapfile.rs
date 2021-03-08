#[allow(unused_imports)]
use crate::page::Page;
#[allow(unused_imports)]
use common::ids::{ContainerId, PageId};
#[allow(unused_imports)]
use common::{CrustyError, PAGE_SIZE};
#[allow(unused_imports)]
use std::fs::{File, OpenOptions};
#[allow(unused_imports)]
use std::io::prelude::*;
#[allow(unused_imports)]
use std::path::PathBuf;
#[allow(unused_imports)]
use std::sync::atomic::{AtomicU16, Ordering};
#[allow(unused_imports)]
use std::sync::{Arc, RwLock};
#[allow(unused_imports)]
use std::io::BufWriter;
#[allow(unused_imports)]
use std::io::{Seek, SeekFrom};
/// The struct for a heap file.  
///
/// HINT: You likely will want to design for interior mutability for concurrent accesses.
/// eg Arc<RwLock<>> on some internal members
pub(crate) struct HeapFile {  
    pub file: Arc<RwLock<File>>,
    pub container_id: ContainerId, // container_id is the ID for the heapfile
    pub read_count: AtomicU16,
    pub write_count: AtomicU16,
}
impl HeapFile {
    /*  new
     *      purpose: Create a new heapfile for the given path and container Id
     *  inputs:
     *      file_path: the path file that we are creating to store our page in 
     *      container_id: a unique identifier to identify the heapfile by
     *  outputs:
     *      Return Result<Self> if able to create.
     *  Notes:
     *      Errors could arise from permissions, space, etc when trying to create the file used by HeapFile.
     */ 
    pub(crate) fn new(file_path: PathBuf, container_id: ContainerId) -> Result<Self, CrustyError> {
        let mut options :OpenOptions = OpenOptions::new();
        let file  = options.read(true).write(true).create(true).open(&file_path).unwrap();
        let lock = RwLock::new(file);
        let new_file = Arc::new(lock);

        Ok(HeapFile {
            file: new_file,
            container_id: container_id,
          //  page_count: num_pages as PageId,
            read_count: AtomicU16::new(0),
            write_count: AtomicU16::new(0),
        })
    }   
    /*  num_pages
     *      purpose: get the number of pages in the heapfile
     *  inputs:
     *      &self: a reference the heapfile that we want to find how many pages are in 
     *  outputs: 
     *      a number of type PageId that represents how many pages the heapfile contains
     *  Notes:
     *      we cannot have more pages than PageId can hold.
     */ 
    pub fn num_pages(&self) -> PageId {
        let mut file = &*self.file.read().unwrap(); 
        let file_len = file.metadata().unwrap().len();
        let num_pages = file_len as usize / PAGE_SIZE;
        return num_pages as PageId; 

        //return file_len as u16;
    }
    /*  write_page_to_file
     *      purpose: given a page, we want to add it to the heapfile
     *  inputs: 
     *      &self: a reference to the heapfile that we want to add the page to 
     *      page: the page that we want to add to the heapfile
     *  outputs:
     *      Just () if we were able to add the page, else a CrustyError
     *  Notes:
     *      - This could be an existing page or a new page
     *      - The underlying file can be part of your HeapFile implementation (e.g. stored as part of the struct).
     *      - you don't need to add new pages directly to your HeapFile struct (i.e. as long as you have other ways of accessing the pages).
     */ 
    pub(crate) fn write_page_to_file(&self, page: Page) -> Result<(), CrustyError> {
        #[cfg(feature = "profile")]
        {
            self.write_count.fetch_add(1, Ordering::Relaxed);
        }
        // get access to the file we're working with and other pertinent info
        let mut file = &*self.file.read().unwrap(); 
        //get pertinent information for the page
        let page_id = page.header.page_id;

        // move the cursor to where we want to start inputting data
        file.seek(SeekFrom::Start((page_id as usize * PAGE_SIZE) as u64));
        
        // everything should be right up until this point
        let mut buffer = BufWriter::new(file);
        let bytes = page.get_bytes();
        for i in 0..PAGE_SIZE{
            buffer.write(&bytes[i..i+1]).unwrap();
        }
        buffer.flush().unwrap();
        return Ok(());
    }
    /* read_page_from_file
     *      purpose: read a specific page from the heapfile
     *  inputs:   
     *      &self: a reference to the heapfile that we're pulling the specific page from
     *      pid: the specific page we want to pull from the heapfile
     *  outputs:
     *      either the page that we wanted to retrieve or a CrustyError
     *  Notes:
     *      - Errors could arise from the filesystem or invalid pageId
     *      - Given a page_id we need the right offset for the page and we need to return the page itself
     */ 
    pub(crate) fn read_page_from_file(&self, pid: PageId) -> Result<Page, CrustyError> {
        #[cfg(feature = "profile")]
        {
            self.read_count.fetch_add(1, Ordering::Relaxed);
        }
        let mut file = &*self.file.read().unwrap();
        let start_index= PAGE_SIZE * pid as usize;
        // we need to find the right place to start
        file.seek(SeekFrom::Start(start_index as u64));
        let mut buffer = [0; PAGE_SIZE];
        file.read_exact(&mut buffer);

        let new_page = Page::from_bytes(&buffer);
        
        Ok(new_page)
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use common::testutil::*;
    use temp_testdir::TempDir;

    #[test]
    fn hs_hf_insert() {
        init();

        //Create a temp file
        let f = gen_random_dir();
        let tdir = TempDir::new(f, true);
        let mut f = tdir.to_path_buf();
        f.push(gen_rand_string(4));
        f.set_extension("hf");
        // creates a new heapfile with a path and container_id = 1
        let mut hf = HeapFile::new(f.to_path_buf(), 1).unwrap();

        // Make a page and write
        let mut p0 = Page::new(0);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes); // add first value to the page
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes); // add second value to the page
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes); // add third value to the page
        let p0_bytes = p0.get_bytes();
        hf.write_page_to_file(p0); // write page 0 into the heapfile
        assert_eq!(1, hf.num_pages()); // check the number of pages 
        let checkp0 = hf.read_page_from_file(0).unwrap(); // check that the data in the page in the heapfile is right
        assert_eq!(p0_bytes, checkp0.get_bytes()); 

        //Add another page
        let mut p1 = Page::new(1);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes); // adding data to page 2
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes); // adding data to page 2
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes); // adding data to page 2
        let p1_bytes = p1.get_bytes(); // converts the page to a vector of bytes

        hf.write_page_to_file(p1); // add p1 to the heapfile

        assert_eq!(2, hf.num_pages()); // check that the total number of pages in the heapfile is 2
        //Recheck page0
        let checkp0 = hf.read_page_from_file(0).unwrap(); // read the first page from the file
        assert_eq!(p0_bytes, checkp0.get_bytes()); // check that the first page is accurate

        //check page 1
        let checkp1 = hf.read_page_from_file(1).unwrap(); //read the second page from the heapfile
        assert_eq!(p1_bytes, checkp1.get_bytes()); // check that the second page is accurate

        // what do these mean?
        #[cfg(feature = "profile")]
        {
            assert_eq!(*hf.read_count.get_mut(), 3);
            assert_eq!(*hf.write_count.get_mut(), 2);
        }
    }
}
