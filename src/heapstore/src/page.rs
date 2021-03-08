#[allow(unused_imports)]
use common::ids::{PageId, SlotId};
#[allow(unused_imports)]
use common::PAGE_SIZE;
#[allow(unused_imports)]
use std::convert::TryInto;
#[allow(unused_imports)]
use std::mem;
#[allow(unused_imports)]
use std::ptr;


/// The struct for a page. Note this can hold more elements/meta data when created,
/// but it must be able to be packed/serialized/marshalled into the data array of size
/// PAGE_SIZE. In the header, you are allowed to allocate 8 bytes for general page metadata and
/// 6 bytes per value/entry/slot stored. For example a page that has stored 3 values, can use
/// up to 8+3*6=26 bytes, leaving the rest (PAGE_SIZE-26 for data) when serialized.
/// You do not need reclaim header information for a value inserted (eg 6 bytes per value ever inserted)
/// The rest must filled as much as possible to hold values. 

/*  struct Page
 *  Purpose: 
 *      To hold records that will be inserted into the database
 *  Elements:
 *      header: contains metadata about the page
 *      data: the actual data that the page holds
 */
pub(crate) struct Page {
    /// The data for data
    pub header: Header, 
    pub data: [u8; PAGE_SIZE], // slots go in data array

}
/*  struct Slot
 *  Purpose: 
 *      Small chunks that identify each record within the page
 *  Elements:
 *      slot_id: a unique identifier for each slot/record
 *      slot_offset: the index at which the data for the slot begins in the data array
 *      size: the sizes of the data that the slot holds
 *  Note: slot metadata can't exceed 6 bytes
 */ 
// the slot metadata can't exceed 6 bytes
pub struct Slot{
    pub slot_id: SlotId, 
    pub slot_offset: u16 , 
    pub size: u16, 
}
/*  struct Header
 *  Purpose:
 *      To store the metadata for a page
 *  Elements:
 *      page_id: the unique identifier for the page
 *      slots: a vector of the slots/records found in the page
 *      largest_free_space: the largest amount of free contiguous space in the page
 */
pub struct Header{
    pub page_id: PageId, //u8 - 1byte
    pub slots: Vec<Slot>, // 
    pub largest_free_space: u16, 
}

impl Slot{
    /*  new
     *      purpose: creates a new slot given the necessary data for a slot    
     *  inputs:
     *      slot_id: the unique identifier for the slot
     *      slot_offset: the index in the data array where the slot actually begins
     *      size: the size of the slot in terms of bytes
     *  outputs:
     *      a new slot with all the parts of the struct filled in correctly
     */ 
    pub fn new(slot_id: SlotId, slot_offset: u16, size: u16) -> Self{
        let new_slot = Slot{
            slot_id: slot_id,
            slot_offset: slot_offset,
            size: size
        };
        return new_slot;
    }
}

impl Header {
    /*  get_size
     *      purpose: get the current size of the header since there is a static and dynamic part
     *  inputs:
     *      &self: the header that we want to get the size of
     *  outputs:
     *      a number that represents the size of the header in bytes
     *  Note: static metadata can be 8 bytes while each additional slot is allowed to be 6 bytes max
     */ 
    pub(crate) fn get_size(&self) -> usize {
       return mem::size_of::<PageId>() + (mem::size_of::<Slot>() * self.slots.len()) +mem::size_of::<u16>(); 
    }
}

impl Page {
    /*  new
     *      purpose: creates a new page given a new page_id
     *  inputs:
     *      page_id: the way to identify the new page
     *  ouputs:
     *      a new page with all the parts of the struct filled in correctly
     */ 
    pub fn new(page_id: PageId) -> Self {
        let new_header = Header{
            page_id: page_id,
            slots: Vec::new(),
            //largest_free_space is the size of the data array without the size of the header
            largest_free_space: (PAGE_SIZE - mem::size_of::<PageId>() - mem::size_of::<u16>()) as u16, 
        };
        let new_page = Page{
            header: new_header,
            data: [0; PAGE_SIZE], // initialize the whole page to zeros
        }; 
        return new_page;
    }
    /*  get_page_id
     *      purpose: retrieves the page_id from the page
     *  inputs: 
     *      &self: the page that we're retrievingt the page_id from
     *  outputs:
     *      PageId: the page_id of the page
     */ 
    pub fn get_page_id(&self) -> PageId {
        return self.header.page_id;        
    }
    /*  find_free
     *      purpose: find the next availabe free space to store data
     *  inputs:
     *      &mut self: a mutable reference to the page that we want to find available space from
     *      input_size: the size of the data that we want to put into the page
     *  outputs:
     *      a vector with 2 elements with the first element being the new slot_id and the second 
     *      element being the index in the data array where we can begin inserting data
     */ 
    pub fn find_free(&mut self, input_size: usize) -> Vec<usize> {
        let mut slot_vec = &self.header.slots;
        let vec_len = slot_vec.len();
        let mut new_s_id = 0;
        let mut start_index;
        let mut counter = 0;
        let mut offset_vec = Vec::new();
        let mut id_vec = Vec::new();
        let mut ret_vec = Vec::new();

        // create a vector that holds all the offset values and id values that will help with our calculations
        for slot in slot_vec{
            offset_vec.push(slot.slot_offset);
            id_vec.push(slot.slot_id);
        }
        // reverse offset_vec to make calculations easier
        offset_vec.sort();
        offset_vec.reverse();
        id_vec.sort();
        // find the new id value
        while new_s_id <= id_vec.len() {
            if new_s_id < id_vec.len() && new_s_id == id_vec[new_s_id] as usize {
                new_s_id += 1;
                continue;
            } else {
                ret_vec.push(new_s_id);
                break;
            }
        }
        if vec_len ==0{
            new_s_id = 0;
            start_index = PAGE_SIZE - input_size as usize;
            ret_vec.push(start_index);
        } else {
            while counter <= vec_len {
                if vec_len == 0 {
                    let space_bet = PAGE_SIZE - (slot_vec[counter].slot_offset as usize + slot_vec[counter].size as usize);
                    if space_bet >= input_size.into(){
                        start_index = (slot_vec[counter].slot_offset + slot_vec[counter].size).into();
                        ret_vec.push(start_index);
                        break;
                    } else {
                        counter += 1;
                    }
                } else if counter == vec_len {
                    start_index = (slot_vec[counter - 1].slot_offset - input_size as u16).into();
                    ret_vec.push(start_index);
                    break;
                // you just need to figure out when vec_len is getting to 0
                // the problem is that the slot_vec isn't sorted!
                } else if counter != vec_len - 1 &&  slot_vec[counter].slot_offset > (slot_vec[counter+1].slot_offset + slot_vec[counter+1].size) && slot_vec[counter].slot_offset  - (slot_vec[counter+1].slot_offset + slot_vec[counter+1].size) >= input_size as u16{
                    start_index = (slot_vec[counter+1].slot_offset+slot_vec[counter+1].size) as usize;
                    ret_vec.push(start_index);
                    counter += 1;
                    break;
                } else {
                    counter += 1;
                }
            }
        }
        return ret_vec;
    }
    /*  add_value
     *      purpose: given an array of values, insert it into the page's array
     *  inputs: 
     *      &mut self: a mutable reference the the page that we are adding the
     *                 new array of bytes into 
     *      bytes: the new array of bytes to be inserted into the data array
     *             of the page
     *  ouputs:
     *      Option<SlotId>: either we return Some(SlotId) if we've corectly
     *                      inserted the bytes array into it or we return
     *                      None if we weren't able to add the array of bytes
     *                      into the page
     */
    pub fn add_value(&mut self, bytes: &Vec<u8>) -> Option<SlotId> {
        let input_len = bytes.len();
        let place_in = Page::find_free(self,input_len);
        let slot_vec = &mut self.header.slots;
        let new_id = place_in[0];
        let start_index = place_in[1];
        let end_index = start_index + input_len;
        if input_len > self.header.largest_free_space as usize{
            return None;
        } else {
            self.data[start_index..end_index].clone_from_slice(&bytes);
            let new_slot = Slot::new(new_id as SlotId, start_index as u16, input_len as u16);
            slot_vec.push(new_slot);
            self.header.largest_free_space -= (input_len + mem::size_of::<Slot>()) as u16;
            return Some(new_id as u16);
        }
    }  
    /*  get_value
     *      purpose: return the bytes for the slotId
     *  inputs: 
     *      &self: a reference to the page that we want to extract data from
     *      slot_id: the right slot to extract data from
     *  outputs:
     *      Option<Vec<u8>>: if the slot_id is valid, we return the bytes in 
     *                       vector form. If the slot_id is invalid, we return
     *                       None.
     */ 
    pub fn get_value(&self, slot_id: SlotId) -> Option<Vec<u8>> {
        // get corresponding information for the slotId
        let slot_vec = &self.header.slots;
        let start_index;
        let end_index;
        let ret_val;
        let mut id_vec = Vec::new();
        for slot in slot_vec{
            id_vec.push(slot.slot_id);
        }
        id_vec.sort();

        if !id_vec.contains(&slot_id){
            return None;
        } else {
            let index = slot_vec.into_iter().position(|s| s.slot_id == slot_id).unwrap();
            start_index = slot_vec[index].slot_offset;                  
            end_index = slot_vec[index].slot_offset + slot_vec[index].size;
            ret_val = self.data[usize::from(start_index)..usize::from(end_index)].to_vec();
            return Some(ret_val);
        }
    }
    /*  delete_value
     *      purpose: delete the bytes/slot for the slotId
     *  inputs: 
     *      &mut self: a mutable reference to the page, because will need to 
     *                 change the data stored AND the size of the slot
     *      slot_id: the slot to be deleted
     *  outputs:
     *      Option<()>: we either return Some(()) or None, since there's really
     *                  nothing we'd return bc changes are just being made to 
     *                  data array
     */ 
    pub fn delete_value(&mut self, slot_id: SlotId) -> Option<()> {
        let mut offset_vec = Vec::new();
        let mut id_vec = Vec::new();


        // create a vector that holds all the offset values and id values that will help with our calculations
        for slot in &mut self.header.slots{
            offset_vec.push(slot.slot_offset);
            id_vec.push(slot.slot_id);
        }
        // reverse offset_vec to make calculations easier
        offset_vec.sort();
        offset_vec.reverse();
        id_vec.sort();
        let start_index;
        let end_index; 
        let slot_vec = &mut self.header.slots;
        let mut counter:u16 = 0;
       
        if !id_vec.contains(&slot_id){
            return None;
        } else {
            if slot_id == 0 {
                // deleting the very first slot
                start_index = slot_vec[slot_id as usize].slot_offset; 
                end_index = PAGE_SIZE;
                while counter < (end_index - start_index as usize) as u16{
                    self.data[(start_index+counter) as usize] = 0;
                    counter += 1;
                }
                let index = slot_vec.into_iter().position(|s| s.slot_id == slot_id).unwrap();
                slot_vec.remove(index);
                return Some(());
            } else {
                let index = slot_vec.into_iter().position(|s| s.slot_id == slot_id).unwrap();
                
                start_index = slot_vec[index].slot_offset;
                end_index = (slot_vec[index].slot_offset + slot_vec[index].size) as usize;
                while counter < (end_index - start_index as usize) as u16{
                    self.data[(start_index+counter) as usize] = 0;
                    counter += 1;
                }
                slot_vec.remove(index);
                return Some(());
            }
        }
    }
    /*  from_bytes
     *      purpose: given a data array create a page out of it
     *  inputs: 
     *      data: the data array that we will be extracting pertinent data from to 
     *            construct a new page
     *  outputs:
     *      a new page
     *  Notes: 
     *      HINT to create a primitive data type from a slice you can use the following
     *      (the example is for a u16 type and the data store in little endian)
     *      u16::from_le_bytes(data[X..Y].try_into().unwrap());
     */ 
    pub fn from_bytes(data: &[u8]) -> Self {
        let mut index = 4;
        let mut counter = 0;
        //find page_id and num_slots
        let page_id = u16::from_le_bytes(data[0..2].try_into().unwrap());
        let slot_num = u16::from_le_bytes(data[2..4].try_into().unwrap());
        let mut largest_free_space = PAGE_SIZE - (mem::size_of::<PageId>() + (mem::size_of::<Slot>() * slot_num as usize) + mem::size_of::<u16>());
        //build the slot arary
        let mut slot_vec = Vec::new();
        while counter < slot_num {
            let slot_id = SlotId::from_le_bytes(data[index..index+2].try_into().unwrap());
            index+=2;
            let offset = u16::from_le_bytes(data[index..index+2].try_into().unwrap());
            index+=2;
            let size = u16::from_le_bytes(data[index..index+2].try_into().unwrap());
            index+=2;
            let new_slot = Slot::new(slot_id, offset, size as u16);
            if largest_free_space > size as usize {
                largest_free_space -= size as usize;
            } else {
                largest_free_space = 0;
            }
            slot_vec.push(new_slot);
            counter += 1;
        }
        // build up the data array
        let mut data_array = [0; PAGE_SIZE];
        data_array.clone_from_slice(&data);
        let header = Header{page_id: page_id, 
                            slots: slot_vec, 
                            largest_free_space: largest_free_space as u16
                        };
        let page = Page{header: header,
                        data: data_array
                        };
        return page;
    }
    /*  get_bytes
     *      purpose: given a page, serialize it and turn it into an array 
     *  inputs: 
     *      &self: a reference to the page that we want to serialize into an array of bytes
     *  outputs: 
     *      a vector that is the serial representation of the page
     *  Notes:
     *      HINT: To convert a vec of bytes using little endian, use
     *      to_le_bytes().to_vec()
     */  
    pub fn get_bytes(&self) -> Vec<u8> {  // converts a page struct into a vector of bytes SERIALIZATION
        let mut ret_vec = self.data;
        let slot_vec = &self.header.slots;
        let page_id : PageId = self.header.page_id;
        let num_slots : u16 = self.header.slots.len() as u16;
        let mut header_info = Vec::new();
        // put page_id and num_slots into ret_vec
        header_info.extend(page_id.to_le_bytes().to_vec());
        header_info.extend(num_slots.to_le_bytes().to_vec());
        // go through the slots
        for slot in slot_vec {
            header_info.extend(slot.slot_id.to_le_bytes().to_vec());
            header_info.extend(slot.slot_offset.to_le_bytes().to_vec());
            header_info.extend(slot.size.to_le_bytes().to_vec());
        }
        // check taht header doesn't overlap with the data
        if header_info.len() > self.header.largest_free_space as usize{
            panic!("Header information and data overlap!");
        }
        // put header info into the ret_vec
        ret_vec[0..header_info.len()].clone_from_slice(&header_info);
        return ret_vec.to_vec();
    }
    
    /// A utility function to determine the size of the header in the page
    /// when serialized/to_bytes.
    /// Will be used by tests. Optional for you to use in your code
    pub(crate) fn get_header_size(&self) -> usize {
        return Header::get_size(&self.header);
    }
    /// A utility function to determine the largest block of free space in the page.
    /// Will be used by tests. Optional for you to use in your code
    #[allow(dead_code)]
    pub(crate) fn get_largest_free_contiguous_space(&self) -> usize {
        return self.header.largest_free_space.into();
    } 
}

/// The (consuming) iterator struct for a page.
/// This should iterate through all valid values of the page.
/// See https://stackoverflow.com/questions/30218886/how-to-implement-iterator-and-intoiterator-for-a-simple-struct
/* struct PageIter
 *  Purpose: 
 *      iterate through the slots of the page
 *  Elements:
 *      slot: the slot we begin at
 *      slot_count: the total number of slots in the page
 *      page: the page that we're iterating through
 */
pub struct PageIter {
    slot: usize,
    slot_count: usize,
    page: Page, 
     
}

impl Iterator for PageIter { 
    type Item = Vec<u8>;
    /*  next
     *      purpose: move onto the next slot
     *  inputs: 
     *      &mut self: a mutable reference to the page that we are iterating through
     *  outputs:
     *      the current slot we're at
     */ 
    fn next(&mut self) -> Option<Self::Item> {
        while self.slot <= self.slot_count {
            match self.page.get_value(self.slot as u16){
                Some(data) => {
                    self.slot += 1;
                    return Some(data);
                }
                None => {
                    self.slot += 1;
                }
            }
        }
        return None;
    }
}

impl IntoIterator for Page {
    type Item = Vec<u8>;
    type IntoIter = PageIter;
    /*  into_iter
     *      purpose: allows an iterator to be created
     *  inputs:
     *      self: the page that will be iterated through
     *  outputs:
     *      an iterator sorta deal
     */
    fn into_iter(self) -> Self::IntoIter {
        PageIter{
            slot: 0,
            slot_count: self.header.slots.len(),
            page: self,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::testutil::init;
    use common::testutil::*;
    use common::Tuple;


    /// Limits how on how many bytes we can use for page metadata / header
    pub const FIXED_HEADER_SIZE: usize = 8;
    pub const HEADER_PER_VAL_SIZE: usize = 6;
    // DONE
    #[test]
    fn hs_page_create() {
        init();
        let p = Page::new(0);
        assert_eq!(0, p.get_page_id());
        assert_eq!(
            PAGE_SIZE - p.get_header_size(),
            p.get_largest_free_contiguous_space()
        );
    }

    // DONE
    #[test]
    fn hs_page_simple_insert() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        let byte_len = tuple_bytes.len();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        assert_eq!(
            PAGE_SIZE - byte_len - p.get_header_size(),
            p.get_largest_free_contiguous_space()
        );
        let tuple_bytes2 = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - byte_len - byte_len,
            p.get_largest_free_contiguous_space()
        );
    }

    // DONE
    #[test]
    fn hs_page_space() {
        init();
        let mut p = Page::new(0);
        let size = 10;
        let bytes = get_random_byte_vec(size);
        assert_eq!(10, bytes.len());
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_largest_free_contiguous_space()
        );
    }

    //DONE
    #[test]
    fn hs_page_get_value() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);
        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        //Recheck
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);
        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));
    }

    // DONE
    #[test]
    fn hs_page_header_size_small() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let bytes = get_random_byte_vec(10);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE);
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(Some(3), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE * 4);
    }

    // DONE
    #[test]
    fn hs_page_header_size_full() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let byte_size = 10;
        let bytes = get_random_byte_vec(byte_size);
        // how many vals can we hold with 8 bytes
        let num_vals: usize = (((PAGE_SIZE - FIXED_HEADER_SIZE) as f64
            / (byte_size + HEADER_PER_VAL_SIZE) as f64)
            .floor()) as usize;
        if PAGE_SIZE == 4096 && FIXED_HEADER_SIZE == 8 && HEADER_PER_VAL_SIZE == 6 {
            assert_eq!(255, num_vals);
        }
        for _ in 0..num_vals {
            p.add_value(&bytes);
        }
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + (num_vals * HEADER_PER_VAL_SIZE));
        assert!(
            p.get_largest_free_contiguous_space()
                >= PAGE_SIZE
                    - (byte_size * num_vals)
                    - FIXED_HEADER_SIZE
                    - (num_vals * HEADER_PER_VAL_SIZE)
        );
    }

    // DONE
    #[test]
    fn hs_page_no_space() {
        init();
        let mut p = Page::new(0);
        let size = PAGE_SIZE / 4;
        let bytes = get_random_byte_vec(size);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_largest_free_contiguous_space()
        );
        //Should reject here
        assert_eq!(None, p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_largest_free_contiguous_space()
        );
        // Take small amount of data
        let small_bytes = get_random_byte_vec(size / 4);
        assert_eq!(Some(3), p.add_value(&small_bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3 - small_bytes.len(),
            p.get_largest_free_contiguous_space()
        );
    }

    // DONE
    #[test]
    fn hs_page_simple_delete() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);
        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        assert_eq!(Some(()), p.delete_value(0));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(0));
        assert_eq!(None, p.get_value(2));
        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));
        assert_eq!(None, p.get_value(1));
    }

    // DONE
    #[test]
    fn hs_page_get_first_free_space() {
        init();
        let p = Page::new(0);
        assert_eq!(1,1);

        let _b1 = get_random_byte_vec(100);
        let _b2 = get_random_byte_vec(50);
    }


    //ASK ABOUT THIS AT WILL'S OH
    #[test]
    fn hs_page_delete_insert() {
        init();
        let mut p = Page::new(0);
        let tuple_bytes = get_random_byte_vec(20);
        let tuple_bytes2 = get_random_byte_vec(20);
        let tuple_bytes3 = get_random_byte_vec(20);
        let tuple_bytes4 = get_random_byte_vec(20);
        let tuple_bytes_big = get_random_byte_vec(40);
        let tuple_bytes_small1 = get_random_byte_vec(5);
        let tuple_bytes_small2 = get_random_byte_vec(5);
        //Add 3 values
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes2, check_bytes);
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));
        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);
        assert_eq!(Some(()), p.delete_value(1));
        //Verify slot 1 is gone
        assert_eq!(None, p.get_value(1));
        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);
        assert_eq!(Some(1), p.add_value(&tuple_bytes4));
        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes4, check_bytes);
        //Delete 0
        assert_eq!(Some(()), p.delete_value(0));
        assert_eq!(Some(0), p.add_value(&tuple_bytes_big));
        //Insert small, should go to 3
        assert_eq!(Some(3), p.add_value(&tuple_bytes_small1));
        //Insert small, should go to new
        assert_eq!(Some(4), p.add_value(&tuple_bytes_small2));
    }

    // DONE
    #[test]
    fn hs_page_size() {
        init();
        let mut p = Page::new(2);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let page_bytes = p.get_bytes();
        assert_eq!(PAGE_SIZE, page_bytes.len());
    }

    //DONE
    #[test]
    fn hs_page_simple_byte_serialize() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        
        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        //Get bytes and create from bytes
        let bytes = p.get_bytes();
        let mut p2 = Page::from_bytes(&bytes);
        assert_eq!(0, p2.get_page_id());
        
        //Check reads

        let check_bytes2 = p2.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p2.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Add a new tuple to the new page
        let tuple3 = int_vec_to_tuple(vec![4, 3, 2]);
        let tuple_bytes3 = tuple3.get_bytes();
        assert_eq!(Some(2), p2.add_value(&tuple_bytes3));
        assert_eq!(tuple_bytes3, p2.get_value(2).unwrap());
        assert_eq!(tuple_bytes2, p2.get_value(1).unwrap());
        assert_eq!(tuple_bytes, p2.get_value(0).unwrap());
    }


    #[test]
    fn hs_page_iter() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 0, 1]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let tuple2 = int_vec_to_tuple(vec![0, 0, 2]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let tuple3 = int_vec_to_tuple(vec![0, 0, 3]);
        let tuple_bytes3 = serde_cbor::to_vec(&tuple3).unwrap();
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let tuple4 = int_vec_to_tuple(vec![0, 0, 4]);
        let tuple_bytes4 = serde_cbor::to_vec(&tuple4).unwrap();
        assert_eq!(Some(3), p.add_value(&tuple_bytes4));

        let tup_vec = vec![
            tuple_bytes.clone(),
            tuple_bytes2.clone(),
            tuple_bytes3.clone(),
            tuple_bytes4.clone(),
        ];
        let page_bytes = p.get_bytes();
        
        // Test iteration 1
        let mut iter = p.into_iter();
        assert_eq!(Some(tuple_bytes.clone()), iter.next());
        assert_eq!(Some(tuple_bytes2.clone()), iter.next());
        assert_eq!(Some(tuple_bytes3.clone()), iter.next());
        assert_eq!(Some(tuple_bytes4.clone()), iter.next());
        assert_eq!(None, iter.next());
        
        //Check another way
        let p = Page::from_bytes(&page_bytes);
        assert_eq!(Some(tuple_bytes.clone()), p.get_value(0));

        for (i, x) in p.into_iter().enumerate() {
            println!("tup_vec[{:?}]: {:?}", i, tup_vec[i]);
            println!("x: {:?}", x);
            assert_eq!(tup_vec[i], x);
        }

        let p = Page::from_bytes(&page_bytes);
        let mut count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 4);
        
        //Add a value and check
        let mut p = Page::from_bytes(&page_bytes);
        assert_eq!(Some(4), p.add_value(&tuple_bytes));
        //get the updated bytes
        let page_bytes = p.get_bytes();
        count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 5);
        
        //Delete
        let mut p = Page::from_bytes(&page_bytes);
        p.delete_value(2);
        let mut iter = p.into_iter();
        assert_eq!(Some(tuple_bytes.clone()), iter.next());
        assert_eq!(Some(tuple_bytes2.clone()), iter.next());
        assert_eq!(Some(tuple_bytes4.clone()), iter.next());        
        assert_eq!(Some(tuple_bytes.clone()), iter.next());
        assert_eq!(None, iter.next());
    }
}   
