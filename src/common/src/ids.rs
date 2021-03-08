use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};

static TXN_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Permissions for locks.
pub enum Permissions {
    ReadOnly,
    ReadWrite,
}

/// Implementation of transaction id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransactionId {
    /// Id of transaction.
    id: u64,
}

impl TransactionId {
    /// Creates a new transaction id.
    pub fn new() -> Self {
        Self {
            id: TXN_COUNTER.fetch_add(1, Ordering::SeqCst),
        }
    }

    /// Returns the transaction id.
    pub fn id(&self) -> u64 {
        self.id
    }
}

impl Default for TransactionId {
    fn default() -> Self {
        TransactionId::new()
    }
}

/// The type for the container ID and the associated atomic type (for use within a Storage Manager)
pub type ContainerId = u16; 
    // ContainerIds are used by the storage manager to keep track of the separate heapfiles 
    // the storage manager must be able to keep track of which container_id corresponds to which heapfile
pub type AtomicContainerId = AtomicU16;
pub type SegmentId = u8;
pub type PageId = u16;
pub type SlotId = u16;

/// Holds information to find a record or value's bytes in a storage manager.
/// Depending on storage manager (SM), various elements may be used.
/// For example a disk-based SM may use pages to store the records, where
/// a main-memory based storage manager may not.
/// It is up to a particular SM to determine how and when to use
#[derive(PartialEq, Clone, Copy, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct ValueId {
    /// The source of the value. This could represent a table, index, or other data structure.
    /// All values stored must be associated with a container that is created by the storage manager.
    pub container_id: ContainerId,
    /// An optional segment or partition ID
    pub segment_id: Option<SegmentId>,
    /// An optional page id
    pub page_id: Option<PageId>,
    /// An optional slot id. This could represent a physical or logical ID.
    pub slot_id: Option<SlotId>,
}

impl ValueId {
    pub fn new(container_id: ContainerId) -> Self {
        ValueId {
            container_id,
            segment_id: None,
            page_id: None,
            slot_id: None,
        }
    }

    pub fn new_page(container_id: ContainerId, page_id: PageId) -> Self {
        ValueId {
            container_id,
            segment_id: None,
            page_id: Some(page_id),
            slot_id: None,
        }
    }
}
