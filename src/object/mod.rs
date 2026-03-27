mod store;

#[cfg(feature = "azure")]
pub mod azure;

pub use store::{
    ObjectStore, ObjectConfig, ObjectStoreBackend, ObjectError,
    ObjectEntry, ObjectMetadata, InMemoryObjectStore,
};
