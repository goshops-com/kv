//! Object Storage Layer (L3)
//!
//! Cold storage in S3-compatible backends with trait-based abstraction
//! for easy mocking in tests.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ObjectError {
    #[error("Object not found: {0}")]
    NotFound(String),
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Configuration error: {0}")]
    Config(String),
}

/// Configuration for object storage
#[derive(Debug, Clone)]
pub struct ObjectConfig {
    /// S3 bucket or Azure container name
    pub bucket: String,
    /// S3 endpoint (for MinIO or custom S3-compatible storage)
    pub endpoint: Option<String>,
    /// AWS region
    pub region: String,
    /// Prefix for all keys
    pub prefix: String,
    /// Azure storage account name (when using Azure backend)
    pub azure_account: Option<String>,
    /// Azure storage access key (when using Azure backend)
    pub azure_access_key: Option<String>,
}

impl Default for ObjectConfig {
    fn default() -> Self {
        Self {
            bucket: "tieredkv".to_string(),
            endpoint: None,
            region: "us-east-1".to_string(),
            prefix: "data/".to_string(),
            azure_account: None,
            azure_access_key: None,
        }
    }
}

/// Metadata stored with each object
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub size_bytes: usize,
    pub created_at: DateTime<Utc>,
    pub original_key: String,
    pub content_type: String,
}

/// An object entry retrieved from storage
#[derive(Debug, Clone)]
pub struct ObjectEntry {
    pub key: String,
    pub value: Bytes,
    pub metadata: ObjectMetadata,
}

/// Trait for object storage backends (allows mocking)
#[async_trait]
pub trait ObjectStoreBackend: Send + Sync {
    /// Get an object by key
    async fn get(&self, key: &str) -> Result<ObjectEntry, ObjectError>;

    /// Put an object
    async fn put(&self, key: &str, value: Bytes, metadata: ObjectMetadata) -> Result<(), ObjectError>;

    /// Delete an object
    async fn delete(&self, key: &str) -> Result<(), ObjectError>;

    /// Check if an object exists
    async fn exists(&self, key: &str) -> Result<bool, ObjectError>;

    /// List objects with a prefix
    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectError>;

    /// Get object metadata without fetching the full object
    async fn head(&self, key: &str) -> Result<ObjectMetadata, ObjectError>;
}

/// In-memory implementation for testing
pub struct InMemoryObjectStore {
    objects: Arc<RwLock<HashMap<String, (Bytes, ObjectMetadata)>>>,
}

impl InMemoryObjectStore {
    pub fn new() -> Self {
        Self {
            objects: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn len(&self) -> usize {
        self.objects.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.objects.read().is_empty()
    }

    pub fn clear(&self) {
        self.objects.write().clear();
    }
}

impl Default for InMemoryObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ObjectStoreBackend for InMemoryObjectStore {
    async fn get(&self, key: &str) -> Result<ObjectEntry, ObjectError> {
        let objects = self.objects.read();
        match objects.get(key) {
            Some((value, metadata)) => Ok(ObjectEntry {
                key: key.to_string(),
                value: value.clone(),
                metadata: metadata.clone(),
            }),
            None => Err(ObjectError::NotFound(key.to_string())),
        }
    }

    async fn put(&self, key: &str, value: Bytes, metadata: ObjectMetadata) -> Result<(), ObjectError> {
        let mut objects = self.objects.write();
        objects.insert(key.to_string(), (value, metadata));
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectError> {
        let mut objects = self.objects.write();
        objects.remove(key);
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool, ObjectError> {
        let objects = self.objects.read();
        Ok(objects.contains_key(key))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectError> {
        let objects = self.objects.read();
        let keys: Vec<String> = objects
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        Ok(keys)
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata, ObjectError> {
        let objects = self.objects.read();
        match objects.get(key) {
            Some((_, metadata)) => Ok(metadata.clone()),
            None => Err(ObjectError::NotFound(key.to_string())),
        }
    }
}

/// Main object storage layer that wraps a backend
pub struct ObjectStore {
    backend: Arc<dyn ObjectStoreBackend>,
    config: ObjectConfig,
}

impl ObjectStore {
    /// Create a new object store with the given backend
    pub fn new(backend: Arc<dyn ObjectStoreBackend>, config: ObjectConfig) -> Self {
        Self { backend, config }
    }

    /// Create with in-memory backend (for testing)
    pub fn in_memory(config: ObjectConfig) -> Self {
        Self {
            backend: Arc::new(InMemoryObjectStore::new()),
            config,
        }
    }

    /// Build the full key with prefix
    fn full_key(&self, key: &str) -> String {
        format!("{}{}", self.config.prefix, key)
    }

    /// Get an object
    pub async fn get(&self, key: &str) -> Result<ObjectEntry, ObjectError> {
        let full_key = self.full_key(key);
        self.backend.get(&full_key).await
    }

    /// Put an object (from raw bytes)
    pub async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectError> {
        let full_key = self.full_key(key);
        let metadata = ObjectMetadata {
            size_bytes: value.len(),
            created_at: Utc::now(),
            original_key: key.to_string(),
            content_type: "application/octet-stream".to_string(),
        };
        self.backend.put(&full_key, value, metadata).await
    }

    /// Put an object with custom metadata
    pub async fn put_with_metadata(
        &self,
        key: &str,
        value: Bytes,
        metadata: ObjectMetadata,
    ) -> Result<(), ObjectError> {
        let full_key = self.full_key(key);
        self.backend.put(&full_key, value, metadata).await
    }

    /// Delete an object
    pub async fn delete(&self, key: &str) -> Result<(), ObjectError> {
        let full_key = self.full_key(key);
        self.backend.delete(&full_key).await
    }

    /// Check if an object exists
    pub async fn exists(&self, key: &str) -> Result<bool, ObjectError> {
        let full_key = self.full_key(key);
        self.backend.exists(&full_key).await
    }

    /// List objects with a prefix (relative to config prefix)
    pub async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectError> {
        let full_prefix = self.full_key(prefix);
        let keys = self.backend.list(&full_prefix).await?;

        // Strip the config prefix from results
        let stripped: Vec<String> = keys
            .into_iter()
            .map(|k| k.strip_prefix(&self.config.prefix).unwrap_or(&k).to_string())
            .collect();
        Ok(stripped)
    }

    /// Get object metadata
    pub async fn head(&self, key: &str) -> Result<ObjectMetadata, ObjectError> {
        let full_key = self.full_key(key);
        self.backend.head(&full_key).await
    }

    /// Delete multiple objects
    pub async fn delete_batch(&self, keys: &[String]) -> Result<usize, ObjectError> {
        let mut deleted = 0;
        for key in keys {
            if self.exists(key).await? {
                self.delete(key).await?;
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// Get the bucket name
    pub fn bucket(&self) -> &str {
        &self.config.bucket
    }

    /// Get the prefix
    pub fn prefix(&self) -> &str {
        &self.config.prefix
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_store() -> ObjectStore {
        ObjectStore::in_memory(ObjectConfig {
            bucket: "test-bucket".to_string(),
            endpoint: None,
            region: "us-east-1".to_string(),
            prefix: "test/".to_string(),
            azure_account: None,
            azure_access_key: None,
        })
    }

    // ==================== IN-MEMORY BACKEND ====================

    #[tokio::test]
    async fn test_in_memory_backend_new_is_empty() {
        let backend = InMemoryObjectStore::new();
        assert!(backend.is_empty());
        assert_eq!(backend.len(), 0);
    }

    #[tokio::test]
    async fn test_in_memory_backend_put_and_get() {
        let backend = InMemoryObjectStore::new();

        let metadata = ObjectMetadata {
            size_bytes: 6,
            created_at: Utc::now(),
            original_key: "key1".to_string(),
            content_type: "text/plain".to_string(),
        };

        backend.put("key1", Bytes::from("value1"), metadata).await.unwrap();

        let entry = backend.get("key1").await.unwrap();
        assert_eq!(entry.value, Bytes::from("value1"));
        assert_eq!(entry.key, "key1");
    }

    #[tokio::test]
    async fn test_in_memory_backend_get_not_found() {
        let backend = InMemoryObjectStore::new();
        let result = backend.get("nonexistent").await;
        assert!(matches!(result, Err(ObjectError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_in_memory_backend_delete() {
        let backend = InMemoryObjectStore::new();

        let metadata = ObjectMetadata {
            size_bytes: 6,
            created_at: Utc::now(),
            original_key: "key1".to_string(),
            content_type: "text/plain".to_string(),
        };

        backend.put("key1", Bytes::from("value1"), metadata).await.unwrap();
        assert!(backend.exists("key1").await.unwrap());

        backend.delete("key1").await.unwrap();
        assert!(!backend.exists("key1").await.unwrap());
    }

    #[tokio::test]
    async fn test_in_memory_backend_list() {
        let backend = InMemoryObjectStore::new();

        let metadata = ObjectMetadata {
            size_bytes: 5,
            created_at: Utc::now(),
            original_key: "".to_string(),
            content_type: "text/plain".to_string(),
        };

        backend.put("prefix/key1", Bytes::from("value"), metadata.clone()).await.unwrap();
        backend.put("prefix/key2", Bytes::from("value"), metadata.clone()).await.unwrap();
        backend.put("other/key3", Bytes::from("value"), metadata).await.unwrap();

        let keys = backend.list("prefix/").await.unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"prefix/key1".to_string()));
        assert!(keys.contains(&"prefix/key2".to_string()));
    }

    // ==================== OBJECT STORE WRAPPER ====================

    #[tokio::test]
    async fn test_object_store_put_and_get() {
        let store = create_test_store();

        store.put("mykey", Bytes::from("myvalue")).await.unwrap();

        let entry = store.get("mykey").await.unwrap();
        assert_eq!(entry.value, Bytes::from("myvalue"));
    }

    #[tokio::test]
    async fn test_object_store_prefix_is_applied() {
        let store = create_test_store();

        store.put("mykey", Bytes::from("myvalue")).await.unwrap();

        // The backend should have the full key with prefix
        let entry = store.backend.get("test/mykey").await.unwrap();
        assert_eq!(entry.value, Bytes::from("myvalue"));
    }

    #[tokio::test]
    async fn test_object_store_exists() {
        let store = create_test_store();

        assert!(!store.exists("mykey").await.unwrap());

        store.put("mykey", Bytes::from("myvalue")).await.unwrap();

        assert!(store.exists("mykey").await.unwrap());
    }

    #[tokio::test]
    async fn test_object_store_delete() {
        let store = create_test_store();

        store.put("mykey", Bytes::from("myvalue")).await.unwrap();
        assert!(store.exists("mykey").await.unwrap());

        store.delete("mykey").await.unwrap();
        assert!(!store.exists("mykey").await.unwrap());
    }

    #[tokio::test]
    async fn test_object_store_list() {
        let store = create_test_store();

        store.put("folder/key1", Bytes::from("value1")).await.unwrap();
        store.put("folder/key2", Bytes::from("value2")).await.unwrap();
        store.put("other/key3", Bytes::from("value3")).await.unwrap();

        let keys = store.list("folder/").await.unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"folder/key1".to_string()));
        assert!(keys.contains(&"folder/key2".to_string()));
    }

    #[tokio::test]
    async fn test_object_store_head() {
        let store = create_test_store();

        let value = Bytes::from("hello world");
        store.put("mykey", value.clone()).await.unwrap();

        let metadata = store.head("mykey").await.unwrap();
        assert_eq!(metadata.size_bytes, value.len());
        assert_eq!(metadata.original_key, "mykey");
    }

    #[tokio::test]
    async fn test_object_store_delete_batch() {
        let store = create_test_store();

        store.put("key1", Bytes::from("value1")).await.unwrap();
        store.put("key2", Bytes::from("value2")).await.unwrap();
        store.put("key3", Bytes::from("value3")).await.unwrap();

        let deleted = store.delete_batch(&["key1".to_string(), "key3".to_string()]).await.unwrap();

        assert_eq!(deleted, 2);
        assert!(!store.exists("key1").await.unwrap());
        assert!(store.exists("key2").await.unwrap());
        assert!(!store.exists("key3").await.unwrap());
    }

    #[tokio::test]
    async fn test_object_store_put_with_metadata() {
        let store = create_test_store();

        let custom_metadata = ObjectMetadata {
            size_bytes: 11,
            created_at: Utc::now(),
            original_key: "original".to_string(),
            content_type: "application/json".to_string(),
        };

        store.put_with_metadata("mykey", Bytes::from("hello world"), custom_metadata.clone()).await.unwrap();

        let retrieved = store.head("mykey").await.unwrap();
        assert_eq!(retrieved.content_type, "application/json");
        assert_eq!(retrieved.original_key, "original");
    }

    #[tokio::test]
    async fn test_object_store_config_accessors() {
        let store = create_test_store();

        assert_eq!(store.bucket(), "test-bucket");
        assert_eq!(store.prefix(), "test/");
    }
}
