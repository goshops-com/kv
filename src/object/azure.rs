//! Azure Blob Storage Backend
//!
//! Implements ObjectStoreBackend using the `object_store` crate
//! for Azure Blob Storage.

use async_trait::async_trait;
use bytes::Bytes;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore as ObjStore, ObjectStoreExt, PutPayload};
use std::sync::Arc;

use super::store::{ObjectEntry, ObjectError, ObjectMetadata, ObjectStoreBackend};

/// Configuration for Azure Blob Storage
#[derive(Debug, Clone)]
pub struct AzureBlobConfig {
    /// Azure Storage account name
    pub account_name: String,
    /// Azure Storage access key
    pub access_key: String,
    /// Container name
    pub container_name: String,
}

/// Azure Blob Storage backend using the object_store crate
pub struct AzureBlobBackend {
    store: Arc<dyn ObjStore>,
}

impl AzureBlobBackend {
    /// Create a new Azure Blob backend
    pub fn new(config: &AzureBlobConfig) -> Result<Self, ObjectError> {
        let store = MicrosoftAzureBuilder::new()
            .with_account(&config.account_name)
            .with_access_key(&config.access_key)
            .with_container_name(&config.container_name)
            .build()
            .map_err(|e| ObjectError::Config(format!("Failed to create Azure client: {}", e)))?;

        Ok(Self {
            store: Arc::new(store),
        })
    }

    /// Create from environment variables (AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY, etc.)
    pub fn from_env(container_name: &str) -> Result<Self, ObjectError> {
        let store = MicrosoftAzureBuilder::from_env()
            .with_container_name(container_name)
            .build()
            .map_err(|e| ObjectError::Config(format!("Failed to create Azure client: {}", e)))?;

        Ok(Self {
            store: Arc::new(store),
        })
    }
}

#[async_trait]
impl ObjectStoreBackend for AzureBlobBackend {
    async fn get(&self, key: &str) -> Result<ObjectEntry, ObjectError> {
        let path = ObjectPath::from(key);

        let result = self.store.get(&path).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => ObjectError::NotFound(key.to_string()),
            other => ObjectError::Storage(other.to_string()),
        })?;

        let meta = result.meta.clone();
        let value = result
            .bytes()
            .await
            .map_err(|e| ObjectError::Storage(e.to_string()))?;

        Ok(ObjectEntry {
            key: key.to_string(),
            value,
            metadata: ObjectMetadata {
                size_bytes: meta.size as usize,
                created_at: meta.last_modified.into(),
                original_key: key.to_string(),
                content_type: "application/octet-stream".to_string(),
            },
        })
    }

    async fn put(
        &self,
        key: &str,
        value: Bytes,
        _metadata: ObjectMetadata,
    ) -> Result<(), ObjectError> {
        let path = ObjectPath::from(key);
        let payload = PutPayload::from(value);

        ObjectStoreExt::put(&*self.store, &path, payload)
            .await
            .map_err(|e| ObjectError::Storage(e.to_string()))?;

        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectError> {
        let path = ObjectPath::from(key);

        ObjectStoreExt::delete(&*self.store, &path)
            .await
            .map_err(|e| ObjectError::Storage(e.to_string()))?;

        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool, ObjectError> {
        let path = ObjectPath::from(key);

        match ObjectStoreExt::head(&*self.store, &path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(ObjectError::Storage(e.to_string())),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectError> {
        use futures::TryStreamExt;

        let prefix_path = ObjectPath::from(prefix);
        let mut keys = Vec::new();

        let mut stream = self.store.list(Some(&prefix_path));
        while let Some(meta) = stream
            .try_next()
            .await
            .map_err(|e| ObjectError::Storage(e.to_string()))?
        {
            keys.push(meta.location.to_string());
        }

        Ok(keys)
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata, ObjectError> {
        let path = ObjectPath::from(key);

        let meta = ObjectStoreExt::head(&*self.store, &path)
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { .. } => ObjectError::NotFound(key.to_string()),
                other => ObjectError::Storage(other.to_string()),
            })?;

        Ok(ObjectMetadata {
            size_bytes: meta.size as usize,
            created_at: meta.last_modified.into(),
            original_key: key.to_string(),
            content_type: "application/octet-stream".to_string(),
        })
    }
}
