use std::path::{Path, PathBuf};

use async_trait::async_trait;

use crate::StoreError;

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn put_bytes(&self, key: &str, bytes: &[u8]) -> Result<(), StoreError>;
    fn resolve_path(&self, key: &str) -> PathBuf;
}

pub struct LocalFsObjectStore {
    pub root: PathBuf,
}

#[async_trait]
impl ObjectStore for LocalFsObjectStore {
    async fn put_bytes(&self, key: &str, bytes: &[u8]) -> Result<(), StoreError> {
        let path = self.root.join(key);
        if let Some(p) = path.parent() {
            tokio::fs::create_dir_all(p).await?;
        }
        tokio::fs::write(&path, bytes).await?;
        Ok(())
    }

    fn resolve_path(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }
}

impl LocalFsObjectStore {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }
}
