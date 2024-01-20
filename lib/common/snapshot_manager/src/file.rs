use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct SnapshotFile {
    pub name: String,
    pub collection: Option<String>,
}

impl SnapshotFile {
    pub fn new_full(name: impl Into<String>) -> Self {
        SnapshotFile {
            name: name.into(),
            collection: None,
        }
    }

    pub fn new_collection(name: impl Into<String>, collection: impl Into<String>) -> Self {
        SnapshotFile {
            name: name.into(),
            collection: Some(collection.into()),
        }
    }

    pub fn has_collection(&self) -> bool {
        self.collection.is_some()
    }

    pub fn get_path(&self, base: impl Into<PathBuf>) -> PathBuf {
        let path: PathBuf = base.into();
        let path = if let Some(collection) = &self.collection {
            path.join(collection)
        } else {
            path
        };
        path.join(&self.name)
    }

    pub fn get_checksum_path(&self, base: impl Into<PathBuf>) -> PathBuf {
        let path: PathBuf = base.into();
        let path = if let Some(collection) = &self.collection {
            path.join(collection)
        } else {
            path
        };
        path.join(format!("{}.checksum", &self.name))
    }
}
