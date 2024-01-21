use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct SnapshotFile {
    pub name: String,
    pub collection: Option<String>,
    pub shard: Option<u32>,
}

impl SnapshotFile {
    pub fn new_full(name: impl Into<String>) -> Self {
        SnapshotFile {
            name: name.into(),
            collection: None,
            shard: None,
        }
    }

    pub fn new_collection(name: impl Into<String>, collection: impl Into<String>) -> Self {
        SnapshotFile {
            name: name.into(),
            collection: Some(collection.into()),
            shard: None,
        }
    }

    pub fn new_shard(
        name: impl Into<String>,
        collection: impl Into<String>,
        shard: impl Into<u32>,
    ) -> Self {
        SnapshotFile {
            name: name.into(),
            collection: Some(collection.into()),
            shard: Some(shard.into()),
        }
    }

    pub fn has_collection(&self) -> bool {
        self.collection.is_some()
    }

    pub(super) fn get_directory(&self, base: impl Into<PathBuf>) -> PathBuf {
        let path: PathBuf = base.into();

        let path =
            if let Some(collection) = &self.collection {
                path.join(collection)
            } else {
                path
            };

        if let Some(shard) = &self.shard {
            path.join(format!("shards/{}", shard))
        } else {
            path
        }
    }

    pub fn get_path(&self, base: impl Into<PathBuf>) -> PathBuf {
        let path = self.get_directory(base);
        path.join(&self.name)
    }

    pub fn get_checksum_path(&self, base: impl Into<PathBuf>) -> PathBuf {
        let path = self.get_directory(base);
        path.join(format!("{}.checksum", &self.name))
    }
}
